import os
import os.path
import sys
import json
from functools import lru_cache
from datetime import datetime
from typing import Any, Optional, Tuple, Union
from collections.abc import MutableSequence as List, \
                            MutableMapping as Dict

import numpy
import numpy.typing
import numpy.core.records

from . import loading
from .types import EDDNSystem, EDDNBody, EDDNFaction, EDDNFile, \
                   EDSMFile, EDDNRegion, EDDNStation, DTypeEDSMSystem, \
                   DTypeEDDBSystem, DTypeEDSMBody, KnownBody, \
                   NPTypeEDSMBody
from .timer import Timer
from . import constants
from .util import from_db_string
from . import sqlqueries
from .database import DBConnection
from .systems import getsystem
from .stations import getstation
from .bodies import getbody


class EDDNSysDB(object):
    conn: DBConnection
    regions: Dict[str, EDDNRegion]
    regionaddrs: Dict[int, EDDNRegion]
    namedsystems: Dict[str, List[EDDNSystem]]
    namedbodies: Dict[int, Dict[str, List[EDDNBody]]]
    parentsets: Dict[Tuple[int, str], int]
    bodydesigs: Dict[str, Tuple[int, int]]
    software: Dict[str, int]
    factions: Dict[str, List[EDDNFaction]]
    edsmsysids: numpy.core.records.recarray
    edsmbodyids: numpy.core.records.recarray
    eddbsysids: numpy.core.records.recarray
    edsmsyscachefile: str
    edsmbodycachefile: str
    knownbodies: Dict[str, Dict[str, List[KnownBody]]]

    def __init__(self,
                 conn: DBConnection,
                 loadedsmsys: bool,
                 loadedsmbodies: bool,
                 loadeddbsys: bool,
                 edsm_systems_cache_file: str,
                 edsm_bodies_cache_file: str,
                 known_bodies_sheet_uri: str
                 ):
        timer = Timer()

        try:
            self.conn = conn

            self.edsmsysids = numpy.empty(
                0,
                DTypeEDSMSystem
            ).view(numpy.core.records.recarray)

            self.edsmbodyids = numpy.empty(
                0,
                DTypeEDSMBody
            ).view(numpy.core.records.recarray)

            self.eddbsysids = numpy.empty(
                0,
                DTypeEDDBSystem
            ).view(numpy.core.records.recarray)

            self.edsmsyscachefile = edsm_systems_cache_file
            self.edsmbodycachefile = edsm_bodies_cache_file

            (self.regions, self.regionaddrs) = loading.loadregions(conn, timer)
            self.namedsystems = loading.loadnamedsystems(conn, timer)
            self.namedbodies = loading.loadnamedbodies(conn, timer)
            self.parentsets = loading.loadparentsets(conn, timer)
            self.software = loading.loadsoftware(conn, timer)
            self.bodydesigs = loading.loadbodydesigs(conn, timer)
            self.factions = loading.loadfactions(conn, timer)

            self.knownbodies = loading.loadknownbodies(
                conn,
                timer,
                self.bodydesigs,
                known_bodies_sheet_uri
            )

            if loadedsmsys or loadedsmbodies:
                self.edsmsysids = loading.loadedsmsystems(
                    conn,
                    timer,
                    self.edsmsyscachefile
                )

            if loadedsmbodies:
                self.edsmbodyids = loading.loadedsmbodies(
                    conn,
                    timer,
                    self.edsmbodycachefile
                )

            if loadeddbsys:
                self.eddbsysids = loading.loadeddbsystems(
                    conn,
                    timer
                )

        finally:
            timer.printstats()

    def commit(self):
        self.conn.commit()

    @lru_cache(maxsize=262144)
    def getsystem(self,
                  timer: Timer,
                  sysname: str,
                  x: Optional[float],
                  y: Optional[float],
                  z: Optional[float],
                  sysaddr: Optional[int]
                  ) -> Union[Tuple[EDDNSystem, None, None],
                             Tuple[None, str, dict]]:
        return getsystem(
            self.conn,
            timer,
            sysname,
            x,
            y,
            z,
            sysaddr,
            self.namedsystems,
            self.regions,
            self.regionaddrs
        )

    def getstation(self,
                   timer: Timer,
                   name: str,
                   sysname: str,
                   marketid: Union[int, None],
                   timestamp: datetime,
                   system: Union[EDDNSystem, None] = None,
                   stationtype: Union[str, None] = None,
                   bodyname: Union[str, None] = None,
                   bodyid: Union[int, None] = None,
                   bodytype: Union[str, None] = None,
                   eventtype: Union[str, None] = None,
                   test: bool = False
                   ) -> Union[Tuple[EDDNStation, None, None],
                              Tuple[None, str, Union[List[dict], None]]]:
        return getstation(
            self.conn,
            timer,
            name,
            sysname,
            marketid,
            timestamp,
            system,
            stationtype,
            bodyname,
            bodyid,
            bodytype,
            eventtype,
            test
        )

    def insertbodyparents(self,
                          timer: Timer,
                          scanbodyid: int,
                          system: EDDNSystem,
                          bodyid: int,
                          parents: List[Dict]
                          ):
        if parents is not None and bodyid is not None:
            parentjson = json.dumps(parents)

            if (bodyid, parentjson) not in self.parentsets:
                rowid = sqlqueries.insert_parent_set(
                    self.conn,
                    (bodyid, parentjson)
                )

                self.parentsets[(bodyid, parentjson)] = rowid

            parentsetid = self.parentsets[(bodyid, parentjson)]

            sqlqueries.insert_parent_set_link(
                self.conn,
                (scanbodyid, parentsetid)
            )

    def insertsoftware(self, softwarename: str):
        if softwarename not in self.software:
            self.software[softwarename] = sqlqueries.insert_software(
                self.conn,
                (softwarename,)
            )

    def insertedsmfile(self, filename: str):
        return sqlqueries.insert_edsm_file(self.conn, (filename,))

    def getbody(self,
                timer: Timer,
                name: str,
                sysname: str,
                bodyid: Optional[int],
                system,
                body,
                timestamp
                ):
        return getbody(
            self.conn,
            timer,
            name,
            sysname,
            bodyid,
            system,
            body,
            timestamp,
            self.namedbodies,
            self.knownbodies,
            self.bodydesigs,
            self.namedsystems,
            self.regions
        )

    def getfaction(self,
                   timer: Timer,
                   name: str,
                   government: str,
                   allegiance: Optional[str]
                   ):
        factions = None

        if government[:12] == '$government_' and government[-1] == ';':
            government = government[12:-1]

        if name in self.factions:
            factions = self.factions[name]
            for faction in factions:
                if (faction.government == government
                        and faction.allegiance in [allegiance]):
                    return faction

        if allegiance is None:
            return None

        factionid = sqlqueries.insert_faction(
            self.conn,
            (name, government, allegiance)
        )

        faction = EDDNFaction(factionid, name, government, allegiance)

        if factions is None:
            self.factions[name] = [faction]
        else:
            factions.append(faction)

        return faction

    def getsystembyid(self, sysid: int) -> Union[EDDNSystem, None]:
        row = sqlqueries.get_system_by_id(self.conn, (sysid,))

        if row:
            return EDDNSystem(
                row[0],
                row[1],
                from_db_string(row[2]),
                row[3] / 32.0 - 49985,
                row[4] / 32.0 - 40985,
                row[5] / 32.0 - 24105,
                row[3] != 0 and row[4] != 0 and row[5] != 0
            )
        else:
            return None

    def findedsmsysid(self, edsmid: int)\
            -> Union[Tuple[int, int, bool, Any],
                     Tuple[None, None, None, None]]:
        if self.edsmsysids is not None and len(self.edsmsysids) > edsmid:
            row = self.edsmsysids[edsmid]

            if row[0] != 0:
                return (int(row[0]), int(row[2]), bool(row[3]), row)

        row = sqlqueries.get_system_by_edsm_id(self.conn, (edsmid,))

        if row:
            return (int(row[0]), int(row[1]), bool(row[2] == b'\x01'), None)
        else:
            return (None, None, None, None)

    def findedsmbodyid(self, edsmid: int)\
            -> Union[Tuple[int, int, Optional[NPTypeEDSMBody]],
                     Tuple[None, None, None]]:
        if len(self.edsmbodyids) > edsmid:
            row = self.edsmbodyids[edsmid]

            if row[0] != 0:
                return (int(row[0]), int(row[2]), row)

        row = sqlqueries.get_body_by_edsm_id

        if row:
            return (int(row[0]), int(row[1]), None)
        else:
            return (None, None, None)

    def saveedsmsyscache(self):
        with open(self.edsmsyscachefile + '.tmp', 'wb') as f:
            self.edsmsysids.tofile(f)
        os.rename(self.edsmsyscachefile + '.tmp', self.edsmsyscachefile)

    def saveedsmbodycache(self):
        with open(self.edsmbodycachefile + '.tmp', 'wb') as f:
            self.edsmbodyids.tofile(f)
        os.rename(self.edsmbodycachefile + '.tmp', self.edsmbodycachefile)

    def updateedsmsysid(self,
                        edsmid: int,
                        sysid: int,
                        ts: Union[int, datetime],
                        hascoords: bool,
                        ishidden: bool,
                        isdeleted: bool
                        ):
        if type(ts) is datetime:
            ts = int((ts - constants.timestamp_base_date).total_seconds())

        sqlqueries.upsert_edsm_system(
            self.conn,
            (
                edsmid,
                sysid,
                ts,
                1 if hascoords else 0,
                1 if ishidden else 0,
                1 if isdeleted else 0,
                sysid,
                ts,
                1 if hascoords else 0,
                1 if ishidden else 0,
                1 if isdeleted else 0
            )
        )

        if edsmid < len(self.edsmsysids):
            rec = self.edsmsysids[edsmid]
            rec[0] = sysid
            rec[1] = edsmid
            rec[2] = ts
            rec[3] = 1 if hascoords else 0
            rec[4] = 1 if ishidden else 0
            rec[5] = 1 if isdeleted else 0
            return rec
        else:
            return None

    def updateedsmbodyid(self, bodyid: int, edsmid: int, ts: datetime):
        tssec = int((ts - constants.timestamp_base_date).total_seconds())

        sqlqueries.upsert_edsm_body(
            self.conn,
            (edsmid, bodyid, ts, bodyid, ts)
        )

        if edsmid < len(self.edsmbodyids):
            rec = self.edsmbodyids[edsmid]
            rec[0] = bodyid
            rec[1] = edsmid
            rec[2] = tssec
            return rec
        else:
            return None

    def updateedsmstationid(self, edsmid: int, stationid: int, ts: datetime):
        sqlqueries.upsert_edsm_station(
            self.conn,
            (edsmid, stationid, ts, stationid, ts)
        )

    def findeddbsysid(self, eddbid: int):
        if len(self.eddbsysids) > eddbid:
            row = self.eddbsysids[eddbid]

            if row[0] != 0:
                return (row[0], row[2])

        row = sqlqueries.get_system_by_eddb_id(self.conn, (eddbid,))

        if row:
            return (row[0], row[1])
        else:
            return (None, None)

    def updateeddbsysid(self, eddbid: int, sysid: int, ts: int):
        sqlqueries.upsert_eddb_system(
            self.conn,
            (eddbid, sysid, ts, sysid, ts)
        )

    def addfilelinestations(self,
                            linelist: List[Tuple[int, int, EDDNStation]]
                            ):
        values = [(fileid, lineno, station.id)
                  for fileid, lineno, station in linelist]
        sqlqueries.insert_file_line_stations(self.conn, values)

    def addfilelineinfo(self,
                        linelist: List[Tuple[
                            int, int, datetime, datetime, int, int,
                            int, int, float, int, int, int
                        ]]):
        sqlqueries.insert_file_line_info(self.conn, linelist)

    def addfilelinefactions(self,
                            linelist: List[Tuple[int, int, EDDNFaction, int]]
                            ):
        values = [(fileid, lineno, faction.id, entrynum)
                  for fileid, lineno, faction, entrynum in linelist]
        sqlqueries.insert_file_line_factions(self.conn, values)

    def addfilelineroutesystems(self,
                                linelist: List[
                                        Tuple[int, int, EDDNSystem, int]
                                    ]
                                ):
        values = [(fileid, lineno, system.id, entrynum)
                  for fileid, lineno, system, entrynum in linelist]
        sqlqueries.insert_file_line_route_systems(self.conn, values)

    def addedsmfilelinebodies(self,
                              linelist: List[Tuple[int, int, int]]
                              ):
        values = [(fileid, lineno, edsmbodyid)
                  for fileid, lineno, edsmbodyid in linelist]
        sqlqueries.insert_edsm_file_line_systems(self.conn, values)

    def getstationfilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_stations_by_file(self.conn, (fileid,))
        return {row[0]: row[1] for row in rows}

    def getinfofilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_info_by_file(self.conn, (fileid,))
        return {row[0]: (row[1], row[2], row[3]) for row in rows}

    def getfactionfilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_factions_by_file(self.conn, (fileid,))

        lines: Dict[int, List[str]] = {}
        for row in rows:
            if row[0] not in lines:
                lines[row[0]] = []
            lines[row[0]].append(row[1])

        return lines

    def getnavroutefilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_routes_by_file(self.conn, (fileid,))

        lines = {}
        for row in rows:
            lines[(row[0], row[1])] = row[2]

        return lines

    def getedsmbodyfilelines(self, fileid: int):
        maxline = sqlqueries.get_max_edsm_body_file_lineno(
            self.conn,
            (fileid,)
        )

        if maxline is None:
            return []

        filelinearray = numpy.zeros(maxline + 1, numpy.int32)

        cursor = sqlqueries.get_edsm_body_file_lines_by_file(
            self.conn,
            (fileid,)
        )

        for row in cursor:
            filelinearray[row[0]] = row[1]

        return filelinearray

    def geteddnfiles(self):

        sys.stderr.write('    Getting station line counts\n')
        stnlinecounts = {
            row[0]: row[1]
            for row in sqlqueries.get_station_file_line_counts(self.conn)
        }

        sys.stderr.write('    Getting info line counts\n')
        infolinecounts = {
            row[0]: row[1]
            for row in sqlqueries.get_info_file_line_counts(self.conn)
        }

        sys.stderr.write('    Getting faction line counts\n')
        factionlinecounts = {
            row[0]: row[1]
            for row in sqlqueries.get_faction_file_line_counts(self.conn)
        }

        sys.stderr.write('    Getting nav route line counts\n')
        navroutelinecounts = {
            row[0]: row[1]
            for row in sqlqueries.get_route_file_line_counts(self.conn)
        }

        sys.stderr.write('    Getting file info\n')
        return {
            row[1]: EDDNFile(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                stnlinecounts.get(row[0]) or 0,
                infolinecounts.get(row[0]) or 0,
                factionlinecounts.get(row[0]) or 0,
                navroutelinecounts.get(row[0]) or 0,
                row[5],
                row[6],
                row[7],
                row[8]
            ) for row in sqlqueries.get_files(self.conn)
        }

    def getedsmfiles(self):
        sys.stderr.write('    Getting body line counts\n')
        bodylinecounts = {
            row[0]: row[1]
            for row in sqlqueries.get_edsm_body_file_line_counts(self.conn)
        }

        sys.stderr.write('    Getting file info\n')
        return {
            row[1]: EDSMFile(
                row[0],
                row[1],
                row[2],
                row[3],
                bodylinecounts[row[0]] if row[0] in bodylinecounts else 0,
                row[4]
            ) for row in sqlqueries.get_edsm_files(self.conn)
        }

    def updatefileinfo(self,
                       fileid: int,
                       linecount: int,
                       totalsize: int,
                       comprsize: int,
                       poplinecount: int,
                       stnlinecount: int,
                       navroutesystemcount: int
                       ):
        sqlqueries.update_file_info(
            self.conn,
            (
                linecount,
                comprsize,
                totalsize,
                poplinecount,
                stnlinecount,
                navroutesystemcount,
                fileid
            )
        )

    def updateedsmfileinfo(self,
                           fileid: int,
                           linecount: int,
                           totalsize: int,
                           comprsize: int
                           ):
        sqlqueries.update_edsm_file_info(
            self.conn,
            (linecount, comprsize, totalsize, fileid)
        )
