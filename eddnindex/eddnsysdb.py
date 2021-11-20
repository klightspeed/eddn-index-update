import os
import os.path
import sys
import json
import math
from functools import lru_cache
from datetime import datetime, timedelta
import urllib.request
import urllib.error
from typing import Iterable, MutableSequence, Optional, Set, Tuple, List, Dict, Union, Sequence

import numpy
import numpy.typing
import numpy.core.records

from . import edtslookup
from .types import EDDNSystem, EDDNBody, EDDNFaction, EDDNFile, EDSMFile, EDDNRegion, EDDNStation, \
    DTypeEDSMSystem, DTypeEDDBSystem, DTypeEDSMBody, NPTypeEDSMSystem, NPTypeEDDBSystem, NPTypeEDSMBody
from .timer import Timer
from . import constants
from .util import id64_to_modsysaddr, modsysaddr_to_id64, from_db_string
from .config import Config
from . import sqlqueries
from .database import DBConnection


class EDDNSysDB(object):
    conn: DBConnection
    regions: Dict[str, EDDNRegion]
    regionaddrs: Dict[int, EDDNRegion]
    namedsystems: Dict[str, Union[EDDNSystem, List[EDDNSystem]]]
    namedbodies: Dict[int, Dict[str, Union[EDDNBody, List[EDDNBody]]]]
    parentsets: Dict[Tuple[int, str], int]
    bodydesigs: Dict[str, Tuple[int, int]]
    software: Dict[str, int]
    factions: Dict[str, Union[EDDNFaction, List[EDDNFaction]]]
    edsmsysids: Union[MutableSequence[NPTypeEDSMSystem], numpy.ndarray, None]
    edsmbodyids: Union[MutableSequence[NPTypeEDDBSystem], numpy.ndarray, None]
    eddbsysids: Union[MutableSequence[NPTypeEDSMBody], numpy.ndarray, None]
    edsmsyscachefile: str
    knownbodiessheeturi: str
    edsmbodycachefile: str

    def __init__(self, conn: DBConnection, loadedsmsys: bool, loadedsmbodies: bool, loadeddbsys: bool,
                 config: Config):
        self.conn = conn
        self.regions = {}
        self.regionaddrs = {}
        self.namedsystems = {}
        self.namedbodies = {}
        self.parentsets = {}
        self.bodydesigs = {}
        self.software = {}
        self.factions = {}
        self.knownbodies = {}
        self.edsmsysids = None
        self.edsmbodyids = None
        self.eddbsysids = None
        self.edsmsyscachefile = config.edsm_systems_cache_file
        self.knownbodiessheeturi = config.known_bodies_sheet_uri
        self.edsmbodycachefile = config.edsm_bodies_cache_file

        timer = Timer()

        try:
            self.loadregions(conn, timer)
            self.loadnamedsystems(conn, timer)
            self.loadnamedbodies(conn, timer)
            self.loadparentsets(conn, timer)
            self.loadsoftware(conn, timer)
            self.loadbodydesigs(conn, timer)
            self.loadfactions(conn, timer)
            self.loadknownbodies(timer)

            if loadedsmsys or loadedsmbodies:
                self.loadedsmsystems(conn, timer)

            if loadedsmbodies:
                self.loadedsmbodies(conn, timer)

            if loadeddbsys:
                self.loadeddbsystems(conn, timer)

        finally:
            timer.printstats()

    def loadedsmsystems(self, conn: DBConnection, timer: Timer):
        maxedsmsysid = sqlqueries.get_max_edsm_system_id(conn)

        timer.time('sql')

        if maxedsmsysid:
            sys.stderr.write('Loading EDSM System IDs\n')
            if os.path.exists(self.edsmsyscachefile):
                with open(self.edsmsyscachefile, 'rb') as f:
                    edsmsysarray = numpy.fromfile(f, dtype=DTypeEDSMSystem)

                if len(edsmsysarray) > maxedsmsysid:
                    if len(edsmsysarray) < maxedsmsysid + 524288:
                        edsmsysarray = numpy.resize(edsmsysarray, maxedsmsysid + 1048576)
                    self.edsmsysids = edsmsysarray.view(numpy.core.records.recarray)

                timer.time('loadedsmsys', len(edsmsysarray))

            if self.edsmsysids is None:
                c = sqlqueries.get_edsm_systems(conn)

                edsmsysarray = numpy.zeros(maxedsmsysid + 1048576, dtype=DTypeEDSMSystem)
                self.edsmsysids = edsmsysarray.view(numpy.core.records.recarray)
                timer.time('sql')

                i = 0
                maxedsmid = 0
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqledsmsys', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        edsmid = row[1]
                        rec = edsmsysarray[edsmid]
                        rec[0] = row[0]
                        rec[1] = edsmid
                        rec[2] = row[2]
                        rec[3] = 1 if row[3] == b'\x01' else 0
                        rec[4] = 1 if row[4] == b'\x01' else 0
                        rec[5] = 1 if row[5] == b'\x01' else 0
                        rec[6] = 3
                        i += 1
                        if edsmid > maxedsmid:
                            maxedsmid = edsmid
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxedsmsysid, maxedsmid))
                    sys.stderr.flush()
                    timer.time('loadedsmsys', len(rows))
                sys.stderr.write('  {0} / {1}\n'.format(i, maxedsmsysid))
                with open(self.edsmsyscachefile + '.tmp', 'wb') as f:
                    self.edsmsysids.tofile(f)
                os.rename(self.edsmsyscachefile + '.tmp', self.edsmsyscachefile)

    def loadeddbsystems(self, conn: DBConnection, timer: Timer):
        maxeddbsysid = sqlqueries.get_max_eddb_system_id(conn)

        timer.time('sql')

        if maxeddbsysid:
            sys.stderr.write('Loading EDDB System IDs\n')
            c = sqlqueries.get_eddb_systems(conn)

            eddbsysarray = numpy.zeros(maxeddbsysid + 1048576, dtype=DTypeEDDBSystem)
            self.eddbsysids = eddbsysarray.view(numpy.core.records.recarray)
            timer.time('sql')

            i = 0
            maxeddbid = 0
            while True:
                rows = c.fetchmany(10000)
                timer.time('sqleddbsys', len(rows))
                if len(rows) == 0:
                    break
                for row in rows:
                    eddbid = row[1]
                    rec = eddbsysarray[eddbid]
                    rec[0] = row[0]
                    rec[1] = eddbid
                    rec[2] = row[2]
                    i += 1
                    if eddbid > maxeddbid:
                        maxeddbid = eddbid
                sys.stderr.write('.')
                if (i % 640000) == 0:
                    sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxeddbsysid, maxeddbid))
                sys.stderr.flush()
                timer.time('loadeddbsys', len(rows))
            sys.stderr.write('  {0} / {1}\n'.format(i, maxeddbsysid))

    def loadedsmbodies(self, conn: DBConnection, timer: Timer):
        maxedsmbodyid = sqlqueries.get_max_edsm_body_id(conn)

        timer.time('sql')

        if maxedsmbodyid:
            sys.stderr.write('Loading EDSM Body IDs\n')
            if os.path.exists(self.edsmbodycachefile):
                with open(self.edsmbodycachefile, 'rb') as f:
                    edsmbodyarray = numpy.fromfile(f, dtype=DTypeEDSMBody)

                if len(edsmbodyarray) > maxedsmbodyid:
                    if len(edsmbodyarray) < maxedsmbodyid + 524288:
                        edsmbodyarray = numpy.resize(edsmbodyarray, maxedsmbodyid + 1048576)
                    self.edsmbodyids = edsmbodyarray.view(numpy.core.records.recarray)

                timer.time('loadedsmbody', len(edsmbodyarray))

            if self.edsmbodyids is None:
                c = sqlqueries.get_edsm_bodies(conn)

                edsmbodyarray = numpy.zeros(maxedsmbodyid + 1048576, dtype=DTypeEDSMBody)
                self.edsmbodyids = edsmbodyarray.view(numpy.core.records.recarray)
                timer.time('sql')

                i = 0
                maxedsmid = 0
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqledsmbody', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        edsmid = row[1]
                        rec = edsmbodyarray[edsmid]
                        rec[0] = row[0]
                        rec[1] = edsmid
                        rec[2] = row[2]
                        i += 1
                        if edsmid > maxedsmid:
                            maxedsmid = edsmid
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxedsmbodyid, maxedsmid))
                    sys.stderr.flush()
                    timer.time('loadedsmbody', len(rows))
                sys.stderr.write('  {0} / {1}\n'.format(i, maxedsmbodyid))

    def loadparentsets(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Parent Sets\n')
        rows = sqlqueries.get_parent_sets(conn)
        timer.time('sqlparents', len(rows))
        for row in rows:
            self.parentsets[(int(row[1]), row[2])] = int(row[0])
        timer.time('loadparents', len(rows))

    def loadsoftware(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Software\n')
        rows = sqlqueries.get_software(conn)
        timer.time('sqlsoftware', len(rows))
        for row in rows:
            self.software[row[1]] = int(row[0])
        timer.time('loadsoftware', len(rows))

    def loadbodydesigs(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Body Designations\n')
        rows = sqlqueries.get_body_designations(conn)
        timer.time('sqlbodydesigs', len(rows))
        for row in rows:
            self.bodydesigs[row[1]] = (int(row[0]), int(row[2]))
        timer.time('loadbodydesigs', len(rows))

    def loadnamedbodies(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Named Bodies\n')
        rows = sqlqueries.get_named_bodies(conn)
        timer.time('sqlbodyname', len(rows))
        for row in rows:
            bi = EDDNBody(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
            if bi.system_id not in self.namedbodies:
                self.namedbodies[bi.system_id] = {}
            snb = self.namedbodies[bi.system_id]
            if bi.name not in snb:
                snb[bi.name] = bi
            elif type(snb[bi.name]) is not list:
                snb[bi.name] = [snb[bi.name]]
                snb[bi.name] += [bi]
            else:
                snb[bi.name] += [bi]

        timer.time('loadbodyname')

    def loadnamedsystems(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Named Systems\n')
        rows = sqlqueries.get_named_systems(conn)
        timer.time('sqlname', len(rows))

        for row in rows:
            si = EDDNSystem(row[0], row[1], row[2], row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105,
                            row[3] != 0 and row[4] != 0 and row[5] != 0)
            if si.name not in self.namedsystems:
                self.namedsystems[si.name] = si
            elif type(self.namedsystems[si.name]) is not list:
                self.namedsystems[si.name] = [self.namedsystems[si.name]]
                self.namedsystems[si.name] += [si]
            else:
                self.namedsystems[si.name] += [si]

        timer.time('loadname', len(rows))

    def loadregions(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Regions\n')
        rows = sqlqueries.get_regions(conn)
        timer.time('sqlregion', len(rows))
        for row in rows:
            ri = EDDNRegion(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9] == b'\x01')
            self.regions[ri.name.lower()] = ri
            if ri.region_address is not None:
                self.regionaddrs[ri.region_address] = ri

        timer.time('loadregion', len(rows))

    def loadfactions(self, conn: DBConnection, timer: Timer):
        sys.stderr.write('Loading Factions\n')
        rows = sqlqueries.get_factions(conn)
        timer.time('sqlfactions')

        for row in rows:
            fi = EDDNFaction(row[0], row[1], row[2], row[3])
            if fi.name not in self.factions:
                self.factions[fi.name] = fi
            elif type(self.factions[fi.name]) is not list:
                self.factions[fi.name] = [self.factions[fi.name]]
                self.factions[fi.name].append(fi)
            else:
                self.factions[fi.name].append(fi)

        timer.time('loadfactions')

    def loadknownbodies(self, timer: Timer):
        sys.stderr.write('Loading Known Bodies\n')
        knownbodies = {}

        with urllib.request.urlopen(self.knownbodiessheeturi) as f:
            for line in f:
                fields = line.decode('utf-8').strip().split('\t')
                if (len(fields) >= 7
                        and fields[0] != 'SystemAddress'
                        and fields[0] != ''
                        and fields[3] != ''
                        and fields[4] != ''
                        and fields[6] != ''):
                    sysaddr = int(fields[0])
                    sysname = fields[2]
                    bodyid = int(fields[3])
                    bodyname = fields[4]
                    bodydesig = fields[6]
                    desig = bodydesig[len(sysname):]

                    if desig not in self.bodydesigs:
                        row = sqlqueries.query_body_designation(self.conn, (desig,))

                        if row and row[1] == desig:
                            desigid = int(row[0])
                            category = int(row[2])
                            self.bodydesigs[desig] = (desigid, category)
                            sqlqueries.set_body_designation_used(self.conn, (desigid,))

                    if desig in self.bodydesigs:
                        desigid, category = self.bodydesigs[desig]
                        if sysname not in knownbodies:
                            knownbodies[sysname] = {}
                        sysknownbodies = knownbodies[sysname]
                        if bodyname not in sysknownbodies:
                            sysknownbodies[bodyname] = []
                        sysknownbodies[bodyname] += [
                            {
                                'SystemAddress': sysaddr,
                                'SystemName': sysname,
                                'BodyID': bodyid,
                                'BodyName': bodyname,
                                'BodyDesignation': bodydesig,
                                'BodyDesignationId': desigid
                            }
                        ]
                    else:
                        import pdb
                        pdb.set_trace()

        self.knownbodies = knownbodies
        timer.time('loadknownbodies')

    def commit(self):
        self.conn.commit()

    def _findsystem(self,
                    cursor: Union[Sequence[EDDNSystem], Sequence[Sequence]],
                    sysname: str,
                    starpos: Union[Sequence[float, float, float], List[float]],
                    sysaddr: Optional[int],
                    syslist: Set[EDDNSystem]):
        rows = list(cursor)
        systems: Union[MutableSequence[EDDNSystem], Set[EDDNSystem]]

        if len(rows) > 0 and type(rows[0]) is EDDNSystem:
            systems = rows
        else:
            systems = set(
                EDDNSystem(
                    row[0],
                    row[1],
                    from_db_string(row[2]),
                    row[3] / 32.0 - 49985,
                    row[4] / 32.0 - 40985,
                    row[5] / 32.0 - 24105,
                    row[3] != 0 and row[4] != 0 and row[5] != 0
                ) for row in rows
            )

        if starpos is not None or sysaddr is not None:
            matches: Set[EDDNSystem] = set()
            for system in systems:
                if (sysname is None or system.name.lower() == sysname.lower()) and (
                        starpos is None or not system.has_coords or (
                        system.x == starpos[0] and system.y == starpos[1] and system.z == starpos[2])) and (
                        sysaddr is None or sysaddr == system.id64):
                    matches.add(system)

            if len(matches) == 1:
                system = next(iter(matches))
                if not system.has_coords and starpos is not None:
                    vx = int((starpos[0] + 49985) * 32)
                    vy = int((starpos[1] + 40985) * 32)
                    vz = int((starpos[2] + 24105) * 32)
                    sqlqueries.set_system_coords(self.conn, (vx, vy, vz, system.id))
                    system = system._replace(x=starpos[0], y=starpos[1], z=starpos[2], has_coords=True)

                return system

        syslist |= set(systems)
        return None

    def findsystemsbyname(self, sysname: str) -> List[EDDNSystem]:
        systems: List[EDDNSystem] = []

        if sysname in self.namedsystems:
            nsystems = self.namedsystems[sysname]
            if type(nsystems) is EDDNSystem:
                systems = [nsystems]
            else:
                systems = nsystems
            systems = [s for s in systems]

        procgen_sysname_match = constants.procgen_sysname_re.match(sysname)
        region_info = None
        modsysaddr = None

        if procgen_sysname_match:
            regionname: str = procgen_sysname_match[1]
            mid1_2: str = procgen_sysname_match[2].upper()
            sizecls: str = procgen_sysname_match[3].lower()
            mid3 = int(procgen_sysname_match[4] or "0")
            seq = int(procgen_sysname_match[5])
            mid1a = ord(mid1_2[0]) - 65
            mid1b = ord(mid1_2[1]) - 65
            mid2 = ord(mid1_2[3]) - 65
            mid = (((mid3 * 26 + mid2) * 26 + mid1b) * 26 + mid1a)
            sz = ord(sizecls) - 97
            sx = 7 - sz
            sp = 320 << sz
            sb = 0x7F >> sz

            if regionname.lower() in self.regions:
                region_info = self.regions[regionname.lower()]
                modsysaddr = None
                if region_info.is_sphere_sector:
                    x0 = math.floor(region_info.x0 / sp) + (mid & 0x7F)
                    y0 = math.floor(region_info.y0 / sp) + ((mid >> 7) & 0x7F)
                    z0 = math.floor(region_info.z0 / sp) + ((mid >> 14) & 0x7F)
                    x1 = x0 & sb
                    x2 = x0 >> sx
                    y1 = y0 & sb
                    y2 = y0 >> sx
                    z1 = z0 & sb
                    z2 = z0 >> sx
                    modsysaddr = (z2 << 53) | (y2 << 47) | (x2 << 40) | (sz << 37) | (z1 << 30) | (y1 << 23) | (
                                x1 << 16) | seq
                elif region_info.region_address is not None:
                    modsysaddr = (region_info.region_address << 40) | (sz << 37) | (mid << 16) | seq

                if modsysaddr is not None:
                    rows = sqlqueries.get_systems_by_modsysaddr(self.conn, (modsysaddr,))

                    systems += [
                        EDDNSystem(
                            row[0],
                            row[1],
                            from_db_string(row[2]),
                            row[3] / 32.0 - 49985,
                            row[4] / 32.0 - 40985,
                            row[5] / 32.0 - 24105,
                            row[3] != 0 or row[4] != 0 or row[5] != 0
                        ) for row in rows
                    ]

        return systems

    def getrejectdata(self, sysname: str, sysaddr: int, systems: Optional[Iterable[EDDNSystem]]):
        id64name = None
        nameid64 = None
        pgsysmatch = constants.procgen_sysname_re.match(sysname)
        rejectdata = {}

        if sysaddr is not None:
            modsysaddr = id64_to_modsysaddr(sysaddr)
            regionaddr = modsysaddr >> 40
            if regionaddr in self.regionaddrs:
                region_info = self.regionaddrs[regionaddr]
                masscode = chr(((modsysaddr >> 37) & 7) + 97)
                seq = str(modsysaddr & 65535)
                mid = (modsysaddr >> 16) & 2097151
                mid1a = chr((mid % 26) + 65)
                mid1b = chr(((mid // 26) % 26) + 65)
                mid2 = chr(((mid // (26 * 26)) % 26) + 65)
                mid3 = mid // (26 * 26 * 26)
                mid3 = '' if mid3 == 0 else str(mid3) + '-'
                rejectdata['id64name'] = '{0} {1}{2}-{3} {4}{5}{6}'.format(
                    region_info.name,
                    mid1a,
                    mid1b,
                    mid2,
                    masscode,
                    mid3,
                    seq
                )

        if pgsysmatch:
            regionname: str = pgsysmatch[1]
            mid1_2: str = pgsysmatch[2].upper()
            sizecls: str = pgsysmatch[3].lower()
            mid3 = int(pgsysmatch[4] or "0")
            seq = int(pgsysmatch[5])
            mid1a = ord(mid1_2[0]) - 65
            mid1b = ord(mid1_2[1]) - 65
            mid2 = ord(mid1_2[3]) - 65
            mid = (((mid3 * 26 + mid2) * 26 + mid1b) * 26 + mid1a)
            sz = ord(sizecls) - 97
            sx = 7 - sz
            sp = 320 << sz
            sb = 0x7F >> sz

            if regionname.lower() in self.regions:
                region_info = self.regions[regionname.lower()]
                modsysaddr = None

                if region_info.is_sphere_sector:
                    x0 = math.floor(region_info.x0 / sp) + (mid & 0x7F)
                    y0 = math.floor(region_info.y0 / sp) + ((mid >> 7) & 0x7F)
                    z0 = math.floor(region_info.z0 / sp) + ((mid >> 14) & 0x7F)
                    x1 = x0 & sb
                    x2 = x0 >> sx
                    y1 = y0 & sb
                    y2 = y0 >> sx
                    z1 = z0 & sb
                    z2 = z0 >> sx
                    modsysaddr = (
                            (z2 << 53) |
                            (y2 << 47) |
                            (x2 << 40) |
                            (sz << 37) |
                            (z1 << 30) |
                            (y1 << 23) |
                            (x1 << 16) |
                            seq
                    )

                    rejectdata['nameid64'] = modsysaddr_to_id64(modsysaddr)
                elif region_info.region_address is not None:
                    modsysaddr = (region_info.region_address << 40) | (sz << 37) | (mid << 16) | seq
                    rejectdata['nameid64'] = modsysaddr_to_id64(modsysaddr)

        if systems is not None:
            rejectdata['systems'] = [{
                'id': int(s.id),
                'id64': int(s.id64),
                'x': float(s.x),
                'y': float(s.y),
                'z': float(s.z),
                'name': s.name
            } for s in systems]

        return rejectdata

    @lru_cache(maxsize=262144)
    def getsystem(self,
                  timer: Timer,
                  sysname: str,
                  x: Optional[float],
                  y: Optional[float],
                  z: Optional[float],
                  sysaddr: Optional[int]
                  ) -> Union[Tuple[EDDNSystem, None, None], Tuple[None, str, dict]]:
        if x is not None and y is not None and z is not None:
            starpos = [math.floor(v * 32 + 0.5) / 32.0 for v in (x, y, z)]
            vx = int((starpos[0] + 49985) * 32)
            vy = int((starpos[1] + 40985) * 32)
            vz = int((starpos[2] + 24105) * 32)
        else:
            starpos = None
            vx = 0
            vy = 0
            vz = 0

        systems: Set[EDDNSystem] = set()

        if sysname in self.namedsystems:
            namedsystems = self.namedsystems[sysname]
            if type(namedsystems) is not list:
                namedsystems = [namedsystems]

            system = self._findsystem(namedsystems, sysname, starpos, sysaddr, systems)
            if system is not None:
                return (system, None, None)

        timer.time('sysquery', 0)
        pgsysmatch = constants.procgen_sysname_re.match(sysname)
        region_info = None
        modsysaddr = None
        mid1a = mid1b = mid2 = sz = mid3 = seq = None

        if pgsysmatch:
            regionname: str = pgsysmatch[1]
            mid1_2: str = pgsysmatch[2].upper()
            sizecls: str = pgsysmatch[3].lower()
            mid3 = int(pgsysmatch[4] or "0")
            seq = int(pgsysmatch[5])
            mid1a = ord(mid1_2[0]) - 65
            mid1b = ord(mid1_2[1]) - 65
            mid2 = ord(mid1_2[3]) - 65
            mid = (((mid3 * 26 + mid2) * 26 + mid1b) * 26 + mid1a)
            sz = ord(sizecls) - 97
            sx = 7 - sz
            sp = 320 << sz
            sb = 0x7F >> sz

            if regionname.lower() in self.regions:
                region_info = self.regions[regionname.lower()]
                modsysaddr = None
                if region_info.is_sphere_sector:
                    x0 = math.floor(region_info.x0 / sp) + (mid & 0x7F)
                    y0 = math.floor(region_info.y0 / sp) + ((mid >> 7) & 0x7F)
                    z0 = math.floor(region_info.z0 / sp) + ((mid >> 14) & 0x7F)
                    x1 = x0 & sb
                    x2 = x0 >> sx
                    y1 = y0 & sb
                    y2 = y0 >> sx
                    z1 = z0 & sb
                    z2 = z0 >> sx
                    modsysaddr = (
                            (z2 << 53) |
                            (y2 << 47) |
                            (x2 << 40) |
                            (sz << 37) |
                            (z1 << 30) |
                            (y1 << 23) |
                            (x1 << 16) |
                            seq
                    )

                elif region_info.region_address is not None:
                    modsysaddr = (region_info.region_address << 40) | (sz << 37) | (mid << 16) | seq

                timer.time('sysquerypgre')
                if modsysaddr is not None:
                    rows = sqlqueries.get_systems_by_modsysaddr(self.conn, (modsysaddr,))
                    system = self._findsystem(rows, sysname, starpos, sysaddr, systems)
                    timer.time('sysselectmaddr')

                    if system is not None:
                        return (system, None, None)
                else:
                    errmsg = 'Unable to resolve system address for system {0} [{1}] ({2},{3},{4})\n'.format(
                        sysname,
                        sysaddr,
                        x,
                        y,
                        z
                    )
                    sys.stderr.write(errmsg)
                    sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
                    return (
                        None,
                        errmsg,
                        self.getrejectdata(sysname, sysaddr, systems)
                    )
                    # raise ValueError('Unable to resolve system address')
            else:
                errmsg = 'Region {5} not found for system {0} [{1}] ({2},{3},{4})\n'.format(
                    sysname,
                    sysaddr,
                    x,
                    y,
                    z,
                    regionname
                )
                sys.stderr.write(errmsg)
                sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
                return (
                    None,
                    errmsg,
                    self.getrejectdata(sysname, sysaddr, systems)
                )
                # raise ValueError('Region not found')

        if sysaddr is not None:
            modsysaddr = id64_to_modsysaddr(sysaddr)
            rows = sqlqueries.get_systems_by_modsysaddr(self.conn, (modsysaddr,))
            system = self._findsystem(rows, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                return (system, None, None)

        rows = sqlqueries.get_systems_by_name(self.conn, (sysname,))
        system = self._findsystem(rows, sysname, starpos, sysaddr, systems)
        timer.time('sysselectname')

        if system is not None:
            return (system, None, None)

        timer.time('sysquery', 0)
        edtsid64 = edtslookup.find_edts_system_id64(sysname, sysaddr, starpos)

        if edtsid64 is not None:
            timer.time('sysqueryedts', 0)
            modsysaddr = id64_to_modsysaddr(edtsid64)
            rows = sqlqueries.get_systems_by_modsysaddr(self.conn, (modsysaddr,))
            system = self._findsystem(rows, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                timer.time('sysqueryedts')
                return (system, None, None)

        timer.time('sysqueryedts', 0)
        rows = sqlqueries.get_systems_by_name(self.conn, (sysname,))
        system = self._findsystem(rows, sysname, starpos, None, systems)
        timer.time('sysselectname')

        if system is not None:
            return (system, None, None)

        # if starpos is None:
        #    import pdb; pdb.set_trace()

        if region_info is not None and modsysaddr is not None:
            raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
            if starpos is None or raddr == modsysaddr >> 40:
                cursor = sqlqueries.insert_system(
                    self.conn,
                    (modsysaddr, vx, vy, vz, region_info.is_sphere_sector, 0)
                )
                sysid = cursor.lastrowid
                if region_info.is_sphere_sector:
                    sqlqueries.insert_sphere_sector_system(
                        self.conn,
                        (sysid, modsysaddr, region_info.id, mid1a, mid1b, mid2, sz, mid3, seq)
                    )

                if starpos is not None:
                    return (
                        EDDNSystem(
                            sysid,
                            modsysaddr_to_id64(modsysaddr),
                            sysname,
                            starpos[0],
                            starpos[1],
                            starpos[2],
                            True
                        ),
                        None,
                        None
                    )
                else:
                    return (
                        EDDNSystem(
                            sysid,
                            modsysaddr_to_id64(modsysaddr),
                            sysname,
                            -49985,
                            -40985,
                            -24105,
                            False
                        ),
                        None,
                        None
                    )

        elif sysaddr is not None:
            modsysaddr = id64_to_modsysaddr(sysaddr)
            raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
            if starpos is None or raddr == modsysaddr >> 40:
                sysid = sqlqueries.insert_system(
                    self.conn,
                    (modsysaddr, vx, vy, vz, 0, 1)
                )

                sqlqueries.insert_named_system(self.conn, (sysid, sysname))
                sqlqueries.set_system_invalid(self.conn, (sysid,))

                if starpos is not None:
                    return (
                        EDDNSystem(
                            sysid,
                            modsysaddr_to_id64(modsysaddr),
                            sysname,
                            starpos[0],
                            starpos[1],
                            starpos[2],
                            True
                        ),
                        None,
                        None
                    )

                else:
                    return (
                        EDDNSystem(
                            sysid,
                            modsysaddr_to_id64(modsysaddr),
                            sysname,
                            -49985,
                            -40985,
                            -24105,
                            False
                        ),
                        None,
                        None
                    )

        raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)

        if starpos is not None:
            for mc in range(0, 8):
                rx = (vx % 40960) >> mc
                ry = (vy % 40960) >> mc
                rz = (vz % 40960) >> mc
                baddr = (raddr << 40) | (mc << 37) | (rz << 30) | (ry << 23) | (rx << 16)
                rows = sqlqueries.find_systems_in_boxel(self.conn, (baddr, baddr + 65536))

                for row in rows:
                    if (vx - 2 <= row[3] <= vx + 2
                            and vy - 2 <= row[4] <= vy + 2
                            and vz - 2 <= row[5] <= vz + 2):
                        systems += [
                            EDDNSystem(
                                row[0],
                                row[1],
                                from_db_string(row[2]),
                                row[3] / 32.0 - 49985,
                                row[4] / 32.0 - 40985,
                                row[5] / 32.0 - 24105,
                                row[3] != 0 and row[4] != 0 and row[5] != 0
                            )
                        ]

        timer.time('sysselectmaddr')

        errmsg = 'Unable to resolve system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, x, y, z)
        # sys.stderr.write(errmsg)
        # sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
        # raise ValueError('Unable to find system')
        return (
            None,
            errmsg,
            self.getrejectdata(sysname, sysaddr, systems)
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
                   ) -> Union[Tuple[EDDNStation, None, None], Tuple[None, str, Union[List[dict], None]]]:
        sysid = system.id if system is not None else None

        if name is None or name == '':
            return (None, 'No station name', None)

        if sysname is None or sysname == '':
            return (None, 'No system name', None)

        if timestamp is None:
            return (None, 'No timestamp', None)

        if stationtype is not None and stationtype == '':
            stationtype = None

        if bodyname is not None and bodyname == '':
            bodyname = None

        if stationtype not in ['SurfaceStation', 'CraterOutpost', 'CraterPort', 'OnFootSettlement'] or bodyid is None:
            bodyname = None

        if bodytype is not None and bodytype == '':
            bodytype = None

        if marketid is not None and marketid == 0:
            marketid = None

        if (stationtype is not None and stationtype == 'FleetCarrier') or constants.carrier_name_re.match(name):
            sysid = None
            sysname = ''
            bodyname = None
            bodytype = None
            bodyid = None
            stationtype = 'FleetCarrier'

        stationtype_location = None

        if (eventtype is not None
                and eventtype == 'Location'
                and stationtype is not None
                and stationtype == 'Bernal'
                and timestamp > constants.ed_3_3_2_date):
            stationtype_location = 'Bernal'
            stationtype = 'Ocellus'

        rows = sqlqueries.find_stations(self.conn, (sysname, name))

        stations = [
            EDDNStation(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9] == b'\x01',
                row[10],
                row[11],
                row[12] == b'\x01'
            ) for row in rows
        ]

        candidates = []

        for station in stations:
            replace = {}

            if marketid is not None:
                if station.market_id is not None and marketid != station.market_id:
                    continue
                else:
                    replace['marketid'] = marketid

            if sysid is not None:
                if station.system_id is not None and sysid != station.system_id:
                    continue
                else:
                    replace['systemid'] = sysid

            if stationtype is not None:
                if station.type is not None and stationtype != station.type:
                    continue
                else:
                    replace['type'] = stationtype

            if bodyname is not None and ((bodytype is None and bodyname != name) or bodytype == 'Planet'):
                if station.body is not None and bodyname != station.body:
                    continue
                else:
                    replace['body'] = bodyname

            if bodyid is not None:
                if station.bodyid is not None and bodyid != station.bodyid:
                    continue
                else:
                    replace['bodyid'] = bodyid

            candidates += [(station, replace)]

        if len(candidates) > 1 and bodyid is not None:
            bidcandidates = [c for c in candidates if c[0].body_id is not None]
            if len(bidcandidates) == 1:
                candidates = bidcandidates

        if len(candidates) > 1 and marketid is not None:
            midcandidates = [c for c in candidates if c[0].market_id is not None]
            if len(midcandidates) == 1:
                candidates = midcandidates

        if len(candidates) > 1 and stationtype is not None:
            stcandidates = [c for c in candidates if c[0].type is not None]
            if len(stcandidates) == 1:
                candidates = stcandidates

        if len(candidates) > 1 and sysid is not None:
            sidcandidates = [c for c in candidates if c[0].system_id is not None]
            if len(sidcandidates) == 1:
                candidates = sidcandidates

        if len(candidates) > 1 and not test:
            tcandidates = [c for c in candidates if not c[0].test]
            if len(tcandidates) == 1:
                candidates = tcandidates

        if stationtype == 'Megaship':
            candidates = [
                c
                for c in candidates
                if c[0].valid_from <= timestamp < c[0].valid_until
            ]

        if len(candidates) > 1:
            candidates = [
                c for c in candidates
                if not c[0].is_rejected and c[0].valid_from <= timestamp < c[0].valid_until
            ]

        if len(candidates) == 2:
            if (candidates[0][0].valid_from > candidates[1][0].valid_from
                    and candidates[0][0].valid_until < candidates[1][0].valid_until):
                candidates = [candidates[0]]
            elif (candidates[1][0].valid_from > candidates[0][0].valid_from
                  and candidates[1][0].valid_until < candidates[0][0].valid_until):
                candidates = [candidates[1]]
            elif candidates[0][0].valid_until == candidates[1][0].valid_from + timedelta(hours=15):
                if timestamp < candidates[0][0].valid_until - timedelta(hours=13):
                    candidates = [candidates[0]]
                else:
                    candidates = [candidates[1]]
            elif candidates[1][0].valid_until == candidates[0][0].valid_from + timedelta(hours=15):
                if timestamp < candidates[1][0].valid_until - timedelta(hours=13):
                    candidates = [candidates[1]]
                else:
                    candidates = [candidates[0]]

        if len(candidates) == 1:
            station, replace = candidates[0]

            if len(replace) != 0:
                station = self.updatestation(station, **replace)

            return (station, None, None)
        elif len(candidates) > 1:
            # import pdb; pdb.set_trace()
            return (
                None,
                'More than 1 match',
                [{
                    'station': {
                        'id': s.id,
                        'stationName': s.name,
                        'marketId': s.market_id,
                        'systemName': s.system_name,
                        'systemId': s.system_id,
                        'stationType': s.type,
                        'locationStationType': s.type_location,
                        'bodyName': s.body,
                        'bodyId': s.body_id,
                        'isRejected': True if s.is_rejected else False,
                        'validFrom': s.valid_from.isoformat(),
                        'validUntil': s.valid_until.isoformat(),
                        'test': True if s.test else False
                    },
                    'replace': r
                } for s, r in candidates])

        if bodyname is not None and not ((bodytype is None and bodyname != name) or bodytype == 'Planet'):
            bodyname = None

        validfrom = constants.timestamp_base_date
        validuntil = constants.timestamp_max_date

        if stationtype is not None:
            if stationtype == 'SurfaceStation':
                validuntil = constants.ed_3_3_0_date
            elif (marketid is not None and marketid >= 3789600000) or stationtype == 'OnFootSettlement':
                validfrom = constants.ed_4_0_0_date
            elif (marketid is not None and marketid >= 3700000000) or stationtype == 'FleetCarrier':
                validfrom = constants.ed_3_7_0_date
            elif stationtype in ['CraterPort', 'CraterOutpost']:
                validfrom = constants.ed_3_3_0_date
            elif stationtype == 'Ocellus':
                validfrom = constants.ed_3_3_2_date
                stationtype_location = 'Bernal'
            elif stationtype == 'Bernal' and timestamp < constants.ed_3_3_2_date:
                validuntil = constants.ed_3_3_2_date
            elif stationtype == 'Megaship' and marketid is not None and marketid >= 3400000000:
                validfrom = constants.megaship_week_0 + timedelta(
                    weeks=math.floor((timestamp - constants.megaship_week_0).total_seconds() / 86400 / 7),
                    hours=-2
                )
                validuntil = validfrom + timedelta(days=7, hours=15)

        if (sysid is None and sysname != '') or stationtype is None or marketid is None:
            return (
                None,
                'Station mismatch',
                [{
                    'MarketId': marketid,
                    'StationName': name,
                    'SystemName': sysname,
                    'SystemId': sysid,
                    'StationType': stationtype,
                    'LocationStationType': stationtype_location,
                    'Body': bodyname,
                    'BodyID': bodyid,
                    'IsTest': 1 if test else 0
                }]
            )

        stationid = sqlqueries.insert_station(
            self.conn,
            (marketid, name, sysname, sysid, stationtype, stationtype_location,
             bodyname, bodyid, validfrom, validuntil, test)
        )
        return (
            EDDNStation(
                stationid,
                marketid,
                name,
                sysname,
                sysid,
                stationtype,
                stationtype_location or stationtype,
                bodyname,
                bodyid,
                False,
                validfrom,
                validuntil,
                test
            ),
            None,
            None
        )

    def insertbodyparents(self, timer: Timer, scanbodyid: int, system: EDDNSystem, bodyid: int, parents: List[Dict]):
        if parents is not None and bodyid is not None:
            parentjson = json.dumps(parents)

            if (bodyid, parentjson) not in self.parentsets:
                rowid = sqlqueries.insert_parent_set(self.conn, (bodyid, parentjson))
                self.parentsets[(bodyid, parentjson)] = rowid

            parentsetid = self.parentsets[(bodyid, parentjson)]

            sqlqueries.insert_parent_set_link(self.conn, (scanbodyid, parentsetid))

    def insertsoftware(self, softwarename: str):
        if softwarename not in self.software:
            self.software[softwarename] = sqlqueries.insert_software(self.conn, (softwarename,))

    def insertedsmfile(self, filename: str):
        return sqlqueries.insert_edsm_file(self.conn, (filename,))

    def getbody(self, timer: Timer, name: str, sysname: str, bodyid: int, system, body, timestamp):
        if system.id in self.namedbodies and name in self.namedbodies[system.id]:
            timer.time('bodyquery', 0)
            rows = self.namedbodies[system.id][name]

            if type(rows) is not list:
                rows = [rows]

            multimatch = len(rows) > 1

            if bodyid is not None:
                rows = [row for row in rows if row.bodyid is None or row.bodyid == bodyid]

            if len(rows) > 1 and name == sysname:
                if 'PlanetClass' in body:
                    rows = [row for row in rows if row.category == 6]
                elif 'StarType' in body:
                    rows = [row for row in rows if row.category == 2]

            if multimatch and 'Periapsis' in body:
                aop = body['Periapsis']
                if len(rows) == 1 and rows[0].arg_of_periapsis is None:
                    pass
                elif len(rows) > 1:
                    rows = [row for row in rows if
                            row.arg_of_periapsis is None or ((aop + 725 - row.arg_of_periapsis) % 360) < 10]

            if len(rows) > 1:
                rows = [row for row in rows if row.valid_from < timestamp < row.valid_until]

            if len(rows) > 1:
                rows = [row for row in rows if row.is_rejected == 0]

            timer.time('bodylookupname')
            if len(rows) == 1:
                return (rows[0], None, None)

        ispgname = name.startswith(sysname)
        if name == sysname:
            if 'SemiMajorAxis' in body and body['SemiMajorAxis'] is not None:
                ispgname = False
            elif bodyid is not None and bodyid != 0:
                ispgname = False
            elif 'BodyType' in body and body['BodyType'] != 'Star':
                ispgname = False

        desigid = None
        category = None
        sysknownbodies = None
        knownbodies = None

        if sysname in self.knownbodies:
            sysknownbodies = self.knownbodies[sysname]
            if name in sysknownbodies:
                knownbodies = self.knownbodies[sysname][name]
                if bodyid is not None:
                    knownbodies = [row for row in knownbodies if row['BodyID'] == bodyid]
                if len(knownbodies) == 1:
                    knownbody = knownbodies[0]
                    if knownbody['BodyDesignation'] != knownbody['BodyName']:
                        ispgname = False
                        desigid = knownbody['BodyDesignationId']

        if ispgname:
            timer.time('bodyquery', 0)
            desig = name[len(sysname):]
            match = constants.procgen_body_name_re.match(desig)

            if desig in self.bodydesigs:
                desigid, category = self.bodydesigs[desig]
            else:
                row = sqlqueries.get_body_designation(self.conn, (desig,))

                if row and row[1] == desig:
                    desigid = int(row[0])
                    category = int(row[2])
                    self.bodydesigs[desig] = (desigid, category)
                    sqlqueries.set_body_designation_used(self.conn, (desigid,))
                elif match:
                    stars = match['stars']
                    nebula = match['nebula']
                    belt = match['belt']
                    cluster = match['cluster']
                    stellarcomet = match['stellarcomet']
                    planetstr = match['planet']
                    ring1 = match['planetring']
                    comet1 = match['planetcomet']
                    moon1str = match['moon1']
                    ring2 = match['moon1ring']
                    comet2 = match['moon1comet']
                    moon2str = match['moon2']
                    ring3 = match['moon2ring']
                    comet3 = match['moon2comet']
                    moon3str = match['moon3']

                    bodycategory = 0
                    planet = 0
                    moon1 = 0
                    moon2 = 0
                    moon3 = 0

                    if planetstr is not None:
                        if '+' in planetstr:
                            planetlist = planetstr.split('+')
                            planet = int(planetlist[0])
                            moon1 = int(planetlist[-1]) - planet
                            bodycategory = 5
                        else:
                            planet = int(planetstr)
                            bodycategory = 6
                    elif belt is not None:
                        planet = ord(belt) - 64
                        if cluster is not None:
                            moon1 = int(cluster)
                            bodycategory = 4
                        else:
                            bodycategory = 3
                    elif stellarcomet is not None:
                        bodycategory = 15
                    elif nebula is not None:
                        bodycategory = 19
                    elif stars is not None and len(stars) > 1:
                        bodycategory = 1
                    else:
                        bodycategory = 2

                    if bodycategory == 6:
                        if moon1str is not None:
                            if '+' in moon1str:
                                moon1list = moon1str.split('+')
                                moon1 = ord(moon1list[0]) - 96
                                moon2 = ord(moon1list[-1]) - 96 - moon1
                                bodycategory = 8
                            else:
                                moon1 = ord(moon1str) - 96
                                bodycategory = 9
                        elif ring1 is not None:
                            moon1 = ord(ring1) - 64
                            bodycategory = 7
                        elif comet1 is not None:
                            moon1 = int(comet1)
                            bodycategory = 16

                    if bodycategory == 9:
                        if moon2str is not None:
                            if '+' in moon2str:
                                moon2list = moon2str.split('+')
                                moon2 = ord(moon2list[0]) - 96
                                moon3 = ord(moon2list[-1]) - 96 - moon2
                                bodycategory = 11
                            else:
                                moon2 = ord(moon2str) - 96
                                bodycategory = 12
                        elif ring2 is not None:
                            moon2 = ord(ring2) - 64
                            bodycategory = 10
                        elif comet2 is not None:
                            moon2 = int(comet2)
                            bodycategory = 17

                    if bodycategory == 12:
                        if moon3str is not None:
                            moon3 = ord(moon3str) - 96
                            bodycategory = 14
                        elif ring3 is not None:
                            moon3 = ord(ring3) - 64
                            bodycategory = 13
                        elif comet3 is not None:
                            moon3 = int(comet3)
                            bodycategory = 18

                    star = 0
                    if stars is not None:
                        for i in range(ord(stars[0]) - 65, ord(stars[-1]) - 64):
                            star |= 1 << i

                    # import pdb; pdb.set_trace()
                    return (
                        None,
                        'Body designation not in database',
                        [{
                            'Designation': desig,
                            'BodyCategory': bodycategory,
                            'Stars': stars,
                            'Planet': planet,
                            'Moon1': moon1,
                            'Moon2': moon2,
                            'Moon3': moon3
                        }]
                    )

            timer.time('bodyquerypgre')

        timer.time('bodyquery', 0)
        rows = sqlqueries.get_bodies_by_name(self.conn, (system.id, name, 1))
        rows += sqlqueries.get_bodies_by_name(self.conn, (system.id, name, 0))
        timer.time('bodyselectname')
        ufrows = rows

        multimatch = len(rows) > 1

        if bodyid is not None:
            rows = [row for row in rows if row[4] == bodyid or row[4] is None]

        if len(rows) > 1 and name == sysname:
            if 'PlanetClass' in body:
                rows = [row for row in rows if row[5] == 'PlanetaryBody']
            elif 'StarType' in body:
                rows = [row for row in rows if row[5] == 'StellarBody']

        if multimatch and 'Periapsis' in body:
            aop = body['Periapsis']
            if len(rows) == 1 and rows[0][6] is None:
                pass
            elif len(rows) > 1:
                rows = [row for row in rows if row[6] is None or ((aop + 725 - row[6]) % 360) < 10]

        if len(rows) > 1:
            xrows = [row for row in rows if row[7] < timestamp < row[8]]
            if len(xrows) > 0:
                rows = xrows

        if len(rows) > 1:
            xrows = [row for row in rows if row[9]]
            if len(xrows) > 0:
                rows = xrows

        timer.time('bodyqueryname')
        if len(rows) == 1:
            row = rows[0]
            if row[4] is None and bodyid is not None:
                sqlqueries.set_body_bodyid(self.conn, (bodyid, row[0]))
                timer.time('bodyupdateid')
            return (
                EDDNBody(
                    row[0],
                    name,
                    sysname,
                    system.id,
                    row[4] or bodyid,
                    row[10],
                    (body['Periapsis'] if 'Periapsis' in body else None),
                    constants.timestamp_base_date,
                    constants.timestamp_max_date,
                    False,
                    row[11]
                ),
                None,
                None
            )
        elif len(rows) > 1:
            return (
                None,
                'Multiple body matches',
                [{
                    'id': int(row[0]),
                    'bodyName': row[1],
                    'systemName': row[2],
                    'systemId': int(row[3]),
                    'bodyId': int(row[4]) if row[4] is not None else None,
                    'bodyCategory': row[5] if row[5] is not None else None,
                    'argOfPeriapsis': float(row[6]) if row[6] is not None else None,
                    'validFrom': row[7].isoformat(),
                    'validUntil': row[8].isoformat(),
                    'isRejected': True if row[9] else False
                } for row in rows]
            )
        else:
            allrows = sqlqueries.get_system_bodies(self.conn, (system.id, 1))
            allrows += sqlqueries.get_system_bodies(self.conn, (system.id, 0))
            frows = [r for r in allrows if r[1].lower() == name.lower()]

            if bodyid is not None:
                frows = [r for r in frows if r[4] is None or r[4] == bodyid]

            if len(frows) > 0:
                if sysname in self.namedsystems:
                    systems = self.namedsystems[sysname]
                    if type(systems) is not list:
                        systems = [systems]
                    for xsystem in systems:
                        allrows += sqlqueries.get_system_bodies(self.conn, (xsystem.id, 1))
                        allrows += sqlqueries.get_system_bodies(self.conn, (xsystem.id, 0))
                frows = [r for r in allrows if r[1].lower() == name.lower()]

                import pdb
                pdb.set_trace()
                return (
                    None,
                    'Body Mismatch',
                    [{
                        'id': int(row[0]),
                        'bodyName': row[1],
                        'systemName': row[2],
                        'systemId': int(row[3]),
                        'bodyId': int(row[4]) if row[4] is not None else None,
                        'bodyCategory': int(row[5]) if row[5] is not None else None,
                        'argOfPeriapsis': float(row[6]) if row[6] is not None else None,
                        'validFrom': row[7].isoformat(),
                        'validUntil': row[8].isoformat(),
                        'isRejected': True if row[9] else False
                    } for row in rows]
                )

            if ispgname and desigid is not None:
                rowid = sqlqueries.insert_body(
                    self.conn,
                    (system.id, 1 if bodyid is not None else 0, bodyid or 0, desigid, 0)
                )
                timer.time('bodyinsertpg')
                return (
                    EDDNBody(
                        rowid,
                        name,
                        sysname,
                        system.id,
                        bodyid,
                        category,
                        body.get('Periapsis'),
                        constants.timestamp_base_date,
                        constants.timestamp_max_date,
                        False,
                        desigid
                    ),
                    None,
                    None
                )

            if (not ispgname and constants.procgen_sysname_re.match(name)) or desigid is None:
                allrows = sqlqueries.get_bodies_by_custom_name(self.conn, (name,))
                pgsysbodymatch = constants.procgen_sys_body_name_re.match(name)
                dupsystems = []

                if pgsysbodymatch:
                    dupsysname = pgsysbodymatch['sysname']
                    desig = pgsysbodymatch['desig']
                    dupsystems = self.findsystemsbyname(dupsysname)

                    for dupsystem in dupsystems:
                        allrows += sqlqueries.get_system_bodies(self.conn, (dupsystem.id, 1))
                        allrows += sqlqueries.get_system_bodies(self.conn, (dupsystem.id, 0))

                frows = [r for r in allrows if r[1].lower() == name.lower()]

                if len(frows) > 0:
                    return (
                        None,
                        'Body in wrong system',
                        [{
                            'id': int(row[0]),
                            'bodyName': row[1],
                            'systemName': row[2],
                            'systemId': int(row[3]),
                            'bodyId': int(row[4]) if row[4] is not None else None,
                            'bodyCategory': int(row[5]) if row[5] is not None else None,
                            'argOfPeriapsis': float(row[6]) if row[6] is not None else None,
                            'validFrom': row[7].isoformat(),
                            'validUntil': row[8].isoformat(),
                            'isRejected': True if row[9] else False
                        } for row in rows]
                    )
                elif pgsysbodymatch:
                    if len(dupsystems) > 0:
                        return (
                            None,
                            'Procgen body in wrong system',
                            [{
                                'System': sysname,
                                'Body': name,
                                'Systems': [{
                                    'id': s.id,

                                } for s in dupsystems]
                            }]
                        )
                    return (
                        None,
                        'Procgen body in wrong system',
                        [{'System': sysname, 'Body': name}])
                else:
                    if ('debugunknownbodies' in os.environ
                            and (sysknownbodies is not None
                                 or 'debugunknownbodysystems' in os.environ)):
                        import pdb
                        pdb.set_trace()

                    return (None, 'Unknown named body', [{'System': sysname, 'Body': name}])

            rowid = sqlqueries.insert_body(
                self.conn,
                (system.id, 1 if bodyid is not None else 0, bodyid or 0, desigid)
            )
            if rowid is None:
                import pdb
                pdb.set_trace()

            sqlqueries.insert_named_body(self.conn, (rowid, system.id, name))
            # sqlqueries.set_body_invalid(self.conn, (rowid,))

            return (
                EDDNBody(
                    rowid,
                    name,
                    sysname,
                    system.id,
                    bodyid,
                    category,
                    (body['Periapsis'] if 'Periapsis' in body else None),
                    constants.timestamp_base_date,
                    constants.timestamp_max_date,
                    False,
                    desigid
                ),
                None,
                None
            )

    def getfaction(self, timer: Timer, name: str, government: str, allegiance: Optional[str]):
        factions = None

        if government[:12] == '$government_' and government[-1] == ';':
            government = government[12:-1]

        if name in self.factions:
            factions = self.factions[name]
            if type(factions) is not list:
                factions = [factions]
            for faction in factions:
                if faction.government == government and (allegiance is None or faction.allegiance == allegiance):
                    return faction

        if allegiance is None:
            return None

        factionid = sqlqueries.insert_faction(self.conn, (name, government, allegiance))
        faction = EDDNFaction(factionid, name, government, allegiance)

        if factions is None:
            self.factions[name] = faction
        elif type(self.factions[name]) is not list:
            self.factions[name] = [self.factions[name]]
            self.factions[name].append(faction)
        else:
            self.factions[name].append(faction)

        return faction

    def updatestation(self, station: EDDNStation, **kwargs):
        station = station._replace(**kwargs)

        sqlqueries.update_station(
            self.conn,
            (station.market_id, station.system_id, station.type, station.body, station.bodyid, station.id)
        )

        return station

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
            -> Union[Tuple[int, int, bool, Optional[NPTypeEDDBSystem]], Tuple[None, None, None, None]]:
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
            -> Union[Tuple[int, int, Optional[NPTypeEDSMBody]], Tuple[None, None, None]]:
        if self.edsmbodyids is not None and len(self.edsmbodyids) > edsmid:
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
        ts = int((ts - constants.timestamp_base_date).total_seconds())
        sqlqueries.upsert_edsm_body(self.conn, (edsmid, bodyid, ts, bodyid, ts))

        if edsmid < len(self.edsmbodyids):
            rec = self.edsmbodyids[edsmid]
            rec[0] = bodyid
            rec[1] = edsmid
            rec[2] = ts
            return rec
        else:
            return None

    def updateedsmstationid(self, edsmid: int, stationid: int, ts: datetime):
        sqlqueries.upsert_edsm_station(self.conn, (edsmid, stationid, ts, stationid, ts))

    def findeddbsysid(self, eddbid: int):
        if self.eddbsysids is not None and len(self.eddbsysids) > eddbid:
            row = self.eddbsysids[eddbid]

            if row[0] != 0:
                return (row[0], row[2])

        row = sqlqueries.get_system_by_eddb_id(self.conn, (eddbid,))

        if row:
            return (row[0], row[1])
        else:
            return (None, None)

    def updateeddbsysid(self, eddbid: int, sysid: int, ts: int):
        sqlqueries.upsert_eddb_system(self.conn, (eddbid, sysid, ts, sysid, ts))

    def addfilelinestations(self, linelist: List[Tuple[int, int, EDDNStation]]):
        values = [(fileid, lineno, station.id) for fileid, lineno, station in linelist]
        sqlqueries.insert_file_line_stations(self.conn, values)

    def addfilelineinfo(self,
                        linelist: List[Tuple[
                            int, int, datetime, datetime, int, int, int, int, float, bool, bool, bool
                        ]]):
        sqlqueries.insert_file_line_info(self.conn, linelist)

    def addfilelinefactions(self, linelist: List[Tuple[int, int, EDDNFaction, int]]):
        values = [(fileid, lineno, faction.id, entrynum) for fileid, lineno, faction, entrynum in linelist]
        sqlqueries.insert_file_line_factions(self.conn, values)

    def addfilelineroutesystems(self, linelist: List[Tuple[int, int, EDDNSystem, int]]):
        values = [(fileid, lineno, system.id, entrynum) for fileid, lineno, system, entrynum in linelist]
        sqlqueries.query_insert_file_line_route_systems(self.conn, values)

    def addedsmfilelinebodies(self, linelist: List[Tuple[int, int, int]]):
        values = [(fileid, lineno, edsmbodyid) for fileid, lineno, edsmbodyid in linelist]
        sqlqueries.query_insert_edsm_file_line_systems(self.conn, values)

    def getstationfilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_stations_by_file(self.conn, (fileid,))
        return {row[0]: row[1] for row in rows}

    def getinfofilelines(self, fileid: int):
        rows = sqlqueries.query_file_line_info_by_file(self.conn, (fileid,))
        return {row[0]: (row[1], row[2], row[3]) for row in rows}

    def getfactionfilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_factions_by_file(self.conn, (fileid,))

        lines = {}
        for row in rows:
            if row[0] not in lines:
                lines[row[0]] = []
            lines[row[0]] += [row[1]]

        return lines

    def getnavroutefilelines(self, fileid: int):
        rows = sqlqueries.get_file_line_routes_by_file(self.conn, (fileid,))

        lines = {}
        for row in rows:
            lines[(row[0], row[1])] = row[2]

        return lines

    def getedsmbodyfilelines(self, fileid: int):
        maxline = sqlqueries.get_max_edsm_body_file_lineno(self.conn, (fileid,))

        if maxline is None:
            return []

        filelinearray = numpy.zeros(maxline + 1, numpy.int32)

        cursor = sqlqueries.get_edsm_body_file_lines_by_file(self.conn, (fileid,))

        for row in cursor:
            filelinearray[row[0]] = row[1]

        return filelinearray

    def geteddnfiles(self):

        sys.stderr.write('    Getting station line counts\n')
        stnlinecounts = {row[0]: row[1] for row in sqlqueries.get_station_file_line_counts(self.conn)}

        sys.stderr.write('    Getting info line counts\n')
        infolinecounts = {row[0]: row[1] for row in sqlqueries.get_info_file_line_counts(self.conn)}

        sys.stderr.write('    Getting faction line counts\n')
        factionlinecounts = {row[0]: row[1] for row in sqlqueries.get_faction_file_line_counts(self.conn)}

        sys.stderr.write('    Getting nav route line counts\n')
        navroutelinecounts = {row[0]: row[1] for row in sqlqueries.get_route_file_line_counts(self.conn)}

        sys.stderr.write('    Getting file info\n')
        return {
            row[1]: EDDNFile(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                stnlinecounts[row[0]] if row[0] in stnlinecounts else 0,
                infolinecounts[row[0]] if row[0] in infolinecounts else 0,
                factionlinecounts[row[0]] if row[0] in factionlinecounts else 0,
                navroutelinecounts[row[0]] if row[0] in navroutelinecounts else 0,
                row[5],
                row[6],
                row[7],
                row[8]
            ) for row in sqlqueries.get_files(self.conn)
        }

    def getedsmfiles(self):
        sys.stderr.write('    Getting body line counts\n')
        bodylinecounts = {row[0]: row[1] for row in sqlqueries.get_edsm_body_file_line_counts(self.conn)}

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
            (linecount, comprsize, totalsize, poplinecount, stnlinecount, navroutesystemcount, fileid)
        )

    def updateedsmfileinfo(self, fileid: int, linecount: int, totalsize: int, comprsize: int):
        sqlqueries.update_edsm_file_info(self.conn, (linecount, comprsize, totalsize, fileid))
