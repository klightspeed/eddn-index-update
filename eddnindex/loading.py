import os
import os.path
import sys
import urllib.request
import urllib.error
from typing import Tuple
from collections.abc import MutableSequence as List, \
                            MutableMapping as Dict

import numpy
import numpy.typing
import numpy.core.records

from eddnindex.bodies import get_body_designation

from .types import BodyDesignation, EDDNSystem, EDDNBody, EDDNFaction, \
                   EDDNRegion, DTypeEDSMSystem, \
                   DTypeEDDBSystem, DTypeEDSMBody, KnownBody
from .timer import Timer
from . import sqlqueries
from .database import DBConnection


def loadedsmsystems(conn: DBConnection,
                    timer: Timer,
                    edsmsyscachefile: str
                    ) -> numpy.core.records.recarray:
    maxedsmsysid = sqlqueries.get_max_edsm_system_id(conn, None)
    edsmsysids: numpy.core.records.recarray = numpy.empty(
        0,
        DTypeEDSMSystem
    ).view(numpy.core.records.recarray)

    timer.time('sql')

    if maxedsmsysid:
        sys.stderr.write('Loading EDSM System IDs\n')
        if os.path.exists(edsmsyscachefile):
            with open(edsmsyscachefile, 'rb') as f:
                edsmsysarray = numpy.fromfile(f, dtype=DTypeEDSMSystem)

            if len(edsmsysarray) > maxedsmsysid:
                if len(edsmsysarray) < maxedsmsysid + 524288:
                    edsmsysarray = numpy.resize(
                        edsmsysarray,
                        maxedsmsysid + 1048576
                    )
                edsmsysids = edsmsysarray.view(
                    numpy.core.records.recarray
                )

            timer.time('loadedsmsys', len(edsmsysarray))

        if len(edsmsysids) == 0:
            c = sqlqueries.get_edsm_systems(conn, None)

            edsmsysarray = numpy.zeros(
                maxedsmsysid + 1048576,
                dtype=DTypeEDSMSystem
            )

            edsmsysids = edsmsysarray.view(
                numpy.core.records.recarray
            )

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
                    sys.stderr.write(
                        f'  {i} / {maxedsmsysid} ({maxedsmid})\n'
                    )

                sys.stderr.flush()
                timer.time('loadedsmsys', len(rows))

            sys.stderr.write(
                f'  {i} / {maxedsmsysid}\n'
            )

            with open(edsmsyscachefile + '.tmp', 'wb') as f:
                edsmsysids.tofile(f)

            os.rename(
                edsmsyscachefile + '.tmp',
                edsmsyscachefile
            )

    return edsmsysids


def loadeddbsystems(conn: DBConnection,
                    timer: Timer
                    ) -> numpy.core.records.recarray:
    maxeddbsysid = sqlqueries.get_max_eddb_system_id(conn, None)
    eddbsysids: numpy.core.records.recarray = numpy.empty(
        0,
        DTypeEDDBSystem
    ).view(numpy.core.records.recarray)

    timer.time('sql')

    if maxeddbsysid:
        sys.stderr.write('Loading EDDB System IDs\n')
        c = sqlqueries.get_eddb_systems(conn, None)

        eddbsysarray = numpy.zeros(
            maxeddbsysid + 1048576,
            dtype=DTypeEDDBSystem
        )

        eddbsysids = eddbsysarray.view(numpy.core.records.recarray)
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
                sys.stderr.write(
                    f'  {i} / {maxeddbsysid} ({maxeddbid})\n'
                )

            sys.stderr.flush()
            timer.time('loadeddbsys', len(rows))

        sys.stderr.write(
            f'  {i} / {maxeddbsysid}\n'
        )

    return eddbsysids


def loadedsmbodies(conn: DBConnection,
                   timer: Timer,
                   edsmbodycachefile: str
                   ) -> numpy.core.records.recarray:
    maxedsmbodyid = sqlqueries.get_max_edsm_body_id(conn, None)
    edsmbodyids = numpy.empty(
        0,
        DTypeEDSMBody
    ).view(numpy.core.records.recarray)

    timer.time('sql')

    if maxedsmbodyid:
        sys.stderr.write('Loading EDSM Body IDs\n')
        if os.path.exists(edsmbodycachefile):
            with open(edsmbodycachefile, 'rb') as f:
                edsmbodyarray = numpy.fromfile(f, dtype=DTypeEDSMBody)

            if len(edsmbodyarray) > maxedsmbodyid:
                if len(edsmbodyarray) < maxedsmbodyid + 524288:
                    edsmbodyarray = numpy.resize(
                        edsmbodyarray,
                        maxedsmbodyid + 1048576
                    )

                edsmbodyids = edsmbodyarray.view(
                    numpy.core.records.recarray
                )

            timer.time('loadedsmbody', len(edsmbodyarray))

        if len(edsmbodyids) == 0:
            c = sqlqueries.get_edsm_bodies(conn, None)

            edsmbodyarray = numpy.zeros(
                maxedsmbodyid + 1048576,
                dtype=DTypeEDSMBody
            )

            edsmbodyids = edsmbodyarray.view(
                numpy.core.records.recarray
            )

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
                    sys.stderr.write(
                        f'  {i} / {maxedsmbodyid} ({maxedsmid})\n'
                    )

                sys.stderr.flush()
                timer.time('loadedsmbody', len(rows))

            sys.stderr.write(
                f'  {i} / {maxedsmbodyid}\n'
            )

    return edsmbodyids


def loadparentsets(conn: DBConnection,
                   timer: Timer
                   ) -> Dict[Tuple[int, str], int]:
    sys.stderr.write('Loading Parent Sets\n')
    rows = sqlqueries.get_parent_sets(conn, None)
    timer.time('sqlparents', len(rows))
    parentsets: Dict[Tuple[int, str], int] = {}

    for row in rows:
        parentsets[(int(row[1]), row[2])] = int(row[0])

    timer.time('loadparents', len(rows))

    return parentsets


def loadsoftware(conn: DBConnection,
                 timer: Timer
                 ) -> Dict[str, int]:
    sys.stderr.write('Loading Software\n')
    rows = sqlqueries.get_software(conn, None)
    timer.time('sqlsoftware', len(rows))
    software: Dict[str, int] = {}

    for row in rows:
        software[row[1]] = int(row[0])

    timer.time('loadsoftware', len(rows))

    return software


def loadbodydesigs(conn: DBConnection,
                   timer: Timer
                   ) -> Dict[str, Tuple[int, BodyDesignation]]:
    sys.stderr.write('Loading Body Designations\n')
    rows = sqlqueries.get_body_designations(conn, None)
    timer.time('sqlbodydesigs', len(rows))
    bodydesigs: Dict[str, Tuple[int, BodyDesignation]] = {}

    for row in rows:
        bodydesigs[row[1]] = (
            row[0],
            BodyDesignation(
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[1]
            )
        )

    timer.time('loadbodydesigs', len(rows))

    return bodydesigs


def loadnamedbodies(conn: DBConnection,
                    timer: Timer
                    ) -> Dict[int, Dict[str, List[EDDNBody]]]:
    sys.stderr.write('Loading Named Bodies\n')
    rows = sqlqueries.get_named_bodies(conn, None)
    timer.time('sqlbodyname', len(rows))
    namedbodies: Dict[int, Dict[str, List[EDDNBody]]] = {}

    for row in rows:
        bi = EDDNBody(
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10]
        )

        if bi.system_id not in namedbodies:
            namedbodies[bi.system_id] = {}

        snb = namedbodies[bi.system_id]

        snbent = snb.get(bi.name)
        if snbent is None:
            snb[bi.name] = [bi]
        else:
            snbent.append(bi)

    timer.time('loadbodyname')

    return namedbodies


def loadnamedsystems(conn: DBConnection,
                     timer: Timer
                     ) -> Dict[str, List[EDDNSystem]]:
    sys.stderr.write('Loading Named Systems\n')
    rows = sqlqueries.get_named_systems(conn, None)
    timer.time('sqlname', len(rows))
    namedsystems: Dict[str, List[EDDNSystem]] = {}

    for row in rows:
        si = EDDNSystem(
            row[0],
            row[1],
            row[2],
            row[3] / 32.0 - 49985,
            row[4] / 32.0 - 40985,
            row[5] / 32.0 - 24105,
            row[3] != 0 and row[4] != 0 and row[5] != 0
        )

        nsent = namedsystems.get(si.name)

        if nsent is None:
            namedsystems[si.name] = [si]
        else:
            nsent.append(si)

    timer.time('loadname', len(rows))

    return namedsystems


def loadregions(conn: DBConnection,
                timer: Timer
                ) -> Tuple[Dict[str, EDDNRegion], Dict[int, EDDNRegion]]:
    sys.stderr.write('Loading Regions\n')
    rows = sqlqueries.get_regions(conn, None)
    timer.time('sqlregion', len(rows))
    regions: Dict[str, EDDNRegion] = {}
    regionaddrs: Dict[int, EDDNRegion] = {}

    for row in rows:
        ri = EDDNRegion(
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9] == b'\x01'
        )

        regions[ri.name.lower()] = ri

        if ri.region_address is not None:
            regionaddrs[ri.region_address] = ri

    timer.time('loadregion', len(rows))

    return (regions, regionaddrs)


def loadfactions(conn: DBConnection,
                 timer: Timer
                 ) -> Dict[str, List[EDDNFaction]]:
    sys.stderr.write('Loading Factions\n')
    rows = sqlqueries.get_factions(conn, None)
    timer.time('sqlfactions')
    factions: Dict[str, List[EDDNFaction]] = {}

    for row in rows:
        fi = EDDNFaction(row[0], row[1], row[2], row[3])

        faction_entry = factions.get(fi.name)

        if faction_entry is None:
            factions[fi.name] = [fi]
        else:
            faction_entry.append(fi)

    timer.time('loadfactions')

    return factions


def loadknownbodies(conn: DBConnection,
                    timer: Timer,
                    bodydesigs: Dict[str, Tuple[int, BodyDesignation]],
                    known_bodies_sheet_uri: str
                    ) -> Dict[str, Dict[str, List[KnownBody]]]:
    sys.stderr.write('Loading Known Bodies\n')
    knownbodies: Dict[str, Dict[str, List[KnownBody]]] = {}

    with urllib.request.urlopen(known_bodies_sheet_uri) as f:
        line: bytes

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

                desigid, _ = get_body_designation(conn, bodydesigs, desig)

                if desigid is not None:
                    if sysname not in knownbodies:
                        knownbodies[sysname] = {}

                    sysknownbodies = knownbodies[sysname]

                    if bodyname not in sysknownbodies:
                        sysknownbodies[bodyname] = []

                    sysknownbodies[bodyname].append(
                        {
                            'SystemAddress': sysaddr,
                            'SystemName': sysname,
                            'BodyID': bodyid,
                            'BodyName': bodyname,
                            'BodyDesignation': bodydesig,
                            'BodyDesignationId': desigid
                        }
                    )
                else:
                    import pdb
                    pdb.set_trace()

    timer.time('loadknownbodies')
    return knownbodies
