import sys
import math
from typing import Iterable, Optional, Set, \
                   Tuple, List, Dict, Union, Sequence

from . import edtslookup
from .types import EDDNSystem, EDDNRegion, RejectData
from .timer import Timer
from . import constants
from .util import id64_to_modsysaddr, modsysaddr_to_id64, from_db_string
from . import sqlqueries
from .database import DBConnection


def findsystem(conn: DBConnection,
               cursor: Union[Sequence[EDDNSystem],
                             Sequence[Sequence]],
               sysname: str,
               starpos: Union[Tuple[float, float, float],
                              List[float],
                              None],
               sysaddr: Optional[int],
               syslist: Set[EDDNSystem]
               ) -> Optional[EDDNSystem]:
    rows = list(cursor)
    systems: Set[EDDNSystem] = set()

    for row in rows:
        if isinstance(row, EDDNSystem):
            systems.add(row)
        else:
            systems.add(
                EDDNSystem(
                    row[0],
                    row[1],
                    from_db_string(row[2]),
                    row[3] / 32.0 - 49985,
                    row[4] / 32.0 - 40985,
                    row[5] / 32.0 - 24105,
                    row[3] != 0 and row[4] != 0 and row[5] != 0
                )
            )

    if starpos is not None or sysaddr is not None:
        matches: Set[EDDNSystem] = set()
        for system in systems:
            if ((sysname is None or system.name.lower() == sysname.lower())
                    and (starpos is None or not system.has_coords
                         or (system.x == starpos[0]
                             and system.y == starpos[1]
                             and system.z == starpos[2]))
                    and (sysaddr is None or sysaddr == system.id64)):
                matches.add(system)

        if len(matches) == 1:
            system = next(iter(matches))

            if not system.has_coords and starpos is not None:
                vx = int((starpos[0] + 49985) * 32)
                vy = int((starpos[1] + 40985) * 32)
                vz = int((starpos[2] + 24105) * 32)

                sqlqueries.set_system_coords(
                    conn,
                    (vx, vy, vz, system.id)
                )

                system = system._replace(
                    x=starpos[0],
                    y=starpos[1],
                    z=starpos[2],
                    has_coords=True
                )

            return system

    syslist |= set(systems)
    return None


def findsystemsbyname(conn: DBConnection,
                      namedsystems: Dict[str, List[EDDNSystem]],
                      regions: Dict[str, EDDNRegion],
                      sysname: str
                      ) -> List[EDDNSystem]:
    systems: List[EDDNSystem] = []

    if sysname in namedsystems:
        systems = namedsystems[sysname]
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

        if regionname.lower() in regions:
            region_info = regions[regionname.lower()]
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

                modsysaddr = ((z2 << 53)
                              | (y2 << 47)
                              | (x2 << 40)
                              | (sz << 37)
                              | (z1 << 30)
                              | (y1 << 23)
                              | (x1 << 16)
                              | seq)

            elif region_info.region_address is not None:
                modsysaddr = ((region_info.region_address << 40)
                              | (sz << 37)
                              | (mid << 16)
                              | seq)

            if modsysaddr is not None:
                rows = sqlqueries.get_systems_by_modsysaddr(
                    conn,
                    (modsysaddr,)
                )

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


def getrejectdata(regions: Dict[str, EDDNRegion],
                  regionaddrs: Dict[int, EDDNRegion],
                  sysname: str,
                  sysaddr: Optional[int],
                  systems: Optional[Iterable[EDDNSystem]]
                  ):
    pgsysmatch = constants.procgen_sysname_re.match(sysname)
    rejectdata: RejectData = {
        'id64name': None,
        'nameid64': None,
        'systems': []
    }

    if sysaddr is not None:
        rejectdata_add_id64name(
            regionaddrs,
            sysaddr,
            rejectdata
        )

    if pgsysmatch:
        rejectdata_add_nameid64(
            regions,
            pgsysmatch,
            rejectdata
        )

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


def rejectdata_add_nameid64(regions: Dict[str, EDDNRegion],
                            pgsysmatch: Sequence[str],
                            rejectdata
                            ):
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

    if regionname.lower() in regions:
        region_info = regions[regionname.lower()]
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
            modsysaddr = ((z2 << 53)
                          | (y2 << 47)
                          | (x2 << 40)
                          | (sz << 37)
                          | (z1 << 30)
                          | (y1 << 23)
                          | (x1 << 16)
                          | seq)

            rejectdata['nameid64'] = modsysaddr_to_id64(modsysaddr)
        elif region_info.region_address is not None:
            modsysaddr = ((region_info.region_address << 40)
                          | (sz << 37)
                          | (mid << 16)
                          | seq)

            rejectdata['nameid64'] = modsysaddr_to_id64(modsysaddr)


def rejectdata_add_id64name(regionaddrs: Dict[int, EDDNRegion],
                            sysaddr: int,
                            rejectdata: RejectData):
    modsysaddr = id64_to_modsysaddr(sysaddr)
    regionaddr = modsysaddr >> 40
    if regionaddr in regionaddrs:
        region_info = regionaddrs[regionaddr]
        masscode = chr(((modsysaddr >> 37) & 7) + 97)
        seq = int(modsysaddr & 65535)
        mid = (modsysaddr >> 16) & 2097151
        mid1a = chr((mid % 26) + 65)
        mid1b = chr(((mid // 26) % 26) + 65)
        mid2 = chr(((mid // (26 * 26)) % 26) + 65)
        mid3 = mid // (26 * 26 * 26)
        mid3s = '' if mid3 == 0 else str(mid3) + '-'
        rejectdata['id64name'] = '{0} {1}{2}-{3} {4}{5}{6}'.format(
                region_info.name,
                mid1a,
                mid1b,
                mid2,
                masscode,
                mid3s,
                seq
            )


def getsystem(conn: DBConnection,
              timer: Timer,
              sysname: str,
              x: Optional[float],
              y: Optional[float],
              z: Optional[float],
              sysaddr: Optional[int],
              namedsystems: Dict[str, List[EDDNSystem]],
              regions: Dict[str, EDDNRegion],
              regionaddrs: Dict[int, EDDNRegion],
              ) -> Union[Tuple[EDDNSystem, None, None],
                         Tuple[None, str, dict]]:
    starpos: Optional[List[float]]

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

    if sysname in namedsystems:
        namedsystemlist = namedsystems[sysname]

        system = findsystem(
            conn,
            namedsystemlist,
            sysname,
            starpos,
            sysaddr,
            systems
        )

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

        if regionname.lower() in regions:
            region_info = regions[regionname.lower()]
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
                modsysaddr = ((z2 << 53)
                              | (y2 << 47)
                              | (x2 << 40)
                              | (sz << 37)
                              | (z1 << 30)
                              | (y1 << 23)
                              | (x1 << 16)
                              | seq)

            elif region_info.region_address is not None:
                modsysaddr = ((region_info.region_address << 40)
                              | (sz << 37)
                              | (mid << 16)
                              | seq)

            timer.time('sysquerypgre')

            if modsysaddr is not None:
                rows = sqlqueries.get_systems_by_modsysaddr(
                    conn,
                    (modsysaddr,)
                )

                system = findsystem(
                    conn,
                    rows,
                    sysname,
                    starpos,
                    sysaddr,
                    systems
                )

                timer.time('sysselectmaddr')

                if system is not None:
                    return (system, None, None)
            else:
                errmsg = ('Unable to resolve system address for system '
                          f'{sysname} [{sysaddr}] ({x},{y},{z})\n')
                sys.stderr.write(errmsg)
                sys.stderr.writelines([f'{s}\n' for s in systems])

                return (
                    None,
                    errmsg,
                    getrejectdata(
                        regions,
                        regionaddrs,
                        sysname,
                        sysaddr,
                        systems
                    )
                )
                # raise ValueError('Unable to resolve system address')
        else:
            errmsg = (f'Region {regionname} not found for system '
                      f'{sysname} [{sysaddr}] ({x},{y},{z})\n')
            sys.stderr.write(errmsg)
            sys.stderr.writelines([f'{s}\n' for s in systems])

            return (
                None,
                errmsg,
                getrejectdata(
                    regions,
                    regionaddrs,
                    sysname,
                    sysaddr,
                    systems
                )
            )
            # raise ValueError('Region not found')

    if sysaddr is not None:
        modsysaddr = id64_to_modsysaddr(sysaddr)

        rows = sqlqueries.get_systems_by_modsysaddr(
            conn,
            (modsysaddr,)
        )

        system = findsystem(
            conn,
            rows,
            sysname,
            starpos,
            sysaddr,
            systems
        )

        timer.time('sysselectmaddr')

        if system is not None:
            return (system, None, None)

    rows = sqlqueries.get_systems_by_name(
        conn,
        (sysname,)
    )

    system = findsystem(
        conn,
        rows,
        sysname,
        starpos,
        sysaddr,
        systems
    )

    timer.time('sysselectname')

    if system is not None:
        return (system, None, None)

    timer.time('sysquery', 0)
    edtsid64 = edtslookup.find_edts_system_id64(
        sysname,
        sysaddr,
        starpos
    )

    if edtsid64 is not None:
        timer.time('sysqueryedts', 0)
        modsysaddr = id64_to_modsysaddr(edtsid64)
        rows = sqlqueries.get_systems_by_modsysaddr(
            conn,
            (modsysaddr,)
        )

        system = findsystem(
            conn,
            rows,
            sysname,
            starpos,
            sysaddr,
            systems
        )

        timer.time('sysselectmaddr')

        if system is not None:
            timer.time('sysqueryedts')
            return (system, None, None)

    timer.time('sysqueryedts', 0)
    rows = sqlqueries.get_systems_by_name(
        conn,
        (sysname,)
    )

    system = findsystem(
        conn,
        rows,
        sysname,
        starpos,
        None,
        systems
    )

    timer.time('sysselectname')

    if system is not None:
        return (system, None, None)

    # if starpos is None:
    #    import pdb; pdb.set_trace()

    if region_info is not None and modsysaddr is not None:
        raddr = (((vz // 40960) << 13)
                 | ((vy // 40960) << 7)
                 | (vx // 40960))

        if starpos is None or raddr == modsysaddr >> 40:
            sysid = sqlqueries.insert_system(
                conn,
                (
                    modsysaddr,
                    vx,
                    vy,
                    vz,
                    region_info.is_sphere_sector,
                    0
                )
            )

            if region_info.is_sphere_sector:
                sqlqueries.insert_sphere_sector_system(
                    conn,
                    (
                        sysid,
                        modsysaddr,
                        region_info.id,
                        mid1a,
                        mid1b,
                        mid2,
                        sz,
                        mid3,
                        seq
                    )
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
        raddr = (((vz // 40960) << 13)
                 | ((vy // 40960) << 7)
                 | (vx // 40960))

        if starpos is None or raddr == modsysaddr >> 40:
            sysid = sqlqueries.insert_system(
                conn,
                (modsysaddr, vx, vy, vz, 0, 1)
            )

            sqlqueries.insert_named_system(conn, (sysid, sysname))
            sqlqueries.set_system_invalid(conn, (sysid,))

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

    raddr = (((vz // 40960) << 13)
             | ((vy // 40960) << 7)
             | (vx // 40960))

    if starpos is not None:
        for mc in range(0, 8):
            rx = (vx % 40960) >> mc
            ry = (vy % 40960) >> mc
            rz = (vz % 40960) >> mc

            baddr = ((raddr << 40)
                     | (mc << 37)
                     | (rz << 30)
                     | (ry << 23)
                     | (rx << 16))

            rows = sqlqueries.find_systems_in_boxel(
                conn,
                (baddr, baddr + 65536)
            )

            for row in rows:
                if (vx - 2 <= row[3] <= vx + 2
                        and vy - 2 <= row[4] <= vy + 2
                        and vz - 2 <= row[5] <= vz + 2):
                    systems.add(
                        EDDNSystem(
                            row[0],
                            row[1],
                            from_db_string(row[2]),
                            row[3] / 32.0 - 49985,
                            row[4] / 32.0 - 40985,
                            row[5] / 32.0 - 24105,
                            row[3] != 0 and row[4] != 0 and row[5] != 0
                        )
                    )

    timer.time('sysselectmaddr')

    errmsg = ('Unable to resolve system '
              f'{sysname} [{sysaddr}] ({x},{y},{z})\n')
    # sys.stderr.write(errmsg)
    # sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
    # raise ValueError('Unable to find system')
    return (
        None,
        errmsg,
        getrejectdata(
            regions,
            regionaddrs,
            sysname,
            sysaddr,
            systems
        )
    )
