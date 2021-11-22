import math
from typing import Iterable, MutableSet, NamedTuple, Optional, \
                   Tuple, TypedDict, Union, Sequence
from collections.abc import MutableSequence as List, \
                            MutableMapping as Dict

from . import edtslookup
from .types import EDDNSystem, EDDNRegion
from .timer import Timer
from . import constants
from .util import id64_to_modsysaddr, modsysaddr_to_id64, from_db_string
from . import sqlqueries
from .database import DBConnection


class RejectDataSystem(TypedDict):
    id: int
    id64: int
    x: float
    y: float
    z: float
    name: str


class RejectData(TypedDict):
    nameid64: Optional[int]
    nameid64_error: Optional[str]
    id64name: Optional[str]
    id64name_error: Optional[str]
    systems: List[RejectDataSystem]


class PGSysInfo(NamedTuple):
    region_info: EDDNRegion
    c1: int
    c2: int
    c3: int
    masscode: int
    n1: int
    n2: int


def findsystem(conn: DBConnection,
               cursor: Union[Sequence[EDDNSystem],
                             Sequence[Sequence]],
               sysname: str,
               starpos: Union[Tuple[float, float, float],
                              Tuple[float, float, float],
                              None],
               sysaddr: Optional[int],
               syslist: MutableSet[EDDNSystem]
               ) -> Optional[EDDNSystem]:
    rows = list(cursor)
    systems: MutableSet[EDDNSystem] = set()

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
        matches: MutableSet[EDDNSystem] = set()
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
    modsysaddr: Optional[int] = None

    systems: List[EDDNSystem] = namedsystems.get(sysname) or []
    systems = [s for s in systems]

    _, modsysaddr, _, _ = pgname_to_modsysaddr(regions, sysname)

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
    rejectdata: RejectData = {
        'id64name': None,
        'id64name_error': None,
        'nameid64': None,
        'nameid64_error': None,
        'systems': []
    }

    if sysaddr is not None:
        pgname, errmsg = id64_to_pgname(regionaddrs, sysaddr)
        rejectdata['id64name'] = pgname
        rejectdata['id64name_error'] = errmsg

    ispgname, modsysaddr, _, errmsg = pgname_to_modsysaddr(
        regions,
        sysname
    )

    if ispgname:
        if modsysaddr is not None:
            rejectdata['nameid64'] = modsysaddr_to_id64(modsysaddr)
        else:
            rejectdata['nameid64_error'] = errmsg

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


def pgname_to_modsysaddr(regions: Dict[str, EDDNRegion],
                         sysname: str
                         ) -> Union[Tuple[bool, int, PGSysInfo, None],
                                    Tuple[bool, None, None, str],
                                    Tuple[bool, None, None, None]]:
    pgsysmatch = constants.procgen_sysname_re.match(sysname)

    if pgsysmatch:
        regionname: str = pgsysmatch[1]
        mid1_2: str = pgsysmatch[2].upper()
        masscodestr: str = pgsysmatch[3].lower()
        n1 = int(pgsysmatch[4] or "0")
        n2 = int(pgsysmatch[5])
        c1 = ord(mid1_2[0]) - 65
        c2 = ord(mid1_2[1]) - 65
        c3 = ord(mid1_2[3]) - 65
        mid = (((n1 * 26 + c3) * 26 + c2) * 26 + c1)
        masscode = ord(masscodestr) - 97
        sx = 7 - masscode
        sp = 320 << masscode
        sb = 0x7F >> masscode

        region_info = regions.get(regionname.lower())
        if region_info is not None:
            pginfo = PGSysInfo(region_info, c1, c2, c3, masscode, n1, n2)

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
                              | (masscode << 37)
                              | (z1 << 30)
                              | (y1 << 23)
                              | (x1 << 16)
                              | n2)
                return (True, modsysaddr, pginfo, None)
            elif region_info.region_address is not None:
                modsysaddr = ((region_info.region_address << 40)
                              | (masscode << 37)
                              | (mid << 16)
                              | n2)
                return (True, modsysaddr, pginfo, None)
            else:
                errmsg = f'Region {regionname} is corrupt'
                return (True, None, None, errmsg)
        else:
            errmsg = f'Region {regionname} not found'
            return (True, None, None, errmsg)

    return (False, None, None, None)


def id64_to_pgname(regionaddrs: Dict[int, EDDNRegion],
                   id64: int
                   ) -> Union[Tuple[str, None],
                              Tuple[None, str]]:
    modsysaddr = id64_to_modsysaddr(id64)
    regionaddr = modsysaddr >> 40
    region_info = regionaddrs.get(regionaddr)

    if region_info is not None:
        region = region_info.name
        mc = chr(((modsysaddr >> 37) & 7) + 97)
        mid = (modsysaddr >> 16) & 2097151
        c1 = chr((mid % 26) + 65)
        c2 = chr(((mid // 26) % 26) + 65)
        c3 = chr(((mid // (26 * 26)) % 26) + 65)
        n1 = mid // (26 * 26 * 26)
        n1s = '' if n1 == 0 else str(n1) + '-'
        n2 = int(modsysaddr & 65535)
        return (f'{region} {c1}{c2}-{c3} {mc}{n1s}{n2}', None)
    else:
        errmsg = f'Region Address {regionaddr} not found'
        return (None, errmsg)


def find_named_system(conn: DBConnection,
                      sysname: str,
                      starpos: Optional[Tuple[float, float, float]],
                      sysaddr: Optional[int],
                      namedsystems: Dict[str, List[EDDNSystem]],
                      systems: MutableSet[EDDNSystem]
                      ):
    namedsystemlist = namedsystems.get(sysname)

    if namedsystemlist is not None:
        return findsystem(
            conn, namedsystemlist, sysname, starpos, sysaddr, systems
        )
    else:
        return None


def add_system(conn: DBConnection,
               namedsystems: Dict[str, List[EDDNSystem]],
               sysname: str,
               starpos: Optional[Tuple[float, float, float]],
               modsysaddr: Optional[int],
               pginfo: Optional[PGSysInfo],
               region_info: Optional[EDDNRegion]
               ):
    if starpos is not None:
        vx = int((starpos[0] + 49985) * 32)
        vy = int((starpos[1] + 40985) * 32)
        vz = int((starpos[2] + 24105) * 32)
        raddr = (((vz // 40960) << 13)
                 | ((vy // 40960) << 7)
                 | (vx // 40960))
    else:
        vx = 0
        vy = 0
        vz = 0
        raddr = 0

    if (region_info is not None and modsysaddr is not None
            and (starpos is None or raddr == modsysaddr >> 40)):
        sysid = sqlqueries.insert_system(
            conn,
            (
                modsysaddr,
                vx,
                vy,
                vz,
                region_info.is_sphere_sector,
                0 if pginfo is not None else 1
            )
        )

        if region_info.is_sphere_sector and pginfo is not None:
            sqlqueries.insert_sphere_sector_system(
                conn,
                (
                    sysid,
                    modsysaddr,
                    pginfo.region_info.id,
                    pginfo.c1,
                    pginfo.c2,
                    pginfo.c3,
                    pginfo.masscode,
                    pginfo.n1,
                    pginfo.n2
                )
            )

        if starpos is not None:
            system = EDDNSystem(
                sysid,
                modsysaddr_to_id64(modsysaddr),
                sysname,
                starpos[0],
                starpos[1],
                starpos[2],
                True
            )
        else:
            system = EDDNSystem(
                sysid,
                modsysaddr_to_id64(modsysaddr),
                sysname,
                -49985,
                -40985,
                -24105,
                False
            )

        if pginfo is None:
            sqlqueries.insert_named_system(
                conn,
                (sysid, sysname)
            )

            sqlqueries.set_system_invalid(
                conn,
                (sysid, 1)
            )

            namedsystemlist = namedsystems.get(sysname)

            if namedsystemlist is None:
                namedsystems[sysname] = [system]
            else:
                namedsystemlist.append(system)

        return system
    else:
        return None


def find_candidates(conn: DBConnection,
                    timer: Timer,
                    starpos: Optional[Tuple[float, float, float]],
                    systems: MutableSet[EDDNSystem]
                    ):
    if starpos is not None:
        vx = int((starpos[0] + 49985) * 32)
        vy = int((starpos[1] + 40985) * 32)
        vz = int((starpos[2] + 24105) * 32)
        raddr = (((vz // 40960) << 13)
                 | ((vy // 40960) << 7)
                 | (vx // 40960))

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


def find_system_by_name(conn: DBConnection,
                        timer: Timer,
                        sysname: str,
                        sysaddr: Optional[int],
                        starpos: Optional[Tuple[float, float, float]],
                        systems: MutableSet[EDDNSystem]
                        ) -> Optional[EDDNSystem]:
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

    return system


def find_system_by_modsysaddr(conn: DBConnection,
                              timer: Timer,
                              sysname: str,
                              sysaddr: Optional[int],
                              starpos: Optional[Tuple[float, float, float]],
                              systems: MutableSet[EDDNSystem],
                              modsysaddr: int
                              ) -> Optional[EDDNSystem]:
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

    return system


def find_system(conn: DBConnection,
                timer: Timer,
                sysname: str,
                starpos: Optional[Tuple[float, float, float]],
                sysaddr: Optional[int],
                namedsystems: Dict[str, List[EDDNSystem]],
                regions: Dict[str, EDDNRegion],
                regionaddrs: Dict[int, EDDNRegion]
                ) -> Tuple[Optional[EDDNSystem],
                           Optional[str],
                           MutableSet[EDDNSystem]]:
    pginfo = None
    region_info = None
    errmsg = None
    systems: MutableSet[EDDNSystem] = set()

    system = find_named_system(
        conn, sysname, starpos, sysaddr, namedsystems, systems
    )

    timer.time('sysquery', 0)

    if system is None:
        ispgname, modsysaddr, pginfo, errmsg = pgname_to_modsysaddr(
            regions, sysname
        )

        if ispgname and modsysaddr is not None and pginfo is not None:
            region_info = pginfo.region_info

            system = find_system_by_modsysaddr(
                conn, timer, sysname, sysaddr, starpos, systems, modsysaddr
            )

    if system is None and errmsg is None and sysaddr is not None:
        modsysaddr = id64_to_modsysaddr(sysaddr)

        system = find_system_by_modsysaddr(
            conn, timer, sysname, sysaddr, starpos, systems, modsysaddr
        )

    if system is None and errmsg is None:
        system = find_system_by_name(
            conn, timer, sysname, sysaddr, starpos, systems
        )

    if system is None and errmsg is None:
        timer.time('sysquery', 0)
        edtsid64 = edtslookup.find_edts_system_id64(
            sysname,
            sysaddr,
            starpos
        )

        if edtsid64 is not None:
            timer.time('sysqueryedts', 0)
            edtsmodsysaddr = id64_to_modsysaddr(edtsid64)
            system = find_system_by_modsysaddr(
                conn, timer, sysname, sysaddr,
                starpos, systems, edtsmodsysaddr
            )

            if system is not None:
                timer.time('sysqueryedts')

    if system is None and errmsg is None:
        system = find_system_by_name(
            conn, timer, sysname, None, starpos, systems
        )

    if system is None and errmsg is None:
        if region_info is None and modsysaddr is not None:
            region_info = regionaddrs.get(modsysaddr >> 40)

        system = add_system(
            conn, namedsystems, sysname, starpos,
            modsysaddr, pginfo, region_info
        )

    if system is None and errmsg is None:
        find_candidates(conn, timer, starpos, systems)

        errmsg = 'Unable to resolve system'

    return (system, errmsg, systems)


def getsystem(conn: DBConnection,
              timer: Timer,
              sysname: str,
              x: Optional[float],
              y: Optional[float],
              z: Optional[float],
              sysaddr: Optional[int],
              namedsystems: Dict[str, List[EDDNSystem]],
              regions: Dict[str, EDDNRegion],
              regionaddrs: Dict[int, EDDNRegion]
              ) -> Union[Tuple[EDDNSystem, None, None],
                         Tuple[None, str, dict]]:
    starpos: Optional[Tuple[float, float, float]]

    if x is not None and y is not None and z is not None:
        starpos = (
            math.floor(x * 32 + 0.5) / 32.0,
            math.floor(y * 32 + 0.5) / 32.0,
            math.floor(z * 32 + 0.5) / 32.0
        )
    else:
        starpos = None

    system, errmsg, systems = find_system(
        conn, timer, sysname, starpos, sysaddr,
        namedsystems, regions, regionaddrs
    )

    if system is not None:
        return (system, None, None)
    else:
        errmsg = f'{errmsg} {sysname} [{sysaddr}] ({x},{y},{z})\n'
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
