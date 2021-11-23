from datetime import datetime
from typing import Any, Optional, Tuple
from collections.abc import MutableMapping as Dict, \
                            MutableSequence as List

from .types import EDDNRegion, EDDNSystem, EDDNBody, KnownBody, BodyDesignation
from .timer import Timer
from . import constants
from . import sqlqueries
from .database import DBConnection
from .systems import findsystemsbyname


body_categories: List[str] = [
    'Unknown',
    'StellarBarycentre',
    'StellarBody',
    'Belt',
    'AsteroidCluster',
    'PlanetaryBarycentre',
    'PlanetaryBody',
    'PlanetaryRing',
    'Moon1Barycentre',
    'Moon1',
    'Moon1Ring',
    'Moon2Barycentre',
    'Moon2',
    'Moon2Ring',
    'Moon3',
    'Comet',
    'PlanetComet',
    'Moon1Comet',
    'Moon2Comet',
    'Nebula'
]


def make_body_desig_moon2(desig: BodyDesignation,
                          parts: List[str]
                          ):
    parts.append(' ')
    parts.append(chr(96 + desig.Moon2))
    if desig.BodyCategory == 11:
        for i in range(1, desig.Moon3):
            parts.append('+')
            parts.append(chr(96 + desig.Moon2 + i))
    elif desig.BodyCategory == 10:
        parts.append(' ')
        parts.append(chr(64 + desig.Moon3))
        parts.append(' Ring')
    elif desig.BodyCategory == 18:
        parts.append(' Comet ')
        parts.append(str(desig.Moon3))
    elif desig.Moon3 != 0:
        parts.append(' ')
        parts.append(chr(96 + desig.Moon3))


def make_body_desig_moon1(desig: BodyDesignation,
                          parts: List[str]
                          ):
    parts.append(' ')
    parts.append(chr(96 + desig.Moon1))
    if desig.BodyCategory == 8:
        for i in range(1, desig.Moon2):
            parts.append('+')
            parts.append(chr(96 + desig.Moon1 + i))
    elif desig.BodyCategory == 10:
        parts.append(' ')
        parts.append(chr(64 + desig.Moon2))
        parts.append(' Ring')
    elif desig.BodyCategory == 17:
        parts.append(' Comet ')
        parts.append(str(desig.Moon2))
    elif desig.BodyCategory >= 11 and desig.Moon2 != 0:
        make_body_desig_moon2(desig, parts)


def make_body_desig_planet(desig: BodyDesignation,
                           parts: List[str]
                           ):
    parts.append(' ')
    parts.append(str(desig.Planet))
    if desig.BodyCategory == 5:
        for i in range(1, desig.Moon1):
            parts.append('+')
            parts.append(str(desig.Planet + i))
    elif desig.BodyCategory == 7:
        parts.append(' ')
        parts.append(chr(64 + desig.Moon1))
        parts.append(' Ring')
    elif desig.BodyCategory == 16:
        parts.append(' Comet ')
        parts.append(str(desig.Moon1))
    elif desig.BodyCategory >= 8 and desig.Moon1 != 0:
        make_body_desig_moon1(desig, parts)


def make_body_designation(desig: BodyDesignation) -> str:
    parts = []

    if desig.Stars != 0:
        parts.append(' ')
        for i in range(0, 16):
            if desig.Stars & (1 << i):
                parts.append(chr(65 + i))

    if desig.BodyCategory == 19:
        parts.append(' Nebula')
    elif desig.BodyCategory in (3, 4):
        parts.append(' ')
        parts.append(chr(desig.Planet + 64))
        parts.append(' Belt')
        if desig.Moon1 != 0:
            parts.append(' Cluster ')
            parts.append(str(desig.Moon1))
    elif desig.BodyCategory == 15:
        parts.append(' Comet ')
        parts.append(str(desig.Planet))
    elif desig.BodyCategory >= 5 and desig.Planet != 0:
        make_body_desig_planet(desig, parts)

    return ''.join(parts)


def extract_planet(stars: str,
                   nebula: str,
                   belt: str,
                   cluster: str,
                   stellarcomet: str,
                   planetstr: str
                   ):
    moon1 = 0
    planet = 0

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

    return planet, moon1, bodycategory


def extract_moon1(ring1: str,
                  comet1: str,
                  moon1str: str
                  ):
    if moon1str is not None:
        if '+' in moon1str:
            moon1list = moon1str.split('+')
            moon1 = ord(moon1list[0]) - 96
            moon2 = ord(moon1list[-1]) - 96 - moon1
            bodycategory = 8
        else:
            moon1 = ord(moon1str) - 96
            moon2 = 0
            bodycategory = 9
    elif ring1 is not None:
        moon1 = ord(ring1) - 64
        moon2 = 0
        bodycategory = 7
    elif comet1 is not None:
        moon1 = int(comet1)
        moon2 = 0
        bodycategory = 16
    else:
        moon1 = 0
        moon2 = 0
        bodycategory = 6

    return moon1, moon2, bodycategory


def extract_moon2(ring2: str,
                  comet2: str,
                  moon2str: str):
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
        moon3 = 0
        bodycategory = 10
    elif comet2 is not None:
        moon2 = int(comet2)
        moon3 = 0
        bodycategory = 17
    else:
        moon2 = 0
        moon3 = 0
        bodycategory = 9

    return moon2, moon3, bodycategory


def extract_moon3(ring3: str,
                  comet3: str,
                  moon3str: str
                  ):
    if moon3str is not None:
        moon3 = ord(moon3str) - 96
        bodycategory = 14
    elif ring3 is not None:
        moon3 = ord(ring3) - 64
        bodycategory = 13
    elif comet3 is not None:
        moon3 = int(comet3)
        bodycategory = 18
    else:
        moon3 = 0
        bodycategory = 12

    return moon3, bodycategory


def split_body_designation(desig: str) -> Optional[BodyDesignation]:
    match = constants.procgen_body_name_re.match(desig)

    if match:
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

        star = 0
        planet = 0
        moon1 = 0
        moon2 = 0
        moon3 = 0

        planet, moon1, bodycategory = extract_planet(
            stars, nebula, belt, cluster, stellarcomet, planetstr
        )

        if bodycategory == 6:
            moon1, moon2, bodycategory = extract_moon1(
                ring1, comet1, moon1str
            )

        if bodycategory == 9:
            moon2, moon3, bodycategory = extract_moon2(
                ring2, comet2, moon2str
            )

        if bodycategory == 12:
            moon3, bodycategory = extract_moon3(
                ring3, comet3, moon3str
            )

        if stars is not None:
            for i in range(ord(stars[0]) - 65, ord(stars[-1]) - 64):
                star |= 1 << i

        return BodyDesignation(
            bodycategory,
            star,
            planet,
            moon1,
            moon2,
            moon3,
            False,
            desig
        )
    else:
        return None


def get_body_designation(conn: DBConnection,
                         bodydesigs: Dict[str, Tuple[int, BodyDesignation]],
                         desig: str):
    if desig in bodydesigs:
        return bodydesigs[desig]

    row = sqlqueries.get_body_designation(
        conn,
        (desig,)
    )

    if row and row[1] == desig:
        desigid = int(row[0])

        bodydesig = BodyDesignation(
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[1]
        )

        bodydesigs[desig] = (
            desigid,
            bodydesig
        )

        sqlqueries.set_body_designation_used(
            conn,
            (desigid,)
        )

        return (desigid, bodydesig)
    else:
        return (None, None)


def get_bodies_by_name(conn: DBConnection,
                       sysid: int,
                       name: str
                       ) -> List[EDDNBody]:

    dbrows = list(sqlqueries.get_bodies_by_name(
        conn,
        (sysid, name, 1)
    ))

    dbrows.append(sqlqueries.get_bodies_by_name(
        conn,
        (sysid, name, 0))
    )

    return [
        EDDNBody(
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
        for row in dbrows
    ]


def add_procgen_body(conn: DBConnection,
                     timer: Timer,
                     name: str,
                     sysname: str,
                     bodyid: Optional[int],
                     system: EDDNSystem,
                     body: Dict[str, Any],
                     desiginfo: BodyDesignation,
                     desigid: int
                     ):
    rowid = sqlqueries.insert_body(
        conn,
        (
            system.id,
            1 if bodyid is not None else 0,
            bodyid or 0,
            desigid,
            0
        )
    )

    timer.time('bodyinsertpg')

    periapsis = body.get('Periapsis')

    bodydata = EDDNBody(
        rowid,
        name,
        sysname,
        system.id,
        bodyid,
        desiginfo.BodyCategory,
        float(periapsis) if periapsis is not None else None,
        constants.timestamp_base_date,
        constants.timestamp_max_date,
        False,
        desigid
    )

    return bodydata


def add_named_body(conn: DBConnection,
                   name: str,
                   sysname: str,
                   bodyid: Optional[int],
                   system: EDDNSystem,
                   desiginfo: BodyDesignation,
                   desigid: int,
                   body: Dict[str, Any]
                   ):
    rowid = sqlqueries.insert_body(
        conn,
        (
            system.id,
            1 if bodyid is not None else 0,
            bodyid or 0,
            desigid
        )
    )

    sqlqueries.insert_named_body(conn, (rowid, system.id, name))

    bodydata = EDDNBody(
        rowid,
        name,
        sysname,
        system.id,
        bodyid,
        desiginfo.BodyCategory,
        body.get('Periapsis'),
        constants.timestamp_base_date,
        constants.timestamp_max_date,
        False,
        desigid
    )

    return bodydata


def get_error_data(conn: DBConnection,
                   name: str,
                   sysname: str,
                   namedsystems: Dict[str, List[EDDNSystem]],
                   regions: Dict[str, EDDNRegion],
                   dbrows: List[EDDNBody]
                   ) -> Tuple[None, str, List[Any]]:
    allrows = list(sqlqueries.get_bodies_by_custom_name(conn, (name,)))
    pgsysbodymatch = constants.procgen_sys_body_name_re.match(name)
    dupsystems: List[EDDNSystem] = []

    if pgsysbodymatch:
        dupsysname = pgsysbodymatch['sysname']
        dupsystems = findsystemsbyname(
            conn,
            namedsystems,
            regions,
            dupsysname
        )

        for dupsystem in dupsystems:
            allrows.extend(sqlqueries.get_system_bodies(
                conn,
                (dupsystem.id, 1)
            ))

            allrows.extend(sqlqueries.get_system_bodies(
                conn,
                (dupsystem.id, 0)
            ))

    frows = [r for r in allrows if r[1].lower() == name.lower()]

    if len(frows) > 0:
        return get_reject_data(dbrows, 'Body in wrong system')
    elif not pgsysbodymatch:
        return (
            None,
            'Unknown named body',
            [{'System': sysname, 'Body': name}]
        )
    elif len(dupsystems) > 0:
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
    else:
        return (
            None,
            'Procgen body in wrong system',
            [{'System': sysname, 'Body': name}]
        )


def filter_bodies(name: str,
                  sysname: str,
                  bodyid: Optional[int],
                  body: Dict[str, Any],
                  timestamp: datetime,
                  rows: List[EDDNBody]
                  ) -> List[EDDNBody]:
    multimatch = len(rows) > 1

    if bodyid is not None:
        rows = [row for row in rows
                if row.bodyid is None or row.bodyid == bodyid]

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
            rows = [row for row in rows
                    if row.arg_of_periapsis is None
                    or ((aop + 5 - row.arg_of_periapsis) % 360) < 10]

    if len(rows) > 1:
        xrows = [row for row in rows
                 if row.valid_from < timestamp < row.valid_until]
        if len(xrows) > 0:
            rows = xrows

    if len(rows) > 1:
        xrows = [row for row in rows if row.is_rejected == 0]
        if len(xrows) > 0:
            rows = xrows

    return rows


def get_desig_id(name: str,
                 sysname: str,
                 bodyid: Optional[int],
                 body: Dict[str, Any],
                 knownbodies: Dict[str, Dict[str, List[KnownBody]]]):
    desigid = None
    ispgname = name.startswith(sysname)

    if name == sysname:
        if 'SemiMajorAxis' in body and body['SemiMajorAxis'] is not None:
            ispgname = False
        elif bodyid is not None and bodyid != 0:
            ispgname = False
        elif 'BodyType' in body and body['BodyType'] != 'Star':
            ispgname = False

    if sysname in knownbodies:
        sysknownbodies = knownbodies[sysname]
        if name in sysknownbodies:
            knownbodylist = knownbodies[sysname][name]
            if bodyid is not None:
                knownbodylist = [row for row in knownbodylist
                                 if row['BodyID'] == bodyid]
            if len(knownbodylist) == 1:
                knownbody = knownbodylist[0]
                if knownbody['BodyDesignation'] != knownbody['BodyName']:
                    ispgname = False
                    desigid = knownbody['BodyDesignationId']

    return ispgname, desigid


def get_reject_data(dbrows: List[EDDNBody],
                    reason: str
                    ) -> Tuple[None, str, List[Any]]:
    return (
        None,
        reason,
        [row._asdict() for row in dbrows]
    )


def getbody(conn: DBConnection,
            timer: Timer,
            name: str,
            sysname: str,
            bodyid: Optional[int],
            system: EDDNSystem,
            body: Dict[str, Any],
            timestamp: datetime,
            namedbodies: Dict[int, Dict[str, List[EDDNBody]]],
            knownbodies: Dict[str, Dict[str, List[KnownBody]]],
            bodydesigs: Dict[str, Tuple[int, BodyDesignation]],
            namedsystems: Dict[str, List[EDDNSystem]],
            regions: Dict[str, EDDNRegion]
            ):
    if system.id in namedbodies and name in namedbodies[system.id]:
        timer.time('bodyquery', 0)
        nrows = namedbodies[system.id][name]

        nrows = filter_bodies(name, sysname, bodyid, body, timestamp, nrows)

        timer.time('bodylookupname')
        if len(nrows) == 1:
            return (nrows[0], None, None)

    ispgname, desigid = get_desig_id(name, sysname, bodyid, body, knownbodies)

    if ispgname:
        timer.time('bodyquery', 0)
        desig = name[len(sysname):]

        desigid, bodydesig = get_body_designation(conn, bodydesigs, desig)

        if desigid is None:
            bodydesig = split_body_designation(desig)

            if bodydesig is not None:
                return (
                    None,
                    'Body designation not in database',
                    [bodydesig._asdict()]
                )

        timer.time('bodyquerypgre')

    timer.time('bodyquery', 0)

    dbrows = get_bodies_by_name(conn, system.id, name)

    timer.time('bodyselectname')

    dbrows = filter_bodies(name, sysname, bodyid, body, timestamp, dbrows)

    timer.time('bodyqueryname')
    if len(dbrows) == 1:
        dbrow = dbrows[0]
        if dbrow[4] is None and bodyid is not None:
            sqlqueries.set_body_bodyid(conn, (bodyid, dbrow[0]))
            timer.time('bodyupdateid')

        return (
            EDDNBody(
                dbrow.id,
                dbrow.name,
                dbrow.system_name,
                dbrow.system_id,
                dbrow.bodyid or bodyid,
                dbrow.category,
                dbrow.arg_of_periapsis or body.get('Periapsis'),
                dbrow.valid_from,
                dbrow.valid_until,
                dbrow.is_rejected,
                dbrow.designation_id
            ),
            None,
            None
        )
    elif len(dbrows) > 1:
        return get_reject_data(dbrows, 'Multiple matches')
    else:
        allrows = list(sqlqueries.get_system_bodies(conn, (system.id, 1)))
        allrows.extend(sqlqueries.get_system_bodies(conn, (system.id, 0)))
        frows = [r for r in allrows if r[1].lower() == name.lower()]

        if bodyid is not None:
            frows = [r for r in frows if r[4] is None or r[4] == bodyid]

        if len(frows) > 0:
            return get_reject_data(dbrows, 'Body Mismatch')

        if ispgname and desigid is not None:
            bodydata = add_procgen_body(
                conn, timer, name, sysname, bodyid,
                system, body, bodydesig, desigid
            )

            return (
                bodydata,
                None,
                None
            )

        if ((not ispgname and constants.procgen_sysname_re.match(name))
                or desigid is None):
            return get_error_data(
                conn, name, sysname, namedsystems, regions, dbrows
            )

        bodydata = add_named_body(
            conn, name, sysname, bodyid, system, bodydesig, desigid, body
        )

        return (
            bodydata,
            None,
            None
        )
