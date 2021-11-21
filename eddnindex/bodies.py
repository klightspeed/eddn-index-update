from datetime import datetime
from typing import Any, Optional
from collections.abc import MutableMapping as Dict, \
                            MutableSequence as List

from .types import EDDNSystem, EDDNBody
from .timer import Timer
from . import constants
from . import sqlqueries
from .database import DBConnection
from .systems import findsystemsbyname


def getbody(conn: DBConnection,
            timer: Timer,
            name: str,
            sysname: str,
            bodyid: Optional[int],
            system: EDDNSystem,
            body: Dict[str, Any],
            timestamp: datetime,
            namedbodies,
            knownbodies,
            bodydesigs,
            namedsystems,
            regions
            ):
    if system.id in namedbodies and name in namedbodies[system.id]:
        timer.time('bodyquery', 0)
        nrows = namedbodies[system.id][name]

        multimatch = len(nrows) > 1

        if bodyid is not None:
            nrows = [row for row in nrows
                     if row.bodyid is None or row.bodyid == bodyid]

        if len(nrows) > 1 and name == sysname:
            if 'PlanetClass' in body:
                nrows = [row for row in nrows if row.category == 6]
            elif 'StarType' in body:
                nrows = [row for row in nrows if row.category == 2]

        if multimatch and 'Periapsis' in body:
            aop = body['Periapsis']
            if len(nrows) == 1 and nrows[0].arg_of_periapsis is None:
                pass
            elif len(nrows) > 1:
                nrows = [row for row in nrows
                         if row.arg_of_periapsis is None
                         or ((aop + 725 - row.arg_of_periapsis) % 360) < 10]

        if len(nrows) > 1:
            nrows = [row for row in nrows
                     if row.valid_from < timestamp < row.valid_until]

        if len(nrows) > 1:
            nrows = [row for row in nrows if row.is_rejected == 0]

        timer.time('bodylookupname')
        if len(nrows) == 1:
            return (nrows[0], None, None)

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

    if ispgname:
        timer.time('bodyquery', 0)
        desig = name[len(sysname):]
        match = constants.procgen_body_name_re.match(desig)

        if desig in bodydesigs:
            desigid, category = bodydesigs[desig]
        else:
            dbrow = sqlqueries.get_body_designation(conn, (desig,))

            if dbrow and dbrow[1] == desig:
                desigid = int(dbrow[0])
                category = int(dbrow[2])
                bodydesigs[desig] = (desigid, category)
                sqlqueries.set_body_designation_used(conn, (desigid,))
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

    dbrows = list(sqlqueries.get_bodies_by_name(
        conn,
        (system.id, name, 1)
    ))

    dbrows.append(sqlqueries.get_bodies_by_name(
        conn,
        (system.id, name, 0))
    )

    timer.time('bodyselectname')

    multimatch = len(dbrows) > 1

    if bodyid is not None:
        dbrows = [row for row in dbrows if row[4] == bodyid or row[4] is None]

    if len(dbrows) > 1 and name == sysname:
        if 'PlanetClass' in body:
            dbrows = [row for row in dbrows if row[5] == 'PlanetaryBody']
        elif 'StarType' in body:
            dbrows = [row for row in dbrows if row[5] == 'StellarBody']

    if multimatch and 'Periapsis' in body:
        aop = body['Periapsis']
        if len(dbrows) == 1 and dbrows[0][6] is None:
            pass
        elif len(dbrows) > 1:
            dbrows = [row for row in dbrows
                      if row[6] is None
                      or ((aop + 725 - row[6]) % 360) < 10]

    if len(dbrows) > 1:
        xrows = [row for row in dbrows if row[7] < timestamp < row[8]]
        if len(xrows) > 0:
            dbrows = xrows

    if len(dbrows) > 1:
        xrows = [row for row in dbrows if row[9]]
        if len(xrows) > 0:
            dbrows = xrows

    timer.time('bodyqueryname')
    if len(dbrows) == 1:
        dbrow = dbrows[0]
        if dbrow[4] is None and bodyid is not None:
            sqlqueries.set_body_bodyid(conn, (bodyid, dbrow[0]))
            timer.time('bodyupdateid')
        return (
            EDDNBody(
                dbrow[0],
                name,
                sysname,
                system.id,
                dbrow[4] or bodyid,
                dbrow[10],
                (body['Periapsis'] if 'Periapsis' in body else None),
                constants.timestamp_base_date,
                constants.timestamp_max_date,
                False,
                dbrow[11]
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
            if sysname in namedsystems:
                systems = namedsystems[sysname]
                for xsystem in systems:
                    allrows.extend(sqlqueries.get_system_bodies(
                        conn,
                        (xsystem.id, 1)
                    ))

                    allrows.extend(sqlqueries.get_system_bodies(
                        conn,
                        (xsystem.id, 0)
                    ))

            return get_reject_data(dbrows, 'Body Mismatch')

        if ispgname and desigid is not None:
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

            return (
                EDDNBody(
                    rowid,
                    name,
                    sysname,
                    system.id,
                    bodyid,
                    category,
                    float(periapsis) if periapsis is not None else None,
                    constants.timestamp_base_date,
                    constants.timestamp_max_date,
                    False,
                    desigid
                ),
                None,
                None
            )

        if ((not ispgname and constants.procgen_sysname_re.match(name))
                or desigid is None):
            allrows = list(sqlqueries.get_bodies_by_custom_name(conn, (name,)))
            pgsysbodymatch = constants.procgen_sys_body_name_re.match(name)
            dupsystems: List[EDDNSystem] = []

            if pgsysbodymatch:
                dupsysname = pgsysbodymatch['sysname']
                dupsystems = findsystemsbyname(
                    conn,
                    namedbodies,
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
                get_reject_data(dbrows, 'Body in wrong system')
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
                return (
                    None,
                    'Unknown named body',
                    [{'System': sysname, 'Body': name}]
                )

        rowid = sqlqueries.insert_body(
            conn,
            (system.id, 1 if bodyid is not None else 0, bodyid or 0, desigid)
        )

        sqlqueries.insert_named_body(conn, (rowid, system.id, name))
        # sqlqueries.set_body_invalid(conn, (rowid,))

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


def get_reject_data(dbrows, reason: str):
    return (
        None,
        reason,
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
        } for row in dbrows]
    )
