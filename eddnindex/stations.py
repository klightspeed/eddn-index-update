import math
from datetime import datetime, timedelta
from typing import Any, Tuple, List, Dict, Union

from .types import EDDNSystem, EDDNStation
from .timer import Timer
from . import constants
from . import sqlqueries
from .database import DBConnection


surface_settlement_types = [
    'SurfaceStation',
    'CraterOutpost',
    'CraterPort',
    'OnFootSettlement'
]


def updatestation(conn: DBConnection, station: EDDNStation, **kwargs):
    station = station._replace(**kwargs)

    sqlqueries.update_station(
        conn,
        (
            station.market_id,
            station.system_id,
            station.type,
            station.body,
            station.bodyid,
            station.id
        )
    )

    return station


def getstation(conn: DBConnection,
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

    if stationtype not in surface_settlement_types or bodyid is None:
        bodyname = None

    if bodytype is not None and bodytype == '':
        bodytype = None

    if marketid is not None and marketid == 0:
        marketid = None

    if (stationtype in ['FleetCarrier']
            or constants.carrier_name_re.match(name)):
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

    rows = sqlqueries.find_stations(conn, (sysname, name))

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
        replace: Dict[str, Any] = {}

        if marketid is not None:
            if station.market_id not in [None, marketid]:
                continue
            else:
                replace['marketid'] = marketid

        if sysid is not None:
            if station.system_id not in [None, sysid]:
                continue
            else:
                replace['systemid'] = sysid

        if stationtype is not None:
            if station.type not in [None, stationtype]:
                continue
            else:
                replace['type'] = stationtype

        if (bodyname is not None
                and ((bodytype is None and bodyname != name)
                     or bodytype == 'Planet')):
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
        bidcandidates = [c for c in candidates if c[0].bodyid is not None]
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
            if not c[0].is_rejected
            and c[0].valid_from <= timestamp < c[0].valid_until
        ]

    if len(candidates) == 2:
        c0 = candidates[0][0]
        c1 = candidates[1][0]

        if (c0.valid_from > c1.valid_from
                and c0.valid_until < c1.valid_until):
            candidates = [candidates[0]]
        elif (c1.valid_from > c0.valid_from
                and c1.valid_until < c0.valid_until):
            candidates = [candidates[1]]
        elif c0.valid_until == c1.valid_from + timedelta(hours=15):
            if timestamp < c0.valid_until - timedelta(hours=13):
                candidates = [candidates[0]]
            else:
                candidates = [candidates[1]]
        elif c1.valid_until == c0.valid_from + timedelta(hours=15):
            if timestamp < c1.valid_until - timedelta(hours=13):
                candidates = [candidates[1]]
            else:
                candidates = [candidates[0]]

    if len(candidates) == 1:
        station, replace = candidates[0]

        if len(replace) != 0:
            station = updatestation(conn, station, **replace)

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
                    'bodyId': s.bodyid,
                    'isRejected': True if s.is_rejected else False,
                    'validFrom': s.valid_from.isoformat(),
                    'validUntil': s.valid_until.isoformat(),
                    'test': True if s.test else False
                },
                'replace': r
            } for s, r in candidates])

    if (bodyname is not None
            and not ((bodytype is None and bodyname != name)
                     or bodytype == 'Planet')):
        bodyname = None

    validfrom = constants.timestamp_base_date
    validuntil = constants.timestamp_max_date

    if stationtype is not None:
        if stationtype == 'SurfaceStation':
            validuntil = constants.ed_3_3_0_date
        elif ((marketid is not None and marketid >= 3789600000)
                or stationtype == 'OnFootSettlement'):
            validfrom = constants.ed_4_0_0_date
        elif ((marketid is not None and marketid >= 3700000000)
                or stationtype == 'FleetCarrier'):
            validfrom = constants.ed_3_7_0_date
        elif stationtype in ['CraterPort', 'CraterOutpost']:
            validfrom = constants.ed_3_3_0_date
        elif stationtype == 'Ocellus':
            validfrom = constants.ed_3_3_2_date
            stationtype_location = 'Bernal'
        elif (stationtype == 'Bernal'
                and timestamp < constants.ed_3_3_2_date):
            validuntil = constants.ed_3_3_2_date
        elif (stationtype == 'Megaship'
                and marketid is not None
                and marketid >= 3400000000):
            since_week0 = timestamp - constants.megaship_week_0
            seconds = since_week0.total_seconds()
            weeks = math.floor(seconds / 86400 / 7)
            validfrom = constants.megaship_week_0 + timedelta(
                weeks=weeks,
                hours=-2
            )
            validuntil = validfrom + timedelta(days=7, hours=15)

    if ((sysid is None and sysname != '')
            or stationtype is None
            or marketid is None):
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
        conn,
        (
            marketid,
            name,
            sysname,
            sysid,
            stationtype,
            stationtype_location,
            bodyname,
            bodyid,
            validfrom,
            validuntil,
            test
        )
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
