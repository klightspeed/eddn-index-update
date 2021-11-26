import math
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Tuple, Union
from collections.abc import MutableSequence as List, \
                            MutableMapping as Dict

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

    sysname, marketid, stationtype, bodyname, bodyid, bodytype, sysid = \
        filter_station_props(
            name, sysname, marketid, timestamp, stationtype,
            bodyname, bodyid, bodytype, eventtype, sysid
        )

    candidates = get_stations(
        conn, name, sysname, marketid, timestamp, stationtype,
        bodyname, bodyid, test, sysid
    )

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
                'Body': bodyname,
                'BodyID': bodyid,
                'IsTest': 1 if test else 0
            }]
        )

    station = add_station(
        conn, name, sysname, marketid, stationtype, bodyname,
        bodyid, test, sysid, timestamp
    )

    return (station, None, None)


def filter_station_props(name: str,
                         sysname: str,
                         marketid: Optional[int],
                         timestamp: datetime,
                         stationtype: Optional[str],
                         bodyname: Optional[str],
                         bodyid: Optional[int],
                         bodytype: Optional[str],
                         eventtype: Optional[str],
                         sysid: Optional[int]
                         ):
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

    if (eventtype is not None
            and eventtype == 'Location'
            and stationtype is not None
            and stationtype == 'Bernal'
            and timestamp > constants.ed_3_3_2_date):
        stationtype = 'Ocellus'

    if (bodyname is not None
            and not ((bodytype is None and bodyname != name)
                     or bodytype == 'Planet')):
        bodyname = None

    return sysname, marketid, stationtype, bodyname, bodyid, bodytype, sysid


def get_stations(conn: DBConnection,
                 name: str,
                 sysname: str,
                 marketid: Optional[int],
                 timestamp: datetime,
                 stationtype: Optional[str],
                 bodyname: Optional[str],
                 bodyid: Optional[int],
                 test: bool,
                 sysid: Optional[int]
                 ) -> List[Tuple[EDDNStation, Dict[str, Any]]]:
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

    candidates = filter_stations(
        marketid, timestamp, stationtype, bodyname,
        bodyid, test, sysid, stations
    )

    return candidates


def add_station(conn: DBConnection,
                name: str,
                sysname: str,
                marketid: int,
                stationtype: str,
                bodyname: Optional[str],
                bodyid: Optional[int],
                test: bool,
                sysid: Optional[int],
                timestamp: datetime
                ) -> EDDNStation:
    stationtype_location, validfrom, validuntil = station_validity(
        marketid, timestamp, stationtype
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

    return EDDNStation(
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
    )


def station_validity(marketid: Optional[int],
                     timestamp: datetime,
                     stationtype: Optional[str]
                     ) -> Tuple[Optional[str], datetime, datetime]:
    validfrom = constants.timestamp_base_date
    validuntil = constants.timestamp_max_date
    stationtype_location = None

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

    return stationtype_location, validfrom, validuntil


def filter_notnull(candidates: List[Tuple[EDDNStation, Dict[str, Any]]],
                   sel: Callable[[EDDNStation], Any]
                   ) -> List[Tuple[EDDNStation, Dict[str, Any]]]:
    if len(candidates) > 1:
        mcandidates = [c for c in candidates if sel(c[0]) is not None]
        if len(candidates) == 1:
            return mcandidates

    return candidates


def filter_nottrue(candidates: List[Tuple[EDDNStation, Dict[str, Any]]],
                   sel: Callable[[EDDNStation], Any]
                   ) -> List[Tuple[EDDNStation, Dict[str, Any]]]:
    if len(candidates) > 1:
        mcandidates = [c for c in candidates if sel(c[0]) is not True]
        if len(candidates) == 1:
            return mcandidates

    return candidates


def get_replace(lines: List[Tuple[str, Any, Any]]
                ) -> Optional[Dict[str, Any]]:
    replace: Dict[str, Any] = {}

    for name, left, right in lines:
        if right is not None:
            if left is not None and left != right:
                return None
            else:
                replace[name] = right

    return replace


def filter_stations(marketid: Optional[int],
                    timestamp: datetime,
                    stationtype: Optional[str],
                    bodyname: Optional[str],
                    bodyid: Optional[int],
                    test: bool,
                    sysid: Optional[int],
                    stations: List[EDDNStation]
                    ) -> List[Tuple[EDDNStation, Dict[str, Any]]]:
    candidates: List[Tuple[EDDNStation, Dict[str, Any]]] = []

    for station in stations:
        replace = get_replace([
            ('marketid', station.market_id, marketid),
            ('systemid', station.system_id, sysid),
            ('type', station.type, stationtype),
            ('bodyname', station.body, bodyname),
            ('bodyid', station.bodyid, bodyid)
        ])

        if replace is not None:
            candidates.append((station, replace))

    if bodyid is not None:
        candidates = filter_notnull(candidates, lambda e: e.bodyid)

    if marketid is not None:
        candidates = filter_notnull(candidates, lambda e: e.market_id)

    if stationtype is not None:
        candidates = filter_notnull(candidates, lambda e: e.type)

    if sysid is not None:
        candidates = filter_notnull(candidates, lambda e: e.system_id)

    if not test:
        candidates = filter_nottrue(candidates, lambda e: e.test)

    candidates = filter_validity(timestamp, stationtype, candidates)

    return candidates


def filter_validity(timestamp: datetime,
                    stationtype: Optional[str],
                    candidates: List[Tuple[EDDNStation, Dict[str, Any]]]
                    ) -> List[Tuple[EDDNStation, Dict[str, Any]]]:
    if stationtype == 'Megaship' or len(candidates) > 1:
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

    return candidates
