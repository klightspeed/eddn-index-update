from . import mysqlutils as mysql
from typing import Sequence, Union, Callable, Optional
from datetime import datetime
from functools import partial


query_max_edsm_systemid = 'SELECT MAX(EdsmId) FROM Systems_EDSM'
query_max_eddb_systemid = 'SELECT MAX(EddbId) FROM Systems_EDDB'
query_max_edsm_bodyid = 'SELECT MAX(EdsmId) FROM SystemBodies_EDSM'
query_edsm_systems = 'SELECT Id, EdsmId, TimestampSeconds, HasCoords, IsHidden, IsDeleted FROM Systems_EDSM'
query_eddb_systems = 'SELECT Id, EddbId, TimestampSeconds FROM Systems_EDDB'
query_edsm_bodies = 'SELECT Id, EdsmId, TimestampSeconds FROM SystemBodies_EDSM'
query_parentsets = 'SELECT Id, BodyID, ParentJson FROM ParentSets'
query_software = 'SELECT Id, Name FROM Software'
query_body_designations = 'SELECT Id, BodyDesignation FROM SystemBodyDesignations WHERE IsUsed = 1'
query_regions = 'SELECT Id, Name, X0, Y0, Z0, SizeX, SizeY, SizeZ, RegionAddress, IsHARegion FROM Regions'
query_factions = 'SELECT Id, Name, Government, Allegiance FROM Factions'
query_body_designation = 'SELECT Id, BodyDesignation FROM SystemBodyDesignations WHERE BodyDesignation = %s'
query_update_body_designation_used = 'UPDATE SystemBodyDesignations SET IsUsed = 1 WHERE Id = %s'
query_update_system_coords = 'UPDATE Systems SET X = %s, Y = %s, Z = %s WHERE Id = %s'

query_named_bodies = '''
    SELECT
        nb.Id,
        nb.BodyName,
        nb.SystemName,
        nb.SystemId,
        nb.BodyID,
        nb.BodyCategory,
        nb.ArgOfPeriapsis,
        nb.ValidFrom,
        nb.ValidUntil,
        nb.IsRejected
    FROM SystemBodyNames nb
    JOIN SystemBodies_Named sbn ON sbn.Id = nb.Id
'''

query_named_systems = '''
    SELECT
        ns.Id,
        ns.SystemAddress,
        ns.Name,
        ns.X,
        ns.Y,
        ns.Z
    FROM SystemNames ns
    JOIN Systems_Named sn ON sn.Id = ns.Id
'''

query_systems_by_modsysaddr = '''
    SELECT
        ns.Id,
        ns.SystemAddress,
        ns.Name,
        ns.X,
        ns.Y,
        ns.Z
    FROM SystemNames ns
    WHERE ModSystemAddress = %s
'''

query_systems_by_name = '''
    SELECT
        ns.Id,
        ns.SystemAddress,
        ns.Name,
        ns.X,
        ns.Y,
        ns.Z
    FROM SystemNames ns
    JOIN Systems_Named sn ON sn.Id = ns.Id
    WHERE sn.Name = %s
'''

query_insert_system = '''
    INSERT INTO Systems (
        ModSystemAddress,
        X,
        Y,
        Z,
        IsHASystem,
        IsNamedSystem
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
'''

query_insert_hasystem = '''
    INSERT INTO Systems_HASector (
        Id,
        ModSystemAddress,
        RegionId,
        Mid1a,
        Mid1b,
        Mid2,
        SizeClass,
        Mid3,
        Sequence
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
'''

query_insert_named_system = '''
    INSERT INTO Systems_Named (
        Id,
        Name
    )
    VALUES
    (
        %s,
        %s
    )
'''

query_set_system_invalid = '''
    INSERT INTO Systems_Validity (
        Id,
        IsRejected
    )
    VALUES
    (
        %s,
        1
    )
'''

query_find_systems_in_boxel = '''
    SELECT
        ns.Id,
        ns.SystemAddress,
        ns.Name,
        ns.X,
        ns.Y,
        ns.Z
    FROM SystemNames ns
    WHERE ModSystemAddress >= %s
    AND ModSystemAddress < %s
'''

query_find_stations = '''
    SELECT
        Id,
        MarketId,
        StationName,
        SystemName,
        SystemId,
        StationType,
        COALESCE(StationType_Location, StationType),
        Body,
        BodyID,
        IsRejected,
        ValidFrom,
        ValidUntil,
        Test
    FROM Stations
    WHERE SystemName = %s
    AND StationName = %s
    ORDER BY ValidUntil - ValidFrom
'''

query_insert_station = '''
    INSERT INTO Stations (
        MarketId,
        StationName,
        SystemName,
        SystemId,
        StationType,
        StationType_Location,
        Body,
        BodyID,
        ValidFrom,
        ValidUntil,
        Test
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
'''


def fetch_scalar(conn: mysql.DBConnection, query: str, params: Sequence = None) -> Union[int, str, datetime, float]:
    c = conn.cursor()
    c.execute(query, params)
    row = c.fetchone()
    return row[0]


def execute(conn: mysql.DBConnection, query: str, params: Sequence = None) -> mysql.DBCursor:
    c = mysql.make_streaming_cursor(conn)
    c.execute(query, params)
    return c


def execute_getrowid(conn: mysql.DBConnection, query: str, params: Sequence = None) -> int:
    c = mysql.make_streaming_cursor(conn)
    c.execute(query, params)
    return c.lastrowid


def fetch_one(conn: mysql.DBConnection, query: str, params: Sequence = None) -> Sequence:
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.fetchone()


def fetch_all(conn: mysql.DBConnection, query: str, params: Sequence = None) -> Sequence[Sequence]:
    c = conn.cursor()
    c.execute(query, params)
    return c.fetchall()


def fetch_scalar_partial(query: str)\
        -> Callable[[mysql.DBConnection, Optional[Sequence]], Union[int, str, datetime, float]]:
    return lambda conn, params: fetch_scalar(conn, query, params)


def execute_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], mysql.DBCursor]:
    return lambda conn, params: execute(conn, query, params)


def execute_getrowid_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], int]:
    return lambda conn, params: execute_getrowid(conn, query, params)


def fetch_one_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], Sequence]:
    return lambda conn, params: fetch_one(conn, query, params)


def fetch_all_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], Sequence[Sequence]]:
    return lambda conn, params: execute(conn, query, params)


get_max_edsm_systemid = fetch_scalar_partial(query_max_edsm_systemid)
get_max_eddb_systemid = fetch_scalar_partial(query_max_eddb_systemid)
get_max_edsm_bodyid = fetch_scalar_partial(query_max_edsm_bodyid)
get_edsm_systems = execute_partial(query_edsm_systems)
get_eddb_systems = execute_partial(query_eddb_systems)
get_edsm_bodies = execute_partial(query_edsm_bodies)
get_parentsets = fetch_all_partial(query_parentsets)
get_software = fetch_all_partial(query_software)
get_body_designations = fetch_all_partial(query_body_designations)
get_named_bodies = fetch_all_partial(query_named_bodies)
get_named_systems = fetch_all_partial(query_named_systems)
get_regions = fetch_all_partial(query_regions)
get_factions = fetch_all_partial(query_factions)
get_body_designation = fetch_one_partial(query_body_designation)
set_body_designation_used = execute_partial(query_update_body_designation_used)
set_system_coords = execute_partial(query_update_system_coords)
get_systems_by_modsysaddr = fetch_all_partial(query_systems_by_modsysaddr)
get_systems_by_name = fetch_all_partial(query_systems_by_name)
insert_system = execute_getrowid_partial(query_insert_system)
insert_hasystem = execute_partial(query_insert_hasystem)
insert_named_system = execute_partial(query_insert_named_system)
set_system_invalid = execute_partial(query_set_system_invalid)
find_systems_in_boxel = fetch_all_partial(query_find_systems_in_boxel)
find_stations = fetch_all_partial(query_find_stations)
insert_station = execute_getrowid(query_insert_station)
