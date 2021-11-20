from . import mysqlutils as mysql
from typing import Sequence, Union, Callable, Optional
from datetime import datetime


def execute(conn: mysql.DBConnection, query: str, params: Sequence = None) -> None:
    cursor = conn.cursor()
    cursor.execute(query, params)


def executemany(conn: mysql.DBConnection, query: str, params: Sequence[Sequence]) -> None:
    cursor = conn.cursor()
    cursor.executemany(query, params)


def execute_getrowid(conn: mysql.DBConnection, query: str, params: Sequence = None) -> int:
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.lastrowid


def fetch_scalar(conn: mysql.DBConnection, query: str, params: Sequence = None)\
        -> Union[int, float, bool, str, bytes, datetime, None]:
    cursor = conn.cursor()
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row[0]


def fetch_scalar_int(conn: mysql.DBConnection, query: str, params: Sequence = None) -> int:
    cursor = conn.cursor()
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row[0]


def fetch_one(conn: mysql.DBConnection, query: str, params: Sequence = None) -> Sequence:
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.fetchone()


def fetch_all(conn: mysql.DBConnection, query: str, params: Sequence = None) -> Sequence[Sequence]:
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.fetchall()


def fetch_streaming(conn: mysql.DBConnection, query: str, params: Sequence = None) -> mysql.DBCursor:
    cursor = mysql.make_streaming_cursor(conn)
    cursor.execute(query, params)
    return cursor


def execute_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], None]:
    return lambda conn, params: execute(conn, query, params)


def executemany_partial(query: str) \
        -> Callable[[mysql.DBConnection, Sequence[Sequence]], None]:
    return lambda conn, params: executemany(conn, query, params)


def execute_getrowid_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], int]:
    return lambda conn, params: execute_getrowid(conn, query, params)


def fetch_scalar_partial(query: str)\
        -> Callable[[mysql.DBConnection, Optional[Sequence]],
                    Union[int, float, bool, str, bytes, datetime, None]]:
    return lambda conn, params: fetch_scalar(conn, query, params)


def fetch_scalar_int_partial(query: str)\
        -> Callable[[mysql.DBConnection, Optional[Sequence]], int]:
    return lambda conn, params: fetch_scalar_int(conn, query, params)


def fetch_one_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], Sequence]:
    return lambda conn, params: fetch_one(conn, query, params)


def fetch_all_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], Sequence[Sequence]]:
    return lambda conn, params: fetch_all(conn, query, params)


def fetch_streaming_partial(query: str) \
        -> Callable[[mysql.DBConnection, Optional[Sequence]], mysql.DBCursor]:
    return lambda conn, params: fetch_streaming(conn, query, params)


query_max_edsm_systemid = 'SELECT MAX(EdsmId) FROM Systems_EDSM'
query_max_eddb_systemid = 'SELECT MAX(EddbId) FROM Systems_EDDB'
query_max_edsm_bodyid = 'SELECT MAX(EdsmId) FROM SystemBodies_EDSM'
query_edsm_systems = 'SELECT Id, EdsmId, TimestampSeconds, HasCoords, IsHidden, IsDeleted FROM Systems_EDSM'
query_eddb_systems = 'SELECT Id, EddbId, TimestampSeconds FROM Systems_EDDB'
query_edsm_bodies = 'SELECT Id, EdsmId, TimestampSeconds FROM SystemBodies_EDSM'
query_parentsets = 'SELECT Id, BodyID, ParentJson FROM ParentSets'
query_software = 'SELECT Id, Name FROM Software'
query_body_designations = 'SELECT Id, BodyDesignation, BodyCategory FROM SystemBodyDesignations WHERE IsUsed = 1'
query_regions = 'SELECT Id, Name, X0, Y0, Z0, SizeX, SizeY, SizeZ, RegionAddress, IsHARegion FROM Regions'
query_factions = 'SELECT Id, Name, Government, Allegiance FROM Factions'
query_body_designation = 'SELECT Id, BodyDesignation, BodyCategory FROM SystemBodyDesignations WHERE BodyDesignation = %s'
query_update_body_designation_used = 'UPDATE SystemBodyDesignations SET IsUsed = 1 WHERE Id = %s'
query_update_system_coords = 'UPDATE Systems SET X = %s, Y = %s, Z = %s WHERE Id = %s'
query_insert_software = 'INSERT INTO Software (Name) VALUES (%s)'
query_insert_edsmfile = 'INSERT INTO EDSMFiles (FileName) VALUES (%s)'
query_system_by_edsmid = 'SELECT Id, TimestampSeconds, HasCoords FROM Systems_EDSM WHERE EdsmId = %s'
query_body_by_edsmid = 'SELECT Id, TimestampSeconds FROM SystemBodies_EDSM WHERE EdsmId = %s'
query_system_by_eddbid = 'SELECT Id, TimestampSeconds FROM Systems_EDDB WHERE EddbId = %s'
query_insert_file_line_stations = 'INSERT INTO FileLineStations (FileId, LineNo, StationId) VALUES (%s, %s, %s)'
query_file_line_stations_by_file = 'SELECT LineNo, StationId FROM FileLineStations WHERE FileId = %s'
query_file_line_info_by_file = 'SELECT LineNo, Timestamp, SystemId, BodyId FROM FileLineInfo WHERE FileId = %s'
query_file_line_factions_by_file = 'SELECT LineNo, FactionId FROM FileLineFactions WHERE FileId = %s'
query_file_line_routes_by_file = 'SELECT LineNo, EntryNum, SystemId FROM FileLineNavRoutes WHERE FileId = %s'
query_max_edsm_body_file_lineno = 'SELECT MAX(LineNo) FROM EDSMFileLineBodies WHERE FileId = %s'
query_edsm_body_file_lines_by_file = 'SELECT LineNo, EdsmBodyId FROM EDSMFileLineBodies WHERE FileId = %s'
query_station_file_line_counts = 'SELECT FileId, COUNT(LineNo) FROM FileLineStations GROUP BY FileId'
query_info_file_line_counts = 'SELECT FileId, COUNT(LineNo) FROM FileLineInfo GROUP BY FileId'
query_faction_file_line_counts = 'SELECT FileId, COUNT(DISTINCT LineNo) FROM FileLineFactions GROUP BY FileId'
query_route_file_line_counts = 'SELECT FileId, COUNT(*) FROM FileLineNavRoutes GROUP BY FileId'
query_set_body_bodyid = 'UPDATE SystemBodies SET HasBodyId = 1, BodyID = %s WHERE Id = %s'

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
        nb.IsRejected,
        nb.BodyDesignationId
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

query_insert_parentset = '''
    INSERT INTO ParentSets (
        BodyId,
        ParentJson
    )
    VALUES
    (
        %s,
        %s
    )
'''

query_insert_parentset_link = '''
    INSERT IGNORE INTO SystemBodies_ParentSet (
        Id,
        ParentSetId
    )
    VALUES
    (
        %s,
        %s
    )
'''

query_bodies_byname = '''
    SELECT
        Id,
        BodyName,
        SystemName,
        SystemId,
        BodyId,
        BodyCategoryDescription,
        ArgOfPeriapsis,
        ValidFrom,
        ValidUntil,
        IsRejected,
        BodyCategory,
        BodyDesignationId
    FROM SystemBodyNames sn
    WHERE SystemId = %s
    AND BodyName = %s
    AND IsNamedBody = %s
'''

query_system_bodies = '''
    SELECT
        Id,
        BodyName,
        SystemName,
        SystemId,
        BodyId,
        BodyCategoryDescription,
        ArgOfPeriapsis,
        ValidFrom,
        ValidUntil,
        IsRejected,
        BodyCategory,
        BodyDesignationId
    FROM SystemBodyNames sn
    WHERE SystemId = %s AND IsNamedBody = %s
'''

query_insert_body = '''
    INSERT INTO SystemBodies (
        SystemId,
        HasBodyId,
        BodyId,
        BodyDesignationId,
        IsNamedBody
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s
    )
'''

query_bodies_bycustomname = '''
    SELECT
        Id,
        BodyName,
        SystemName,
        SystemId,
        BodyId,
        BodyCategoryDescription,
        ArgOfPeriapsis,
        ValidFrom,
        ValidUntil,
        IsRejected,
        BodyCategory,
        BodyDesignationId
    FROM SystemBodyNames sb
    WHERE sb.CustomName = %s
'''

query_insert_named_body = '''
    INSERT INTO SystemBodies_Named (
        Id,
        SystemId,
        Name
    )
    VALUES
    (
        %s,
        %s,
        %s
    )
'''

query_set_body_invalid = '''
    INSERT INTO SystemBodies_Validity (
        Id,
        IsRejected
    )
    VALUES
    (
        %s,
        1
    )
'''

query_insert_faction = '''
    INSERT INTO Factions (
        Name,
        Government,
        Allegiance
    )
    VALUES
    (
        %s,
        %s,
        %s
    )
'''

query_update_station = '''
    UPDATE Stations SET
        MarketId = %s,
        SystemId = %s,
        StationType = %s,
        Body = %s,
        BodyID = %s
    WHERE Id = %s
'''

query_system_by_id = '''
    SELECT
        ns.Id,
        ns.SystemAddress,
        ns.Name,
        ns.X,
        ns.Y,
        ns.Z
    FROM SystemNames ns
    WHERE Id = %s'
'''

query_insert_edsm_system = '''
    INSERT INTO Systems_EDSM SET
        EdsmId = %s,
        Id = %s,
        TimestampSeconds = %s,
        HasCoords = %s,
        IsHidden = %s,
        IsDeleted = %s
    ON DUPLICATE KEY UPDATE
        Id = %s,
        TimestampSeconds = %s,
        HasCoords = %s,
        IsHidden = %s,
        IsDeleted = %s
'''

query_insert_edsm_body = '''
    INSERT INTO SystemBodies_EDSM SET
        EdsmId = %s,
        Id = %s,
        TimestampSeconds = %s
    ON DUPLICATE KEY UPDATE
        Id = %s,
        TimestampSeconds = %s
'''

query_insert_edsm_station = '''
    INSERT INTO Stations_EDSM SET
        EdsmStationId = %s,
        Id = %s,
        Timestamp = %s
    ON DUPLICATE KEY UPDATE
        Id = %s,
        Timestamp = %s
'''

query_insert_eddb_system = '''
    INSERT INTO Systems_EDDB SET
        EddbId = %s,
        Id = %s,
        TimestampSeconds = %s
    ON DUPLICATE KEY UPDATE
        Id = %s,
        TimestampSeconds = %s
'''

query_insert_file_line_info = '''
    INSERT INTO FileLineInfo (
        FileId,
        LineNo,
        Timestamp,
        GatewayTimestamp,
        SoftwareId,
        SystemId,
        BodyId,
        LineLength,
        DistFromArrivalLS,
        HasBodyId,
        HasSystemAddress,
        HasMarketId
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
        %s,
        %s
    )
'''

query_insert_file_line_factions = '''
    INSERT INTO FileLineFactions (
        FileId,
        LineNo,
        FactionId,
        EntryNum
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s
    )
'''

query_insert_file_line_route_systems = '''
    INSERT INTO FileLineNavRoutes (
        FileId,
        LineNo,
        SystemId,
        EntryNum
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s
    )
'''

query_insert_edsm_file_line_systems = '''
    INSERT INTO EDSMFileLineBodies (
        FileId,
        LineNo,
        EdsmBodyId
    )
    VALUES
    (
        %s,
        %s,
        %s
    )
'''

query_files = '''
    SELECT
        Id,
        FileName,
        Date,
        EventType,
        LineCount,
        PopulatedLineCount,
        StationLineCount,
        NavRouteSystemCount,
        IsTest
    FROM Files f
'''

query_edsm_body_file_line_counts = '''
    SELECT FileId, COUNT(LineNo)
    FROM EDSMFileLineBodies flb
    JOIN SystemBodies_EDSM sb ON sb.EdsmId = flb.EdsmBodyId
    GROUP BY FileId
'''

query_edsm_files = '''
    SELECT
        Id,
        FileName,
        Date,
        LineCount,
        CompressedSize
    FROM EDSMFiles f
    ORDER BY Date
'''

query_update_file_info = '''
    UPDATE Files SET
        LineCount = %s,
        CompressedSize = %s,
        UncompressedSize = %s,
        PopulatedLineCount = %s,
        StationLineCount = %s,
        NavRouteSystemCount = %s
    WHERE Id = %s
'''

query_update_edsm_file_info = '''
    UPDATE EDSMFiles SET
        LineCount = %s,
        CompressedSize = %s,
        UncompressedSize = %s
    WHERE Id = %s
'''

get_max_edsm_systemid = fetch_scalar_int_partial(query_max_edsm_systemid)
get_max_eddb_systemid = fetch_scalar_int_partial(query_max_eddb_systemid)
get_max_edsm_bodyid = fetch_scalar_int_partial(query_max_edsm_bodyid)
get_edsm_systems = fetch_streaming_partial(query_edsm_systems)
get_eddb_systems = fetch_streaming_partial(query_eddb_systems)
get_edsm_bodies = fetch_streaming_partial(query_edsm_bodies)
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
insert_station = execute_getrowid_partial(query_insert_station)
insert_parentset = execute_getrowid_partial(query_insert_parentset)
insert_parentset_link = execute_partial(query_insert_parentset_link)
insert_software = execute_getrowid_partial(query_insert_software)
insert_edsmfile = execute_getrowid_partial(query_insert_edsmfile)
get_bodies_byname = fetch_all_partial(query_bodies_byname)
get_system_bodies = fetch_all_partial(query_system_bodies)
insert_body = execute_getrowid_partial(query_insert_body)
get_bodies_bycustomname = fetch_all_partial(query_bodies_bycustomname)
insert_named_body = execute_partial(query_insert_named_body)
set_body_invalid = execute_partial(query_set_body_invalid)
insert_faction = execute_getrowid_partial(query_insert_faction)
update_station = execute_partial(query_update_station)
get_system_by_id = fetch_one_partial(query_system_by_id)
get_system_by_edsmid = fetch_one_partial(query_system_by_edsmid)
get_body_by_edsmid = fetch_one_partial(query_body_by_edsmid)
insert_edsm_system = execute_partial(query_insert_edsm_system)
insert_edsm_body = execute_partial(query_insert_edsm_body)
insert_edsm_station = execute_partial(query_insert_edsm_station)
get_system_by_eddbid = fetch_one_partial(query_system_by_eddbid)
insert_eddb_system = execute_partial(query_insert_eddb_system)
insert_file_line_stations = executemany_partial(query_insert_file_line_stations)
insert_file_line_info = executemany_partial(query_insert_file_line_info)
insert_file_line_factions = executemany_partial(query_insert_file_line_factions)
insert_file_line_route_systems = executemany_partial(query_insert_file_line_route_systems)
insert_edsm_file_line_systems = executemany_partial(query_insert_edsm_file_line_systems)
get_file_line_stations_by_file = fetch_all_partial(query_file_line_stations_by_file)
get_file_line_info_by_file = fetch_all_partial(query_file_line_info_by_file)
get_file_line_factions_by_file = fetch_all_partial(query_file_line_factions_by_file)
get_file_line_routes_by_file = fetch_all_partial(query_file_line_routes_by_file)
get_max_edsm_body_file_lineno = fetch_scalar_int_partial(query_max_edsm_body_file_lineno)
get_edsm_body_file_lines_by_file = fetch_streaming_partial(query_edsm_body_file_lines_by_file)
get_station_file_line_counts = fetch_streaming_partial(query_station_file_line_counts)
get_info_file_line_counts = fetch_streaming_partial(query_info_file_line_counts)
get_faction_file_line_counts = fetch_streaming_partial(query_faction_file_line_counts)
get_route_file_line_counts = fetch_streaming_partial(query_route_file_line_counts)
get_files = fetch_streaming_partial(query_files)
get_edsm_body_file_line_counts = fetch_streaming_partial(query_edsm_body_file_line_counts)
get_edsm_files = fetch_streaming_partial(query_edsm_files)
update_file_info = execute_partial(query_update_file_info)
update_edsm_file_info = execute_partial(query_update_edsm_file_info)
set_body_bodyid = execute_partial(query_set_body_bodyid)
