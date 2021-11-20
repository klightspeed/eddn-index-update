from typing import Union, Callable, Optional
from collections.abc import Mapping, Sequence
from datetime import datetime
from .database import DBConnection, DBCursor, SQLQuery


def get_last_row_id(conn: DBConnection,
                    cursor: DBCursor):
    if conn.dialect in ['mysql', 'sqlite3']:
        return cursor.lastrowid
    else:
        row = cursor.fetchone()
        return row[0]


def sql_query_identity(command_and_columns: str,
                       primary_key_col: str,
                       values: str
                       ):
    return SQLQuery(
        f'{command_and_columns} {values}',
        last_row_func=get_last_row_id,
        mssql=f'{command_and_columns} OUTPUT Inserted.{primary_key_col} {values}',
        pgsql=f'{command_and_columns} {values} RETURNING {primary_key_col}'
    )


def execute(conn: DBConnection,
            query: Union[str, SQLQuery],
            params: Union[Sequence, Mapping] = None
            ) -> None:
    cursor = conn.cursor()
    conn.execute(cursor, query, params)


def executemany(conn: DBConnection,
                query: Union[str, SQLQuery],
                params: Sequence[Union[Sequence, Mapping]]
                ) -> None:
    cursor = conn.cursor()
    conn.executemany(cursor, query, params)


def execute_identity(conn: DBConnection,
                     query: SQLQuery,
                     params: Union[Sequence, Mapping, None] = None
                     ) -> int:
    cursor = conn.cursor()
    return conn.execute_identity(cursor, query, params)


def execute_upsert(conn: DBConnection,
                   update_query: SQLQuery,
                   insert_query: SQLQuery,
                   params: Union[Sequence, Mapping, None] = None
                   ) -> None:
    cursor = conn.cursor()
    conn.execute(cursor, update_query, params)
    if cursor.rowcount == 0:
        conn.execute(cursor, insert_query, params)


def fetch_scalar(conn: DBConnection,
                 query: Union[str, SQLQuery],
                 params: Union[Sequence, Mapping, None] = None
                 ) -> Union[int, float, bool, str, bytes, datetime, None]:
    cursor = conn.cursor()
    conn.execute(cursor, query, params)
    row = cursor.fetchone()
    return row[0]


def fetch_scalar_int(conn: DBConnection,
                     query: Union[str, SQLQuery],
                     params: Union[Sequence, Mapping] = None
                     ) -> Union[int, None]:
    cursor = conn.cursor()
    conn.execute(cursor, query, params)
    row = cursor.fetchone()
    return row[0]


def fetch_one(conn: DBConnection,
              query: Union[str, SQLQuery],
              params: Union[Sequence, Mapping, None] = None
              ) -> Union[Sequence, Mapping, None]:
    cursor = conn.cursor()
    conn.execute(cursor, query, params)
    return cursor.fetchone()


def fetch_all(conn: DBConnection,
              query: Union[str, SQLQuery],
              params: Union[Sequence, Mapping, None] = None
              ) -> Sequence[Union[Sequence, Mapping]]:
    cursor = conn.cursor()
    conn.execute(cursor, query, params)
    return cursor.fetchall()


def fetch_streaming(conn: DBConnection,
                    query: Union[str, SQLQuery],
                    params: Union[Sequence, Mapping, None] = None
                    ) -> DBCursor:
    cursor = conn.cursor(streaming=True)
    conn.execute(cursor, query, params)
    return cursor


def execute_partial(query: Union[str, SQLQuery]) \
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]], None]:
    return lambda conn, params: execute(conn, query, params)


def executemany_partial(query: Union[str, SQLQuery]) \
        -> Callable[[DBConnection, Sequence[Union[Sequence, Mapping]]], None]:
    return lambda conn, params: executemany(conn, query, params)


def execute_identity_partial(query: SQLQuery) \
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]], int]:
    return lambda conn, params: execute_identity(conn, query, params)


def execute_upsert_partial(update_query: SQLQuery, insert_query: SQLQuery) \
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]], None]:
    return lambda conn, params: execute_upsert(conn, update_query, insert_query, params)


def fetch_scalar_partial(query: Union[str, SQLQuery])\
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]],
                    Union[int, float, bool, str, bytes, datetime, None]]:
    return lambda conn, params: fetch_scalar(conn, query, params)


def fetch_scalar_int_partial(query: Union[str, SQLQuery])\
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]],
                    Union[int, None]]:
    return lambda conn, params: fetch_scalar_int(conn, query, params)


def fetch_one_partial(query: Union[str, SQLQuery]) \
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]],
                    Union[Sequence, Mapping, None]]:
    return lambda conn, params: fetch_one(conn, query, params)


def fetch_all_partial(query: Union[str, SQLQuery]) \
        -> Callable[[DBConnection, Union[Sequence, Mapping, None]],
                    Sequence[Union[Sequence, Mapping]]]:
    return lambda conn, params: fetch_all(conn, query, params)


def fetch_streaming_partial(query: Union[str, SQLQuery]) \
        -> Callable[[DBConnection, Optional[Sequence]], DBCursor]:
    return lambda conn, params: fetch_streaming(conn, query, params)


# region Scalar Select Statements

query_max_edsm_system_id = 'SELECT MAX(EdsmId) FROM Systems_EDSM'
query_max_eddb_system_id = 'SELECT MAX(EddbId) FROM Systems_EDDB'
query_max_edsm_body_id = 'SELECT MAX(EdsmId) FROM SystemBodies_EDSM'
query_max_edsm_body_file_lineno = 'SELECT MAX(LineNo) FROM EDSMFileLineBodies WHERE FileId = %s'

# endregion

# region Scalar Select Functions

get_max_edsm_system_id = fetch_scalar_int_partial(query_max_edsm_system_id)
get_max_eddb_system_id = fetch_scalar_int_partial(query_max_eddb_system_id)
get_max_edsm_body_id = fetch_scalar_int_partial(query_max_edsm_body_id)
get_max_edsm_body_file_lineno = fetch_scalar_int_partial(query_max_edsm_body_file_lineno)

# endregion

# region Singleton Select Statements

query_body_designation = '''
    SELECT
        Id,
        BodyDesignation,
        BodyCategory
    FROM SystemBodyDesignations
    WHERE BodyDesignation = %s
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

query_system_by_edsm_id = 'SELECT Id, TimestampSeconds, HasCoords FROM Systems_EDSM WHERE EdsmId = %s'
query_body_by_edsm_id = 'SELECT Id, TimestampSeconds FROM SystemBodies_EDSM WHERE EdsmId = %s'
query_system_by_eddb_id = 'SELECT Id, TimestampSeconds FROM Systems_EDDB WHERE EddbId = %s'


# endregion

# region Singleton Select Functions

get_body_designation = fetch_one_partial(query_body_designation)
get_system_by_id = fetch_one_partial(query_system_by_id)
get_system_by_edsm_id = fetch_one_partial(query_system_by_edsm_id)
get_body_by_edsm_id = fetch_one_partial(query_body_by_edsm_id)
get_system_by_eddb_id = fetch_one_partial(query_system_by_eddb_id)

# endregion

# region Streaming Select Statements

query_edsm_systems = 'SELECT Id, EdsmId, TimestampSeconds, HasCoords, IsHidden, IsDeleted FROM Systems_EDSM'
query_eddb_systems = 'SELECT Id, EddbId, TimestampSeconds FROM Systems_EDDB'
query_edsm_bodies = 'SELECT Id, EdsmId, TimestampSeconds FROM SystemBodies_EDSM'
query_edsm_body_file_lines_by_file = 'SELECT LineNo, EdsmBodyId FROM EDSMFileLineBodies WHERE FileId = %s'
query_station_file_line_counts = 'SELECT FileId, COUNT(LineNo) FROM FileLineStations GROUP BY FileId'
query_info_file_line_counts = 'SELECT FileId, COUNT(LineNo) FROM FileLineInfo GROUP BY FileId'
query_faction_file_line_counts = 'SELECT FileId, COUNT(DISTINCT LineNo) FROM FileLineFactions GROUP BY FileId'
query_route_file_line_counts = 'SELECT FileId, COUNT(*) FROM FileLineNavRoutes GROUP BY FileId'

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


# endregion

# region Streaming Select Functions

get_edsm_systems = fetch_streaming_partial(query_edsm_systems)
get_eddb_systems = fetch_streaming_partial(query_eddb_systems)
get_edsm_bodies = fetch_streaming_partial(query_edsm_bodies)
get_edsm_body_file_lines_by_file = fetch_streaming_partial(query_edsm_body_file_lines_by_file)
get_station_file_line_counts = fetch_streaming_partial(query_station_file_line_counts)
get_info_file_line_counts = fetch_streaming_partial(query_info_file_line_counts)
get_faction_file_line_counts = fetch_streaming_partial(query_faction_file_line_counts)
get_route_file_line_counts = fetch_streaming_partial(query_route_file_line_counts)
get_files = fetch_streaming_partial(query_files)
get_edsm_body_file_line_counts = fetch_streaming_partial(query_edsm_body_file_line_counts)
get_edsm_files = fetch_streaming_partial(query_edsm_files)

# endregion

# region FetchAll Select Statements

query_parent_sets = 'SELECT Id, BodyID, ParentJson FROM ParentSets'
query_software = 'SELECT Id, Name FROM Software'
query_body_designations = 'SELECT Id, BodyDesignation, BodyCategory FROM SystemBodyDesignations WHERE IsUsed = 1'
query_regions = 'SELECT Id, Name, X0, Y0, Z0, SizeX, SizeY, SizeZ, RegionAddress, IsHARegion FROM Regions'
query_factions = 'SELECT Id, Name, Government, Allegiance FROM Factions'
query_file_line_stations_by_file = 'SELECT LineNo, StationId FROM FileLineStations WHERE FileId = %s'
query_file_line_info_by_file = 'SELECT LineNo, Timestamp, SystemId, BodyId FROM FileLineInfo WHERE FileId = %s'
query_file_line_factions_by_file = 'SELECT LineNo, FactionId FROM FileLineFactions WHERE FileId = %s'
query_file_line_routes_by_file = 'SELECT LineNo, EntryNum, SystemId FROM FileLineNavRoutes WHERE FileId = %s'

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

query_bodies_by_name = '''
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

query_bodies_by_custom_name = '''
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

# endregion

# region FetchAll Select Functions

get_parent_sets = fetch_all_partial(query_parent_sets)
get_software = fetch_all_partial(query_software)
get_body_designations = fetch_all_partial(query_body_designations)
get_named_bodies = fetch_all_partial(query_named_bodies)
get_named_systems = fetch_all_partial(query_named_systems)
get_regions = fetch_all_partial(query_regions)
get_factions = fetch_all_partial(query_factions)
get_systems_by_modsysaddr = fetch_all_partial(query_systems_by_modsysaddr)
get_systems_by_name = fetch_all_partial(query_systems_by_name)
find_systems_in_boxel = fetch_all_partial(query_find_systems_in_boxel)
find_stations = fetch_all_partial(query_find_stations)
get_bodies_by_name = fetch_all_partial(query_bodies_by_name)
get_system_bodies = fetch_all_partial(query_system_bodies)
get_file_line_stations_by_file = fetch_all_partial(query_file_line_stations_by_file)
get_file_line_info_by_file = fetch_all_partial(query_file_line_info_by_file)
get_file_line_factions_by_file = fetch_all_partial(query_file_line_factions_by_file)
get_file_line_routes_by_file = fetch_all_partial(query_file_line_routes_by_file)
get_bodies_by_custom_name = fetch_all_partial(query_bodies_by_custom_name)

# endregion

# region Update Statements

query_update_body_designation_used = 'UPDATE SystemBodyDesignations SET IsUsed = 1 WHERE Id = %s'
query_update_system_coords = 'UPDATE Systems SET X = %s, Y = %s, Z = %s WHERE Id = %s'
query_update_body_bodyid = 'UPDATE SystemBodies SET HasBodyId = 1, BodyID = %s WHERE Id = %s'

query_update_station = '''
    UPDATE Stations SET
        MarketId = %s,
        SystemId = %s,
        StationType = %s,
        Body = %s,
        BodyID = %s
    WHERE Id = %s
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

# endregion

# region Update Functions

set_body_designation_used = execute_partial(query_update_body_designation_used)
set_system_coords = execute_partial(query_update_system_coords)
update_station = execute_partial(query_update_station)
update_file_info = execute_partial(query_update_file_info)
update_edsm_file_info = execute_partial(query_update_edsm_file_info)
set_body_bodyid = execute_partial(query_update_body_bodyid)

# endregion

# region Insert Identity Statements

query_insert_software = sql_query_identity(
    'INSERT INTO Software (Name)',
    'Id',
    'VALUES (%s)'
)

query_insert_edsm_file = sql_query_identity(
    'INSERT INTO EDSMFiles (FileName)',
    'Id',
    'VALUES (%s)'
)

query_insert_file_line_stations = sql_query_identity(
    'INSERT INTO FileLineStations (FileId, LineNo, StationId)',
    'Id',
    'VALUES (%s, %s, %s)'
)

query_insert_system = sql_query_identity(
    '''INSERT INTO Systems (
        ModSystemAddress,
        X,
        Y,
        Z,
        IsHASystem,
        IsNamedSystem
    )''',
    'Id',
    '''VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )'''
)

query_insert_station = sql_query_identity(
    '''INSERT INTO Stations (
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
    )''',
    'Id',
    '''VALUES
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
    )'''
)

query_insert_parent_set = sql_query_identity(
    '''INSERT INTO ParentSets (
        BodyId,
        ParentJson
    )''',
    'Id',
    '''VALUES
    (
        %s,
        %s
    )'''
)

query_insert_body = sql_query_identity(
    '''INSERT INTO SystemBodies (
        SystemId,
        HasBodyId,
        BodyId,
        BodyDesignationId,
        IsNamedBody
    )''',
    'Id',
    '''VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s
    )'''
)

query_insert_faction = sql_query_identity(
    '''INSERT INTO Factions (
        Name,
        Government,
        Allegiance
    )''',
    'Id',
    '''VALUES
    (
        %s,
        %s,
        %s
    )'''
)

# endregion

# region Insert Identity Functions

insert_system = execute_identity_partial(query_insert_system)
insert_station = execute_identity_partial(query_insert_station)
insert_parent_set = execute_identity_partial(query_insert_parent_set)
insert_software = execute_identity_partial(query_insert_software)
insert_edsm_file = execute_identity_partial(query_insert_edsm_file)
insert_body = execute_identity_partial(query_insert_body)
insert_faction = execute_identity_partial(query_insert_faction)

# endregion

# region Bulk Insert Statements

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

# endregion

# region Bulk Insert Functions

insert_file_line_stations = executemany_partial(query_insert_file_line_stations)
insert_file_line_info = executemany_partial(query_insert_file_line_info)
insert_file_line_factions = executemany_partial(query_insert_file_line_factions)
insert_file_line_route_systems = executemany_partial(query_insert_file_line_route_systems)
insert_edsm_file_line_systems = executemany_partial(query_insert_edsm_file_line_systems)

# endregion

# region Insert Statements

query_insert_sphere_sector_system = '''
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

query_insert_system_invalid = '''
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

query_insert_parent_set_link = '''
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

query_insert_body_invalid = '''
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

# endregion

# region Insert Functions

insert_sphere_sector_system = execute_partial(query_insert_sphere_sector_system)
insert_named_system = execute_partial(query_insert_named_system)
set_system_invalid = execute_partial(query_insert_system_invalid)
insert_parent_set_link = execute_partial(query_insert_parent_set_link)
insert_named_body = execute_partial(query_insert_named_body)
set_body_invalid = execute_partial(query_insert_body_invalid)

# endregion

# region Upsert Statements

query_update_edsm_system = SQLQuery(
    '''UPDATE Systems_EDSM SET
        Id = %s,
        TimestampSeconds = %s,
        HasCoords = %s,
        IsHidden = %s,
        IsDeleted = %s
    WHERE EdsmId = %s'''
)

query_insert_edsm_system = SQLQuery(
    '''INSERT INTO Systems_EDSM (
        Id,
        TimestampSeconds,
        HasCoords
        IsHidden,
        IsDeleted,
        EdsmId
    )
    VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )'''
)

query_update_edsm_body = SQLQuery(
    '''
        UPDATE SystemBodies_EDSM SET
            Id = %s,
            TimestampSeconds = %s
        WHERE EdsmId = %s
    '''
)

query_insert_edsm_body = SQLQuery(
    '''
        INSERT INTO SystemBodies_EDSM (
            Id,
            TimestampSeconds,
            EdsmId
        )
        VALUES
        (
            %s,
            %s,
            %s
        )
    '''
)

query_update_edsm_station = SQLQuery(
    '''
        UPDATE Stations_EDSM SET
            Id = %s,
            Timestamp = %s
        WHERE EdsmStationId = %s
    '''
)

query_insert_edsm_station = SQLQuery(
    '''
        INSERT INTO Stations_EDSM (
            Id,
            Timestamp,
            EdsmStationId
        )
        VALUES
        (
            %s,
            %s,
            %s
        )
    '''
)

query_update_eddb_system = SQLQuery(
    '''
        UPDATE Systems_EDDB SET
            Id = %s,
            TimestampSeconds = %s
        WHERE EddbId = %s
    '''
)

query_insert_eddb_system = SQLQuery(
    '''
        INSERT INTO Systems_EDDB (
            Id,
            TimestampSeconds,
            EddbId
        )
        VALUES
        (
            %s,
            %s,
            %s
        )
    '''
)

# endregion

# region Upsert Functions

upsert_edsm_system = execute_upsert_partial(
    query_update_edsm_system,
    query_insert_edsm_system
)

upsert_edsm_body = execute_upsert_partial(
    query_update_edsm_body,
    query_insert_edsm_body
)

upsert_edsm_station = execute_upsert_partial(
    query_update_edsm_station,
    query_insert_edsm_station
)

upsert_eddb_system = execute_upsert_partial(
    query_update_eddb_system,
    query_insert_eddb_system
)

# endregion
