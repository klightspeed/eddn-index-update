from datetime import datetime
from typing import NamedTuple, Optional, TypedDict, Protocol
from collections.abc import MutableSequence as List, \
                            MutableMapping as Dict
import numpy


class EDDNSystem(NamedTuple):
    id: int
    id64: int
    name: str
    x: float
    y: float
    z: float
    has_coords: bool


class EDDNStation(NamedTuple):
    id: int
    market_id: int
    name: str
    system_name: str
    system_id: Optional[int]
    type: str
    type_location: str
    body: Optional[str]
    bodyid: Optional[int]
    is_rejected: bool
    valid_from: datetime
    valid_until: datetime
    test: bool


class EDDNFile(NamedTuple):
    id: int
    name: str
    date: datetime
    event_type: str
    line_count: int
    station_file_line_count: int
    info_file_line_count: int
    faction_file_line_count: int
    nav_route_system_count: int
    populated_line_count: int
    station_line_count: int
    route_system_count: int
    test: bool


class EDDNRegion(NamedTuple):
    id: int
    name: str
    x0: float
    y0: float
    z0: float
    size_x: float
    size_y: float
    size_z: float
    region_address: int
    is_sphere_sector: bool


class EDDNBody(NamedTuple):
    id: int
    name: str
    system_name: str
    system_id: int
    bodyid: Optional[int]
    category: Optional[int]
    arg_of_periapsis: Optional[float]
    valid_from: datetime
    valid_until: datetime
    is_rejected: bool
    designation_id: Optional[int]


class EDDNFaction(NamedTuple):
    id: int
    name: str
    government: str
    allegiance: str


class EDSMFile(NamedTuple):
    id: int
    name: str
    date: datetime
    line_count: int
    body_line_count: int
    compressed_size: int


class EDSMBody(TypedDict):
    id: int
    id64: Optional[int]
    bodyId: Optional[int]
    name: str
    systemName: str
    systemId: int
    systemId64: Optional[int]
    updateTime: str
    argOfPeriapsis: Optional[float]
    semiMajorAxis: Optional[float]
    type: str
    subType: str


class EDSMStation(TypedDict):
    id: int
    marketId: Optional[int]
    name: str
    type: str
    systemId: int
    systemName: str
    updateTime: Dict[str, str]


class Writable(Protocol):
    def write(self, text: str):
        ...


class ProcessorArgs(Protocol):
    reprocess: bool
    reprocess_all: bool
    no_journal: bool
    market: bool
    nav_route: bool
    edsm_systems: bool
    edsm_bodies: bool
    edsm_missing_bodies: bool
    edsm_stations: bool
    eddb_systems: bool
    eddb_stations: bool
    no_eddn: bool
    process_title_progress: bool
    config_file: str
    print_config: bool


class KnownBody(TypedDict):
    SystemAddress: int
    SystemName: str
    BodyID: int
    BodyName: str
    BodyDesignation: str
    BodyDesignationId: int


class RejectDataSystem(TypedDict):
    id: int
    id64: int
    x: float
    y: float
    z: float
    name: str


class RejectData(TypedDict):
    nameid64: Optional[int]
    id64name: Optional[str]
    systems: List[RejectDataSystem]


DTypeEDSMSystem = numpy.dtype(
    [
        ('system_id', '<i4'),
        ('edsm_id', '<i4'),
        ('timestamp_seconds', '<i4'),
        ('has_coords', 'i1'),
        ('is_hidden', 'i1'),
        ('is_deleted', 'i1'),
        ('processed', 'i1')
    ]
)

DTypeEDDBSystem = numpy.dtype(
    [
        ('system_id', '<i4'),
        ('eddb_id', '<i4'),
        ('timestamp_seconds', '<i4')
    ]
)

DTypeEDSMBody = numpy.dtype(
    [
        ('body_id', '<i4'),
        ('edsm_id', '<i4'),
        ('timestamp_seconds', '<i4')
    ]
)


class NPTypeEDSMSystem(Protocol):
    system_id: numpy.intc
    edsm_id: numpy.intc
    timestamp_seconds: numpy.intc
    has_coords: numpy.byte
    is_hidden: numpy.byte
    is_deleted: numpy.byte
    processed: numpy.byte

    def __getitem__(self, index: int):
        ...

    def __setitem__(self, index: int):
        ...


class NPTypeEDDBSystem(Protocol):
    system_id: numpy.intc
    eddb_id: numpy.intc
    timestamp_seconds: numpy.intc

    def __getitem__(self, index: int):
        ...

    def __setitem__(self, index: int):
        ...


class NPTypeEDSMBody(Protocol):
    body_id: numpy.intc
    edsm_id: numpy.intc
    timestamp_seconds: numpy.intc

    def __getitem__(self, index: int):
        ...

    def __setitem__(self, index: int):
        ...
