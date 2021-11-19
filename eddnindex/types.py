from datetime import datetime
from typing import NamedTuple, TypedDict, Protocol
import numpy
import numpy.typing

class EDDNSystem(NamedTuple):
    id: int
    id64: int
    name: str
    x: float
    y: float
    z: float
    hascoords: bool

class EDDNStation(NamedTuple):
    id: int
    marketid: int
    name: str
    systemname: str
    systemid: int
    type: str
    loctype: str
    body: str
    bodyid: int
    isrejected: bool
    validfrom: datetime
    validuntil: datetime
    test: bool

class EDDNFile(NamedTuple):
    id: int
    name: str
    date: datetime
    eventtype: str
    linecount: int
    stnlinecount: int
    infolinecount: int
    factionlinecount: int
    navroutesystemcount: int
    populatedlinecount: int
    stationlinecount: int
    routesystemcount: int
    test: bool

class EDDNRegion(NamedTuple):
    id: int
    name: str
    x0: float
    y0: float
    z0: float
    sizex: float
    sizey: float
    sizez: float
    regionaddr: int
    isharegion: bool

class EDDNBody(NamedTuple):
    id: int
    name: str
    systemname: str
    systemid: int
    bodyid: int
    category: int
    argofperiapsis: float
    validfrom: datetime
    validuntil: datetime
    isrejected: bool

class EDDNFaction(NamedTuple):
    id: int
    name: str
    government: str
    allegiance: str

class EDSMFile(NamedTuple):
    id: int
    name: str
    date: datetime
    linecount: int
    bodylinecount: int
    comprsize: int

class EDSMBody(TypedDict):
    id: int
    id64: int
    bodyId: int
    name: str
    systemName: str
    systemId: int

class Writable(Protocol):
    def write(self, text: str):
        pass

class ProcessorArgs(Protocol):
    reprocess: bool
    reprocessall: bool
    nojournal: bool
    market: bool
    navroute: bool
    edsmsys: bool
    edsmbodies: bool
    edsmmissingbodies: bool
    edsmstations: bool
    eddbsys: bool
    eddbstations: bool
    noeddn: bool

NumpyEDSMSystem = numpy.dtype([('sysid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4'), ('hascoords', 'i1'), ('ishidden', 'i1'), ('isdeleted', 'i1'), ('processed', 'i1')])
NumpyEDDBSystem = numpy.dtype([('sysid', '<i4'), ('eddbid', '<i4'), ('timestampseconds', '<i4')])
NumpyEDSMBody = numpy.dtype([('bodyid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4')])