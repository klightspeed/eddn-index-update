import configparser
from typing import Union
import appdirs
import os.path
import sys

class DatabaseConfig(object):
    ConnectionType: str
    Hostname: str
    DatabaseName: str
    Username: str
    Password: str

    def __init__(self, config: dict):
        self.ConnectionType = config['ConnectionType']
        self.Hostname = config['Hostname']
        self.DatabaseName = config['DatabaseName']
        self.Username = config['Username']
        self.Password = config['Password']

class Config(object):
    database: DatabaseConfig
    outdir: str
    eddndir: str
    edsmdumpdir: str
    edsmbodiesdir: str
    eddbdir: str
    cachedir: str

    edsmsysfile: str
    edsmsyswithoutcoordsfile: str
    edsmsyswithoutcoordsprepurgefile: str
    edsmhiddensysfile: str
    edsmbodiesfile: str
    edsmstationsfile: str

    eddbsysfile: str
    eddbstationsfile: str

    edsmsyscachefile: str
    edsmbodycachefile: str

    eddnrejectdir: str
    edsmsysrejectfile: str
    edsmbodiesrejectfile: str
    edsmstationsrejectfile: str
    eddbsysrejectfile: str
    eddbstationsrejectfile: str

    knownbodiessheeturi: str

    allow303bodies: bool
    
    def __init__(self, configfilename: str, overrideconfig: Union[str, None] = None):
        config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())

        appconfig = os.path.join(os.path.dirname(sys.argv[0]), configfilename)
        siteconfig = os.path.join(appdirs.site_config_dir, configfilename)
        userconfig = os.path.join(appdirs.user_config_dir, configfilename)

        config.read([appconfig, siteconfig, userconfig, overrideconfig])

        self.database = DatabaseConfig(config["Database"])

        paths = config['Paths']
        edsm = config['EDSM']
        eddb = config['EDDB']
        cache = config['Cache']
        rejects = config['Rejects']
        urls = config['URLs']
        options = config['Options']

        self.outdir = paths["Output"]
        self.eddndir = paths["EDDN"]
        self.edsmdumpdir = paths["EDSMDumps"]
        self.edsmbodiesdir = paths["EDSMBodies"]
        self.eddbdir = paths["EDDBDumps"]
        self.cachedir = paths["Cache"]

        self.edsmsysfile = edsm.get('SystemsWithCoordinates', os.path.join(self.edsmdumpdir, 'systemsWithCoordinates.jsonl.bz2'))
        self.edsmsyswithoutcoordsfile = edsm.get('SystemsWithoutCoordinates', os.path.join(self.edsmdumpdir, 'systemsWithoutCoordinates.jsonl.bz2'))
        self.edsmsyswithoutcoordsprepurgefile = edsm.get('SystemsWithoutCoordinatesPrePurge', os.path.join(self.edsmdumpdir, 'systemsWithoutCoordinates-2020-09-30.jsonl.bz2'))
        self.edsmhiddensysfile = edsm.get('HiddenSystems', os.path.join(self.edsmdumpdir, 'hiddenSystems.jsonl.bz2'))
        self.edsmbodiesfile = edsm.get('Bodies', os.path.join(self.edsmdumpdir, 'bodies.jsonl.bz2'))
        self.edsmstationsfile = edsm.get('Stations', os.path.join(self.edsmdumpdir, 'stations.json.gz'))

        self.eddbsysfile = eddb.get('Systems', os.path.join(self.eddbdir, 'systems.csv.bz2'))
        self.eddbstationsfile = eddb.get('Stations', os.path.join(self.eddbdir, 'stations.jsonl'))

        self.edsmsyscachefile = cache.get('EDSMSystems', os.path.join(self.cachedir, 'edsmsys-index-update-syscache.bin'))
        self.edsmbodycachefile = cache.get('EDSMBodies', os.path.join(self.cachedir, 'edsmbody-index-update-bodycache.bin'))

        self.eddnrejectdir = rejects.get('EDDN', os.path.join(self.outdir, 'eddn-index-update-reject'))
        self.edsmsysrejectfile = rejects.get('EDSMSystems', os.path.join(self.outdir, 'edsmsys-index-update-reject.jsonl'))
        self.edsmbodiesrejectfile = rejects.get('EDSMBodies', os.path.join(self.outdir, 'edsmbodies-index-update-reject.jsonl'))
        self.edsmstationsrejectfile = rejects.get('EDSMStations', os.path.join(self.outdir, 'edsmstations-index-update-reject.jsonl'))
        self.eddbsysrejectfile = rejects.get('EDDBSystems', os.path.join(self.outdir, 'eddbsys-index-update-reject.jsonl'))
        self.eddbstationsrejectfile = rejects.get('EDDBStations', os.path.join(self.outdir, 'eddbstations-index-update-reject.jsonl'))

        self.knownbodiessheeturi = urls.get('KnownBodies', 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR9lEav_Bs8rZGRtwcwuOwQ2hIoiNJ_PWYAEgXk7E3Y-UD0r6uER04y4VoQxFAAdjMS4oipPyySoC3t/pub?gid=711269421&single=true&output=tsv')

        self.allow303bodies = options.getboolean('Allow-3.0.3-Bodies', True)


