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

    def load(self, config: dict):
        self.ConnectionType = config['ConnectionType']
        self.Hostname = config['Hostname']
        self.DatabaseName = config['DatabaseName']
        self.Username = config['Username']
        self.Password = config['Password']


class Config(object):

    # Used by DBConnection.open
    database: DatabaseConfig

    # Used by processing.eddnjournalfile.process
    # Used by processing.eddnjournalroute.process
    # Used by processing.eddnmarketfile.process
    eddn_dir: str

    # Used by processing.edsmbodies.process
    edsm_dump_dir: str

    # Used by processing.edsmbodies.process
    # Used by processing.edsmmissingbodies.process
    edsm_bodies_dir: str

    # Used by processing.edsmsystems.process
    edsm_systems_file: str

    # Used by processing.edsmsystemswithoutcoords.process
    edsm_systems_without_coords_file: str

    # Used by processing.edsmsystemswithoutcoordsprepurge.process
    edsm_systems_without_coords_pre_purge_file: str

    # Used by processing.edsmhiddensystems.process
    edsm_hidden_systems_file: str

    # No longer used
    # EDSM bodies file no longer generated
    edsm_bodies_file: str

    # Used by processing.edsmstations.process
    edsm_stations_file: str

    # Used by processing.eddbsystems.process
    eddb_systems_file: str

    # Never used
    eddb_stations_file: str

    # Used by loading.loadedsmsystems
    # Used by EDDNSysDB.saveedsmsyscache
    edsm_systems_cache_file: str

    # Used by loading.loadedsmbodies
    # Used by EDDNSysDB.saveedsmbodycache
    edsm_bodies_cache_file: str

    # Used by processing.main for EDDNRejectData
    eddn_reject_dir: str

    # Used by processing.main for edsm*systems
    edsm_systems_reject_file: str

    # Used by processing.main for edsmbodies
    edsm_bodies_reject_file: str

    # Used by processing.main for edsmstations
    edsm_stations_reject_file: str

    # Used by processing.main for eddbsystems
    eddb_systems_reject_file: str

    # Never used
    eddb_stations_reject_file: str

    # Used by loading.loadknownbodies
    known_bodies_sheet_uri: str

    # Used by processing.eddnjournalfile.process_event
    allow_3_0_3_bodies: bool

    def load(self,
             config_filename: str,
             override_config_filename: Union[str, None] = None
             ):
        config = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation()
        )

        app_config = os.path.join(
            os.path.dirname(sys.argv[0]),
            config_filename
        )

        site_config_dir = appdirs.site_config_dir('eddn-index')
        user_config_dir = appdirs.user_config_dir('eddn-index')
        site_config = os.path.join(site_config_dir, config_filename)
        user_config = os.path.join(user_config_dir, config_filename)

        config_files = [
            app_config,
            site_config,
            user_config,
        ]

        if override_config_filename is not None:
            config_files.append(override_config_filename)

        config.read(config_files)

        self.database = DatabaseConfig()
        self.database.load(dict(config["Database"]))

        paths = config['Paths']
        edsm = config['Paths/EDSM']
        eddb = config['Paths/EDDB']
        cache = config['Paths/Cache']
        rejects = config['Paths/Rejects']
        urls = config['URLs']
        options = config['Options']

        self.eddn_dir = paths["EDDN"]
        self.edsm_dump_dir = paths["EDSMDumps"]
        self.edsm_bodies_dir = paths["EDSMBodies"]

        output_dir = paths["Output"]
        edsm_dump_dir = paths["EDSMDumps"]
        eddb_dir = paths["EDDBDumps"]
        cache_dir = paths["Cache"]

        self.edsm_systems_file = edsm.get(
            'SystemsWithCoordinates',
            os.path.join(
                edsm_dump_dir,
                'systemsWithCoordinates.jsonl.bz2'
            )
        )

        self.edsm_systems_without_coords_file = edsm.get(
            'SystemsWithoutCoordinates',
            os.path.join(
                edsm_dump_dir,
                'systemsWithoutCoordinates.jsonl.bz2'
            )
        )

        self.edsm_systems_without_coords_pre_purge_file = edsm.get(
            'SystemsWithoutCoordinatesPrePurge',
            os.path.join(
                edsm_dump_dir,
                'systemsWithoutCoordinates-2020-09-30.jsonl.bz2'
            )
        )

        self.edsm_hidden_systems_file = edsm.get(
            'HiddenSystems',
            os.path.join(
                edsm_dump_dir,
                'hiddenSystems.jsonl.bz2'
            )
        )

        self.edsm_bodies_file = edsm.get(
            'Bodies',
            os.path.join(
                edsm_dump_dir,
                'bodies.jsonl.bz2'
            )
        )

        self.edsm_stations_file = edsm.get(
            'Stations',
            os.path.join(
                edsm_dump_dir,
                'stations.json.gz'
            )
        )

        self.eddb_systems_file = eddb.get(
            'Systems',
            os.path.join(
                eddb_dir,
                'systems.csv.bz2'
            )
        )

        self.eddb_stations_file = eddb.get(
            'Stations',
            os.path.join(
                eddb_dir,
                'stations.jsonl'
            )
        )

        self.edsm_systems_cache_file = cache.get(
            'EDSMSystems',
            os.path.join(
                cache_dir,
                'edsmsys-index-update-syscache.bin'
            )
        )

        self.edsm_bodies_cache_file = cache.get(
            'EDSMBodies',
            os.path.join(
                cache_dir,
                'edsmbody-index-update-bodycache.bin'
            )
        )

        self.eddn_reject_dir = rejects.get(
            'EDDN',
            os.path.join(
                output_dir,
                'eddn-index-update-reject'
            )
        )

        self.edsm_systems_reject_file = rejects.get(
            'EDSMSystems',
            os.path.join(
                output_dir,
                'edsmsys-index-update-reject.jsonl'
            )
        )

        self.edsm_bodies_reject_file = rejects.get(
            'EDSMBodies',
            os.path.join(
                output_dir,
                'edsmbodies-index-update-reject.jsonl'
            )
        )

        self.edsm_stations_reject_file = rejects.get(
            'EDSMStations',
            os.path.join(
                output_dir,
                'edsmstations-index-update-reject.jsonl'
            )
        )

        self.eddb_systems_reject_file = rejects.get(
            'EDDBSystems',
            os.path.join(
                output_dir,
                'eddbsys-index-update-reject.jsonl'
            )
        )

        self.eddb_stations_reject_file = rejects.get(
            'EDDBStations',
            os.path.join(
                output_dir,
                'eddbstations-index-update-reject.jsonl'
            )
        )

        self.known_bodies_sheet_uri = urls.get(
            'KnownBodies',
            'https://docs.google.com/spreadsheets/d/e/2PACX-1vR9lEav_Bs8rZGRtwcwuOwQ2hIoiNJ_PWYAEgXk7E3Y-UD0r6uER04y4VoQxFAAdjMS4oipPyySoC3t/pub?gid=711269421&single=true&output=tsv'  # noqa: E501
        )

        self.allow_3_0_3_bodies = options.getboolean(
            'Allow-3.0.3-Bodies',
            True
        )
