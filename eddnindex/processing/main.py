import sys
from typing import Callable

from ..config import Config
from ..types import Writable
from ..args import ProcessorArgs
from ..eddnsysdb import EDDNSysDB
from ..database import DBConnection
from ..timer import Timer
from ..rejectdata import EDDNRejectData

from .edsmmissingbodies import process \
    as edsmmissingbodies
from .edsmbodies import process \
    as edsmbodies
from .edsmstations import process \
    as edsmstations
from .edsmsystems import process \
    as edsmsystems
from .edsmsystemswithoutcoords import process \
    as edsmsystemswithoutcoords
from .edsmsystemswithoutcoordsprepurge import process \
    as edsmsystemswithoutcoordsprepurge
from .edsmhiddensystems import process \
    as edsmhiddensystems
from .edsmdeletedsystems import process \
    as edsmdeletedsystems
from .eddbsystems import process \
    as eddbsystems
from .eddnjournalfile import process \
    as eddnjournalfile
from .eddnjournalroute import process \
    as eddnjournalroute
from .eddnmarketfile import process \
    as eddnmarketfile


def main(args: ProcessorArgs,
         config: Config,
         timer: Timer,
         updatetitleprogress: Callable[[str], None]
         ):
    conn = DBConnection()
    conn.open(config.database)

    sysdb = EDDNSysDB(
        conn,
        args.edsm_systems,
        args.edsm_bodies or args.edsm_missing_bodies,
        args.eddb_systems,
        config.edsm_systems_cache_file,
        config.edsm_bodies_cache_file,
        config.known_bodies_sheet_uri
    )

    timer.time('init')

    reject_file: Writable

    if not args.no_eddn:
        process_eddn_data(args, config, timer, updatetitleprogress, sysdb)

    if args.edsm_systems:
        process_edsm_systems(config, timer, updatetitleprogress, sysdb)

    if args.edsm_bodies:
        process_edsm_bodies(args, config, timer, updatetitleprogress, sysdb)

    if args.edsm_missing_bodies:
        edsmmissingbodies(
            sysdb,
            timer,
            updatetitleprogress,
            config.edsm_bodies_dir
        )

    if args.edsm_stations:
        with open(config.edsm_stations_reject_file,
                  'at',
                  encoding='utf-8') as reject_file:
            edsmstations(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config.edsm_stations_file
            )

    if args.eddb_systems:
        with open(config.eddb_systems_reject_file,
                  'at',
                  encoding='utf-8') as reject_file:
            eddbsystems(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config.eddb_systems_file
            )


def process_edsm_bodies(args: ProcessorArgs,
                        config: Config,
                        timer: Timer,
                        updatetitleprogress: Callable[[str], None],
                        sysdb: EDDNSysDB
                        ):
    with open(config.edsm_bodies_reject_file,
              'at',
              encoding='utf-8') as reject_file:
        sys.stderr.write('Retrieving EDSM body files from DB\n')
        sys.stderr.flush()
        files = sysdb.getedsmfiles()
        timer.time('init', 0)
        sys.stderr.write('Processing EDSM bodies files\n')
        sys.stderr.flush()

        for filename, fileinfo in files.items():
            edsmbodies(
                    sysdb,
                    filename,
                    fileinfo,
                    args.reprocess,
                    timer,
                    reject_file,
                    updatetitleprogress,
                    config.edsm_bodies_dir,
                    config.edsm_dump_dir
                )


def process_edsm_systems(config: Config,
                         timer: Timer,
                         updatetitleprogress: Callable[[str], None],
                         sysdb: EDDNSysDB
                         ):
    with open(config.edsm_systems_reject_file,
              'at',
              encoding='utf-8') as reject_file:
        edsmsystems(
            sysdb,
            timer,
            reject_file,
            updatetitleprogress,
            config.edsm_systems_file
        )

        edsmsystemswithoutcoords(
            sysdb,
            timer,
            reject_file,
            updatetitleprogress,
            config.edsm_systems_without_coords_file
        )

        edsmsystemswithoutcoordsprepurge(
            sysdb,
            timer,
            reject_file,
            updatetitleprogress,
            config.edsm_systems_without_coords_pre_purge_file
        )

        edsmhiddensystems(
            sysdb,
            timer,
            reject_file,
            updatetitleprogress,
            config.edsm_hidden_systems_file
        )

        edsmdeletedsystems(
            sysdb,
            timer,
            reject_file,
            updatetitleprogress
        )


def process_eddn_data(args: ProcessorArgs,
                      config: Config,
                      timer: Timer,
                      updatetitleprogress: Callable[[str], None],
                      sysdb: EDDNSysDB
                      ):
    reject_file = EDDNRejectData(config.eddn_reject_dir)
    sys.stderr.write('Retrieving EDDN files from DB\n')
    sys.stderr.flush()
    files = sysdb.geteddnfiles()
    timer.time('init', 0)
    sys.stderr.write('Processing EDDN files\n')
    sys.stderr.flush()
    if not args.no_journal:
        for filename, fileinfo in files.items():
            if fileinfo.event_type not in [None, 'NavRoute']:
                eddnjournalfile(
                    sysdb,
                    timer,
                    filename,
                    fileinfo,
                    args.reprocess,
                    args.reprocess_all,
                    reject_file,
                    updatetitleprogress,
                    config.eddn_dir,
                    config.allow_3_0_3_bodies
                )

    if args.nav_route:
        for filename, fileinfo in files.items():
            if fileinfo.event_type in ['NavRoute']:
                eddnjournalroute(
                    sysdb,
                    timer,
                    filename,
                    fileinfo,
                    args.reprocess,
                    reject_file,
                    updatetitleprogress,
                    config.eddn_dir
                )

    if args.market:
        for filename, fileinfo in files.items():
            if fileinfo.event_type is None:
                eddnmarketfile(
                    sysdb,
                    timer,
                    filename,
                    fileinfo,
                    args.reprocess,
                    reject_file,
                    updatetitleprogress,
                    config.eddn_dir
                )
