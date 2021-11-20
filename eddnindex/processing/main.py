import sys
from typing import Callable

from ..config import Config
from ..types import ProcessorArgs, Writable
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
    conn = DBConnection(config)

    sysdb = EDDNSysDB(
        conn,
        args.edsm_systems,
        args.edsm_bodies or args.edsm_missing_bodies,
        args.eddb_systems,
        config
    )

    timer.time('init')

    reject_file: Writable

    if not args.no_eddn:
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
                        config
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
                        config
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
                        config
                    )

    if args.edsm_systems:
        with open(config.edsm_systems_reject_file, 'at') as reject_file:
            edsmsystems(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )

            edsmsystemswithoutcoords(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )

            edsmsystemswithoutcoordsprepurge(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )

            edsmhiddensystems(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )

            edsmdeletedsystems(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )

    if args.edsm_bodies:
        with open(config.edsm_bodies_reject_file, 'at') as reject_file:
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
                    config
                )

    if args.edsm_missing_bodies:
        edsmmissingbodies(
            sysdb,
            timer,
            updatetitleprogress,
            config
        )

    if args.edsm_stations:
        with open(config.edsm_stations_reject_file, 'at') as reject_file:
            edsmstations(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )

    if args.eddb_systems:
        with open(config.eddb_systems_reject_file, 'at') as reject_file:
            eddbsystems(
                sysdb,
                timer,
                reject_file,
                updatetitleprogress,
                config
            )
