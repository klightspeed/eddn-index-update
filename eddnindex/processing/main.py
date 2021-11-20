import sys
from typing import Callable

from ..config import Config
from ..types import ProcessorArgs
from ..eddnsysdb import EDDNSysDB
from ..database import DBConnection
from ..timer import Timer
from ..rejectdata import EDDNRejectData

from .edsmmissingbodies import process as edsmmissingbodies
from .edsmbodies import process as edsmbodies
from .edsmstations import process as edsmstations
from .edsmsystems import process as edsmsystems
from .edsmsystemswithoutcoords import process as edsmsystemswithoutcoords
from .edsmsystemswithoutcoordsprepurge import process as edsmsystemswithoutcoordsprepurge
from .edsmhiddensystems import process as edsmhiddensystems
from .edsmdeletedsystems import process as edsmdeletedsystems
from .eddbsystems import process as eddbsystems
from .eddnjournalfile import process as eddnjournalfile
from .eddnjournalroute import process as eddnjournalroute
from .eddnmarketfile import process as eddnmarketfile


def main(args: ProcessorArgs, config: Config, timer: Timer, updatetitleprogress: Callable[[str], None]):
    conn = DBConnection(config)
    sysdb = EDDNSysDB(conn, args.edsmsys, args.edsmbodies or args.edsmmissingbodies, args.eddbsys, config)
    timer.time('init')

    if not args.noeddn:
        reject_file = EDDNRejectData(config.eddn_reject_dir)
        sys.stderr.write('Retrieving EDDN files from DB\n')
        sys.stderr.flush()
        files = sysdb.geteddnfiles()
        timer.time('init', 0)
        sys.stderr.write('Processing EDDN files\n')
        sys.stderr.flush()
        if not args.nojournal:
            for filename, fileinfo in files.items():
                if fileinfo.eventtype is not None and fileinfo.eventtype != 'NavRoute':
                    eddnjournalfile(
                        sysdb,
                        timer,
                        filename,
                        fileinfo,
                        args.reprocess,
                        args.reprocessall,
                        reject_file,
                        updatetitleprogress,
                        config
                    )

        if args.navroute:
            for filename, fileinfo in files.items():
                if fileinfo.eventtype is not None and fileinfo.eventtype == 'NavRoute':
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
                if fileinfo.eventtype is None:
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

    if args.edsmsys:
        with open(config.edsm_systems_reject_file, 'at') as reject_file:
            edsmsystems(sysdb, timer, reject_file, updatetitleprogress, config)
            edsmsystemswithoutcoords(sysdb, timer, reject_file, updatetitleprogress, config)
            edsmsystemswithoutcoordsprepurge(sysdb, timer, reject_file, updatetitleprogress, config)
            edsmhiddensystems(sysdb, timer, reject_file, updatetitleprogress, config)
            edsmdeletedsystems(sysdb, timer, reject_file, updatetitleprogress, config)

    if args.edsmbodies:
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

    if args.edsmmissingbodies:
        edsmmissingbodies(sysdb, timer, updatetitleprogress, config)

    if args.edsmstations:
        with open(config.edsm_stations_reject_file, 'at') as reject_file:
            edsmstations(sysdb, timer, reject_file, updatetitleprogress, config)

    if args.eddbsys:
        with open(config.eddb_systems_reject_file, 'at') as reject_file:
            eddbsystems(sysdb, timer, reject_file, updatetitleprogress, config)
