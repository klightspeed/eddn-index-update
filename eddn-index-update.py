#!/usr/bin/python3

import sys
import argparse
from eddnindex.config import Config
from eddnindex.timer import Timer
import eddnindex.processing.main as process
from eddnindex.proctitleprogress import update_title_progress


def unhandledexception(exctype, excvalue, traceback):
    sys.__excepthook__(exctype, excvalue, traceback)
    from bdb import BdbQuit
    if type is not KeyboardInterrupt and type is not BdbQuit:
        import pdb
        pdb.post_mortem(traceback)


def dummytitleprogress(_: str):
    pass


def main():
    argparser = argparse.ArgumentParser(
        description='Index EDDN data into database'
    )

    argparser.add_argument(
        '--reprocess', dest='reprocess',
        action='store_const', const=True, default=False,
        help='Reprocess files with unprocessed entries'
    )

    argparser.add_argument(
        '--reprocess-all', dest='reprocess_all',
        action='store_const', const=True, default=False,
        help='Reprocess all files'
    )

    argparser.add_argument(
        '--no-journal', dest='nojournal',
        action='store_const', const=True, default=False,
        help='Skip EDDN Journal messages'
    )

    argparser.add_argument(
        '--market', dest='market',
        action='store_const', const=True, default=False,
        help='Process market/shipyard/outfitting messages'
    )

    argparser.add_argument(
        '--nav-route', dest='nav_route',
        action='store_const', const=True, default=False,
        help='Process EDDN NavRoute messages'
    )

    argparser.add_argument(
        '--edsm-systems', dest='edsm_systems',
        action='store_const', const=True, default=False,
        help='Process EDSM systems dump'
    )

    argparser.add_argument(
        '--edsm-bodies', dest='edsm_bodies',
        action='store_const', const=True, default=False,
        help='Process EDSM bodies dump'
    )

    argparser.add_argument(
        '--edsmmissingbodies', dest='edsmmissingbodies',
        action='store_const', const=True, default=False,
        help='Process EDSM missing bodies'
    )

    argparser.add_argument(
        '--edsmstations', dest='edsmstations',
        action='store_const', const=True, default=False,
        help='Process EDSM stations dump'
    )

    argparser.add_argument(
        '--eddbsys', dest='eddbsys',
        action='store_const', const=True, default=False,
        help='Process EDDB systems dump'
    )

    argparser.add_argument(
        '--eddbstations', dest='eddbstations',
        action='store_const', const=True, default=False,
        help='Process EDDB stations dump'
    )

    argparser.add_argument(
        '--noeddn', dest='noeddn',
        action='store_const', const=True, default=False,
        help='Skip EDDN processing'
    )

    argparser.add_argument(
        '--processtitleprogress', dest='proctitleprogress',
        action='store_const', const=True, default=False,
        help='Update process title with progress'
    )

    argparser.add_argument(
        '--configfile', dest='configfile',
        default=None,
        help='Configuration file'
    )

    args = argparser.parse_args()

    config = Config('eddn-index-update.ini', args.configfile)

    sys.excepthook = unhandledexception

    if args.proctitleprogress:
        titleprogress = update_title_progress
    else:
        titleprogress = dummytitleprogress

    timer = Timer()

    try:
        process.main(args, config, timer, titleprogress)
    finally:
        timer.printstats()


if __name__ == '__main__':
    main()
