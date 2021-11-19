#!/usr/bin/python3

import sys
import argparse
from eddnindex.config import Config
from eddnindex.timer import Timer
import eddnindex.process as process
from eddnindex.proctitleprogress import updatetitleprogress

def unhandledexception(type, value, traceback):
    sys.__excepthook__(type, value, traceback)
    from bdb import BdbQuit
    if type is not KeyboardInterrupt and type is not BdbQuit:
        import pdb; pdb.post_mortem(traceback)

def dummytitleprogress(text: str):
    pass

def main():
    argparser = argparse.ArgumentParser(description='Index EDDN data into database')
    argparser.add_argument('--reprocess', dest='reprocess', action='store_const', const=True, default=False, help='Reprocess files with unprocessed entries')
    argparser.add_argument('--reprocess-all', dest='reprocessall', action='store_const', const=True, default=False, help='Reprocess all files')
    argparser.add_argument('--nojournal', dest='nojournal', action='store_const', const=True, default=False, help='Skip EDDN Journal messages')
    argparser.add_argument('--market', dest='market', action='store_const', const=True, default=False, help='Process market/shipyard/outfitting messages')
    argparser.add_argument('--navroute', dest='navroute', action='store_const', const=True, default=False, help='Process EDDN NavRoute messages')
    argparser.add_argument('--edsmsys', dest='edsmsys', action='store_const', const=True, default=False, help='Process EDSM systems dump')
    argparser.add_argument('--edsmbodies', dest='edsmbodies', action='store_const', const=True, default=False, help='Process EDSM bodies dump')
    argparser.add_argument('--edsmmissingbodies', dest='edsmmissingbodies', action='store_const', const=True, default=False, help='Process EDSM missing bodies')
    argparser.add_argument('--edsmstations', dest='edsmstations', action='store_const', const=True, default=False, help='Process EDSM stations dump')
    argparser.add_argument('--eddbsys', dest='eddbsys', action='store_const', const=True, default=False, help='Process EDDB systems dump')
    argparser.add_argument('--eddbstations', dest='eddbstations', action='store_const', const=True, default=False, help='Process EDDB stations dump')
    argparser.add_argument('--noeddn', dest='noeddn', action='store_const', const=True, default=False, help='Skip EDDN processing')
    argparser.add_argument('--processtitleprogress', dest='proctitleprogress', action='store_const', const=True, default=False, help='Update process title with progress')
    argparser.add_argument('--configfile', dest='configfile', default=None, help='Configuration file')

    args = argparser.parse_args()

    config = Config('eddn-index-update.ini', args.configfile)

    sys.excepthook = unhandledexception

    titleprogress = updatetitleprogress if args.proctitleprogress else dummytitleprogress

    timer = Timer()

    try:
        process.main(args, config, timer, titleprogress)
    finally:
        timer.printstats()

if __name__ == '__main__':
    main()
