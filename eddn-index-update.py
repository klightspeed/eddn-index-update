#!/usr/bin/python3

import os
import os.path
import sys
import json
import bz2
import gzip
import edts.edtslib.system as edtslib_system
import edts.edtslib.id64data as edtslib_id64data
import glob
import math
from functools import lru_cache
from collections import namedtuple
from timeit import default_timer as timer
import re
import numpy
import numpy.core.records
from datetime import datetime, timedelta
import time
import argparse
import csv
import eddnindex.config as config
import eddnindex.mysqlutils as mysql
import urllib.request
import urllib.error
from setproctitle import getproctitle, setproctitle

eddndir = config.rootdir + '/EDDN/data'
edsmdumpdir = config.rootdir + '/EDSM/dumps'
edsmbodiesdir = config.rootdir + '/EDSM/bodies'
eddbdir = config.rootdir + '/EDDB/dumps'
edsmsysfile = edsmdumpdir + '/systemsWithCoordinates.jsonl.bz2'
edsmsyswithoutcoordsfile = edsmdumpdir + '/systemsWithoutCoordinates.jsonl.bz2'
edsmsyswithoutcoordsprepurgefile = edsmdumpdir + '/systemsWithoutCoordinates-2020-09-30.jsonl.bz2'
edsmhiddensysfile = edsmdumpdir + '/hiddenSystems.jsonl.bz2'
edsmbodiesfile = edsmdumpdir + '/bodies.jsonl.bz2'
edsmstationsfile = edsmdumpdir + '/stations.json.gz'
eddbsysfile = eddbdir + '/systems.csv.bz2'
eddbstationsfile = eddbdir + '/stations.jsonl'
edsmsyscachefile = '/srv/cache/eddata/edsmsys-index-update-syscache.bin'
edsmbodycachefile = '/srv/cache/eddata/edsmbody-index-update-bodycache.bin'
eddnrejectfile = config.outdir + '/eddn-index-update-reject.jsonl'
eddnrejectdir = config.outdir + '/eddn-index-update-reject'
edsmsysrejectfile = config.outdir + '/edsmsys-index-update-reject.jsonl'
edsmbodiesrejectfile = config.outdir + '/edsmbodies-index-update-reject.jsonl'
edsmstationsrejectfile = config.outdir + '/edsmstations-index-update-reject.jsonl'
eddbsysrejectfile = config.outdir + '/eddbsys-index-update-reject.jsonl'
eddbstationsrejectfile = config.outdir + '/eddbstations-index-update-reject.jsonl'

knownbodiessheeturi = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR9lEav_Bs8rZGRtwcwuOwQ2hIoiNJ_PWYAEgXk7E3Y-UD0r6uER04y4VoQxFAAdjMS4oipPyySoC3t/pub?gid=711269421&single=true&output=tsv'

allow303bodies = True

revpgsysre = re.compile('^([0-9]+)(|-[0-9]+)([a-h]) ([A-Z])-([A-Z])([A-Z]) ((rotceS|noigeR) [A-Za-z0-9.\' -]+|[1-6][a-z]+[A-Z]|ZCI|[a-z]+[A-Z](| [a-z]+[A-Z]))$')
pgsysre = re.compile('^([A-Za-z0-9.()\' -]+?) ([A-Z][A-Z]-[A-Z]) ([a-h])(?:([0-9]+)-|)([0-9]+)$')
pgbodyre = re.compile(
    '''
      ^
      (?:|[ ](?P<stars>A?B?C?D?E?F?G?H?I?J?K?L?M?N?O?))
      (?:
       |[ ](?P<nebula>Nebula)
       |[ ](?P<belt>[A-Z])[ ]Belt(?:|[ ]Cluster[ ](?P<cluster>[1-9][0-9]?))
       |[ ]Comet[ ](?P<stellarcomet>[1-9][0-9]?)
       |[ ](?P<planet>[1-9][0-9]?(?:[+][1-9][0-9]?)*)
        (?:
         |[ ](?P<planetring>[A-Z])[ ]Ring
         |[ ]Comet[ ](?P<planetcomet>[1-9][0-9]?)
         |[ ](?P<moon1>[a-z](?:[+][a-z])*)
          (?:
           |[ ](?P<moon1ring>[A-Z])[ ]Ring
           |[ ]Comet[ ](?P<moon1comet>[1-9][0-9]?)
           |[ ](?P<moon2>[a-z](?:[+][a-z])*)
            (?:
             |[ ](?P<moon2ring>[A-Z])[ ]Ring
             |[ ]Comet[ ](?P<moon2comet>[1-9][0-9]?)
             |[ ](?P<moon3>[a-z])
            )
          )
        )
      )
      $
   ''', re.VERBOSE)
pgsysbodyre = re.compile(
    '''
      ^
      (?P<sysname>.+?)
      (?P<desig>|[ ](?P<stars>A?B?C?D?E?F?G?H?I?J?K?L?M?N?O?))
      (?:
       |[ ](?P<nebula>Nebula)
       |[ ](?P<belt>[A-Z])[ ]Belt(?:|[ ]Cluster[ ](?P<cluster>[1-9][0-9]?))
       |[ ]Comet[ ](?P<stellarcomet>[1-9][0-9]?)
       |[ ](?P<planet>[1-9][0-9]?(?:[+][1-9][0-9]?)*)
        (?:
         |[ ](?P<planetring>[A-Z])[ ]Ring
         |[ ]Comet[ ](?P<planetcomet>[1-9][0-9]?)
         |[ ](?P<moon1>[a-z](?:[+][a-z])*)
          (?:
           |[ ](?P<moon1ring>[A-Z])[ ]Ring
           |[ ]Comet[ ](?P<moon1comet>[1-9][0-9]?)
           |[ ](?P<moon2>[a-z](?:[+][a-z])*)
            (?:
             |[ ](?P<moon2ring>[A-Z])[ ]Ring
             |[ ]Comet[ ](?P<moon2comet>[1-9][0-9]?)
             |[ ](?P<moon3>[a-z])
            )
          )
        )
      )
      $
   ''', re.VERBOSE)


timestampre = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-5][0-9]:[0-5][0-9])')
carriernamere = re.compile('^[A-Z0-9]{3}-[A-Z0-9]{3}$')

tsbasedate = datetime.strptime('2014-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
megashipweek0 = datetime.strptime('2016-10-20 07:00:00', '%Y-%m-%d %H:%M:%S')
ed300date = datetime.strptime('2018-02-27 15:00:00', '%Y-%m-%d %H:%M:%S')
ed303date = datetime.strptime('2018-03-19 10:00:00', '%Y-%m-%d %H:%M:%S')
ed304date = datetime.strptime('2018-03-27 16:00:00', '%Y-%m-%d %H:%M:%S')
ed330date = datetime.strptime('2018-12-11 16:00:00', '%Y-%m-%d %H:%M:%S')
ed332date = datetime.strptime('2019-01-17 10:00:00', '%Y-%m-%d %H:%M:%S')
ed370date = datetime.strptime('2020-06-09 10:00:00', '%Y-%m-%d %H:%M:%S')
ed400date = datetime.strptime('2021-05-19 10:00:00', '%Y-%m-%d %H:%M:%S')

EDDNSystem = namedtuple('EDDNSystem', ['id', 'id64', 'name', 'x', 'y', 'z', 'hascoords'])
EDDNStation = namedtuple('EDDNStation', ['id', 'marketid', 'name', 'systemname', 'systemid', 'type', 'loctype', 'body', 'bodyid', 'isrejected', 'validfrom', 'validuntil', 'test'])
EDDNFile = namedtuple('EDDNFile', ['id', 'name', 'date', 'eventtype', 'linecount', 'stnlinecount', 'infolinecount', 'factionlinecount', 'navroutesystemcount', 'marketitemcount', 'populatedlinecount', 'stationlinecount', 'routesystemcount', 'marketitemlinecount', 'test'])
EDDNRegion = namedtuple('EDDNRegion', ['id', 'name', 'x0', 'y0', 'z0', 'sizex', 'sizey', 'sizez', 'regionaddr', 'isharegion'])
EDDNBody = namedtuple('EDDNBody', ['id', 'name', 'systemname', 'systemid', 'bodyid', 'category', 'argofperiapsis', 'validfrom', 'validuntil', 'isrejected'])
EDDNFaction = namedtuple('EDDNFaction', ['id', 'name', 'government', 'allegiance'])
EDDNMarketStation = namedtuple('EDDNMarketStation', ['id', 'marketid', 'name', 'systemname', 'isrejected', 'validfrom', 'validuntil'])
EDDNMarketItem = namedtuple('EDDNMarketItem', ['id', 'name', 'type'])
EDSMFile = namedtuple('EDSMFile', ['id', 'name', 'date', 'linecount', 'bodylinecount', 'comprsize'])

argparser = argparse.ArgumentParser(description='Index EDDN data into database')
argparser.add_argument('--reprocess', dest='reprocess', action='store_const', const=True, default=False, help='Reprocess files with unprocessed entries')
argparser.add_argument('--reprocess-all', dest='reprocessall', action='store_const', const=True, default=False, help='Reprocess all files')
argparser.add_argument('--nojournal', dest='nojournal', action='store_const', const=True, default=False, help='Skip EDDN Journal messages')
argparser.add_argument('--market', dest='market', action='store_const', const=True, default=False, help='Process market/shipyard/outfitting messages')
argparser.add_argument('--navroute', dest='navroute', action='store_const', const=True, default=False, help='Process EDDN NavRoute messages')
argparser.add_argument('--fcmaterials', dest='fcmaterials', action='store_const', const=True, default=False, help='Process EDDN Fleet Carrier Materials messages')
argparser.add_argument('--edsmsys', dest='edsmsys', action='store_const', const=True, default=False, help='Process EDSM systems dump')
argparser.add_argument('--edsmbodies', dest='edsmbodies', action='store_const', const=True, default=False, help='Process EDSM bodies dump')
argparser.add_argument('--edsmmissingbodies', dest='edsmmissingbodies', action='store_const', const=True, default=False, help='Process EDSM missing bodies')
argparser.add_argument('--edsmstations', dest='edsmstations', action='store_const', const=True, default=False, help='Process EDSM stations dump')
argparser.add_argument('--eddbsys', dest='eddbsys', action='store_const', const=True, default=False, help='Process EDDB systems dump')
argparser.add_argument('--eddbstations', dest='eddbstations', action='store_const', const=True, default=False, help='Process EDDB stations dump')
argparser.add_argument('--noeddn', dest='noeddn', action='store_const', const=True, default=False, help='Skip EDDN processing')
argparser.add_argument('--processtitleprogress', dest='proctitleprogress', action='store_const', const=True, default=False, help='Update process title with progress')

EDSMStationTypes = {
    'Asteroid base': 'AsteroidBase',
    'Coriolis Starport': 'Coriolis',
    'Mega ship': 'MegaShip',
    'Ocellus Starport': 'Ocellus',
    'Orbis Starport': 'Orbis',
    'Outpost': 'Outpost',
    'Planetary Outpost': 'CraterOutpost',
    'Planetary Port': 'CraterPort',
    'Odyssey Settlement': 'OnFootSettlement',
}

proctitleprogresspos = None

def updatetitleprogress(progress):
    global proctitleprogresspos

    title = getproctitle()

    if proctitleprogresspos is None:
        proctitleprogresspos = title.find('--processtitleprogress')

    if proctitleprogresspos > 0:
        title = title[0:proctitleprogresspos] + '[{0:20.20s}]'.format(progress) + title[proctitleprogresspos + 22:]
        setproctitle(title)

class EDDNRejectData(object):
    def __init__(self, rejectdir):
        self.rejectdir = rejectdir

    @lru_cache(maxsize=256)
    def open(self, filename):
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return open(filename, 'at', encoding='utf-8')

    def write(self, jsonstr):
        j = json.loads(jsonstr)
        rejectfile = self.rejectdir

        if 'rejectReason' in j and j['rejectReason'] is not None:
            reason = j['rejectReason']
            if reason.startswith('Unable to resolve system '):
                reason = 'Unable to resolve system'

            rejectfile += '/' + reason
        else:
            rejectfile += '/None'

        if 'header' in j and 'gatewayTimestamp' in j['header']:
            date = j['header']['gatewayTimestamp'][:10]
            rejectfile += '/' + date

        rejectfile += '.jsonl'

        outfile = self.open(rejectfile)
        outfile.write(jsonstr)
        outfile.flush()

class EDDNSysDB(object):
    def __init__(self, conn, loadedsmsys, loadedsmbodies, loadeddbsys):
        self.conn = conn
        self.regions = {}
        self.regionaddrs = {}
        self.namedsystems = {}
        self.namedbodies = {}
        self.parentsets = {}
        self.bodydesigs = {}
        self.software = {}
        self.factions = {}
        self.marketitems = {}
        self.knownbodies = {}
        self.edsmsysids = None
        self.edsmbodyids = None
        self.eddbsysids = None

        try:
            timer = Timer({
                'sql',
                'sqlname',
                'sqlregion',
                'sqlbodyname',
                'sqledsmsys',
                'sqledsmbody',
                'sqleddbsys',
                'sqlparents',
                'sqlsoftware',
                'sqlbodydesigs',
                'sqlfactions',
                'sqlmarketitems',
                'load',
                'loadname',
                'loadregion',
                'loadbodyname',
                'loadedsmsys',
                'loadedsmbody',
                'loadeddbsys',
                'loadparents',
                'loadsoftware',
                'loadbodydesigs',
                'loadfactions',
                'loadmarketitems',
                'loadknownbodies'
            })
            self.loadregions(conn, timer)
            self.loadnamedsystems(conn, timer)
            self.loadnamedbodies(conn, timer)
            self.loadparentsets(conn, timer)
            self.loadsoftware(conn, timer)
            self.loadbodydesigs(conn, timer)
            self.loadfactions(conn, timer)
            self.loadmarketitems(conn)
            self.loadknownbodies(timer)

            if loadedsmsys or loadedsmbodies:
                self.loadedsmsystems(conn, timer)

            if loadedsmbodies:
                self.loadedsmbodies(conn, timer)
            
            if loadeddbsys:
                self.loadeddbsystems(conn, timer)

        finally:
            timer.printstats()

    def loadedsmsystems(self, conn, timer):
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT MAX(EdsmId) FROM Systems_EDSM')
        row = c.fetchone()
        maxedsmsysid = row[0]

        timer.time('sql')

        if maxedsmsysid:
            sys.stderr.write('Loading EDSM System IDs\n')
            if os.path.exists(edsmsyscachefile):
                with open(edsmsyscachefile, 'rb') as f:
                    edsmsysarray = numpy.fromfile(f, dtype=[('sysid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4'), ('hascoords', 'i1'), ('ishidden', 'i1'), ('isdeleted', 'i1'), ('processed', 'i1')])

                if len(edsmsysarray) > maxedsmsysid:
                    if len(edsmsysarray) < maxedsmsysid + 524288:
                        edsmsysarray = numpy.resize(edsmsysarray, maxedsmsysid + 1048576)
                    self.edsmsysids = edsmsysarray.view(numpy.core.records.recarray)

                timer.time('loadedsmsys', len(edsmsysarray))

            if self.edsmsysids is None:
                c = mysql.makestreamingcursor(conn)
                c.execute('SELECT Id, EdsmId, TimestampSeconds, HasCoords, IsHidden, IsDeleted FROM Systems_EDSM')

                edsmsysarray = numpy.zeros(maxedsmsysid + 1048576, dtype=[('sysid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4'), ('hascoords', 'i1'), ('ishidden', 'i1'), ('isdeleted', 'i1'), ('processed', 'i1')])
                self.edsmsysids = edsmsysarray.view(numpy.core.records.recarray)
                timer.time('sql')

                i = 0
                maxedsmid = 0
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqledsmsys', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        edsmid = row[1]
                        rec = edsmsysarray[edsmid]
                        rec[0] = row[0]
                        rec[1] = edsmid
                        rec[2] = row[2]
                        rec[3] = 1 if row[3] == b'\x01' else 0
                        rec[4] = 1 if row[4] == b'\x01' else 0
                        rec[5] = 1 if row[5] == b'\x01' else 0
                        rec[6] = 3
                        i += 1
                        if edsmid > maxedsmid:
                            maxedsmid = edsmid
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxedsmsysid, maxedsmid))
                    sys.stderr.flush()
                    timer.time('loadedsmsys', len(rows))
                sys.stderr.write('  {0} / {1}\n'.format(i, maxedsmsysid))
                with open(edsmsyscachefile + '.tmp', 'wb') as f:
                    self.edsmsysids.tofile(f)
                os.rename(edsmsyscachefile + '.tmp', edsmsyscachefile)

    def loadeddbsystems(self, conn, timer):
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT MAX(EddbId) FROM Systems_EDDB')
        row = c.fetchone()
        maxeddbsysid = row[0]

        timer.time('sql')

        if maxeddbsysid:
            sys.stderr.write('Loading EDDB System IDs\n')
            c = mysql.makestreamingcursor(conn)
            c.execute('SELECT Id, EddbId, TimestampSeconds FROM Systems_EDDB')

            eddbsysarray = numpy.zeros(maxeddbsysid + 1048576, dtype=[('sysid', '<i4'), ('eddbid', '<i4'), ('timestampseconds', '<i4')])
            self.eddbsysids = eddbsysarray.view(numpy.core.records.recarray)
            timer.time('sql')

            i = 0
            maxeddbid = 0
            while True:
                rows = c.fetchmany(10000)
                timer.time('sqleddbsys', len(rows))
                if len(rows) == 0:
                    break
                for row in rows:
                    eddbid = row[1]
                    rec = eddbsysarray[eddbid]
                    rec[0] = row[0]
                    rec[1] = eddbid
                    rec[2] = row[2]
                    i += 1
                    if eddbid > maxeddbid:
                        maxeddbid = eddbid
                sys.stderr.write('.')
                if (i % 640000) == 0:
                    sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxeddbsysid, maxeddbid))
                sys.stderr.flush()
                timer.time('loadeddbsys', len(rows))
            sys.stderr.write('  {0} / {1}\n'.format(i, maxeddbsysid))

    def loadedsmbodies(self, conn, timer):
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT MAX(EdsmId) FROM SystemBodies_EDSM')
        row = c.fetchone()
        maxedsmbodyid = row[0]

        timer.time('sql')

        if maxedsmbodyid:
            sys.stderr.write('Loading EDSM Body IDs\n')
            if os.path.exists(edsmbodycachefile):
                with open(edsmbodycachefile, 'rb') as f:
                    edsmbodyarray = numpy.fromfile(f, dtype=[('bodyid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4')])

                if len(edsmbodyarray) > maxedsmbodyid:
                    if len(edsmbodyarray) < maxedsmbodyid + 524288:
                        edsmbodyarray = numpy.resize(edsmbodyarray, maxedsmbodyid + 1048576)
                    self.edsmbodyids = edsmbodyarray.view(numpy.core.records.recarray)

                timer.time('loadedsmbody', len(edsmbodyarray))

            if self.edsmbodyids is None:
                c = mysql.makestreamingcursor(conn)
                c.execute('SELECT Id, EdsmId, TimestampSeconds FROM SystemBodies_EDSM')

                edsmbodyarray = numpy.zeros(maxedsmbodyid + 1048576, dtype=[('bodyid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4')])
                self.edsmbodyids = edsmbodyarray.view(numpy.core.records.recarray)
                timer.time('sql')

                i = 0
                maxedsmid = 0
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqledsmbody', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        edsmid = row[1]
                        rec = edsmbodyarray[edsmid]
                        rec[0] = row[0]
                        rec[1] = edsmid
                        rec[2] = row[2]
                        i += 1
                        if edsmid > maxedsmid:
                            maxedsmid = edsmid
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxedsmbodyid, maxedsmid))
                    sys.stderr.flush()
                    timer.time('loadedsmbody', len(rows))
                sys.stderr.write('  {0} / {1}\n'.format(i, maxedsmbodyid))

    def loadparentsets(self, conn, timer):
        sys.stderr.write('Loading Parent Sets\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, BodyID, ParentJson FROM ParentSets')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlparents', len(rows))
        for row in rows:
            self.parentsets[(int(row[1]),row[2])] = int(row[0])
        timer.time('loadparents', len(rows))

    def loadsoftware(self, conn, timer):
        sys.stderr.write('Loading Software\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, Name FROM Software')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlsoftware', len(rows))
        for row in rows:
            self.software[row[1]] = int(row[0])
        timer.time('loadsoftware', len(rows))

    def loadbodydesigs(self, conn, timer):
        sys.stderr.write('Loading Body Designations\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, BodyDesignation FROM SystemBodyDesignations WHERE IsUsed = 1')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlbodydesigs', len(rows))
        for row in rows:
            self.bodydesigs[row[1]] = int(row[0])
        timer.time('loadbodydesigs', len(rows))

    def loadnamedbodies(self, conn, timer):
        sys.stderr.write('Loading Named Bodies\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT nb.Id, nb.BodyName, nb.SystemName, nb.SystemId, nb.BodyID, nb.BodyCategory, nb.ArgOfPeriapsis, nb.ValidFrom, nb.ValidUntil, nb.IsRejected FROM SystemBodyNames nb JOIN SystemBodies_Named sbn ON sbn.Id = nb.Id')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlbodyname', len(rows))
        for row in rows:
            bi = EDDNBody(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
            if bi.systemid not in self.namedbodies:
                self.namedbodies[bi.systemid] = {}
            snb = self.namedbodies[bi.systemid]
            if bi.name not in snb:
                snb[bi.name] = bi
            elif type(snb[bi.name]) is not list:
                snb[bi.name] = [snb[bi.name]]
                snb[bi.name] += [bi]
            else:
                snb[bi.name] += [bi]

        timer.time('loadbodyname')

    def loadnamedsystems(self, conn, timer):
        sys.stderr.write('Loading Named Systems\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns JOIN Systems_Named sn ON sn.Id = ns.Id')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlname', len(rows))

        for row in rows:
            si = EDDNSystem(row[0], row[1], row[2], row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 and row[4] != 0 and row[5] != 0)
            if si.name not in self.namedsystems:
                self.namedsystems[si.name] = si
            elif type(self.namedsystems[si.name]) is not list:
                self.namedsystems[si.name] = [self.namedsystems[si.name]]
                self.namedsystems[si.name] += [si]
            else:
                self.namedsystems[si.name] += [si]

        timer.time('loadname', len(rows))

    def loadregions(self, conn, timer):
        sys.stderr.write('Loading Regions\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, Name, X0, Y0, Z0, SizeX, SizeY, SizeZ, RegionAddress, IsHARegion FROM Regions')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlregion', len(rows))
        for row in rows:
            ri = EDDNRegion(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9] == b'\x01')
            self.regions[ri.name.lower()] = ri
            if ri.regionaddr is not None:
                self.regionaddrs[ri.regionaddr] = ri

        timer.time('loadregion', len(rows))

    def loadfactions(self, conn, timer):
        sys.stderr.write('Loading Factions\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, Name, Government, Allegiance FROM Factions')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlfactions')

        for row in rows:
            fi = EDDNFaction(row[0], row[1], row[2], row[3])
            if fi.name not in self.factions:
                self.factions[fi.name] = fi
            elif type(self.factions[fi.name]) is not list:
                self.factions[fi.name] = [self.factions[fi.name]]
                self.factions[fi.name] += [fi]
            else:
                self.factions[fi.name] += [fi]

        timer.time('loadfactions')

    def loadmarketitems(self, conn, timer):
        sys.stderr.write('Loading Market Items\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, Name, Type FROM MarketItems')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlmarketitems')

        for row in rows:
            self.marketitems[(row[1], row[2])] = EDDNMarketItem(row[0], row[1], row[2])

        timer.time('loadmarketitems')

    def loadknownbodies(self, timer):
        sys.stderr.write('Loading Known Bodies\n')
        knownbodies = {}

        with urllib.request.urlopen(knownbodiessheeturi) as f:
            for line in f:
                fields = line.decode('utf-8').strip().split('\t')
                if len(fields) >= 7 and fields[0] != 'SystemAddress' and fields[0] != '' and fields[3] != '' and fields[4] != '' and fields[6] != '':
                    sysaddr = int(fields[0])
                    sysname = fields[2]
                    bodyid = int(fields[3])
                    bodyname = fields[4]
                    bodydesig = fields[6]
                    desig = bodydesig[len(sysname):]

                    if desig not in self.bodydesigs:
                        cursor = self.conn.cursor()
                        cursor.execute('SELECT Id, BodyDesignation FROM SystemBodyDesignations WHERE BodyDesignation = %s', (desig,))
                        row = cursor.fetchone()
                        
                        if row and row[1] == desig:
                            desigid = int(row[0])
                            self.bodydesigs[desig] = desigid
                            cursor = self.conn.cursor()
                            cursor.execute('UPDATE SystemBodyDesignations SET IsUsed = 1 WHERE Id = %s', (desigid,))

                    if desig in self.bodydesigs:
                        desigid = self.bodydesigs[desig]
                        if sysname not in knownbodies:
                            knownbodies[sysname] = {}
                        sysknownbodies = knownbodies[sysname]
                        if bodyname not in sysknownbodies:
                            sysknownbodies[bodyname] = []
                        sysknownbodies[bodyname] += [{ 'SystemAddress': sysaddr, 'SystemName': sysname, 'BodyID': bodyid, 'BodyName': bodyname, 'BodyDesignation': bodydesig, 'BodyDesignationId': desigid }]
                    else:
                        import pdb; pdb.set_trace()

        self.knownbodies = knownbodies
        timer.time('loadknownbodies')

    def commit(self):
        self.conn.commit()

    def findmodsysaddr(self, part, modsysaddr, sysname, starpos, start, end, search):
        arr = part.view(numpy.ndarray)
        sysaddr = self.modsysaddrtosysaddr(modsysaddr)
        xstart = arr[start:end].searchsorted(numpy.array(search, arr.dtype)) + start
        if xstart >= start and xstart < end and arr[xstart][0] == modsysaddr:
            return part[xstart:min(xstart+10,end)]

        #sys.stderr.write('Cannot find system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, starpos[0], starpos[1], starpos[2]))
        #sys.stderr.writelines(['{0}\n'.format(s) for s in ])
        #raise ValueError('Unable to find system')
        return None

    def _namestr(self, name):
        if type(name) is str:
            return name
        else:
            return name.decode('utf-8')

    def _findsystem(self, cursor, sysname, starpos, sysaddr, syslist):
        rows = list(cursor)

        if len(rows) > 0 and type(rows[0]) is EDDNSystem:
            systems = rows
        else:
            systems = set([ EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 and row[4] != 0 and row[5] != 0) for row in rows ])

        if starpos is not None or sysaddr is not None:
            matches = set()
            for system in systems:
                if (sysname is None or system.name.lower() == sysname.lower()) and (starpos is None or not system.hascoords or (system.x == starpos[0] and system.y == starpos[1] and system.z == starpos[2])) and (sysaddr is None or sysaddr == system.id64):
                    matches.add(system)

            if len(matches) == 1:
                system = next(iter(matches))
                if not system.hascoords and starpos is not None:
                    vx = int((starpos[0] + 49985) * 32)
                    vy = int((starpos[1] + 40985) * 32)
                    vz = int((starpos[2] + 24105) * 32)
                    c = self.conn.cursor()
                    c.execute('UPDATE Systems SET X = %s, Y = %s, Z = %s WHERE Id = %s', (vx, vy, vz, system.id))
                    system = system._replace(x = starpos[0], y = starpos[1], z = starpos[2], hascoords = True)
               
                return system

        syslist |= set(systems)
        return None

    def sysaddrtomodsysaddr(self, sysaddr):
        sz = sysaddr & 7
        sx = 7 - sz
        z0 = (sysaddr >> 3) & (0x3FFF >> sz)
        y0 = (sysaddr >> (10 + sx)) & (0x1FFF >> sz)
        x0 = (sysaddr >> (16 + sx * 2)) & (0x3FFF >> sz)
        seq = (sysaddr >> (23 + sx * 3)) & 0xFFFF
        sb = 0x7F >> sz
        x1 = x0 & sb
        x2 = x0 >> sx
        y1 = y0 & sb
        y2 = y0 >> sx
        z1 = z0 & sb
        z2 = z0 >> sx
        return (z2 << 53) | (y2 << 47) | (x2 << 40) | (sz << 37) | (z1 << 30) | (y1 << 23) | (x1 << 16) | seq

    def modsysaddrtosysaddr(self, modsysaddr):
        z2 = (modsysaddr >> 53) & 0x7F
        y2 = (modsysaddr >> 47) & 0x3F
        x2 = (modsysaddr >> 40) & 0x7F
        sz = (modsysaddr >> 37) & 7
        z1 = (modsysaddr >> 30) & 0x7F
        y1 = (modsysaddr >> 23) & 0x7F
        x1 = (modsysaddr >> 16) & 0x7F
        seq = modsysaddr & 0xFFFF
        sx = 7 - sz
        x0 = x1 + (x2 << sx)
        y0 = y1 + (y2 << sx)
        z0 = z1 + (z2 << sx)
        return sz | (z0 << 3) | (y0 << (10 + sx)) | (x0 << (16 + sx * 2)) | (seq << (23 + sx * 3))

    def findsystemsbyname(self, sysname):
        systems = []

        if sysname in self.namedsystems:
            systems = self.namedsystems[sysname]
            if type(systems) is not list:
                systems = [systems]
            systems = [ s for s in systems ]

        pgsysmatch = pgsysre.match(sysname)
        ri = None
        modsysaddr = None

        if pgsysmatch:
            regionname = pgsysmatch[1]
            mid1_2 = pgsysmatch[2].upper()
            sizecls = pgsysmatch[3].lower()
            mid3 = int(pgsysmatch[4] or "0")
            seq = int(pgsysmatch[5])
            mid1a = ord(mid1_2[0]) - 65
            mid1b = ord(mid1_2[1]) - 65
            mid2 = ord(mid1_2[3]) - 65
            mid = (((mid3 * 26 + mid2) * 26 + mid1b) * 26 + mid1a)
            sz = ord(sizecls) - 97
            sx = 7 - sz
            sp = 320 << sz
            sb = 0x7F >> sz

            if regionname.lower() in self.regions:
                ri = self.regions[regionname.lower()]
                modsysaddr = None
                if ri.isharegion:
                    x0 = math.floor(ri.x0 / sp) + (mid & 0x7F)
                    y0 = math.floor(ri.y0 / sp) + ((mid >> 7) & 0x7F)
                    z0 = math.floor(ri.z0 / sp) + ((mid >> 14) & 0x7F)
                    x1 = x0 & sb
                    x2 = x0 >> sx
                    y1 = y0 & sb
                    y2 = y0 >> sx
                    z1 = z0 & sb
                    z2 = z0 >> sx
                    modsysaddr = (z2 << 53) | (y2 << 47) | (x2 << 40) | (sz << 37) | (z1 << 30) | (y1 << 23) | (x1 << 16) | seq
                elif ri.regionaddr is not None:
                    modsysaddr = (ri.regionaddr << 40) | (sz << 37) | (mid << 16) | seq

                if modsysaddr is not None:
                    cursor = self.conn.cursor()
                    cursor.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
                    systems += [ EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 or row[4] != 0 or row[5] != 0) for row in cursor ]

        return systems

    def getrejectdata(self, sysname, sysaddr, systems):
        id64name = None
        nameid64 = None
        pgsysmatch = pgsysre.match(sysname)
        rejectdata = {}

        if sysaddr is not None:
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            regionaddr = modsysaddr >> 40
            if regionaddr in self.regionaddrs:
                ri = self.regionaddrs[regionaddr]
                masscode = chr(((modsysaddr >> 37) & 7) + 97)
                seq = str(modsysaddr & 65535)
                mid = (modsysaddr >> 16) & 2097151
                mid1a = chr((mid % 26) + 65)
                mid1b = chr(((mid // 26) % 26) + 65)
                mid2 = chr(((mid // (26 * 26)) % 26) + 65)
                mid3 = mid // (26 * 26 * 26)
                mid3 = '' if mid3 == 0 else str(mid3) + '-'
                rejectdata['id64name'] = '{0} {1}{2}-{3} {4}{5}{6}'.format(
                        ri.name,
                        mid1a,
                        mid1b,
                        mid2,
                        masscode,
                        mid3,
                        seq
                    )
        
        if pgsysmatch:
            regionname = pgsysmatch[1]
            mid1_2 = pgsysmatch[2].upper()
            sizecls = pgsysmatch[3].lower()
            mid3 = int(pgsysmatch[4] or "0")
            seq = int(pgsysmatch[5])
            mid1a = ord(mid1_2[0]) - 65
            mid1b = ord(mid1_2[1]) - 65
            mid2 = ord(mid1_2[3]) - 65
            mid = (((mid3 * 26 + mid2) * 26 + mid1b) * 26 + mid1a)
            sz = ord(sizecls) - 97
            sx = 7 - sz
            sp = 320 << sz
            sb = 0x7F >> sz

            if regionname.lower() in self.regions:
                ri = self.regions[regionname.lower()]
                modsysaddr = None

                if ri.isharegion:
                    x0 = math.floor(ri.x0 / sp) + (mid & 0x7F)
                    y0 = math.floor(ri.y0 / sp) + ((mid >> 7) & 0x7F)
                    z0 = math.floor(ri.z0 / sp) + ((mid >> 14) & 0x7F)
                    x1 = x0 & sb
                    x2 = x0 >> sx
                    y1 = y0 & sb
                    y2 = y0 >> sx
                    z1 = z0 & sb
                    z2 = z0 >> sx
                    modsysaddr = (z2 << 53) | (y2 << 47) | (x2 << 40) | (sz << 37) | (z1 << 30) | (y1 << 23) | (x1 << 16) | seq
                    rejectdata['nameid64'] = self.modsysaddrtosysaddr(modsysaddr)
                elif ri.regionaddr is not None:
                    modsysaddr = (ri.regionaddr << 40) | (sz << 37) | (mid << 16) | seq
                    rejectdata['nameid64'] = self.modsysaddrtosysaddr(modsysaddr)

        if systems is not None:
            rejectdata['systems'] = [{
                'id': int(s.id),
                'id64': int(s.id64),
                'x': float(s.x),
                'y': float(s.y),
                'z': float(s.z),
                'name': s.name
            } for s in systems]

        return rejectdata

    @lru_cache(maxsize=262144)
    def getsystem(self, timer, sysname, x, y, z, sysaddr):
        if x is not None and y is not None and z is not None:
            starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in (x, y, z) ]
            vx = int((starpos[0] + 49985) * 32)
            vy = int((starpos[1] + 40985) * 32)
            vz = int((starpos[2] + 24105) * 32)
        else:
            starpos = None
            vx = 0
            vy = 0
            vz = 0

        systems = set()

        if sysname in self.namedsystems:
            namedsystems = self.namedsystems[sysname]
            if type(namedsystems) is not list:
                namedsystems = [namedsystems]

            system = self._findsystem(namedsystems, sysname, starpos, sysaddr, systems)
            if system is not None:
                return (system, None, None)

        timer.time('sysquery', 0)
        pgsysmatch = pgsysre.match(sysname)
        ri = None
        modsysaddr = None

        if pgsysmatch:
            regionname = pgsysmatch[1]
            mid1_2 = pgsysmatch[2].upper()
            sizecls = pgsysmatch[3].lower()
            mid3 = int(pgsysmatch[4] or "0")
            seq = int(pgsysmatch[5])
            mid1a = ord(mid1_2[0]) - 65
            mid1b = ord(mid1_2[1]) - 65
            mid2 = ord(mid1_2[3]) - 65
            mid = (((mid3 * 26 + mid2) * 26 + mid1b) * 26 + mid1a)
            sz = ord(sizecls) - 97
            sx = 7 - sz
            sp = 320 << sz
            sb = 0x7F >> sz

            if regionname.lower() in self.regions:
                ri = self.regions[regionname.lower()]
                modsysaddr = None
                if ri.isharegion:
                    x0 = math.floor(ri.x0 / sp) + (mid & 0x7F)
                    y0 = math.floor(ri.y0 / sp) + ((mid >> 7) & 0x7F)
                    z0 = math.floor(ri.z0 / sp) + ((mid >> 14) & 0x7F)
                    x1 = x0 & sb
                    x2 = x0 >> sx
                    y1 = y0 & sb
                    y2 = y0 >> sx
                    z1 = z0 & sb
                    z2 = z0 >> sx
                    modsysaddr = (z2 << 53) | (y2 << 47) | (x2 << 40) | (sz << 37) | (z1 << 30) | (y1 << 23) | (x1 << 16) | seq
                elif ri.regionaddr is not None:
                    modsysaddr = (ri.regionaddr << 40) | (sz << 37) | (mid << 16) | seq

                timer.time('sysquerypgre')
                if modsysaddr is not None:
                    c = self.conn.cursor()
                    c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
                    system = self._findsystem(c, sysname, starpos, sysaddr, systems)
                    timer.time('sysselectmaddr')

                    if system is not None:
                        return (system, None, None)
                else:
                    errmsg = 'Unable to resolve system address for system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, x, y, z)
                    sys.stderr.write(errmsg)
                    sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
                    return (
                        None,
                        errmsg,
                        self.getrejectdata(sysname, sysaddr, systems)
                    )
                    #raise ValueError('Unable to resolve system address')
            else:
                errmsg = 'Region {5} not found for system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, x, y, z, regionname)
                sys.stderr.write(errmsg)
                sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
                return (
                    None,
                    errmsg,
                    self.getrejectdata(sysname, sysaddr, systems)
                )
                #raise ValueError('Region not found')
        
        if sysaddr is not None:
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            c = self.conn.cursor()
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
            system = self._findsystem(c, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                return (system, None, None)

        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns JOIN Systems_Named sn ON sn.Id = ns.Id WHERE sn.Name = %s', (sysname,))

        system = self._findsystem(c, sysname, starpos, sysaddr, systems)
        timer.time('sysselectname')

        if system is not None:
            return (system, None, None)

        timer.time('sysquery', 0)
        edtsid64 = None
        edtssys = edtslib_system.from_name(sysname, allow_known = False, allow_id64data = False)
        if edtssys is not None:
            edtsid64 = edtssys.id64
        else:
            edtsid64 = edtslib_id64data.get_id64(sysname, starpos)
        timer.time('sysqueryedts')

        if sysaddr is None and edtsid64 is not None:
            timer.time('sysquery', 0)
            modsysaddr = self.sysaddrtomodsysaddr(edtsid64)
            c = self.conn.cursor()
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
            system = self._findsystem(c, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                return (system, None, None)

        elif sysaddr is not None and (edtsid64 is None or edtsid64 == sysaddr):
            timer.time('sysquery', 0)
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            c = self.conn.cursor()
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
            system = self._findsystem(c, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                return (system, None, None)

        timer.time('sysquery', 0)
        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns JOIN Systems_Named sn ON sn.Id = ns.Id WHERE sn.Name = %s', (sysname,))

        system = self._findsystem(c, sysname, starpos, None, systems)
        timer.time('sysselectname')

        if system is not None:
            return (system, None, None)

        #if starpos is None:
        #    import pdb; pdb.set_trace()

        if ri is not None and modsysaddr is not None:
            raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
            if starpos is None or raddr == modsysaddr >> 40:
                cursor = self.conn.cursor()
                cursor.execute(
                    'INSERT INTO Systems ' +
                    '(ModSystemAddress, X,  Y,  Z,  IsHASystem, IsNamedSystem) VALUES ' +
                    '(%s,               %s, %s, %s, %s,         0)',
                     (modsysaddr,       vx, vy, vz, ri.isharegion)
                )
                sysid = cursor.lastrowid
                if ri.isharegion:
                    cursor.execute(
                        'INSERT INTO Systems_HASector ' +
                        '(Id,    ModSystemAddress, RegionId, Mid1a, Mid1b, Mid2, SizeClass, Mid3, Sequence) VALUES ' +
                        '(%s,    %s,               %s,       %s,    %s,    %s,   %s,        %s,   %s)',
                         (sysid, modsysaddr,       ri.id,    mid1a, mid1b, mid2, sz,        mid3, seq)
                    )

                if starpos is not None:
                    return (EDDNSystem(sysid, self.modsysaddrtosysaddr(modsysaddr), sysname, starpos[0], starpos[1], starpos[2], True), None, None)
                else:
                    return (EDDNSystem(sysid, self.modsysaddrtosysaddr(modsysaddr), sysname, -49985, -40985, -24105, False), None, None)
        elif sysaddr is not None:
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
            if starpos is None or raddr == modsysaddr >> 40:
                cursor = self.conn.cursor()
                cursor.execute(
                    'INSERT INTO Systems ' +
                    '(ModSystemAddress, X,  Y,  Z,  IsHASystem, IsNamedSystem) VALUES ' +
                    '(%s,               %s, %s, %s, 0,         1)',
                     (modsysaddr,       vx, vy, vz)
                )
                sysid = cursor.lastrowid
                cursor.execute(
                    'INSERT INTO Systems_Named ' +
                    '(Id,    Name) VALUES ' +
                    '(%s,    %s)',
                     (sysid, sysname)
                )
                cursor.execute(
                    'INSERT INTO Systems_Validity ' +
                    '(Id,    IsRejected) VALUES ' +
                    '(%s,    1)',
                     (sysid, )
                )
                if starpos is not None:
                    return (EDDNSystem(sysid, self.modsysaddrtosysaddr(modsysaddr), sysname, starpos[0], starpos[1], starpos[2], True), None, None)
                else:
                    return (EDDNSystem(sysid, self.modsysaddrtosysaddr(modsysaddr), sysname, -49985, -40985, -24105, False), None, None)

        raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
        
        if starpos is not None:
            for mc in range(0,8):
                rx = (vx % 40960) >> mc
                ry = (vy % 40960) >> mc
                rz = (vz % 40960) >> mc
                baddr = (raddr << 40) | (mc << 37) | (rz << 30) | (ry << 23) | (rx << 16)
                c = self.conn.cursor()
                c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress >= %s AND ModSystemAddress < %s', (baddr,baddr + 65536))
                for row in c:
                    if row[3] >= vx - 2 and row[3] <= vx + 2 and row[4] >= vy - 2 and row[4] <= vy + 2 and row[5] >= vz - 2 and row[5] <= vz + 2:
                        systems += [ EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 and row[4] != 0 and row[5] != 0) ]

        timer.time('sysselectmaddr')

        errmsg = 'Unable to resolve system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, x, y, z)
        #sys.stderr.write(errmsg)
        #sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
        #raise ValueError('Unable to find system')
        return (
            None,
            errmsg,
            self.getrejectdata(sysname, sysaddr, systems)
        )

    def getstation(self, timer, name, sysname, marketid, timestamp, system = None, stationtype = None, bodyname = None, bodyid = None, bodytype = None, eventtype = None, test = False):
        sysid = system.id if system is not None else None

        if name is None or name == '':
            return (None, 'No station name')

        if sysname is None or sysname == '':
            return (None, 'No system name')

        if timestamp is None:
            return (None, 'No timestamp')

        if stationtype is not None and stationtype == '':
            stationtype = None
        
        if bodyname is not None and bodyname == '':
            bodyname = None

        if stationtype not in ['SurfaceStation', 'CraterOutpost', 'CraterPort', 'OnFootSettlement'] or bodyid is None:
            bodyname = None

        if bodytype is not None and bodytype == '':
            bodytype = None

        if marketid is not None and marketid == 0:
            marketid = None

        if (stationtype is not None and stationtype == 'FleetCarrier') or carriernamere.match(name):
            sysid = None
            sysname = ''
            bodyname = None
            bodytype = None
            bodyid = None
            stationtype = 'FleetCarrier'

        stationtype_location = None

        if eventtype is not None and eventtype == 'Location' and stationtype is not None and stationtype == 'Bernal' and timestamp > ed332date:
            stationtype_location = 'Bernal'
            stationtype = 'Ocellus'

        c = self.conn.cursor()
        c.execute('SELECT Id, MarketId, StationName, SystemName, SystemId, StationType, COALESCE(StationType_Location, StationType), Body, BodyID, IsRejected, ValidFrom, ValidUntil, Test FROM Stations WHERE SystemName = %s AND StationName = %s ORDER BY ValidUntil - ValidFrom', (sysname, name))
        stations = [ EDDNStation(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9] == b'\x01', row[10], row[11], row[12] == b'\x01') for row in c ]

        candidates = []

        for station in stations:
            replace = {}

            if marketid is not None:
                if station.marketid is not None and marketid != station.marketid:
                    continue
                else:
                    replace['marketid'] = marketid

            if sysid is not None:
                if station.systemid is not None and sysid != station.systemid:
                    continue
                else:
                    replace['systemid'] = sysid

            if stationtype is not None:
                if station.type is not None and stationtype != station.type:
                    continue
                else:
                    replace['type'] = stationtype

            if bodyname is not None and ((bodytype is None and bodyname != name) or bodytype == 'Planet'):
                if station.body is not None and bodyname != station.body:
                    continue
                else:
                    replace['body'] = bodyname
                    
            if bodyid is not None:
                if station.bodyid is not None and bodyid != station.bodyid:
                    continue
                else:
                    replace['bodyid'] = bodyid

            candidates += [ (station, replace) ]

        if len(candidates) > 1 and bodyid is not None:
            bidcandidates = [ c for c in candidates if c[0].bodyid is not None ]
            if len(bidcandidates) == 1:
                candidates = bidcandidates

        if len(candidates) > 1 and marketid is not None:
            midcandidates = [ c for c in candidates if c[0].marketid is not None ]
            if len(midcandidates) == 1:
                candidates = midcandidates

        if len(candidates) > 1 and stationtype is not None:
            stcandidates = [ c for c in candidates if c[0].type is not None ]
            if len(stcandidates) == 1:
                candidates = stcandidates

        if len(candidates) > 1 and sysid is not None:
            sidcandidates = [ c for c in candidates if c[0].systemid is not None ]
            if len(sidcandidates) == 1:
                candidates = sidcandidates

        if len(candidates) > 1 and not test:
            tcandidates = [ c for c in candidates if not c[0].test ]
            if len(tcandidates) == 1:
                candidates = tcandidates
        
        if stationtype == 'Megaship':
            candidates = [ c for c in candidates if c[0].validfrom <= timestamp and c[0].validuntil > timestamp ]

        if len(candidates) > 1:
            candidates = [ c for c in candidates if not c[0].isrejected and c[0].validfrom <= timestamp and c[0].validuntil > timestamp ]

        if len(candidates) == 2:
            if candidates[0][0].validfrom > candidates[1][0].validfrom and candidates[0][0].validuntil < candidates[1][0].validuntil:
                candidates = [ candidates[0] ]
            elif candidates[1][0].validfrom > candidates[0][0].validfrom and candidates[1][0].validuntil < candidates[0][0].validuntil:
                candidates = [ candidates[1] ]
            elif candidates[0][0].validuntil == candidates[1][0].validfrom + timedelta(hours = 15):
                if timestamp < candidates[0][0].validuntil - timedelta(hours = 13):
                    candidates = [ candidates[0] ]
                else:
                    candidates = [ candidates[1] ]
            elif candidates[1][0].validuntil == candidates[0][0].validfrom + timedelta(hours = 15):
                if timestamp < candidates[1][0].validuntil - timedelta(hours = 13):
                    candidates = [ candidates[1] ]
                else:
                    candidates = [ candidates[0] ]

        if len(candidates) == 1:
            station, replace = candidates[0]

            if len(replace) != 0:
                station = self.updatestation(station, **replace)

            return (station, None, None)
        elif len(candidates) > 1:
            #import pdb; pdb.set_trace()
            return (
                None, 
                'More than 1 match', 
                [{
                    'station': {
                        'id': s.id,
                        'stationName': s.name,
                        'marketId': s.marketid,
                        'systemName': s.systemname,
                        'systemId': s.systemid,
                        'stationType': s.type,
                        'locationStationType': s.loctype,
                        'bodyName': s.body,
                        'bodyId': s.bodyid,
                        'isRejected': True if s.isrejected else False,
                        'validFrom': s.validfrom.isoformat(),
                        'validUntil': s.validuntil.isoformat(),
                        'test': True if s.test else False
                    },
                    'replace': r
                } for s, r in candidates])
        
        if bodyname is not None and not ((bodytype is None and bodyname != name) or bodytype == 'Planet'):
            bodyname = None

        validfrom = datetime.strptime('2014-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        validuntil = datetime.strptime('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')

        if stationtype is not None:
            if stationtype == 'SurfaceStation':
                validuntil = ed330date
            elif (marketid is not None and marketid >= 3789600000) or stationtype == 'OnFootSettlement':
                validfrom = ed400date
            elif (marketid is not None and marketid >= 3700000000) or stationtype == 'FleetCarrier':
                validfrom = ed370date
            elif stationtype in ['CraterPort', 'CraterOutpost']:
                validfrom = ed330date
            elif stationtype == 'Ocellus':
                validfrom = ed332date
                stationtype_location = 'Bernal'
            elif stationtype == 'Bernal' and timestamp < ed332date:
                validuntil = ed332date
            elif stationtype == 'Megaship' and marketid is not None and marketid >= 3400000000:
                validfrom = megashipweek0 + timedelta(weeks = math.floor((timestamp - megashipweek0).total_seconds() / 86400 / 7), hours = -2)
                validuntil = validfrom + timedelta(days = 7, hours = 15)

        if (sysid is None and sysname != '') or stationtype is None or marketid is None:
            return (
                None,
                'Station mismatch',
                [{
                    'MarketId': marketid,
                    'StationName': name,
                    'SystemName': sysname,
                    'SystemId': sysid,
                    'StationType': stationtype,
                    'LocationStationType': stationtype_location,
                    'Body': bodyname,
                    'BodyID': bodyid,
                    'IsTest': 1 if test else 0
                }]
            )

        c = self.conn.cursor()
        c.execute(
            'INSERT INTO Stations ' +
            '(MarketId, StationName, SystemName, SystemId, StationType, StationType_Location, Body,     BodyID, ValidFrom, ValidUntil, Test) VALUES ' +
            '(%s,       %s,          %s,         %s,       %s,          %s,                   %s,       %s,     %s,        %s,         %s)', 
             (marketid, name,        sysname,    sysid,    stationtype, stationtype_location, bodyname, bodyid, validfrom, validuntil, test))
        return (EDDNStation(c.lastrowid, marketid, name, sysname, sysid, stationtype, stationtype_location or stationtype, bodyname, bodyid, False, validfrom, validuntil, test), None, None)

    def getmarketstation(self, name, sysname, marketid, timestamp):
        if name is None or name == '':
            return (None, 'No station name')

        if sysname is None or sysname == '':
            return (None, 'No system name')

        if timestamp is None:
            return (None, 'No timestamp')

        if carriernamere.match(name):
            sysname = ''

        c = self.conn.cursor()
        c.execute('SELECT Id, MarketId, StationName, SystemName, IsRejected, ValidFrom, ValidUntil FROM MarketStations WHERE SystemName = %s AND StationName = %s ORDER BY ValidUntil - ValidFrom', (sysname, name))
        stations = [ EDDNMarketStation(row[0], row[1], row[2], row[3], row[4] == b'\x01', row[5], row[6]) for row in c ]

        candidates = []

        for station in stations:
            replace = {}

            if marketid is not None:
                if station.marketid is not None and marketid != station.marketid:
                    continue
                else:
                    replace['marketid'] = marketid

            candidates += [ (station, replace) ]

        if marketid is not None and marketid >= 3600000000 and marketid < 3700000000:
            candidates = [ c for c in candidates if c[0].validfrom <= timestamp and c[0].validuntil > timestamp ]

        if len(candidates) > 1 and marketid is not None:
            midcandidates = [ c for c in candidates if c[0].marketid is not None ]
            if len(midcandidates) == 1:
                candidates = midcandidates

        if len(candidates) > 1:
            candidates = [ c for c in candidates if not c[0].isrejected and c[0].validfrom <= timestamp and c[0].validuntil > timestamp ]

        if len(candidates) == 2:
            if candidates[0][0].validfrom > candidates[1][0].validfrom and candidates[0][0].validuntil < candidates[1][0].validuntil:
                candidates = [ candidates[0] ]
            elif candidates[1][0].validfrom > candidates[0][0].validfrom and candidates[1][0].validuntil < candidates[0][0].validuntil:
                candidates = [ candidates[1] ]
            elif candidates[0][0].validuntil == candidates[1][0].validfrom + timedelta(hours = 15):
                if timestamp < candidates[0][0].validuntil - timedelta(hours = 13):
                    candidates = [ candidates[0] ]
                else:
                    candidates = [ candidates[1] ]
            elif candidates[1][0].validuntil == candidates[0][0].validfrom + timedelta(hours = 15):
                if timestamp < candidates[1][0].validuntil - timedelta(hours = 13):
                    candidates = [ candidates[1] ]
                else:
                    candidates = [ candidates[0] ]

        if len(candidates) == 1:
            station, replace = candidates[0]

            if len(replace) != 0:
                station = self.updatemarketstation(station, **replace)

            return (station, None, None)
        elif len(candidates) > 1:
            #import pdb; pdb.set_trace()
            return (
                None,
                'More than 1 match',
                [{
                    'station': {
                        'id': s.id,
                        'stationName': s.name,
                        'marketId': s.marketid,
                        'systemName': s.systemname,
                        'isRejected': True if s.isrejected else False,
                        'validFrom': s.validfrom.isoformat(),
                        'validUntil': s.validuntil.isoformat()
                    },
                    'replace': r
                } for s, r in candidates])

        validfrom = datetime.strptime('2014-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        validuntil = datetime.strptime('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')

        if marketid is not None:
            if marketid >= 3789600000:
                validfrom = ed400date
            elif marketid >= 3700000000:
                validfrom = ed370date
            elif marketid >= 3600000000:
                validfrom = megashipweek0 + timedelta(weeks = math.floor((timestamp - megashipweek0).total_seconds() / 86400 / 7), hours = -2)
                validuntil = validfrom + timedelta(days = 7, hours = 15)

        c = self.conn.cursor()
        c.execute(
            'INSERT INTO MarketStations ' +
            '(MarketId, StationName, SystemName, ValidFrom, ValidUntil) VALUES ' +
            '(%s,       %s,          %s,         %s,        %s)',
             (marketid, name,        sysname,    validfrom, validuntil))
        return (EDDNStation(c.lastrowid, marketid, name, sysname, False, validfrom, validuntil), None, None)

    def insertbodyparents(self, timer, scanbodyid, system, bodyid, parents):
        if parents is not None and bodyid is not None:
            parentjson = json.dumps(parents)
            
            if (bodyid, parentjson) not in self.parentsets:
                c = self.conn.cursor()
                c.execute(
                    'INSERT INTO ParentSets ' +
                    '(BodyId, ParentJson) VALUES ' + 
                    '(%s,     %s)',
                     (bodyid, parentjson))
                self.parentsets[(bodyid, parentjson)] = c.lastrowid

            parentsetid = self.parentsets[(bodyid, parentjson)]

            c = self.conn.cursor()
            c.execute(
                'INSERT IGNORE INTO SystemBodies_ParentSet ' +
                '(Id, ParentSetId) VALUES ' +
                '(%s, %s)',
                 (scanbodyid, parentsetid))

    def insertsoftware(self, softwarename):
        if softwarename not in self.software:
            c = self.conn.cursor()
            c.execute('INSERT INTO Software (Name) VALUES (%s)', (softwarename,))
            self.software[softwarename] = c.lastrowid

    def insertedsmfile(self, filename):
        c = self.conn.cursor()
        c.execute('INSERT INTO EDSMFiles (FileName) VALUES (%s)', (filename,))
        return c.lastrowid

    def getbody(self, timer, name, sysname, bodyid, system, body, timestamp):
        if system.id in self.namedbodies and name in self.namedbodies[system.id]:
            timer.time('bodyquery', 0)
            rows = self.namedbodies[system.id][name]

            if type(rows) is not list:
                rows = [rows]

            multimatch = len(rows) > 1

            if bodyid is not None:
                rows = [row for row in rows if row.bodyid is None or row.bodyid == bodyid]

            if len(rows) > 1 and name == sysname:
                if 'PlanetClass' in body:
                    rows = [row for row in rows if row.category == 6]
                elif 'StarType' in body:
                    rows = [row for row in rows if row.category == 2]

            if multimatch and 'Periapsis' in body:
                aop = body['Periapsis']
                if len(rows) == 1 and rows[0].argofperiapsis is None:
                    pass
                elif len(rows) > 1:
                    rows = [row for row in rows if row.argofperiapsis is None or ((aop + 725 - row.argofperiapsis) % 360) < 10]

            if len(rows) > 1:
                rows = [row for row in rows if row.validfrom < timestamp and row.validuntil > timestamp]
            
            if len(rows) > 1:
                rows = [row for row in rows if row.isrejected == 0]

            timer.time('bodylookupname')
            if len(rows) == 1:
                return (rows[0], None, None)

        ispgname = name.startswith(sysname)
        if name == sysname:
            if 'SemiMajorAxis' in body and body['SemiMajorAxis'] is not None:
                ispgname = False
            elif bodyid is not None and bodyid != 0:
                ispgname = False
            elif 'BodyType' in body and body['BodyType'] != 'Star':
                ispgname = False

        desigid = None
        sysknownbodies = None
        knownbodies = None

        if sysname in self.knownbodies:
            sysknownbodies = self.knownbodies[sysname]
            if name in sysknownbodies:
                knownbodies = self.knownbodies[sysname][name]
                if bodyid is not None:
                    knownbodies = [ row for row in knownbodies if row['BodyID'] == bodyid ]
                if len(knownbodies) == 1:
                    knownbody = knownbodies[0]
                    if knownbody['BodyDesignation'] != knownbody['BodyName']:
                        ispgname = False
                        desigid = knownbody['BodyDesignationId']

        if ispgname:
            timer.time('bodyquery', 0)
            desig = name[len(sysname):]
            match = pgbodyre.match(desig)

            if desig in self.bodydesigs:
                desigid = self.bodydesigs[desig]
            else:
                cursor = self.conn.cursor()
                cursor.execute('SELECT Id, BodyDesignation FROM SystemBodyDesignations WHERE BodyDesignation = %s', (desig,))
                row = cursor.fetchone()
                
                if row and row[1] == desig:
                    desigid = int(row[0])
                    self.bodydesigs[desig] = desigid
                    cursor = self.conn.cursor()
                    cursor.execute('UPDATE SystemBodyDesignations SET IsUsed = 1 WHERE Id = %s', (desigid,))
                elif match:
                    stars = match['stars']
                    nebula = match['nebula']
                    belt = match['belt']
                    cluster = match['cluster']
                    stellarcomet = match['stellarcomet']
                    planetstr = match['planet']
                    ring1 = match['planetring']
                    comet1 = match['planetcomet']
                    moon1str = match['moon1']
                    ring2 = match['moon1ring']
                    comet2 = match['moon1comet']
                    moon2str = match['moon2']
                    ring3 = match['moon2ring']
                    comet3 = match['moon2comet']
                    moon3str = match['moon3']

                    bodycategory = 0
                    planet = 0
                    moon1 = 0
                    moon2 = 0
                    moon3 = 0

                    if planetstr is not None:
                        if '+' in planetstr:
                            planetlist = planetstr.split('+')
                            planet = int(planetlist[0])
                            moon1 = int(planetlist[-1]) - planet
                            bodycategory = 5
                        else:
                            planet = int(planetstr)
                            bodycategory = 6
                    elif belt is not None:
                        planet = ord(belt) - 64
                        if cluster is not None:
                            moon1 = int(cluster)
                            bodycategory = 4
                        else:
                            bodycategory = 3
                    elif stellarcomet is not None:
                        bodycategory = 15
                    elif nebula is not None:
                        bodycategory = 19
                    elif stars is not None and len(stars) > 1:
                        bodycategory = 1
                    else:
                        bodycategory = 2
                        
                    if bodycategory == 6:
                        if moon1str is not None:
                            if '+' in moon1str:
                                moon1list = moon1str.split('+')
                                moon1 = ord(moon1list[0]) - 96
                                moon2 = ord(moon1list[-1]) - 96 - moon1
                                bodycategory = 8
                            else:
                                moon1 = ord(moon1str) - 96
                                bodycategory = 9
                        elif ring1 is not None:
                            moon1 = ord(ring1) - 64
                            bodycategory = 7
                        elif comet1 is not None:
                            moon1 = int(comet1)
                            bodycategory = 16

                    if bodycategory == 9:
                        if moon2str is not None:
                            if '+' in moon2str:
                                moon2list = moon2str.split('+')
                                moon2 = ord(moon2list[0]) - 96
                                moon3 = ord(moon2list[-1]) - 96 - moon2
                                bodycategory = 11
                            else:
                                moon2 = ord(moon2str) - 96
                                bodycategory = 12
                        elif ring2 is not None:
                            moon2 = ord(ring2) - 64
                            bodycategory = 10
                        elif comet2 is not None:
                            moon2 = int(comet2)
                            bodycategory = 17

                    if bodycategory == 12:
                        if moon3str is not None:
                            moon3 = ord(moon3str) - 96
                            bodycategory = 14
                        elif ring3 is not None:
                            moon3 = ord(ring3) - 64
                            bodycategory = 13
                        elif comet3 is not None:
                            moon3 = int(comet3)
                            bodycategory = 18

                    star = 0
                    if stars is not None:
                        for i in range(ord(stars[0]) - 65, ord(stars[-1]) - 64):
                            star |= 1 << i

                    #import pdb; pdb.set_trace()
                    return (
                        None,
                        'Body designation not in database',
                        [{
                            'Designation': desig,
                            'BodyCategory': bodycategory,
                            'Stars': stars,
                            'Planet': planet,
                            'Moon1': moon1,
                            'Moon2': moon2,
                            'Moon3': moon3
                        }]
                    )

            timer.time('bodyquerypgre')

        timer.time('bodyquery', 0)
        cursor = self.conn.cursor()
        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND BodyName = %s AND IsNamedBody = 1', (system.id, name))
        rows = cursor.fetchall()
        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND BodyName = %s AND IsNamedBody = 0', (system.id, name))
        rows += cursor.fetchall()
        timer.time('bodyselectname')
        ufrows = rows

        multimatch = len(rows) > 1

        if bodyid is not None:
            rows = [row for row in rows if row[4] == bodyid or row[4] is None]

        if len(rows) > 1 and name == sysname:
            if 'PlanetClass' in body:
                rows = [row for row in rows if row[5] == 'PlanetaryBody']
            elif 'StarType' in body:
                rows = [row for row in rows if row[5] == 'StellarBody']

        if multimatch and 'Periapsis' in body:
            aop = body['Periapsis']
            if len(rows) == 1 and rows[0][6] is None:
                pass
            elif len(rows) > 1:
                rows = [row for row in rows if row[6] is None or ((aop + 725 - row[6]) % 360) < 10]

        if len(rows) > 1:
            xrows = [row for row in rows if row[7] < timestamp and row[8] > timestamp]
            if len(xrows) > 0:
                rows = xrows
        
        if len(rows) > 1:
            xrows = [row for row in rows if row[9]]
            if len(xrows) > 0:
                rows = xrows

        timer.time('bodyqueryname')
        if len(rows) == 1:
            row = rows[0]
            if row[4] is None and bodyid is not None:
                cursor = self.conn.cursor()
                cursor.execute('UPDATE SystemBodies SET HasBodyId = 1, BodyID = %s WHERE Id = %s', (bodyid, row[0]))
                timer.time('bodyupdateid')
            return (EDDNBody(row[0], name, sysname, system.id, row[4] or bodyid, None, (body.get('Periapsis')), None, None, 0), None, None)
        elif len(rows) > 1:
            return (
                None,
                'Multiple body matches',
                [{
                    'id': int(row[0]),
                    'bodyName': row[1],
                    'systemName': row[2],
                    'systemId': int(row[3]),
                    'bodyId': int(row[4]) if row[4] is not None else None,
                    'bodyCategory': row[5] if row[5] is not None else None,
                    'argOfPeriapsis': float(row[6]) if row[6] is not None else None,
                    'validFrom': row[7].isoformat(),
                    'validUntil': row[8].isoformat(),
                    'isRejected': True if row[9] else False
                } for row in rows]
            )
        else:
            cursor = self.conn.cursor()
            cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 1', (system.id, ))
            allrows = cursor.fetchall()
            cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 0', (system.id, ))
            allrows += cursor.fetchall()
            frows = [ r for r in allrows if r[1].lower() == name.lower() ]

            if bodyid is not None:
                frows = [ r for r in frows if r[4] is None or r[4] == bodyid ]

            if len(frows) > 0:
                if sysname in self.namedsystems:
                    systems = self.namedsystems[sysname]
                    if type(systems) is not list:
                        systems = [systems]
                    for xsystem in systems:
                        cursor = self.conn.cursor()
                        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 1', (xsystem.id, ))
                        allrows += cursor.fetchall()
                        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 0', (xsystem.id, ))
                        allrows += cursor.fetchall()
                frows = [ r for r in allrows if r[1].lower() == name.lower() ]
                        
                import pdb; pdb.set_trace()
                return (
                    None, 
                    'Body Mismatch',
                    [{
                        'id': int(row[0]),
                        'bodyName': row[1],
                        'systemName': row[2],
                        'systemId': int(row[3]),
                        'bodyId': int(row[4]) if row[4] is not None else None,
                        'bodyCategory': int(row[5]) if row[5] is not None else None,
                        'argOfPeriapsis': float(row[6]) if row[6] is not None else None,
                        'validFrom': row[7].isoformat(),
                        'validUntil': row[8].isoformat(),
                        'isRejected': True if row[9] else False
                    } for row in rows]
                )

            if ispgname and desigid is not None:
                cursor = self.conn.cursor()
                cursor.execute(
                    'INSERT INTO SystemBodies ' +
                    '(SystemId,  HasBodyId, BodyId,      BodyDesignationId, IsNamedBody) VALUES ' +
                    '(%s,        %s,        %s,          %s,                0)', 
                     (system.id, 1 if bodyid is not None else 0, bodyid or 0, desigid)
                )
                timer.time('bodyinsertpg')
                return (EDDNBody(cursor.lastrowid, name, sysname, system.id, bodyid, None, (body.get('Periapsis')), None, None, 0), None, None)
                
            if (not ispgname and pgsysre.match(name)) or desigid is None:
                allrows = []
                cursor = self.conn.cursor()
                cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sb WHERE sb.CustomName = %s', (name,))
                allrows += cursor.fetchall()
                pgsysbodymatch = pgsysbodyre.match(name)

                if pgsysbodymatch:
                    dupsysname = pgsysbodymatch['sysname']
                    desig = pgsysbodymatch['desig']
                    dupsystems = self.findsystemsbyname(dupsysname)

                    for dupsystem in dupsystems:
                        cursor = self.conn.cursor()
                        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 1', (dupsystem.id, ))
                        allrows += cursor.fetchall()
                        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 0', (dupsystem.id, ))
                        allrows += cursor.fetchall()

                frows = [ r for r in allrows if r[1].lower() == name.lower() ]

                if len(frows) > 0:
                    return (
                        None,
                        'Body in wrong system',
                        [{
                            'id': int(row[0]),
                            'bodyName': row[1],
                            'systemName': row[2],
                            'systemId': int(row[3]),
                            'bodyId': int(row[4]) if row[4] is not None else None,
                            'bodyCategory': int(row[5]) if row[5] is not None else None,
                            'argOfPeriapsis': float(row[6]) if row[6] is not None else None,
                            'validFrom': row[7].isoformat(),
                            'validUntil': row[8].isoformat(),
                            'isRejected': True if row[9] else False
                        } for row in rows]
                    )
                elif pgsysbodymatch:
                    return (None, 'Procgen body in wrong system', [{'System': sysname, 'Body': name}])
                else:
                    if 'debugunknownbodies' in os.environ and (sysknownbodies is not None or 'debugunknownbodysystems' in os.environ):
                        import pdb; pdb.set_trace()

                    return (None, 'Unknown named body', [{'System': sysname, 'Body': name}])
                
            cursor = self.conn.cursor()
            cursor.execute(
                'INSERT INTO SystemBodies ' +
                '(SystemId,  HasBodyId, BodyId,      BodyDesignationId, IsNamedBody) VALUES '
                '(%s,        %s,        %s,          %s,                 1)',
                 (system.id, 1 if bodyid is not None else 0, bodyid or 0, desigid)
            )
            rowid = cursor.lastrowid
            if rowid is None:
                import pdb; pdb.set_trace()
                
            cursor.execute(
                'INSERT INTO SystemBodies_Named ' +
                '(Id,    SystemId, Name) VALUES ' +
                '(%s,    %s,       %s)', 
                 (rowid, system.id, name)
            )

            '''
            cursor.execute(
                'INSERT INTO SystemBodies_Validity ' +
                '(Id,    IsRejected) VALUES ' +
                '(%s,    1)',
                 (rowid, )
            )
            '''

            return (EDDNBody(rowid, name, sysname, system.id, bodyid, None, (body.get('Periapsis')), None, None, 1), None, None)

    def getfaction(self, timer, name, government, allegiance):
        factions = None

        if government is not None and government[:12] == '$government_' and government[-1] == ';':
            government = government[12:-1]

        if name in self.factions:
            factions = self.factions[name]
            if type(factions) is not list:
                factions = [factions]
            for faction in factions:
                if (government is None or faction.government == government) and (allegiance is None or faction.allegiance == allegiance):
                    return faction

        if allegiance is None:
            return None

        c = self.conn.cursor()
        c.execute(
            'INSERT INTO Factions ' +
            '(Name, Government, Allegiance) VALUES ' +
            '(%s,   %s,         %s)',
            (name, government, allegiance))
        factionid = c.lastrowid

        faction = EDDNFaction(factionid, name, government, allegiance)

        if factions is None:
            self.factions[name] = faction
        elif type(self.factions[name]) is not list:
            self.factions[name] = [self.factions[name]]
            self.factions[name] += [faction]
        else:
            self.factions[name] += [faction]

        return faction

    def getmarketitem(self, timer, name, type):
        item = self.marketitems.get((type, name))

        if item is not None:
            return item

        c = self.conn.cursor()
        c.execute(
            'INSERT INTO MarketItems ' +
            '(Name, Type) VALUES ' +
            '(%s, %s)',
            (name, type)
        )
        itemid = c.lastrowid

        item = EDDNMarketItem(itemid, name, type)

        return item

    def updatestation(self, station, **kwargs):
        station = station._replace(**kwargs)

        c = self.conn.cursor()
        c.execute('UPDATE Stations SET MarketId = %s, SystemId = %s, StationType = %s, Body = %s, BodyID = %s WHERE Id = %s', (station.marketid, station.systemid, station.type, station.body, station.bodyid, station.id))

        return station

    def getsystembyid(self, sysid):
        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE Id = %s', (sysid,))
        row = c.fetchone()

        if row:
            return EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 and row[4] != 0 and row[5] != 0)
        else:
            return None

    def getbodiesfromedsmbyid(self, edsmid, timer):
        url = 'https://www.edsm.net/api-body-v1/get?id={0}'.format(edsmid)
        
        while True:
            try:
                with urllib.request.urlopen(url) as f:
                    msg = json.load(f)
                    info = f.info()
                    ratereset = int(info["X-Rate-Limit-Reset"])
                    rateremain = int(info["X-Rate-Limit-Remaining"])
                    if ratereset > rateremain * 30:
                        time.sleep(30)
                    elif ratereset > rateremain:
                        time.sleep(ratereset / rateremain)
            except urllib.request.URLError:
                time.sleep(60)
            else:
                break

        if type(msg) is dict and 'system' in msg:
            edsmsys = msg['system']
            edsmsysid = edsmsys['id']
        else:
            timer.time('edsmhttp')
            return []

        url = 'https://www.edsm.net/api-system-v1/bodies?systemId={0}'.format(edsmsysid)

        while True:
            try:
                with urllib.request.urlopen(url) as f:
                    msg = json.load(f)
                    info = f.info()
                    ratereset = int(info["X-Rate-Limit-Reset"])
                    rateremain = int(info["X-Rate-Limit-Remaining"])
                    if ratereset > rateremain * 30:
                        time.sleep(30)
                    elif ratereset > rateremain:
                        time.sleep(ratereset / rateremain)
            except urllib.request.URLError:
                time.sleep(30)
            else:
                break

        if type(msg) is dict:
            sysid64 = msg['id64']
            sysname = msg['name']
        else:
            timer.time('edsmhttp')
            return []

        for body in msg['bodies']:
            body['systemId'] = edsmsysid
            body['systemName'] = sysname
            body['systemId64'] = sysid64

        timer.time('edsmhttp')
        return msg['bodies']

    def updatesystemfromedsmbyid(self, edsmid, timer, rejectout):
        url = 'https://www.edsm.net/api-v1/system?systemId={0}&coords=1&showId=1&submitted=1&includeHidden=1'.format(edsmid)
        try:
            while True:
                try:
                    with urllib.request.urlopen(url) as f:
                        msg = json.load(f)
                        info = f.info()
                except urllib.request.URLError:
                    time.sleep(30)
                else:
                    break

            if type(msg) is dict:
                edsmsysid = msg['id']
                sysaddr = msg['id64']
                sysname = msg['name']
                timestamp = msg['date'].replace(' ', 'T')
                if 'coords' in msg:
                    coords = msg['coords']
                    starpos = [coords['x'],coords['y'],coords['z']]
                else:
                    starpos = None
            else:
                timer.time('edsmhttp')
                return False
        except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
            (exctype, excvalue, traceback) = sys.exc_info()
            sys.stderr.write('Error: {0}\n'.format(exctype))
            import pdb; pdb.post_mortem(traceback)
            timer.time('error')
            return True
        else:
            timer.time('edsmhttp')
            sqltimestamp = timestamptosql(timestamp)
            sqlts = int((sqltimestamp - tsbasedate).total_seconds())
            (sysid, ts, hascoord, rec) = self.findedsmsysid(edsmsysid)
            timer.time('sysquery')
            if starpos is not None:
                starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                (system, rejectReason, rejectData) = self.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], sysaddr)
            else:
                (system, rejectReason, rejectData) = self.getsystem(timer, sysname, None, None, None, sysaddr)
                
            timer.time('sysquery', 0)

            if system is not None:
                rec = self.updateedsmsysid(edsmsysid, system.id, sqltimestamp, starpos is not None, False, False)
            else:
                rejectmsg = {
                    'rejectReason': rejectReason,
                    'rejectData': rejectData,
                    'data': msg
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')

            timer.time('edsmupdate')

            if rec is not None:
                rec.processed = 7

            return True

    def findedsmsysid(self, edsmid):
        if self.edsmsysids is not None and len(self.edsmsysids) > edsmid:
            row = self.edsmsysids[edsmid]

            if row[0] != 0:
                return (row[0], row[2], row[3], row)

        c = self.conn.cursor()
        c.execute('SELECT Id, TimestampSeconds, HasCoords FROM Systems_EDSM WHERE EdsmId = %s', (edsmid,))
        row = c.fetchone()

        if row:
            return (row[0], row[1], row[2] == b'\x01', None)
        else:
            return (None, None, None, None)

    def findedsmbodyid(self, edsmid):
        if self.edsmbodyids is not None and len(self.edsmbodyids) > edsmid:
            row = self.edsmbodyids[edsmid]

            if row[0] != 0:
                return (row[0], row[2], row)

        c = self.conn.cursor()
        c.execute('SELECT Id, TimestampSeconds FROM SystemBodies_EDSM WHERE EdsmId = %s', (edsmid,))
        row = c.fetchone()

        if row:
            return (row[0], row[1], None)
        else:
            return (None, None, None)

    def saveedsmsyscache(self):
        with open(edsmsyscachefile + '.tmp', 'wb') as f:
            self.edsmsysids.tofile(f)
        os.rename(edsmsyscachefile + '.tmp', edsmsyscachefile)

    def saveedsmbodycache(self):
        with open(edsmbodycachefile + '.tmp', 'wb') as f:
            self.edsmbodyids.tofile(f)
        os.rename(edsmbodycachefile + '.tmp', edsmbodycachefile)

    def updateedsmsysid(self, edsmid, sysid, ts, hascoords, ishidden, isdeleted):
        if type(ts) is datetime:
            ts = int((ts - tsbasedate).total_seconds())

        c = self.conn.cursor()
        c.execute('INSERT INTO Systems_EDSM SET ' +
                  'EdsmId = %s, Id = %s, TimestampSeconds = %s, HasCoords = %s, IsHidden = %s, IsDeleted = %s ' +
                  'ON DUPLICATE KEY UPDATE ' +
                  'Id = %s, TimestampSeconds = %s, HasCoords = %s, IsHidden = %s, IsDeleted = %s',
                  (edsmid, sysid, ts, 1 if hascoords else 0, 1 if ishidden else 0, 1 if isdeleted else 0,
                           sysid, ts, 1 if hascoords else 0, 1 if ishidden else 0, 1 if isdeleted else 0))

        if edsmid < len(self.edsmsysids):
            rec = self.edsmsysids[edsmid]
            rec[0] = sysid
            rec[1] = edsmid
            rec[2] = ts
            rec[3] = 1 if hascoords else 0
            rec[4] = 1 if ishidden else 0
            rec[5] = 1 if isdeleted else 0
            return rec
        else:
            return None

    
    def updateedsmbodyid(self, bodyid, edsmid, ts):
        ts = int((ts - tsbasedate).total_seconds())
        c = self.conn.cursor()
        c.execute('INSERT INTO SystemBodies_EDSM SET EdsmId = %s, Id = %s, TimestampSeconds = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, TimestampSeconds = %s',
                  (edsmid, bodyid, ts, bodyid, ts))

        if edsmid < len(self.edsmbodyids):
            rec = self.edsmbodyids[edsmid]
            rec[0] = bodyid
            rec[1] = edsmid
            rec[2] = ts
            return rec
        else:
            return None
    
    def updateedsmstationid(self, edsmid, stationid, ts):
        c = self.conn.cursor()
        c.execute('INSERT INTO Stations_EDSM SET EdsmStationId = %s, Id = %s, Timestamp = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, Timestamp = %s',
                  (edsmid, stationid, ts, stationid, ts))
    
    def findeddbsysid(self, eddbid):
        if self.eddbsysids is not None and len(self.eddbsysids) > eddbid:
            row = self.eddbsysids[eddbid]

            if row[0] != 0:
                return (row[0], row[2])

        c = self.conn.cursor()
        c.execute('SELECT Id, TimestampSeconds FROM Systems_EDDB WHERE EddbId = %s', (eddbid,))
        row = c.fetchone()

        if row:
            return (row[0], row[1])
        else:
            return (None, None)

    def updateeddbsysid(self, eddbid, sysid, ts):
        c = self.conn.cursor()
        c.execute('INSERT INTO Systems_EDDB SET EddbId = %s, Id = %s, TimestampSeconds = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, TimestampSeconds = %s',
                  (eddbid, sysid, ts, sysid, ts))
    
    def addfilelinestations(self, linelist):
        values = [(fileid, lineno, station.id) for fileid, lineno, station in linelist]
        self.conn.cursor().executemany('INSERT INTO FileLineStations (FileId, LineNo, StationId) VALUES (%s, %s, %s)', values)

    def addfilelineinfo(self, linelist):
        self.conn.cursor().executemany(
            'INSERT INTO FileLineInfo ' +
            '(FileId, LineNo, Timestamp, GatewayTimestamp, SoftwareId, SystemId, BodyId, LineLength, DistFromArrivalLS, HasBodyId, HasSystemAddress, HasMarketId) VALUES ' +
            '(%s,     %s,     %s,        %s,               %s,         %s,       %s,     %s,         %s,                %s,        %s,               %s)',
            linelist
        )

    def addfilelinefactions(self, linelist):
        values = [(fileid, lineno, faction.id, entrynum) for fileid, lineno, faction, entrynum in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO FileLineFactions ' +
            '(FileId, LineNo, FactionId, EntryNum) VALUES ' +
            '(%s,     %s,     %s,        %s)',
            values
        )

    def addfilelineroutesystems(self, linelist):
        values = [(fileid, lineno, system.id, entrynum) for fileid, lineno, system, entrynum in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO FileLineNavRoutes ' +
            '(FileId, LineNo, SystemId, EntryNum) VALUES ' +
            '(%s,     %s,     %s,       %s)',
            values
        )

    def addfilelinemarketitems(self, linelist):
        values = [(fileid, lineno, station.id, entrynum, marketitem.id) for fileid, lineno, station, entrynum, marketitem in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO FileLineMarketItems ' +
            '(FileId, LineNo, MarketStationId, EntryNum, MarketItemId) VALUES ' +
            '(%s, %s, %s, %s, %s)',
            values
        )

    def addedsmfilelinebodies(self, linelist):
        values = [(fileid, lineno, edsmbodyid) for fileid, lineno, edsmbodyid in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO EDSMFileLineBodies ' +
            '(FileId, LineNo, EdsmBodyId) VALUES ' +
            '(%s,     %s,     %s)',
            values
        )

    def getstationfilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, StationId FROM FileLineStations WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor }

    def getinfofilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, Timestamp, SystemId, BodyId FROM FileLineInfo WHERE FileId = %s', (fileid,))

        return { row[0]: (row[1], row[2], row[3]) for row in cursor }

    def getinfosystemfilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, SystemId FROM FileLineInfo WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor if row[1] is not None }

    def getinfobodyfilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, BodyId FROM FileLineInfo WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor if row[1] is not None }

    def getfactionfilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, FactionId FROM FileLineFactions WHERE FileId = %s', (fileid,))

        lines = {}
        for row in cursor:
            if row[0] not in lines:
                lines[row[0]] = []
            lines[row[0]] += [row[1]]

        return lines

    def getnavroutefilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, EntryNum, SystemId FROM FileLineNavRoutes WHERE FileId = %s', (fileid,))

        lines = {}
        for row in cursor:
            lines[(row[0], row[1])] = row[2]

        return lines

    def getmarketitemfilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, EntryNum, MarketStationId, MarketItemId FROM FileLineMarketLines WHERE FileId = %s', (fileid,))

        lines = {}
        for row in cursor:
            lines[(row[0], row[1])] = (row[2], row[3])

        return lines

    def getedsmbodyfilelines(self, fileid):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT MAX(LineNo) FROM EDSMFileLineBodies WHERE FileId = %s', (fileid,))
        row = cursor.fetchone()
        maxline = row[0]

        if maxline is None:
            return []

        filelinearray = numpy.zeros(maxline + 1, numpy.int32)

        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, EdsmBodyId FROM EDSMFileLineBodies WHERE FileId = %s', (fileid,))

        for row in cursor:
            filelinearray[row[0]] = row[1]

        return filelinearray

    def geteddnfiles(self):
        
        sys.stderr.write('    Getting station line counts\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT FileId, COUNT(LineNo) FROM FileLineStations GROUP BY FileId')
        stnlinecounts = { row[0]: row[1] for row in cursor }

        sys.stderr.write('    Getting info line counts\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT FileId, COUNT(LineNo) FROM FileLineInfo GROUP BY FileId')
        infolinecounts = { row[0]: row[1] for row in cursor }

        sys.stderr.write('    Getting faction line counts\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT FileId, COUNT(DISTINCT LineNo) FROM FileLineFactions GROUP BY FileId')
        factionlinecounts = { row[0]: row[1] for row in cursor }

        sys.stderr.write('    Getting nav route line counts\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT FileId, COUNT(*) FROM FileLineNavRoutes GROUP BY FileId')
        navroutelinecounts = { row[0]: row[1] for row in cursor }

        sys.stderr.write('    Getting market item line counts\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT FileId, COUNT(*) FROM FileLineMarketItems GROUP BY FileId')
        marketitemlinecounts = { row[0]: row[1] for row in cursor }

        sys.stderr.write('    Getting file info\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('''
            SELECT 
                Id, 
                FileName, 
                Date, 
                EventType, 
                LineCount, 
                PopulatedLineCount,
                StationLineCount,
                NavRouteSystemCount,
                MarketItemCount,
                IsTest
            FROM Files f
        ''')

        return { 
            row[1]: EDDNFile(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                stnlinecounts.get(row[0]) or 0,
                infolinecounts.get(row[0]) or 0,
                factionlinecounts.get(row[0]) or 0,
                navroutelinecounts.get(row[0]) or 0,
                marketitemlinecounts.get(row[0]) or 0,
                row[5],
                row[6],
                row[7],
                row[8],
                row[9]
            ) for row in cursor 
        }

    def getedsmfiles(self):
        sys.stderr.write('    Getting body line counts\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('''
            SELECT FileId, COUNT(LineNo)
            FROM EDSMFileLineBodies flb
            JOIN SystemBodies_EDSM sb ON sb.EdsmId = flb.EdsmBodyId
            GROUP BY FileId
        ''')
        bodylinecounts = { row[0]: row[1] for row in cursor }

        sys.stderr.write('    Getting file info\n')
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('''
            SELECT 
                Id, 
                FileName, 
                Date, 
                LineCount,
                CompressedSize
            FROM EDSMFiles f
            ORDER BY Date
        ''')

        return { 
            row[1]: EDSMFile(
                row[0],
                row[1],
                row[2],
                row[3],
                bodylinecounts[row[0]] if row[0] in bodylinecounts else 0,
                row[4]
            ) for row in cursor 
        }

    def updatefileinfo(self, fileid, linecount, totalsize, comprsize, poplinecount, stnlinecount, navroutesystemcount, marketitemcount):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute(
            'UPDATE Files SET ' +
            'LineCount = %s, ' +
            'CompressedSize = %s, ' +
            'UncompressedSize = %s, ' +
            'PopulatedLineCount = %s, ' +
            'StationLineCount = %s, ' +
            'NavRouteSystemCount = %s, ' +
            'MarketItemCount = %s ' +
            'WHERE Id = %s',
            (linecount, comprsize, totalsize, poplinecount, stnlinecount, navroutesystemcount, marketitemcount, fileid)
        )

    def updateedsmfileinfo(self, fileid, linecount, totalsize, comprsize):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute(
            'UPDATE EDSMFiles SET LineCount = %s, CompressedSize = %s, UncompressedSize = %s WHERE Id = %s',
            (linecount, comprsize, totalsize, fileid)
        )

class Timer(object):
    def __init__(self, names):
        self.tstart = timer()
        self.timers = {n: 0 for n in names}
        self.counts = {n: 0 for n in names}

    def time(self, name, count = 1):
        tend = timer()
        self.timers[name] += tend - self.tstart
        self.counts[name] += count
        self.tstart = tend

    def printstats(self):
        sys.stderr.write('\nTimes taken:\n')
        for name, time in sorted(self.timers.items()):
            count = self.counts[name]
            sys.stderr.write('  {0}: {1}s / {2} ({3}ms/iteration)\n'.format(name, time, count, time * 1000 / (count or 1)))

def timestamptosql(timestamp):
    if timestamp is None:
        return None
    else:
        if timestamp[-1] == 'Z':
            timestamp = timestamp[:-1]
        if len(timestamp) == 26 and timestamp[19] == '.':
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
        else:
            return datetime.strptime(timestamp[:19], '%Y-%m-%dT%H:%M:%S')

def processedsmmissingbodies(sysdb, timer):
    sys.stderr.write('Processing EDSM missing bodies\n')
    w = 0
    wg = 0
    w2 = 0
    from timeit import default_timer
    tstart = default_timer()

    fn = 'fetchbodies-{0}.jsonl'.format(datetime.utcnow().isoformat())
    fileid = sysdb.insertedsmfile(fn)

    timer.time('bodyquery')

    with open(edsmbodiesdir + '/' + fn, 'w', encoding='utf-8') as f:
        linecount = 0
        totalsize = 0
        updatecache = False

        for i in range(148480000 - 256000, len(sysdb.edsmbodyids) - 2097152):
            row = sysdb.edsmbodyids[i]

            if row[1] == 0:
                sys.stderr.write('{0:10d}'.format(i) + '\b' * 10)
                sys.stderr.flush()

                bodies = sysdb.getbodiesfromedsmbyid(i, timer)

                if len(bodies) == 0:
                    sysdb.updateedsmbodyid(0, i, tsbasedate)
                else:
                    bodiestoinsert = []
                    for msg in bodies:
                        line = json.dumps(msg)
                        f.write(line + '\n')
                        f.flush()
                        linecount += 1
                        totalsize += len(line) + 1
                        bodiestoinsert += [(fileid, linecount + 1, msg['id'])]
                        wg += 1
                        edsmbodyid = msg['id']
                        bodyid = msg['bodyId']
                        bodyname = msg['name']
                        edsmsysid = msg['systemId']
                        sysname = msg['systemName']
                        timestamp = msg['updateTime'].replace(' ', 'T')
                        periapsis = msg.get('argOfPeriapsis')
                        semimajor = msg.get('semiMajorAxis')
                        bodytype = msg['type']
                        subtype = msg['subType']
                        sqltimestamp = timestamptosql(timestamp)
                        sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                        timer.time('parse')
                        (sysid, _, _, _) = sysdb.findedsmsysid(edsmsysid)
                        (sysbodyid, ts, rec) = sysdb.findedsmbodyid(edsmbodyid)
                        scanbodyid = -1

                        if sysid and ts != sqlts:
                            system = sysdb.getsystembyid(sysid)
                            timer.time('sysquery')

                            if system is not None:
                                body = {}
                                
                                if bodytype == 'Planet':
                                    body['PlanetClass'] = subtype
                                elif bodytype == 'Star':
                                    body['StarType'] = subtype
                                
                                if periapsis is not None:
                                    body['Periapsis'] = periapsis
                                
                                if semimajor:
                                    body['SemiMajorAxis'] = semimajor * 149597870700
                                
                                (scanbody, rejectReason, rejectData) = sysdb.getbody(timer, bodyname, sysname, bodyid, system, body, sqltimestamp)

                                if scanbody:
                                    scanbodyid = scanbody.id
                                
                                timer.time('bodyquery')
                       
                        sysdb.updateedsmbodyid(scanbodyid, edsmbodyid, sqltimestamp)

                    sysdb.addedsmfilelinebodies(bodiestoinsert)
                    timer.time('bodyinsert', len(bodiestoinsert))
                    sysdb.updateedsmfileinfo(fileid, linecount, totalsize, totalsize)
                        
                w += 1
                w2 += 1

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write(('.' if w == 0 else (':' if wg == 0 else '#')) + (' ' * 10) + ('\b' * 10))
                sys.stderr.flush()

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDSMBodyM:{0}'.format(i + 1))
                
                if w2 >= 10:
                    sysdb.saveedsmbodycache()
                    w2 = 0

                timer.time('commit')

                w = 0
                wg = 0
                        
                #if default_timer() - tstart > 18 * 60 * 60:
                #    break

        sys.stderr.write('  {0}\n'.format(i + 1))
        sys.stderr.flush()
        sysdb.commit()
        sysdb.saveedsmbodycache()
        timer.time('commit')


def processedsmbodies(sysdb, filename, fileinfo, reprocess, timer, rejectout):
    fn = None

    if fileinfo.date is not None:
        fn = edsmbodiesdir + '/' + fileinfo.date.isoformat()[:7] + '/' + filename

    if os.path.exists(edsmdumpdir + '/' + filename):
        fn = edsmdumpdir + '/' + filename
    
    if fn is not None and os.path.exists(fn):
        statinfo = os.stat(fn)
        comprsize = statinfo.st_size
        
        if ((fileinfo.date is None and comprsize != fileinfo.comprsize)
            or fileinfo.linecount is None
            or (reprocess == True and fileinfo.linecount != fileinfo.bodylinecount)):
            
            sys.stderr.write('Processing EDSM bodies file {0} ({1} / {2})\n'.format(filename, fileinfo.bodylinecount, fileinfo.linecount))

            with bz2.BZ2File(fn, 'r') as f:
                lines = sysdb.getedsmbodyfilelines(fileinfo.id)
                linecount = 0
                totalsize = 0
                bodiestoinsert = []
                timer.time('load')
                updatecache = False

                for lineno, line in enumerate(f):
                    if ((lineno + 1) >= len(lines)
                    or lines[lineno + 1] == 0
                    or lines[lineno + 1] > len(sysdb.edsmbodyids)
                    or sysdb.edsmbodyids[lines[lineno + 1]][0] == 0):
                        try:
                            msg = json.loads(line)
                            edsmbodyid = msg['id']
                            bodyid = msg['bodyId']
                            bodyname = msg['name']
                            edsmsysid = msg['systemId']
                            sysname = msg['systemName']
                            timestamp = msg['updateTime'].replace(' ', 'T')
                            periapsis = msg.get('argOfPeriapsis')
                            semimajor = msg.get('semiMajorAxis')
                            bodytype = msg['type']
                            subtype = msg['subType']
                        except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                            sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                            rejectmsg = {
                                'rejectReason': 'Invalid',
                                'exception': '{0}'.format(sys.exc_info()[1]),
                                'line': line.decode('utf-8', 'backslashreplace')
                            }
                            rejectout.write(json.dumps(rejectmsg) + '\n')
                            timer.time('error')
                            pass
                        else:
                            sqltimestamp = timestamptosql(timestamp)
                            sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                            timer.time('parse')
                            reject = True
                            rejectReason = None
                            rejectData = None
                            (sysid, _, _, _) = sysdb.findedsmsysid(edsmsysid)
                            (sysbodyid, ts, rec) = sysdb.findedsmbodyid(edsmbodyid)
                            
                            if (lineno + 1) >= len(lines) or lines[lineno + 1] == 0:
                                bodiestoinsert += [(fileinfo.id, lineno + 1, edsmbodyid)]

                            if sysid and ts != sqlts:
                                system = sysdb.getsystembyid(sysid)
                                timer.time('sysquery')

                                if system is not None:
                                    body = {}
                                    
                                    if bodytype == 'Planet':
                                        body['PlanetClass'] = subtype
                                    elif bodytype == 'Star':
                                        body['StarType'] = subtype
                                    
                                    if periapsis is not None:
                                        body['Periapsis'] = periapsis
                                    
                                    if semimajor:
                                        body['SemiMajorAxis'] = semimajor * 149597870700
                                    
                                    (scanbody, rejectReason, rejectData) = sysdb.getbody(timer, bodyname, sysname, bodyid, system, body, sqltimestamp)

                                    if scanbody:
                                        sysdb.updateedsmbodyid(scanbody.id, edsmbodyid, sqltimestamp)
                                        reject = False
                                        updatecache = True
                                    
                                    timer.time('bodyquery')

                            if reject and rejectReason is not None:
                                rejectmsg = {
                                    'rejectReason': rejectReason,
                                    'rejectData': rejectData,
                                    'data': msg
                                }
                                rejectout.write(json.dumps(rejectmsg) + '\n')

                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        sysdb.commit()
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if len(bodiestoinsert) != 0:
                            sysdb.addedsmfilelinebodies(bodiestoinsert)
                            timer.time('bodyinsert', len(bodiestoinsert))
                            bodiestoinsert = []

                        if (linecount % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(linecount))
                            sys.stderr.flush()
                            updatetitleprogress('{0}:{1}'.format(filename,linecount))

                            if updatecache:
                                sysdb.saveedsmbodycache()
                                updatecache = False

            if len(bodiestoinsert) != 0:
                sysdb.addedsmfilelinebodies(bodiestoinsert)
                timer.time('bodyinsert', len(bodiestoinsert))
                bodiestoinsert = []

            sys.stderr.write('  {0}\n'.format(linecount))
            sys.stderr.flush()
            updatetitleprogress('{0}:{1}'.format(filename,linecount))
            sysdb.commit()
            sysdb.saveedsmbodycache()
            timer.time('commit')
            sysdb.updateedsmfileinfo(fileinfo.id, linecount, totalsize, comprsize)

def processedsmstations(sysdb, timer, rejectout):
    sys.stderr.write('Processing EDSM stations\n')
    with gzip.open(edsmstationsfile, 'r') as f:
        stations = json.load(f)
        w = 0
        for i, msg in enumerate(stations):
            timer.time('read')
            try:
                edsmstationid = msg['id']
                marketid = msg['marketId']
                stationname = msg['name']
                stntype = msg['type']
                stntype = EDSMStationTypes[stntype] if stntype in EDSMStationTypes else stntype
                edsmsysid = msg['systemId']
                sysname = msg['systemName']
                timestamp = msg['updateTime']['information'].replace(' ', 'T')
            except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                rejectmsg = {
                    'rejectReason': 'Invalid',
                    'exception': '{0}'.format(sys.exc_info()[1]),
                    'data': msg
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
                timer.time('error')
                pass
            else:
                sqltimestamp = timestamptosql(timestamp)
                sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                timer.time('parse')
                (sysid, ts, hascoord, rec) = sysdb.findedsmsysid(edsmsysid)
                timer.time('sysquery')
                reject = True
                rejectReason = None
                rejectData = None

                if sysid:
                    system = sysdb.getsystembyid(sysid)
                    timer.time('sysquery')

                    if system is not None:
                        if stationname is not None and stationname != '':
                            (station, rejectReason, rejectData) = sysdb.getstation(timer, stationname, sysname, marketid, sqltimestamp, system, stntype)
                            timer.time('stnquery')
                            if station is not None:
                                sysdb.updateedsmstationid(edsmstationid, station.id, sqltimestamp)
                                reject = False
                        else:
                            rejectReason = 'No station name'
                    else:
                        rejectReason = 'System not found'

                if reject:
                    rejectmsg = {
                        'rejectReason': rejectReason,
                        'rejectData': rejectData,
                        'data': msg
                    }
                    rejectout.write(json.dumps(rejectmsg) + '\n')


            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                timer.time('commit')
                    
    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    timer.time('commit')

def processedsmsystems(sysdb, timer, rejectout):
    sys.stderr.write('Processing EDSM systems\n')
    for i, rec in enumerate(sysdb.edsmsysids):
        if rec[1] == i and rec[5] == 0:
            rec.processed -= 1

    with bz2.BZ2File(edsmsysfile, 'r') as f:
        w = 0
        for i, line in enumerate(f):
            timer.time('read')
            try:
                msg = json.loads(line)
                edsmsysid = msg['id']
                sysaddr = msg['id64']
                sysname = msg['name']
                coords = msg['coords']
                starpos = [coords['x'],coords['y'],coords['z']]
                timestamp = msg['date'].replace(' ', 'T')
            except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                rejectmsg = {
                    'rejectReason': 'Invalid',
                    'exception': '{0}'.format(sys.exc_info()[1]),
                    'line': line.decode('utf-8', 'backslashreplace')
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
                timer.time('error')
                pass
            else:
                sqltimestamp = timestamptosql(timestamp)
                sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                timer.time('parse')
                (sysid, ts, hascoord, rec) = sysdb.findedsmsysid(edsmsysid)
                timer.time('sysquery')
                if not sysid or ts != sqlts or not hascoord:
                    starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                    (system, rejectReason, rejectData) = sysdb.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], sysaddr)
                    timer.time('sysquery', 0)

                    if system is not None:
                        rec = sysdb.updateedsmsysid(edsmsysid, system.id, sqltimestamp, True, False, False)
                    else:
                        rejectmsg = {
                            'rejectReason': rejectReason,
                            'rejectData': rejectData,
                            'data': msg
                        }
                        rejectout.write(json.dumps(rejectmsg) + '\n')

                    timer.time('edsmupdate')
                    w += 1
                    
                if rec is not None:
                    rec.processed = 7

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDSMSys:{0}'.format(i + 1))
                    sysdb.saveedsmsyscache()
                timer.time('commit')
                    
    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    sysdb.saveedsmsyscache()
    timer.time('commit')

def processedsmsystemswithoutcoords(sysdb, timer, rejectout):
    sys.stderr.write('Processing EDSM systems without coords\n')
    with bz2.BZ2File(edsmsyswithoutcoordsfile, 'r') as f:
        w = 0
        for i, line in enumerate(f):
            timer.time('read')
            try:
                msg = json.loads(line)
                edsmsysid = msg['id']
                sysaddr = msg['id64']
                sysname = msg['name']
                timestamp = msg['date'].replace(' ', 'T')
            except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                rejectmsg = {
                    'rejectReason': 'Invalid',
                    'exception': '{0}'.format(sys.exc_info()[1]),
                    'line': line.decode('utf-8', 'backslashreplace')
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
                timer.time('error')
                pass
            else:
                sqltimestamp = timestamptosql(timestamp)
                sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                timer.time('parse')
                (sysid, ts, hascoord, rec) = sysdb.findedsmsysid(edsmsysid)
                timer.time('sysquery')
                if not sysid:
                    (system, rejectReason, rejectData) = sysdb.getsystem(timer, sysname, None, None, None, sysaddr)
                    timer.time('sysquery', 0)

                    if system is not None:
                        rec = sysdb.updateedsmsysid(edsmsysid, system.id, sqltimestamp, False, False, False)
                    else:
                        rejectmsg = {
                            'rejectReason': rejectReason,
                            'rejectData': rejectData,
                            'data': msg
                        }
                        rejectout.write(json.dumps(rejectmsg) + '\n')

                    timer.time('edsmupdate')
                    w += 1
                elif ts != sqlts or hascoord != False:
                    sysdb.updateedsmsysid(edsmsysid, sysid, sqltimestamp, False, False, False)
                    w += 1
                    
                if rec is not None:
                    rec.processed = 7

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDSMSysNC:{0}'.format(i + 1))
                    sysdb.saveedsmsyscache()
                timer.time('commit')
                    
    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    sysdb.saveedsmsyscache()
    timer.time('commit')

def processedsmsystemswithoutcoordsprepurge(sysdb, timer, rejectout):
    sys.stderr.write('Processing pre-purge EDSM systems without coords\n')
    with bz2.BZ2File(edsmsyswithoutcoordsprepurgefile, 'r') as f:
        w = 0
        for i, line in enumerate(f):
            timer.time('read')
            try:
                msg = json.loads(line)
                edsmsysid = msg['id']
                sysaddr = msg['id64']
                sysname = msg['name']
                timestamp = msg['date'].replace(' ', 'T')
            except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                rejectmsg = {
                    'rejectReason': 'Invalid',
                    'exception': '{0}'.format(sys.exc_info()[1]),
                    'line': line.decode('utf-8', 'backslashreplace')
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
                timer.time('error')
                pass
            else:
                sqltimestamp = timestamptosql(timestamp)
                sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                timer.time('parse')
                (sysid, ts, hascoord, rec) = sysdb.findedsmsysid(edsmsysid)
                timer.time('sysquery')
                if not sysid:
                    (system, rejectReason, rejectData) = sysdb.getsystem(timer, sysname, None, None, None, sysaddr)
                    timer.time('sysquery', 0)

                    if system is not None:
                        rec = sysdb.updateedsmsysid(edsmsysid, system.id, sqltimestamp, False, False, False)
                    else:
                        rejectmsg = {
                            'rejectReason': rejectReason,
                            'rejectData': rejectData,
                            'data': msg
                        }
                        rejectout.write(json.dumps(rejectmsg) + '\n')

                    timer.time('edsmupdate')
                    w += 1

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDSMSysNCP:{0}'.format(i + 1))
                    sysdb.saveedsmsyscache()
                timer.time('commit')
                    
    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    sysdb.saveedsmsyscache()
    timer.time('commit')

def processedsmhiddensystems(sysdb, timer, rejectout):
    sys.stderr.write('Processing EDSM hidden systems\n')
    with bz2.BZ2File(edsmhiddensysfile, 'r') as f:
        w = 0
        for i, line in enumerate(f):
            timer.time('read')
            try:
                msg = json.loads(line)
                edsmsysid = msg['id']
                sysname = msg['system']
            except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                rejectmsg = {
                    'rejectReason': 'Invalid',
                    'exception': '{0}'.format(sys.exc_info()[1]),
                    'line': line.decode('utf-8', 'backslashreplace')
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
                timer.time('error')
                pass
            else:
                timer.time('parse')
                (sysid, ts, hascoord, rec) = sysdb.findedsmsysid(edsmsysid)
                timer.time('sysquery')

                if sysid:
                    rec = sysdb.updateedsmsysid(edsmsysid, sysid, ts, False, True, False)
                    w += 1
                    
                if rec is not None:
                    rec.processed = 7

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDSMSysHid:{0}'.format(i + 1))
                timer.time('commit')
                    
    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    sysdb.saveedsmsyscache()
    timer.time('commit')

def processedsmdeletedsystems(sysdb, timer, rejectout):
    sys.stderr.write('Processing EDSM deleted systems\n')
    w = 0
    w2 = 0
    from timeit import default_timer
    tstart = default_timer()
    
    #for row in sysdb.edsmsysids:
    #    row[5] = 0
    #
    #sysdb.saveedsmsyscache()

    for i, row in enumerate(sysdb.edsmsysids):
        if row[1] == i and row[6] <= 0 and row[5] == 0:
            sys.stderr.write('{0:10d}'.format(row[1]) + '\b' * 10)
            sys.stderr.flush()
            rec = row
            if not sysdb.updatesystemfromedsmbyid(row[1], timer, rejectout):
                rec = sysdb.updateedsmsysid(row[1], row[0], row[2], False, False, True)

            rec.processed = 7

            w += 1
            w2 += 1

            if w >= 50:
                import pdb; pdb.set_trace();

        if ((i + 1) % 1000) == 0:
            sysdb.commit()
            sys.stderr.write('.' if w == 0 else '*' + (' ' * 10) + ('\b' * 10))
            sys.stderr.flush()
            
            if ((i + 1) % 64000) == 0:
                sys.stderr.write('  {0}\n'.format(i + 1))
                sys.stderr.flush()
                updatetitleprogress('EDSMSysDel:{0}'.format(i + 1))
            
            if w2 >= 10:
                sysdb.saveedsmsyscache()
                w2 = 0

            timer.time('commit')

            w = 0
                    
            if default_timer() - tstart > 18 * 60 * 60:
                break

    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    sysdb.saveedsmsyscache()
    timer.time('commit')

def processeddbsystems(sysdb, timer, rejectout):
    sys.stderr.write('Processing EDDB systems\n')
    with bz2.open(eddbsysfile, 'rt', encoding='utf8') as f:
        csvreader = csv.DictReader(f)
        w = 0
        for i, msg in enumerate(csvreader):
            timer.time('read')
            try:
                eddbsysid = int(msg['id'])
                sysname = msg['name']
                starpos = [float(msg['x']),float(msg['y']),float(msg['z'])]
                timestamp = int(msg['updated_at'])
            except (OverflowError,ValueError,TypeError):
                sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                rejectmsg = {
                    'rejectReason': 'Invalid',
                    'exception': '{0}'.format(sys.exc_info()[1]),
                    'data': msg
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
                timer.time('error')
                pass
            else:
                timer.time('parse')
                (sysid, ts) = sysdb.findeddbsysid(eddbsysid)
                timer.time('sysquery')
                if not sysid or ts != timestamp:
                    starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                    (system, rejectReason, rejectData) = sysdb.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], None)
                    timer.time('sysquery', 0)

                    if system is not None:
                        sysdb.updateeddbsysid(eddbsysid, system.id, timestamp)
                    else:
                        rejectmsg = {
                            'rejectReason': rejectReason,
                            'rejectData': rejectData,
                            'data': msg
                        }
                        rejectout.write(json.dumps(rejectmsg) + '\n')
                    timer.time('eddbupdate')
                    w += 1

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDDBSys:{0}'.format(i + 1))
                timer.time('commit')
                    
    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    timer.time('commit')

def processeddnjournalfile(sysdb, timer, filename, fileinfo, reprocess, reprocessall, rejectout):
    #if fileinfo.eventtype in ('Location'):
    #    continue
    if (fileinfo.linecount is None 
        or fileinfo.populatedlinecount is None
        or (fileinfo.stationlinecount is None and fileinfo.eventtype in ('Docked', 'Location', 'CarrierJump'))
        or (reprocessall == True and fileinfo.eventtype == 'Scan' and fileinfo.date >= ed300date.date()) 
        or (reprocess == True 
            and (fileinfo.linecount != fileinfo.infolinecount
                 or (fileinfo.eventtype in ('Docked', 'Location', 'CarrierJump') and fileinfo.stnlinecount != fileinfo.stationlinecount)
                 or fileinfo.populatedlinecount != fileinfo.factionlinecount))):
        fn = eddndir + '/' + fileinfo.date.isoformat()[:7] + '/' + filename
        if os.path.exists(fn):
            sys.stderr.write('{0}\n'.format(fn))
            updatetitleprogress('{0}:{1}'.format(fileinfo.date.isoformat()[:10], fileinfo.eventtype))
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                stnlines = sysdb.getstationfilelines(fileinfo.id)
                infolines = sysdb.getinfofilelines(fileinfo.id)
                factionlines = sysdb.getfactionfilelines(fileinfo.id)
                linecount = 0
                poplinecount = 0
                stnlinecount = 0
                totalsize = 0
                timer.time('load')
                stntoinsert = []
                infotoinsert = []
                factionstoinsert = []
                for lineno, line in enumerate(f):
                    if (lineno + 1) not in infolines or (reprocessall == True and fileinfo.eventtype == 'Scan'):
                        timer.time('read')
                        msg = None
                        try:
                            msg = json.loads(line)
                            body = msg['message']
                            hdr = msg['header']
                            eventtype = body.get('event')

                            if 'StarSystem' in body:
                                sysname = body['StarSystem']
                            elif 'System' in body:
                                sysname = body['System']
                            elif 'SystemName' in body:
                                sysname = body['SystemName']
                            else:
                                sysname = body['StarSystem']

                            starpos = body['StarPos']
                            sysaddr = body.get('SystemAddress')
                            stationname = body.get('StationName')

                            if fileinfo.eventtype == 'ApproachSettlement':
                                stationname = body.get('Name')

                            marketid = body.get('MarketID')
                            stationtype = body.get('StationType')
                            bodyname = body.get('Body')
                            bodyid = body.get('BodyID')
                            bodytype = body.get('BodyType')
                            scanbodyname = body.get('BodyName')
                            parents = body.get('Parents')
                            factions = body.get('Factions')
                            sysfaction = body['SystemFaction'] if 'SystemFaction' in body else (body.get('Faction'))
                            sysgovern = body['SystemGovernment'] if 'SystemGovernment' in body else (body.get('Government'))
                            sysalleg = body['SystemAllegiance'] if 'SystemAllegiance' in body else (body['Allegiance'] if 'Allegiance' in body else '')
                            stnfaction = body.get('StationFaction')
                            stngovern = body.get('StationGovernment')
                            timestamp = body.get('timestamp')
                            gwtimestamp = hdr.get('gatewayTimestamp')
                            software = hdr.get('softwareName')
                            distfromstar = body.get('DistanceFromArrivalLS')
                        except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                            sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[1]))
                            msg = {
                                'rejectReason': 'Invalid',
                                'exception': '{0}'.format(sys.exc_info()[1]),
                                'rawmessage': line.decode('utf-8')
                            }
                            rejectout.write(json.dumps(msg) + '\n')
                            timer.time('error')
                            pass
                        else:
                            if marketid is not None and (marketid <= 0 or marketid > 1 << 32):
                                marketid = None
                            sqltimestamp = timestamptosql(timestamp)
                            sqlgwtimestamp = timestamptosql(gwtimestamp)
                            timer.time('parse')
                            reject = False
                            rejectReason = None
                            rejectData = None
                            systemid = None
                            sysbodyid = None
                            linelen = len(line)

                            if factions is not None or sysfaction is not None or stnfaction is not None:
                                poplinecount += 1

                            if stationname is not None or marketid is not None:
                                stnlinecount += 1

                            if sqltimestamp is not None and sqlgwtimestamp is not None and sqltimestamp < sqlgwtimestamp + timedelta(days = 1):
                                starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                                (system, rejectReason, rejectData) = sysdb.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], sysaddr)
                                timer.time('sysquery')
                                if system is not None:
                                    systemid = system.id
                                    if (lineno + 1) not in stnlines and sqltimestamp is not None and not (sqltimestamp >= ed303date and sqltimestamp < ed304date and not allow303bodies):
                                        if stationname is not None and stationname != '':
                                            (station, rejectReason, rejectData) = sysdb.getstation(timer, stationname, sysname, marketid, sqltimestamp, system, stationtype, bodyname, bodyid, bodytype, eventtype, fileinfo.test)
                                            timer.time('stnquery')

                                            if station is not None:
                                                stntoinsert += [(fileinfo.id, lineno + 1, station)]
                                            else:
                                                reject = True
                                        elif bodyname is not None and bodytype is not None and bodytype == 'Station':
                                            (station, rejectReason, rejectData) = sysdb.getstation(timer, bodyname, sysname, None, sqltimestamp, system = system, bodyid = bodyid, eventtype = eventtype, test = fileinfo.test)
                                            timer.time('stnquery')

                                            if station is not None:
                                                stntoinsert += [(fileinfo.id, lineno + 1, station)]
                                            else:
                                                reject = True

                                    if (lineno + 1) not in infolines and sqltimestamp is not None and not (sqltimestamp >= ed303date and sqltimestamp < ed304date and not allow303bodies):
                                        if scanbodyname is not None:
                                            (scanbody, rejectReason, rejectData) = sysdb.getbody(timer, scanbodyname, sysname, bodyid, system, body, sqltimestamp)
                                            if scanbody is not None:
                                                sysdb.insertbodyparents(timer, scanbody.id, system, bodyid, parents)
                                                sysbodyid = scanbody.id
                                            else:
                                                reject = True
                                            timer.time('bodyquery')
                                        elif bodyname is not None and bodytype is not None and bodytype != 'Station':
                                            (scanbody, rejectReason, rejectData) = sysdb.getbody(timer, bodyname, sysname, bodyid, system, {}, sqltimestamp)
                                            if scanbody is not None:
                                                sysbodyid = scanbody.id
                                            else:
                                                reject = True
                                            timer.time('bodyquery')
                                    elif (reprocessall == True or (lineno + 1) not in infolines) and sqltimestamp is not None and not (sqltimestamp >= ed303date and sqltimestamp < ed304date and not allow303bodies):
                                        if scanbodyname is not None:
                                            (scanbody, rejectReason, rejectData) = sysdb.getbody(timer, scanbodyname, sysname, bodyid, system, body, sqltimestamp)
                                            if scanbody is not None:
                                                sysbodyid = scanbody.id
                                                sysdb.insertbodyparents(timer, scanbody.id, system, bodyid, parents)
                                            else:
                                                reject = True
                                            timer.time('bodyquery')
                                        elif bodyname is not None and bodytype is not None and bodytype != 'Station':
                                            (scanbody, rejectReason, rejectData) = sysdb.getbody(timer, bodyname, sysname, bodyid, system, {}, sqltimestamp)
                                            if scanbody is not None:
                                                sysbodyid = scanbody.id
                                            else:
                                                reject = True
                                            timer.time('bodyquery')

                                    if (lineno + 1) not in factionlines and not reject:
                                        linefactions = []
                                        linefactiondata = []
                                        if factions is not None:
                                            for n, faction in enumerate(factions):
                                                linefactiondata += [{
                                                    'Name': faction['Name'],
                                                    'Government': faction['Government'],
                                                    'Allegiance': faction.get('Allegiance'),
                                                    'EntryNum': n
                                                }]
                                                linefactions += [(n, sysdb.getfaction(timer, faction['Name'], faction['Government'], faction.get('Allegiance')))]
                                        if sysfaction is not None:
                                            if type(sysfaction) is dict and 'Name' in sysfaction:
                                                sysfaction = sysfaction['Name']
                                            linefactiondata += [{
                                                'Name': sysfaction,
                                                'Government': sysgovern,
                                                'Allegiance': sysalleg,
                                                'EntryNum': -1
                                            }]
                                            linefactions += [(-1, sysdb.getfaction(timer, sysfaction, sysgovern, sysalleg))]
                                        if stnfaction is not None:
                                            if type(stnfaction) is dict and 'Name' in stnfaction:
                                                stnfaction = stnfaction['Name']
                                            if stnfaction != 'FleetCarrier':
                                                linefactiondata += [{
                                                    'Name': stnfaction,
                                                    'Government': stngovern,
                                                    'EntryNum': -2
                                                }]
                                                linefactions += [(-2, sysdb.getfaction(timer, stnfaction, stngovern, None))]

                                        if len(linefactions) != 0:
                                            if len([fid for n, fid in linefactions if fid is None]) != 0:
                                                reject = True
                                                rejectReason = 'Faction not found'
                                                rejectData = linefactiondata
                                            else:
                                                for n, faction in linefactions:
                                                    factionstoinsert += [(fileinfo.id, lineno + 1, faction, n)]

                                        timer.time('factionupdate')

                                    if reject:
                                        msg['rejectReason'] = rejectReason
                                        msg['rejectData'] = rejectData
                                        rejectout.write(json.dumps(msg) + '\n')
                                    else:
                                        if (lineno + 1) not in infolines:
                                            sysdb.insertsoftware(software)
                                            infotoinsert += [(
                                                fileinfo.id,
                                                lineno + 1,
                                                sqltimestamp,
                                                sqlgwtimestamp,
                                                sysdb.software[software],
                                                systemid,
                                                sysbodyid,
                                                linelen,
                                                distfromstar,
                                                1 if 'BodyID' in body else 0,
                                                1 if 'SystemAddress' in body else 0,
                                                1 if 'MarketID' in body else 0
                                            )]

                                else:
                                    msg['rejectReason'] = rejectReason
                                    msg['rejectData'] = rejectData
                                    rejectout.write(json.dumps(msg) + '\n')
                            else:
                                msg['rejectReason'] = 'Timestamp error'
                                rejectout.write(json.dumps(msg) + '\n')

                                
                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        sysdb.commit()
                        if len(stntoinsert) != 0:
                            sysdb.addfilelinestations(stntoinsert)
                            timer.time('stninsert', len(stntoinsert))
                            stntoinsert = []
                        if len(infotoinsert) != 0:
                            sysdb.addfilelineinfo(infotoinsert)
                            timer.time('infoinsert', len(infotoinsert))
                            infotoinsert = []
                        if len(factionstoinsert) != 0:
                            sysdb.addfilelinefactions(factionstoinsert)
                            timer.time('factioninsert', len(factionstoinsert))
                            factionstoinsert = []
                        sysdb.commit()
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(lineno + 1))
                            sys.stderr.flush()
                
                sysdb.commit()
                if len(stntoinsert) != 0:
                    sysdb.addfilelinestations(stntoinsert)
                    timer.time('stninsert', len(stntoinsert))
                    stntoinsert = []
                if len(infotoinsert) != 0:
                    sysdb.addfilelineinfo(infotoinsert)
                    timer.time('infoinsert', len(infotoinsert))
                    infotoinsert = []
                if len(factionstoinsert) != 0:
                    sysdb.addfilelinefactions(factionstoinsert)
                    timer.time('factioninsert', len(factionstoinsert))
                    factionstoinsert = []

                sysdb.commit()

                sys.stderr.write('  {0}\n'.format(linecount))
                sys.stderr.flush()
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, poplinecount, stnlinecount, 0, 0)

def processeddnjournalroute(sysdb, timer, filename, fileinfo, reprocess, rejectout):
    #if fileinfo.eventtype in ('Location'):
    #    continue
    if (fileinfo.linecount is None 
        or (reprocess == True and fileinfo.linecount != fileinfo.infolinecount)
        or (reprocess == True and fileinfo.routesystemcount != fileinfo.navroutesystemcount)):
        fn = eddndir + '/' + fileinfo.date.isoformat()[:7] + '/' + filename
        if os.path.exists(fn):
            sys.stderr.write('{0}\n'.format(fn))
            updatetitleprogress('{0}:{1}'.format(fileinfo.date.isoformat()[:10], fileinfo.eventtype))
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                infolines = sysdb.getinfofilelines(fileinfo.id)
                navroutelines = sysdb.getnavroutefilelines(fileinfo.id)
                linecount = 0
                routesystemcount = 0
                totalsize = 0
                timer.time('load')
                infotoinsert = []
                routesystemstoinsert = []
                for lineno, line in enumerate(f):
                    timer.time('read')
                    msg = None
                    try:
                        msg = json.loads(line)
                        body = msg['message']
                        hdr = msg['header']
                        timestamp = body.get('timestamp')
                        route = list(body['Route'])
                        gwtimestamp = hdr.get('gatewayTimestamp')
                        software = hdr.get('softwareName')
                    except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                        sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[1]))
                        msg = {
                            'rejectReason': 'Invalid',
                            'exception': '{0}'.format(sys.exc_info()[1]),
                            'rawmessage': line.decode('utf-8')
                        }
                        rejectout.write(json.dumps(msg) + '\n')
                        timer.time('error')
                        pass
                    else:
                        sqltimestamp = timestamptosql(timestamp)
                        sqlgwtimestamp = timestamptosql(gwtimestamp)
                        timer.time('parse')
                        reject = False
                        rejectReason = None
                        rejectData = None
                        linelen = len(line)
                        lineroutes = []

                        for n, system in enumerate(route):
                            try:
                                sysname = system['StarSystem']
                                starpos = system['StarPos']
                                sysaddr = system['SystemAddress']
                            except ValueError:
                                lineroutes += [(None, n + 1, "Missing property", system)]
                            else:
                                starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                                (system, sysRejectReason, sysRejectData) = sysdb.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], sysaddr)
                                timer.time('sysquery')
                                lineroutes += [(system, n + 1, sysRejectReason, sysRejectData)]
                            
                        if sqltimestamp is not None and sqlgwtimestamp is not None and sqltimestamp < sqlgwtimestamp + timedelta(days = 1):
                            if len(lineroutes) < 2:
                                reject = True
                                rejectReason = 'Route too short'
                                rejectData = route
                            elif len([system for system, _, _, _ in lineroutes if system is None]) != 0:
                                sysRejects = [(system, rejectReason, rejectData, n) for system, n, rejectReason, rejectData in lineroutes if system is None]
                                reject = True
                                rejectReason = 'One or more systems failed validation'
                                rejectData = [{
                                    'entrynum': n,
                                    'rejectReason': rejectReason,
                                    'rejectData': rejectData
                                } for _, rejectReason, rejectData, n in sysRejects]

                            if reject:
                                msg['rejectReason'] = rejectReason
                                msg['rejectData'] = rejectData
                                rejectout.write(json.dumps(msg) + '\n')
                            else:
                                for system, n, _, _ in lineroutes:
                                    if (lineno + 1, n) not in navroutelines:
                                        routesystemstoinsert += [(fileinfo.id, lineno + 1, system, n)]
                                
                                if (lineno + 1) not in infolines:
                                    sysdb.insertsoftware(software)
                                    system, _, _, _ = lineroutes[0]
                                    infotoinsert += [(
                                        fileinfo.id,
                                        lineno + 1,
                                        sqltimestamp,
                                        sqlgwtimestamp,
                                        sysdb.software[software],
                                        system.id,
                                        None,
                                        linelen,
                                        None,
                                        0,
                                        1,
                                        0
                                    )]

                        else:
                            msg['rejectReason'] = 'Timestamp error'
                            rejectout.write(json.dumps(msg) + '\n')
                        
                        routesystemcount += len(lineroutes)
                                
                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        sysdb.commit()
                        if len(infotoinsert) != 0:
                            sysdb.addfilelineinfo(infotoinsert)
                            timer.time('infoinsert', len(infotoinsert))
                            infotoinsert = []
                        if len(routesystemstoinsert) != 0:
                            sysdb.addfilelineroutesystems(routesystemstoinsert)
                            timer.time('routesysteminsert', len(routesystemstoinsert))
                            routesystemstoinsert = []
                        sysdb.commit()
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(lineno + 1))
                            sys.stderr.flush()
                
                sysdb.commit()
                if len(infotoinsert) != 0:
                    sysdb.addfilelineinfo(infotoinsert)
                    timer.time('infoinsert', len(infotoinsert))
                    infotoinsert = []
                if len(routesystemstoinsert) != 0:
                    sysdb.addfilelineroutesystems(routesystemstoinsert)
                    timer.time('routesysteminsert', len(routesystemstoinsert))
                    routesystemstoinsert = []

                sysdb.commit()

                sys.stderr.write('  {0}\n'.format(linecount))
                sys.stderr.flush()
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, 0, 0, routesystemcount, 0)

def processeddnmarketfile(sysdb, timer, filename, fileinfo, reprocess, rejectout):
    if (fileinfo.linecount is None 
        or (reprocess == True 
            and (fileinfo.linecount != fileinfo.stnlinecount
                 or fileinfo.linecount != fileinfo.infolinecount
                 or fileinfo.marketitemcount != fileinfo.marketitemlinecount))):
        fn = eddndir + '/' + fileinfo.date.isoformat()[:7] + '/' + filename
        if os.path.exists(fn):
            sys.stderr.write('{0}\n'.format(fn))
            updatetitleprogress('{0}:{1}'.format(fileinfo.date.isoformat()[:10], filename.split('-')[0]))
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                stnlines = sysdb.getstationfilelines(fileinfo.id)
                infolines = sysdb.getinfofilelines(fileinfo.id)
                mktlines = sysdb.getmarketitemfilelines(fileinfo.id)
                linecount = 0
                totalsize = 0
                mktitemcount = 0
                stntoinsert = []
                mktitemtoinsert = []
                infotoinsert = []
                nullitem = EDDNMarketItem(0, None, None)
                timer.time('load')
                for lineno, line in enumerate(f):
                    timer.time('read')
                    msg = None
                    try:
                        msg = json.loads(line)
                        body = msg['message']
                        hdr = msg['header']
                        sysname = body['systemName']
                        stationname = body['stationName']
                        marketid = body.get('marketId')
                        timestamp = body.get('timestamp')
                        gwtimestamp = hdr.get('gatewayTimestamp')
                        software = hdr.get('softwareName')
                        commodities = body.get('commodities')
                        prohibited = body.get('prohibited')
                        modules = body.get('modules')
                        ships = body.get('ships')
                    except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                        print('Error: {0}'.format(sys.exc_info()[1]))
                        msg = {
                            'rejectReason': 'Invalid',
                            'exception': '{0}'.format(sys.exc_info()[1]),
                            'rawmessage': line.decode('utf-8')
                        }
                        rejectout.write(json.dumps(msg) + '\n')
                        timer.time('error')
                        pass
                    else:
                        if marketid is not None and (marketid <= 0 or marketid > 1 << 32):
                            marketid = None
                        sqltimestamp = timestamptosql(timestamp)
                        sqlgwtimestamp = timestamptosql(gwtimestamp)
                        timer.time('parse')

                        mktitems = [(0, nullitem)]

                        if commodities is not None:
                            for n, commodity in enumerate(commodities):
                                marketitem = sysdb.getmarketitem(timer, commodity.get('name'), 'Commodity')
                                mktitems += [(n + 1, marketitem)]
                        elif modules is not None:
                            for n, item in enumerate(modules):
                                marketitem = sysdb.getmarketitem(timer, item, 'Module')
                                mktitems += [(n + 1, marketitem)]
                        elif ships is not None:
                            for n, item in enumerate(ships):
                                marketitem = sysdb.getmarketitem(timer, item, 'Ship')
                                mktitems += [(n + 1, marketitem)]

                        if prohibited is not None:
                            for n, item in enumerate(prohibited):
                                marketitem = sysdb.getmarketitem(timer, item, 'Commodity')
                                mktitems += [(-1 - n, marketitem)]

                        if sqltimestamp is not None and sqlgwtimestamp is not None and sqltimestamp < sqlgwtimestamp + timedelta(days = 1):
                            if (lineno + 1, 0) not in mktlines:
                                (mktstation, rejectReason, rejectData) = sysdb.getmarketstation(timer, stationname, sysname, marketid, sqltimestamp)

                                if mktstation is not None:
                                    for n, marketitem in mktitems:
                                        if (lineno + 1, n) not in mktlines:
                                            mktitemtoinsert += [(fileinfo.id, lineno + 1, mktstation, n, marketitem)]

                            if ((lineno + 1) not in stnlines or (lineno + 1) not in infolines):

                                (station, rejectReason, rejectData) = sysdb.getstation(timer, stationname, sysname, marketid, sqltimestamp, test = fileinfo.test)
                                timer.time('stnquery')

                                if station is not None:
                                    if (lineno + 1) not in stnlines:
                                        stntoinsert += [(fileinfo.id, lineno + 1, station)]

                                    if (lineno + 1) not in infolines:
                                        sysdb.insertsoftware(software)
                                        infotoinsert += [(
                                            fileinfo.id,
                                            lineno + 1,
                                            sqltimestamp,
                                            sqlgwtimestamp,
                                            sysdb.software[software],
                                            station.systemid,
                                            None,
                                            len(line),
                                            None,
                                            0,
                                            0,
                                            1 if 'marketId' in body else 0
                                        )]

                                else:
                                    msg['rejectReason'] = rejectReason
                                    msg['rejectData'] = rejectData
                                    rejectout.write(json.dumps(msg) + '\n')
                                    pass

                        mktitemcount += len(mktitems)

                    linecount += 1
                    totalsize += len(line)
                    if (linecount % 1000) == 0:
                        sysdb.commit()
                        if len(stntoinsert) != 0:
                            sysdb.addfilelinestations(stntoinsert)
                            timer.time('stninsert', len(stntoinsert))
                            stntoinsert = []
                        if len(infotoinsert) != 0:
                            sysdb.addfilelineinfo(infotoinsert)
                            timer.time('infoinsert', len(infotoinsert))
                            infotoinsert = []
                        if len(mktitemtoinsert) != 0:
                            sysdb.addfilelinemarketitems(mktitemtoinsert)
                            timer.time('mktiteminsert', len(mktitemtoinsert))
                            mktitemtoinsert = []
                        sysdb.commit()
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(lineno + 1))
                            sys.stderr.flush()

                sysdb.commit()
                if len(stntoinsert) != 0:
                    sysdb.addfilelinestations(stntoinsert)
                    timer.time('stninsert', len(stntoinsert))
                    stntoinsert = []
                if len(infotoinsert) != 0:
                    sysdb.addfilelineinfo(infotoinsert)
                    timer.time('infoinsert', len(infotoinsert))
                    infotoinsert = []
                if len(mktitemtoinsert) != 0:
                    sysdb.addfilelinemarketitems(mktitemtoinsert)
                    timer.time('mktiteminsert', len(mktitemtoinsert))
                    mktitemtoinsert = []
                sysdb.commit()
                sys.stderr.write('\n')
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, 0, linecount, 0, mktitemcount)
        sysdb.commit()
        timer.time('commit')

def processeddnfcmaterials(sysdb, timer, filename, fileinfo, reprocess, rejectout):
    if (fileinfo.linecount is None
        or (reprocess == True
            and (fileinfo.linecount != fileinfo.stnlinecount
                 or fileinfo.linecount != fileinfo.infolinecount
                 or fileinfo.marketitemcount != fileinfo.marketitemlinecount))):
        fn = eddndir + '/' + fileinfo.date.isoformat()[:7] + '/' + filename
        if os.path.exists(fn):
            sys.stderr.write('{0}\n'.format(fn))
            updatetitleprogress('{0}:{1}'.format(fileinfo.date.isoformat()[:10], filename.split('-')[0]))
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                stnlines = sysdb.getstationfilelines(fileinfo.id)
                infolines = sysdb.getinfofilelines(fileinfo.id)
                mktlines = sysdb.getmarketitemfilelines(fileinfo.id)
                linecount = 0
                totalsize = 0
                mktitemcount = 0
                stntoinsert = []
                mktitemtoinsert = []
                infotoinsert = []
                nullitem = EDDNMarketItem(0, None, None)
                timer.time('load')
                for lineno, line in enumerate(f):
                    timer.time('read')
                    msg = None
                    try:
                        msg = json.loads(line)
                        body = msg['message']
                        hdr = msg['header']
                        stationname = body['CarrierID']
                        marketid = body.get('MarketID')
                        timestamp = body.get('timestamp')
                        gwtimestamp = hdr.get('gatewayTimestamp')
                        software = hdr.get('softwareName')
                        fcitems = body.get('Items')
                    except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                        print('Error: {0}'.format(sys.exc_info()[1]))
                        msg = {
                            'rejectReason': 'Invalid',
                            'exception': '{0}'.format(sys.exc_info()[1]),
                            'rawmessage': line.decode('utf-8')
                        }
                        rejectout.write(json.dumps(msg) + '\n')
                        timer.time('error')
                        pass
                    else:
                        if marketid is not None and (marketid <= 0 or marketid > 1 << 32):
                            marketid = None
                        sqltimestamp = timestamptosql(timestamp)
                        sqlgwtimestamp = timestamptosql(gwtimestamp)
                        timer.time('parse')

                        mktitems = [(0, nullitem)]

                        if fcitems is not None:
                            for n, item in fcitems:
                                marketitem = sysdb.getmarketitem(timer, item.get('name'), 'FCMaterial')
                                mktitems += [(n + 1, marketitem)]

                        if sqltimestamp is not None and sqlgwtimestamp is not None and sqltimestamp < sqlgwtimestamp + timedelta(days = 1):
                            if (lineno + 1, 0) not in mktlines:
                                (mktstation, rejectReason, rejectData) = sysdb.getmarketstation(timer, stationname, '', marketid, sqltimestamp)

                                if mktstation is not None:
                                    for n, marketitem in mktitems:
                                        if (lineno + 1, n) not in mktlines:
                                            mktitemtoinsert += [(fileinfo.id, lineno + 1, mktstation, n, marketitem)]

                            if ((lineno + 1) not in stnlines or (lineno + 1) not in infolines):

                                (station, rejectReason, rejectData) = sysdb.getstation(timer, stationname, '', marketid, sqltimestamp, test = fileinfo.test)
                                timer.time('stnquery')

                                if station is not None:
                                    if (lineno + 1) not in stnlines:
                                        stntoinsert += [(fileinfo.id, lineno + 1, station)]

                                    if (lineno + 1) not in infolines:
                                        sysdb.insertsoftware(software)
                                        infotoinsert += [(
                                            fileinfo.id,
                                            lineno + 1,
                                            sqltimestamp,
                                            sqlgwtimestamp,
                                            sysdb.software[software],
                                            station.systemid,
                                            None,
                                            len(line),
                                            None,
                                            0,
                                            0,
                                            1 if 'marketId' in body else 0
                                        )]

                                else:
                                    msg['rejectReason'] = rejectReason
                                    msg['rejectData'] = rejectData
                                    rejectout.write(json.dumps(msg) + '\n')
                                    pass

                        mktitemcount += len(mktitems)

                    linecount += 1
                    totalsize += len(line)
                    if (linecount % 1000) == 0:
                        sysdb.commit()
                        if len(stntoinsert) != 0:
                            sysdb.addfilelinestations(stntoinsert)
                            timer.time('stninsert', len(stntoinsert))
                            stntoinsert = []
                        if len(infotoinsert) != 0:
                            sysdb.addfilelineinfo(infotoinsert)
                            timer.time('infoinsert', len(infotoinsert))
                            infotoinsert = []
                        if len(mktitemtoinsert) != 0:
                            sysdb.addfilelinemarketitems(mktitemtoinsert)
                            timer.time('mktiteminsert', len(mktitemtoinsert))
                            mktitemtoinsert = []
                        sysdb.commit()
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(lineno + 1))
                            sys.stderr.flush()
                
                sysdb.commit()
                if len(stntoinsert) != 0:
                    sysdb.addfilelinestations(stntoinsert)
                    timer.time('stninsert', len(stntoinsert))
                    stntoinsert = []
                if len(infotoinsert) != 0:
                    sysdb.addfilelineinfo(infotoinsert)
                    timer.time('infoinsert', len(infotoinsert))
                    infotoinsert = []
                if len(mktitemtoinsert) != 0:
                    sysdb.addfilelinemarketitems(mktitemtoinsert)
                    timer.time('mktiteminsert', len(mktitemtoinsert))
                    mktitemtoinsert = []
                sysdb.commit()
                sys.stderr.write('\n')
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, 0, linecount, 0, mktitemcount)
        sysdb.commit()
        timer.time('commit')

def unhandledexception(type, value, traceback):
    sys.__excepthook__(type, value, traceback)
    from bdb import BdbQuit
    if type is not KeyboardInterrupt and type is not BdbQuit:
        import pdb; pdb.post_mortem(traceback)

def main():
    args = argparser.parse_args()
    timer = Timer({
        'init',
        'load',
        'read',
        'parse',
        'error',
        'sysquery',
        'sysqueryedts',
        'sysquerypgre',
        'sysquerylookup',
        'sysselectmaddr',
        'sysselectaddr',
        'sysselectname',
        'stnquery',
        'stnselect',
        'stninsert',
        'commit',
        'stats',
        'syslookupbinsearch',
        'bodyquery',
        'bodylookupname',
        'bodyquerypgre',
        'bodylookuppg',
        'bodyselectname',
        'bodyinsertpg',
        'bodyupdateid',
        'bodyqueryname',
        'bodyinsert',
        'edsmupdate',
        'eddbupdate',
        'infoinsert',
        'factionupdate',
        'factioninsert',
        'routesysteminsert',
        'mktiteminsert',
        'edsmhttp'
    })

    sys.excepthook = unhandledexception

    try:
        conn = mysql.createconnection()
        sysdb = EDDNSysDB(conn, args.edsmsys, args.edsmbodies or args.edsmmissingbodies, args.eddbsys)
        timer.time('init')

        if not args.noeddn:
            rf = EDDNRejectData(eddnrejectdir)
            sys.stderr.write('Retrieving EDDN files from DB\n') 
            sys.stderr.flush()
            files = sysdb.geteddnfiles()
            timer.time('init', 0)
            sys.stderr.write('Processing EDDN files\n')
            sys.stderr.flush()
            if not args.nojournal:
                for filename, fileinfo in files.items():
                    if fileinfo.eventtype is not None and fileinfo.eventtype not in ('NavRoute', 'FCMaterials'):
                        processeddnjournalfile(sysdb, timer, filename, fileinfo, args.reprocess, args.reprocessall, rf)
            if args.navroute:
                for filename, fileinfo in files.items():
                    if fileinfo.eventtype is not None and fileinfo.eventtype == 'NavRoute':
                        processeddnjournalroute(sysdb, timer, filename, fileinfo, args.reprocess, rf)
            if args.fcmaterials:
                for filename, fileinfo in files.items():
                    if fileinfo.eventtype is not None and fileinfo.eventtype == "FCMaterials":
                        processeddnfcmaterials(sysdb, timer, filename, fileinfo, args.reprocess, rf)
            if args.market:
                for filename, fileinfo in files.items():
                    if fileinfo.eventtype is None:
                        processeddnmarketfile(sysdb, timer, filename, fileinfo, args.reprocess, rf)

        if args.edsmsys:
            with open(edsmsysrejectfile, 'at') as rf:
                processedsmsystems(sysdb, timer, rf)
                processedsmsystemswithoutcoords(sysdb, timer, rf)
                #processedsmsystemswithoutcoordsprepurge(sysdb, timer, rf)
                processedsmhiddensystems(sysdb, timer, rf)
                processedsmdeletedsystems(sysdb, timer, rf)
        
        if args.edsmbodies:
            with open(edsmbodiesrejectfile, 'at') as rf:
                sys.stderr.write('Retrieving EDSM body files from DB\n')
                sys.stderr.flush()
                files = sysdb.getedsmfiles()
                timer.time('init', 0)
                sys.stderr.write('Processing EDSM bodies files\n')
                sys.stderr.flush()

                for filename, fileinfo in files.items():
                    processedsmbodies(sysdb, filename, fileinfo, args.reprocess, timer, rf)
        
        if args.edsmmissingbodies:
            processedsmmissingbodies(sysdb, timer)

        if args.edsmstations:
            with open(edsmstationsrejectfile, 'at') as rf:
                processedsmstations(sysdb, timer, rf)

        if args.eddbsys:
            with open(eddbsysrejectfile, 'at') as rf:
                processeddbsystems(sysdb, timer, rf)
                            
    finally:
        timer.printstats()

if __name__ == '__main__':
    main()
