#!/usr/bin/python3

import os
import os.path
import sys
import json
import bz2
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
from datetime import datetime
import argparse

eddndir = '/srv/EDDN/data'
edsmdir = '/srv/EDSM/dumps'
edsmsysfile = edsmdir + '/systemsWithCoordinates.jsonl.bz2'
edsmbodiesfile = edsmdir + '/bodies.jsonl.bz2'

#conntype = 'mysql.connector'
conntype = 'mysqlclient'
#conntype = 'pymysql'
sqluser = 'eddata'
sqlhost = 'localhost'
sqlpass = 'P@ssw0rd1234'
sqldb = 'eddata_eddn'

uselookup = False
allow303bodies = True

revpgsysre = re.compile('^([0-9]+)(|-[0-9]+)([a-h]) ([A-Z])-([A-Z])([A-Z]) ((rotceS|noigeR) [A-Za-z0-9.\' -]+|[1-6][a-z]+[A-Z]|ZCI|[a-z]+[A-Z](| [a-z]+[A-Z]))$')
pgsysre = re.compile('^([A-Za-z0-9.()\' -]+?) ([A-Z][A-Z]-[A-Z]) ([a-h])(?:([0-9]+)-|)([0-9]+)$')
pgbodyre = re.compile('^(?:|(?:| (A?B?C?D?E?F?G?H?))(?:| ([A-Z]) Belt(?:| Cluster ([1-9][0-9]?))| ([1-9][0-9]?(?:[+][1-9][0-9]?)*)(?:| ([A-Z]) Ring| ([a-z](?:[+][a-z])*)(?:| ([A-Z]) Ring| ([a-z](?:[+][a-z])*)(?:| ([A-Z]) Ring| ([a-z]))))))$')
timestampre = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-5][0-9]:[0-5][0-9])')

tsbasedate = datetime.strptime('2014-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
ed303date = datetime.strptime('2018-03-19 10:00:00', '%Y-%m-%d %H:%M:%S')
ed304date = datetime.strptime('2018-03-27 16:00:00', '%Y-%m-%d %H:%M:%S')
ed330date = datetime.strptime('2018-12-11 16:00:00', '%Y-%m-%d %H:%M:%S')

EDDNSystem = namedtuple('EDDNSystem', ['id', 'id64', 'name', 'x', 'y', 'z'])
EDDNStation = namedtuple('EDDNStation', ['id', 'marketid', 'name', 'systemname', 'systemid', 'type', 'body', 'bodyid', 'isrejected', 'validfrom', 'validuntil'])
EDDNFile = namedtuple('EDDNFile', ['id', 'name', 'date', 'eventtype', 'linecount', 'syslinecount', 'stnlinecount', 'bodylinecount'])
EDDNRegion = namedtuple('EDDNRegion', ['id', 'name', 'x0', 'y0', 'z0', 'sizex', 'sizey', 'sizez', 'regionaddr', 'isharegion'])
EDDNBody = namedtuple('EDDNBody', ['id', 'name', 'systemname', 'systemid', 'bodyid', 'planet', 'argofperiapsis', 'validfrom', 'validuntil', 'isrejected'])

argparser = argparse.ArgumentParser(description='Index EDDN data into database')
argparser.add_argument('--reprocess', dest='reprocess', action='store_const', const=True, default=False, help='Reprocess files with unprocessed entries')
argparser.add_argument('--market', dest='market', action='store_const', const=True, default=False, help='Process market/shipyard/outfitting messages')
argparser.add_argument('--edsmsys', dest='edsmsys', action='store_const', const=True, default=False, help='Process EDSM systems dump')
argparser.add_argument('--edsmbodies', dest='edsmbodies', action='store_const', const=True, default=False, help='Process EDSM bodies dump')
argparser.add_argument('--edsmstations', dest='edsmstations', action='store_const', const=True, default=False, help='Process EDSM stations dump')
argparser.add_argument('--eddbsys', dest='eddbsys', action='store_const', const=True, default=False, help='Process EDDB systems dump')
argparser.add_argument('--eddbstations', dest='eddbstations', action='store_const', const=True, default=False, help='Process EDDB stations dump')
argparser.add_argument('--noeddn', dest='noeddn', action='store_const', const=True, default=False, help='Skip EDDN processing')

def createconnection():
    if conntype == 'mysql.connector':
        import mysql.connector
        conn = mysql.connector.connect(user=sqluser, host=sqlhost, password=sqlpass, database=sqldb)
        conn.set_charset_collation('utf8')
        return conn
    elif conntype == 'mysqlclient':
        import MySQLdb
        return MySQLdb.connect(user=sqluser, host=sqlhost, password=sqlpass, database=sqldb, charset='utf8')
    elif conntype == 'pymysql':
        import pymysql
        return pymysql.connect(user=sqluser, host=sqlhost, password=sqlpass, database=sqldb)
    else:
        raise ValueError('Invalid connection type {0}'.format(conntype))

def makepreparedcursor(conn):
    if conntype == 'mysql.connector':
        return conn.cursor(prepared=True)
    elif conntype == 'mysqlclient':
        import MySQLdb.cursors
        return conn.cursor(MySQLdb.cursors.SSCursor)
    elif conntype == 'pymysql':
        import pymysql.cursors
        return conn.cursor(pymysql.cursors.SSCursor)
    else:
        raise ValueError('Invalid connection type {0}'.format(conntype))

def makestreamingcursor(conn):
    if conntype == 'mysql.connector':
        return conn.cursor()
    elif conntype == 'mysqlclient':
        import MySQLdb.cursors
        return conn.cursor(MySQLdb.cursors.SSCursor)
    elif conntype == 'pymysql':
        import pymysql.cursors
        return conn.cursor(pymysql.cursors.SSCursor)
    else:
        raise ValueError('Invalid connection type {0}'.format(conntype))

class EDDNSysDB(object):
    def __init__(self, conn, loadedsmsys, loadedsmbodies):
        self.conn = conn
        self.regions = {}
        self.regionaddrs = {}
        self.namedsystems = {}
        self.namedbodies = {}
        self.edsmsysids = None
        self.edsmbodyids = None

        try:
            timer = Timer(['sql', 'sqlpg', 'sqlha', 'sqlname', 'sqlregion', 'sqlpgbody', 'sqlbodyname', 'sqledsmsys', 'sqledsmbody', 'load', 'loadname', 'loadha', 'loadpg', 'loadregion', 'loadpgbody', 'loadbodyname', 'loadedsmsys', 'loadedsmbody'])
            sys.stderr.write('Loading Regions\n')
            c = makestreamingcursor(conn)
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

            c = makestreamingcursor(conn)
            c.execute('SELECT COUNT(*) FROM Systems')
            row = c.fetchone()
            pgsyscount = row[0]

            c = makestreamingcursor(conn)
            c.execute('SELECT MAX(Id) FROM Systems')
            row = c.fetchone()
            maxsysid = row[0]

            c = makestreamingcursor(conn)
            c.execute('SELECT COUNT(*) FROM Systems_HASector')
            row = c.fetchone()
            hasyscount = row[0]
            pgsyscount -= hasyscount

            c = makestreamingcursor(conn)
            c.execute('SELECT MAX(EdsmId) FROM Systems_EDSM')
            row = c.fetchone()
            maxedsmsysid = row[0]

            c = makestreamingcursor(conn)
            c.execute('SELECT MAX(EdsmId) FROM SystemBodies_EDSM')
            row = c.fetchone()
            maxedsmbodyid = row[0]

            c = makestreamingcursor(conn)
            c.execute('SELECT COUNT(*) FROM SystemBodies')
            row = c.fetchone()
            pgbodycount = row[0]

            timer.time('sql')

            timer.time('load')
            sys.stderr.write('Loading Named Systems\n')
            c = makestreamingcursor(conn)
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns JOIN Systems_Named sn ON sn.Id = ns.Id')
            timer.time('sql')
            rows = c.fetchall()
            timer.time('sqlname', len(rows))
            for row in rows:
                si = EDDNSystem(row[0], row[1], row[2], row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105)
                if si.name not in self.namedsystems:
                    self.namedsystems[si.name] = si
                elif type(self.namedsystems[si.name]) is not list:
                    self.namedsystems[si.name] = [self.namedsystems[si.name]]
                    self.namedsystems[si.name] += [si]
                else:
                    self.namedsystems[si.name] += [si]

            pgsyscount -= len(rows)
            timer.time('loadname', len(rows))

            sys.stderr.write('Loading Named Bodies\n')
            c = makestreamingcursor(conn)
            c.execute('SELECT nb.Id, nb.BodyName, nb.SystemName, nb.SystemId, nb.BodyID, nb.Planet, nb.ArgOfPeriapsis, nb.ValidFrom, nb.ValidUntil, nb.IsRejected FROM SystemBodyNames nb JOIN SystemBodies_Named sbn ON sbn.Id = nb.Id')
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

            pgbodycount -= len(rows)
            timer.time('loadbodyname')

            if uselookup:
                self.regionsystemptrs = numpy.recarray([max([r.id for n, r in self.regions.items()]) + 1], dtype=[('first', '<i4'), ('last', '<i4'), ('isharegion', '|b1')])
                self.systembodyptrs = numpy.recarray(maxsysid + 1, dtype=[('first', '<i4'), ('last', '<i4')])

                for n, r in self.regions.items():
                    self.regionsystemptrs[r.id].isharegion = r.isharegion

                sys.stderr.write('Loading Sector Systems\n')
                c = makestreamingcursor(conn)
                c.execute('SELECT s.ModSystemAddress, s.Id, s.X, s.Y, s.Z, h.RegionId, h.PGSuffix FROM Systems s JOIN Systems_HASector h ON h.Id = s.Id ORDER BY h.RegionId, h.ModSystemAddress')
                
                hasysarray = numpy.zeros(hasyscount, dtype=[('modsysaddr', '<i8'), ('id', '<i4'), ('x', '<i4'), ('y', '<i4'), ('z', '<i4'), ('regionid', '<i4'), ('suffix', '|S16')])
                self.hasystems = hasysarray.view(numpy.core.records.recarray)
                timer.time('sql')
                
                i = 0
                lastregionid = -1
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqlha', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        rec = hasysarray[i]
                        rec[0] = row[0]
                        rec[1] = row[1]
                        rec[2] = row[2]
                        rec[3] = row[3]
                        rec[4] = row[4]
                        regionid = rec[5] = row[5]
                        rec[6] = row[6]
                        if regionid != lastregionid:
                            self.regionsystemptrs[lastregionid].last = i
                            lastregionid = regionid
                            self.regionsystemptrs[regionid].first = i
                        i += 1
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1}\n'.format(i, hasyscount))
                    sys.stderr.flush()
                    timer.time('loadha', len(rows))
                self.regionsystemptrs[lastregionid].last = pgsyscount
                sys.stderr.write('  {0} / {1}\n'.format(i, hasyscount))

                sys.stderr.write('Loading Procgen Systems\n')
                c = makestreamingcursor(conn)
                c.execute('SELECT ModSystemAddress, Id, X, Y, Z FROM Systems s WHERE IsHASystem = 0 AND IsNamedSystem = 0 ORDER BY ModSystemAddress')

                pgsysarray = numpy.zeros(pgsyscount, dtype=[('modsysaddr', '<i8'), ('id', '<i4'), ('x', '<i4'), ('y', '<i4'), ('z', '<i4')])
                self.pgsystems = pgsysarray.view(numpy.core.records.recarray)
                timer.time('sql')

                i = 0
                lastregionaddr = -1
                regionid = -1
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqlpg', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        rec = pgsysarray[i]
                        rec[0] = row[0]
                        rec[1] = row[1]
                        rec[2] = row[2]
                        rec[3] = row[3]
                        rec[4] = row[4]
                        regionaddr = row[0] >> 40
                        if regionaddr != lastregionaddr:
                            lastregionaddr = regionaddr
                            self.regionsystemptrs[regionid].last = i
                            regionid = self.regionaddrs[regionaddr].id
                            self.regionsystemptrs[regionid].first = i
                        i += 1
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1}\n'.format(i, pgsyscount))
                    sys.stderr.flush()
                    timer.time('loadpg', len(rows))
                self.regionsystemptrs[regionid].last = pgsyscount
                sys.stderr.write('  {0} / {1}\n'.format(i, pgsyscount))

                sys.stderr.write('Loading Procgen Bodies\n')
                c = makestreamingcursor(conn)
                c.execute('''
                    SELECT 
                        Id, 
                        SystemId, 
                        BodyId | (HasBodyId << 15) | (Moon3 << 16) | (Moon3IsRing << 23) | (Moon2 << 24) | (Moon2IsRing << 31) | (Moon1 << 32) | (Moon1IsRing << 39) | (Planet << 40) | (IsBelt << 47) | (Stars << 48) AS Packed 
                    FROM SystemBodies 
                    WHERE IsNamedBody = 0 
                    GROUP BY SystemId, Stars, IsBelt, Planet, Moon1IsRing, Moon1, Moon2IsRing, Moon2, Moon3IsRing, Moon3 
                    HAVING COUNT(*) = 1 
                    ORDER BY SystemId, Stars, IsBelt, Planet, Moon1IsRing, Moon1, Moon2IsRing, Moon2, Moon3IsRing, Moon3
                ''')

                pgbodyarray = numpy.zeros(pgbodycount, dtype=[('bodyident', '<i8'), ('sysid', '<i4'), ('id', '<i4')])
                self.pgbodies = pgbodyarray.view(numpy.core.records.recarray)
                timer.time('sql')

                i = 0
                lastsystemid = 0
                bodyptrarray = self.systembodyptrs.view(numpy.ndarray)
                while True:
                    rows = c.fetchmany(10000)
                    timer.time('sqlpgbody', len(rows))
                    if len(rows) == 0:
                        break
                    for row in rows:
                        rec = pgbodyarray[i]
                        rec[0] = row[2]
                        systemid = rec[1] = row[1]
                        rec[2] = row[0]
                        if systemid != lastsystemid:
                            bodyptrarray[lastsystemid][1] = i
                            lastsystemid = systemid
                            bodyptrarray[systemid][0] = i
                        i += 1
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1}\n'.format(i, pgbodycount))
                    sys.stderr.flush()
                    timer.time('loadpgbody', len(rows))
                bodyptrarray[lastsystemid][1] = i
                sys.stderr.write('  {0} / {1}\n'.format(i, pgbodycount))

            if (loadedsmsys or loadedsmbodies) and maxedsmsysid:
                sys.stderr.write('Loading EDSM System IDs\n')
                c = makestreamingcursor(conn)
                c.execute('SELECT Id, EdsmId, TimestampSeconds FROM Systems_EDSM')

                edsmsysarray = numpy.zeros(maxedsmsysid + 1048576, dtype=[('sysid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4')])
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
                        i += 1
                        if edsmid > maxedsmid:
                            maxedsmid = edsmid
                    sys.stderr.write('.')
                    if (i % 640000) == 0:
                        sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxedsmsysid, edsmid))
                    sys.stderr.flush()
                    timer.time('loadedsmsys', len(rows))
                sys.stderr.write('  {0} / {1}\n'.format(i, maxedsmsysid))

            if loadedsmbodies and maxedsmbodyid:
                sys.stderr.write('Loading EDSM System IDs\n')
                c = makestreamingcursor(conn)
                c.execute('SELECT Id, EdsmId, TimestampSeconds FROM SystemBodies_EDSM')

                edsmbodyarray = numpy.zeros(maxedsmbodyid + 1048576, dtype=[('sysid', '<i4'), ('edsmid', '<i4'), ('timestampseconds', '<i4')])
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
                        sys.stderr.write('  {0} / {1} ({2})\n'.format(i, maxedsmbodyid, edsmid))
                    sys.stderr.flush()
                    timer.time('loadedsmbody', len(rows))
                sys.stderr.write('  {0} / {1}\n'.format(i, maxedsmbodyid))


        finally:
            timer.printstats()

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

    def lookupsystem(self, region, sysname, starpos, modsysaddr, syslist, timer):
        starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
        sysaddr = self.modsysaddrtosysaddr(modsysaddr)
        sz = sysaddr & 7
        ptr = self.regionsystemptrs[region.id]
        systems = []

        if ptr.isharegion:
            timer.time('sysquerylookup', 0)
            part = self.findmodsysaddr(self.hasystems, modsysaddr, sysname, starpos, ptr.first, ptr.last, (modsysaddr, 0, 0, 0, 0, 0, ''))
            timer.time('syslookupbinsearch')
            if part is not None:
                arr = part.view(numpy.ndarray)
                for rec in arr:
                    if rec[0] != modsysaddr:
                        break
                    x = rec[2] / 32.0 - 49985
                    y = rec[3] / 32.0 - 40985
                    z = rec[4] / 32.0 - 24105
                    suffix = rec[6].decode('utf-8')
                    name = region.name + suffix
                    systems += [ EDDNSystem(rec[1], sysaddr, name, x, y, z) ]
        else:
            timer.time('sysquerylookup', 0)
            part = self.findmodsysaddr(self.pgsystems, modsysaddr, sysname, starpos, ptr.first, ptr.last, (modsysaddr, 0, 0, 0, 0))
            timer.time('syslookupbinsearch')
            if part is not None:
                arr = part.view(numpy.ndarray)
                for rec in arr:
                    if rec[0] != modsysaddr:
                        break
                    x = rec[2] / 32.0 - 49985
                    y = rec[3] / 32.0 - 40985
                    z = rec[4] / 32.0 - 24105
                    mid = (modsysaddr >> 16) & 0x1FFFFF
                    mid1a = chr((mid % 26) + 65)
                    mid1b = chr(((mid // 26) % 26) + 65)
                    mid2 = chr(((mid // (26 * 26)) % 26) + 65)
                    mid3 = str(mid // (26 * 26 * 26))
                    if mid3 == '0':
                        mid3 = ''
                    else:
                        mid3 += '-'
                    sc = chr(sz + 97)
                    seq = str(modsysaddr & 0xFFFF)
                    name = region.name + ' ' + mid1a + mid1b + '-' + mid2 + ' ' + sc + mid3 + seq
                    systems += [ EDDNSystem(rec[1], sysaddr, name, x, y, z) ]

        for system in systems:
            if system.name == sysname and system.x == starpos[0] and system.y == starpos[1] and system.z == starpos[2]:
                return system
        
        syslist += systems
        return None

    def _namestr(self, name):
        if type(name) is str:
            return name
        else:
            return name.decode('utf-8')

    def _findsystem(self, cursor, sysname, starpos, sysaddr, syslist):
        systems = [ EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105) for row in cursor ]
        for system in systems:
            if system.name.lower() == sysname.lower() and system.x == starpos[0] and system.y == starpos[1] and system.z == starpos[2] and (sysaddr is None or sysaddr == system.id64):
                return system
        syslist += systems
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

    @lru_cache(maxsize=262144)
    def getsystem(self, timer, sysname, x, y, z, sysaddr):
        starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in (x, y, z) ]

        systems = []

        if sysname in self.namedsystems:
            systems = self.namedsystems[sysname]
            if type(systems) is not list:
                systems = [systems]
            for system in systems:
                if system.name == sysname and system.x == starpos[0] and system.y == starpos[1] and system.z == starpos[2] and (sysaddr is None or sysaddr == system.id64):
                    return system

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
                    if uselookup:
                        system = self.lookupsystem(ri, sysname, starpos, modsysaddr, systems, timer)
                        timer.time('sysquerylookup')
                        if system is not None:
                            return system

                    c = self.conn.cursor()
                    c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
                    system = self._findsystem(c, sysname, starpos, sysaddr, systems)
                    timer.time('sysselectmaddr')

                    if system is not None:
                        return system
                else:
                    sys.stderr.write('Unable to resolve system address for system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, starpos[0], starpos[1], starpos[2]))
                    sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
                    raise ValueError('Unable to resolve system address')
            else:
                sys.stderr.write('Region {5} not found for system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, starpos[0], starpos[1], starpos[2], regionname))
                sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
                raise ValueError('Region not found')
        
        if sysaddr is not None:
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            c = self.conn.cursor()
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
            system = self._findsystem(c, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                return system

        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns JOIN Systems_Named sn ON sn.Id = ns.Id WHERE sn.Name = %s', (sysname,))

        system = self._findsystem(c, sysname, starpos, sysaddr, systems)
        timer.time('sysselectname')

        if system is not None:
            return system

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
                return system

        elif sysaddr is not None and (edtsid64 is None or edtsid64 == sysaddr):
            timer.time('sysquery', 0)
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            c = self.conn.cursor()
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
            system = self._findsystem(c, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                return system

        timer.time('sysquery', 0)
        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns JOIN Systems_Named sn ON sn.Id = ns.Id WHERE sn.Name = %s', (sysname,))

        system = self._findsystem(c, sysname, starpos, None, systems)
        timer.time('sysselectname')

        if system is not None:
            return system

        if ri is not None and modsysaddr is not None:
            vx = int((starpos[0] + 49985) * 32)
            vy = int((starpos[1] + 40985) * 32)
            vz = int((starpos[2] + 24105) * 32)
            raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
            if raddr == modsysaddr >> 40:
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
                return EDDNSystem(sysid, self.modsysaddrtosysaddr(modsysaddr), sysname, starpos[0], starpos[1], starpos[2])
        elif sysaddr is not None:
            modsysaddr = self.sysaddrtomodsysaddr(sysaddr)
            vx = int((starpos[0] + 49985) * 32)
            vy = int((starpos[1] + 40985) * 32)
            vz = int((starpos[2] + 24105) * 32)
            raddr = ((vz // 40960) << 13) | ((vy // 40960) << 7) | (vx // 40960)
            if raddr == modsysaddr >> 40:
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
                return EDDNSystem(sysid, sysaddr, sysname, starpos[0], starpos[1], starpos[2])

        #sys.stderr.write('Cannot find system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, starpos[0], starpos[1], starpos[2]))
        #sys.stderr.writelines(['{0}\n'.format(s) for s in systems])
        #raise ValueError('Unable to find system')
        return None

    def getstation(self, timer, name, sysname, marketid, timestamp, system = None, stationtype = None, bodyname = None, bodyid = None, bodytype = None):
        sysid = system.id if system is not None else None

        if name is None or name == '':
            return None

        if sysname is None or sysname == '':
            return None

        if timestamp is None:
            return None

        if stationtype is not None and stationtype == '':
            stationtype = None
        
        if bodyname is not None and bodyname == '':
            bodyname = None

        if bodytype is not None and bodytype == '':
            bodytype = None

        if marketid is not None and marketid == 0:
            marketid = None

        c = self.conn.cursor()
        c.execute('SELECT Id, MarketId, StationName, SystemName, SystemId, StationType, Body, BodyID, IsRejected, ValidFrom, ValidUntil FROM Stations WHERE SystemName = %s AND StationName = %s ORDER BY ValidUntil - ValidFrom', (sysname, name))
        stations = [ EDDNStation(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8] == b'\x01', row[9], row[10]) for row in c ]

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

        if len(candidates) > 1:
            candidates = [ c for c in candidates if not c[0].isrejected and c[0].validfrom <= timestamp and c[0].validuntil >= timestamp ]

        if len(candidates) == 2 and candidates[0][0].validfrom > candidates[1][0].validfrom and candidates[0][0].validuntil < candidates[1][0].validuntil:
            candidates = [ candidates[0] ]

        if len(candidates) == 1:
            station, replace = candidates[0]

            if len(replace) != 0:
                station = self.updatestation(station, **replace)

            return station
        elif len(candidates) > 1:
            #import pdb; pdb.set_trace()
            return None
        
        if bodyname is not None and not ((bodytype is None and bodyname != name) or bodytype == 'Planet'):
            bodyname = None

        validfrom = '2014-01-01 00:00:00'
        validuntil = '9999-12-31 00:00:00'

        if stationtype is not None:
            if stationtype == 'SurfaceStation':
                validuntil = ed330date
            elif stationtype in ['CraterPort', 'CraterOutpost']:
                validfrom = ed330date

        c = self.conn.cursor()
        c.execute(
            'INSERT INTO Stations ' +
            '(MarketId, StationName, SystemName, SystemId, StationType, Body,     BodyID, ValidFrom, ValidUntil) VALUES ' +
            '(%s,       %s,          %s,         %s,       %s,          %s,       %s,     %s,        %s)', 
             (marketid, name,        sysname,    sysid,    stationtype, bodyname, bodyid, validfrom, validuntil))
        return EDDNStation(c.lastrowid, marketid, name, sysname, sysid, stationtype, bodyname, bodyid, False, '2014-01-01', '9999-12-31')

    def insertbodyparents(self, timer, scanbodyid, system, bodyid, parents):
        pass

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
                    rows = [row for row in rows if row.planet != 0]
                elif 'StarType' in body:
                    rows = [row for row in rows if row.planet == 0]

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
                return rows[0]

        ispgname = name.startswith(sysname)
        if name == sysname and 'SemiMajorAxis' in body and body['SemiMajorAxis'] is not None:
            ispgname = False

        if ispgname:
            timer.time('bodyquery', 0)
            desig = name[len(sysname):]
            match = pgbodyre.match(desig)
            if match:
                stars = match[1]
                belt = match[2]
                cluster = match[3]
                planetstr = match[4]
                ring1 = match[5]
                moon1str = match[6]
                ring2 = match[7]
                moon2str = match[8]
                ring3 = match[9]
                moon3str = match[10]
                isbelt = 1 if belt is not None else 0
                moon1isring = 1 if ring1 is not None else 0
                moon2isring = 1 if ring2 is not None else 0
                moon3isring = 1 if ring3 is not None else 0
                hasbodyid = 1 if bodyid is not None else 0

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

                if bodycategory == 12:
                    if moon3str is not None:
                        moon3 = ord(moon3str) - 96
                        bodycategory = 14
                    elif ring3 is not None:
                        moon3 = ord(ring3) - 64
                        bodycategory = 13

                star = 0
                if stars is not None:
                    for i in range(ord(stars[0]) - 65, ord(stars[-1]) - 64):
                        star |= 1 << i

            timer.time('bodyquerypgre')

            if match and uselookup:
                retbody = None
                part = []
                if system.id < len(self.systembodyptrs):
                    packed = moon3 | (moon3isring << 7) | (moon2 << 8) | (moon2isring << 15) | (moon1 << 16) | (moon1isring << 23) | (planet << 24) | (isbelt << 31) | (star << 32)
                    ptr = self.systembodyptrs[system.id]
                    part = self.pgbodies[ptr.first:ptr.last].view(numpy.ndarray)
                    if len(part) >= 1:
                        start = part.searchsorted(numpy.array((packed << 16, 0, 0), part.dtype))
                        if start < len(part) and (part[start][0] >> 16) == packed and part[start][1] == system.id:
                            rec = part[start]
                            xhasbodyid = int((rec[0] & 0x8000) >> 15)
                            xbodyid = int(rec[0] & 0x7FFF) if xhasbodyid != 0 else None
                            sysbodyid = int(rec[2])
                            if sysbodyid != 0 and xbodyid is None or bodyid is None or bodyid == xbodyid:
                                if xbodyid is None and bodyid is not None:
                                    part[start][0] |= bodyid | 0x8000
                                    cursor = self.conn.cursor()
                                    cursor.execute('UPDATE SystemBodies SET HasBodyId = 1, BodyID = %s WHERE Id = %s', (bodyid, sysbodyid))
                                retbody = EDDNBody(sysbodyid, name, sysname, system.id, xbodyid or bodyid, planet, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 0)
                            else:
                                #import pdb; pdb.set_trace()
                                rec[1] = 0
                                rec[2] = 0
                timer.time('bodylookuppg')
                if retbody is not None:
                    return retbody

        #if ispgname and match and uselookup and len(part) >= 1:
        #    import pdb; pdb.set_trace()

        timer.time('bodyquery', 0)
        cursor = self.conn.cursor()
        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, Planet, ArgOfPeriapsis FROM SystemBodyNames sn WHERE SystemId = %s AND BodyName = %s AND IsNamedBody = 1', (system.id, name))
        rows = cursor.fetchall()
        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, Planet, ArgOfPeriapsis FROM SystemBodyNames sn WHERE SystemId = %s AND BodyName = %s AND IsNamedBody = 0', (system.id, name))
        rows += cursor.fetchall()
        timer.time('bodyselectname')
        ufrows = rows

        multimatch = len(rows) > 1

        if bodyid is not None:
            rows = [row for row in rows if row[4] == bodyid or row[4] is None]

        if len(rows) > 1 and name == sysname:
            if 'PlanetClass' in body:
                rows = [row for row in rows if row[5] != 0]
            elif 'StarType' in body:
                rows = [row for row in rows if row[5] == 0]

        if multimatch and 'Periapsis' in body:
            aop = body['Periapsis']
            if len(rows) == 1 and rows[0][6] is None:
                pass
            elif len(rows) > 1:
                rows = [row for row in rows if row[6] is None or ((aop + 725 - row[6]) % 360) < 10]

        timer.time('bodyqueryname')
        if len(rows) == 1:
            row = rows[0]
            if row[4] is None and bodyid is not None:
                cursor = self.conn.cursor()
                cursor.execute('UPDATE SystemBodies SET HasBodyId = 1, BodyID = %s WHERE Id = %s', (bodyid, row[0]))
                timer.time('bodyupdateid')
            return EDDNBody(row[0], name, sysname, system.id, row[4] or bodyid, None, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 0)
        elif len(rows) > 1:
            return None
            raise ValueError('Multiple matches')
        else:
            cursor = self.conn.cursor()
            cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, Planet, ArgOfPeriapsis FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 1', (system.id, ))
            allrows = cursor.fetchall()
            cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, Planet, ArgOfPeriapsis FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 0', (system.id, ))
            allrows += cursor.fetchall()
            frows = [ r for r in allrows if r[1].lower() == name.lower() ]

            if len(frows) > 0:
                if sysname in self.namedsystems:
                    systems = self.namedsystems[sysname]
                    if type(systems) is not list:
                        systems = [systems]
                    for xsystem in systems:
                        cursor = self.conn.cursor()
                        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, Planet, ArgOfPeriapsis FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 1', (xsystem.id, ))
                        allrows += cursor.fetchall()
                        cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, Planet, ArgOfPeriapsis FROM SystemBodyNames sn WHERE SystemId = %s AND IsNamedBody = 0', (xsystem.id, ))
                        allrows += cursor.fetchall()
                frows = [ r for r in allrows if r[1].lower() == name.lower() ]
                        
                import pdb; pdb.set_trace()

            if ispgname and match:
                cursor = self.conn.cursor()
                cursor.execute(
                    'INSERT INTO SystemBodies ' +
                    '(SystemId,  HasBodyId, BodyId,      BodyCategory, Stars, IsBelt, Planet, Moon1IsRing, Moon1, Moon2IsRing, Moon2, Moon3IsRing, Moon3, IsNamedBody) VALUES ' +
                    '(%s,        %s,        %s,          %s,           %s,    %s,     %s,     %s,          %s,    %s,          %s,    %s,          %s,    0)', 
                     (system.id, hasbodyid, bodyid or 0, bodycategory, star,  isbelt, planet, moon1isring, moon1, moon2isring, moon2, moon3isring, moon3)
                )
                timer.time('bodyinsertpg')
                return EDDNBody(cursor.lastrowid, name, sysname, system.id, bodyid, None, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 0)
                
            return None
            import pdb; pdb.set_trace()
                
            cursor = self.conn.cursor()
            cursor.execute(
                'INSERT INTO SystemBodies ' +
                '(SystemId,  HasBodyId, BodyId,      BodyCategory, Stars, IsBelt, Planet, Moon1IsRing, Moon1, Moon2IsRing, Moon2, Moon3IsRing, Moon3, IsNamedBody) VALUES '
                '(%s,        %s,        %s,          0,            0,     0,      0,      0,           0,     0,           0,     0,           0,     1)',
                 (system.id, 1 if bodyid is not None else 0, bodyid or 0)
            )
            rowid = cursor.lastrowid
            if rowid is None:
                import pdb; pdb.set_trace()
                
            cursor.execute(
                'INSERT INTO SystemBodies_Named ' +
                '(Id,    Name) VALUES ' +
                '(%s,    %s)', 
                 (rowid, name)
            )
            cursor.execute(
                'INSERT INTO SystemBodies_Validity ' +
                '(Id,    IsRejected) VALUES ' +
                '(%s,    1)',
                 (rowid, )
            )
            return EDDNBody(rowid, name, sysname, system.id, bodyid, None, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 1)

    def updatestation(self, station, **kwargs):
        station = station._replace(**kwargs)

        c = self.conn.cursor()
        c.execute('UPDATE Stations SET MarketId = %s, SystemId = %s, StationType = %s, Body = %s, BodyID = %s WHERE Id = %s', (station.marketid, station.systemid, station.type, station.body, station.bodyid, station.id))

        return station

    def findedsmsysid(self, edsmid):
        if self.edsmsysids is not None and len(self.edsmsysids) > edsmid:
            row = self.edsmsysids[edsmid]

            if row[0] != 0:
                return (row[0], row[2])

        c = self.conn.cursor()
        c.execute('SELECT Id, TimestampSeconds FROM Systems_EDSM WHERE EdsmId = %s', (edsmid,))
        row = c.fetchone()

        if row:
            return (row[0], row[1])
        else:
            return (None, None)

    def getsystembyid(self, sysid):
        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE Id = %s', (sysid,))
        row = c.fetchone()

        if row:
            return EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105)
        else:
            return None

    def updateedsmsysid(self, edsmid, sysid, ts):
        ts = int((ts - tsbasedate).total_seconds())
        c = self.conn.cursor()
        c.execute('INSERT INTO Systems_EDSM SET EdsmId = %s, Id = %s, TimestampSeconds = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, TimestampSeconds = %s',
                  (edsmid, sysid, ts, sysid, ts))
    
    def updateedsmbodyid(self, edsmid, bodyid, ts):
        ts = int((ts - tsbasedate).total_seconds())
        c = self.conn.cursor()
        c.execute('INSERT INTO SystemBodies_EDSM SET EdsmId = %s, Id = %s, TimestampSeconds = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, TimestampSeconds = %s',
                  (edsmid, sysid, ts, bodyid, ts))
    
    def addfilelinesystems(self, linelist):
        values = [(fileid, lineno, system.id) for fileid, lineno, system in linelist]
        self.conn.cursor().executemany('INSERT INTO FileLineSystems (FileId, LineNo, SystemId) VALUES (%s, %s, %s)', values)

    def addfilelinestations(self, linelist):
        values = [(fileid, lineno, station.id) for fileid, lineno, station in linelist]
        self.conn.cursor().executemany('INSERT INTO FileLineStations (FileId, LineNo, StationId) VALUES (%s, %s, %s)', values)

    def addfilelinebodies(self, linelist):
        values = [(fileid, lineno, body.id) for fileid, lineno, body in linelist]
        self.conn.cursor().executemany('INSERT INTO FileLineBodies (FileId, LineNo, BodyId) VALUES (%s, %s, %s)', values)

    def addfilelinetimestamps(self, linelist):
        self.conn.cursor().executemany('INSERT INTO FileLineTimestamps (FileId, LineNo, TimestampSeconds, GatewayTimestampTicks) VALUES (%s, %s, %s, %s)', linelist)

    def addfilelinesystemunresolved(self, fileid, lineno, line, sysname, sysaddr, x, y, z, timestamp):
        self.conn.cursor().execute(
            'INSERT IGNORE INTO FileLineSystems_Unresolved ' +
            '(FileId, LineNo, JsonText, SystemName, SystemAddress, X, Y, Z, Timestamp) VALUES ' +
            '(%s,     %s,     %s,       %s,         %s,           %s,%s,%s, %s)', 
             (fileid, lineno, line,     sysname,    sysaddr,       x, y, z, timestamp)
        )

    def addfilelinestationunresolved(self, fileid, lineno, line, sysname, sysid, name, marketid, timestamp):
        self.conn.cursor().execute(
            'INSERT IGNORE INTO FileLineStations_Unresolved ' +
            '(FileId, LineNo, JsonText, SystemName, SystemId, StationName, MarketId, Timestamp) VALUES ' +
            '(%s,     %s,     %s,       %s,         %s,       %s,          %s,       %s)',
             (fileid, lineno, line,     sysname,    sysid,    name,        marketid, timestamp)
        )

    def addfilelinebodyunresolved(self, fileid, lineno, line, sysname, sysid, bodyname, bodyid, timestamp):
        self.conn.cursor().execute(
            'INSERT IGNORE INTO FileLineBodies_Unresolved ' +
            '(FileId, LineNo, JsonText, SystemName, SystemId, BodyName, BodyID, Timestamp) VALUES ' +
            '(%s,     %s,     %s,       %s,         %s,       %s,       %s,     %s)', 
             (fileid, lineno, line,     sysname,    sysid,    bodyname, bodyid, timestamp)
        )

    def getsystemfilelines(self, fileid):
        cursor = makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, SystemId FROM FileLineSystems WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor }

    def getstationfilelines(self, fileid):
        cursor = makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, StationId FROM FileLineStations WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor }

    def getbodyfilelines(self, fileid):
        cursor = makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, BodyId FROM FileLineBodies WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor }
        
    def gettimestampfilelines(self, fileid):
        cursor = makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, TimestampSeconds FROM FileLineTimestamps WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor }

    def getfiles(self):
        cursor = makestreamingcursor(self.conn)
        cursor.execute('''
            SELECT 
                Id, 
                FileName, 
                Date, 
                EventType, 
                LineCount, 
                (SELECT COUNT(*) FROM FileLineSystems WHERE FileId = f.Id) AS SystemLineCount, 
                (SELECT COUNT(*) FROM FileLineStations WHERE FileId = f.Id) AS StationLineCount, 
                (SELECT COUNT(*) FROM FileLineBodies WHERE FileId = f.Id) AS BodyLineCount
            FROM Files f
        ''')

        return { info.name: info for row in cursor for info in [EDDNFile(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])] }

    def updatelinecount(self, fileid, linecount):
        cursor = makestreamingcursor(self.conn)
        cursor.execute('UPDATE Files SET LineCount = %s WHERE Id = %s', (linecount, fileid))

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

def main():
    args = argparser.parse_args()
    reprocess = args.reprocess
    market = args.market
    timer = Timer({'init', 'load', 'read', 'parse', 'error', 'sysquery', 'sysqueryedts', 'sysquerypgre', 'sysquerylookup', 'sysselectmaddr', 'sysselectaddr', 'sysselectname', 'sysinsert', 'stnquery', 'stnselect', 'stninsert', 'commit', 'stats', 'syslookupbinsearch', 'bodyquery', 'bodyinsert', 'bodylookupname', 'bodyquerypgre', 'bodylookuppg', 'bodyselectname', 'bodyinsertpg', 'bodyupdateid', 'bodyqueryname', 'timestampinsert', 'edsmupdate'})
    try:
        conn = createconnection()
        sysdb = EDDNSysDB(conn, args.edsmsys, args.edsmbodies)
        timer.time('init')

        if not args.noeddn:
            sys.stderr.write('Retrieving EDDN files from DB\n') 
            sys.stderr.flush()
            files = sysdb.getfiles()
            timer.time('init', 0)
            sys.stderr.write('Processing EDDN files\n')
            sys.stderr.flush()
            for filename, fileinfo in files.items():
                if fileinfo.eventtype is not None:
                    #if fileinfo.eventtype in ('Location'):
                    #    continue
                    if fileinfo.linecount is None or (reprocess == True and (fileinfo.linecount != fileinfo.syslinecount or (fileinfo.eventtype == 'Scan' and fileinfo.linecount != fileinfo.bodylinecount) or (fileinfo.eventtype == 'Docked' and fileinfo.linecount != fileinfo.stnlinecount))):
                        fn = eddndir + '/' + filename
                        if os.path.exists(fn):
                            sys.stderr.write('{0}\n'.format(fn))
                            with bz2.BZ2File(fn, 'r') as f:
                                syslines = sysdb.getsystemfilelines(fileinfo.id)
                                stnlines = sysdb.getstationfilelines(fileinfo.id)
                                bodylines = sysdb.getbodyfilelines(fileinfo.id)
                                tslines = sysdb.gettimestampfilelines(fileinfo.id)
                                linecount = 0
                                timer.time('load')
                                systoinsert = []
                                stntoinsert = []
                                bodytoinsert = []
                                timestamptoinsert = []
                                for lineno, line in enumerate(f):
                                    if (lineno + 1) not in syslines or (lineno + 1) not in bodylines:
                                        timer.time('read')
                                        try:
                                            msg = json.loads(line)
                                            body = msg['message']
                                            hdr = msg['header']
                                            sysname = body['StarSystem']
                                            starpos = body['StarPos']
                                            sysaddr = body['SystemAddress'] if 'SystemAddress' in body else None
                                            stationname = body['StationName'] if 'StationName' in body else None
                                            marketid = body['MarketID'] if 'MarketID' in body else None
                                            stationtype = body['StationType'] if 'StationType' in body else None
                                            bodyname = body['Body'] if 'Body' in body else None
                                            bodyid = body['BodyID'] if 'BodyID' in body else None
                                            bodytype = body['BodyType'] if 'BodyType' in body else None
                                            scanbodyname = body['BodyName'] if 'BodyName' in body else None
                                            parents = body['Parents'] if 'Parents' in body else None
                                            timestamp = body['timestamp'] if 'timestamp' in body else None
                                            gwtimestamp = hdr['gatewayTimestamp'] if 'gatewayTimestamp' in hdr else None
                                        except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                                            sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                                            timer.time('error')
                                            pass
                                        else:
                                            if marketid is not None and (marketid <= 0 or marketid > 1 << 32):
                                                marketid = None
                                            sqltimestamp = timestamptosql(timestamp)
                                            sqlgwtimestamp = timestamptosql(gwtimestamp)
                                            timer.time('parse')
                                            if sqltimestamp is not None:
                                                if (lineno + 1) not in tslines and sqltimestamp is not None and sqlgwtimestamp is not None:
                                                    timestamptoinsert += [(fileinfo.id, lineno + 1, (sqltimestamp - tsbasedate).total_seconds(), (sqlgwtimestamp - tsbasedate).total_seconds() * 10000000.0)]
                                                
                                                starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                                                system = sysdb.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], sysaddr)
                                                timer.time('sysquery')
                                                if system is not None:
                                                    reject = False

                                                    if (lineno + 1) not in stnlines and sqltimestamp is not None and not (sqltimestamp >= ed303date and sqltimestamp < ed304date and not allow303bodies):
                                                        if stationname is not None and stationname != '':
                                                            station = sysdb.getstation(timer, stationname, sysname, marketid, sqltimestamp, system, stationtype, bodyname, bodyid, bodytype)
                                                            timer.time('stnquery')

                                                            if station is not None:
                                                                stntoinsert += [(fileinfo.id, lineno + 1, station)]
                                                            else:
                                                                reject = True
                                                                #import pdb; pdb.set_trace()
                                                                #sysdb.addfilelinestationunresolved(fileinfo.id, lineno + 1, line.strip(), sysname, system.id, stationname, marketid, sqltimestamp)
                                                                pass
                                                        elif bodyname is not None and bodytype is not None and bodytype == 'Station':
                                                            station = sysdb.getstation(timer, bodyname, sysname, None, sqltimestamp, system = system, bodyid = bodyid)
                                                            timer.time('stnquery')

                                                            if station is not None:
                                                                stntoinsert += [(fileinfo.id, lineno + 1, station)]
                                                            else:
                                                                reject = True
                                                                #import pdb; pdb.set_trace()
                                                                #sysdb.addfilelinestationunresolved(fileinfo.id, lineno + 1, line.strip(), sysname, system.id, bodyname, marketid, sqltimestamp)
                                                                pass

                                                    if (lineno + 1) not in bodylines and sqltimestamp is not None and not (sqltimestamp >= ed303date and sqltimestamp < ed304date and not allow303bodies):
                                                        if scanbodyname is not None:
                                                            scanbody = sysdb.getbody(timer, scanbodyname, sysname, bodyid, system, body, sqltimestamp)
                                                            if scanbody is not None:
                                                                sysdb.insertbodyparents(timer, scanbody.id, system, bodyid, parents)
                                                                bodytoinsert += [(fileinfo.id, lineno + 1, scanbody)]
                                                            else:
                                                                reject = True
                                                                sysdb.addfilelinebodyunresolved(fileinfo.id, lineno + 1, line.strip(), sysname, system.id, scanbodyname, bodyid, sqltimestamp)
                                                            timer.time('bodyquery')

                                                    if (lineno + 1) not in syslines and not reject:
                                                        systoinsert += [(fileinfo.id, lineno + 1, system)]

                                                else:
                                                    sysdb.addfilelinesystemunresolved(fileinfo.id, lineno + 1, line.strip(), sysname, sysaddr, starpos[0], starpos[1], starpos[2], sqltimestamp)
                                                
                                    linecount += 1
                                    if (linecount % 1000) == 0:
                                        conn.commit()
                                        if len(systoinsert) != 0:
                                            sysdb.addfilelinesystems(systoinsert)
                                            timer.time('sysinsert', len(systoinsert))
                                            systoinsert = []
                                        if len(stntoinsert) != 0:
                                            sysdb.addfilelinestations(stntoinsert)
                                            timer.time('stninsert', len(stntoinsert))
                                            stntoinsert = []
                                        if len(bodytoinsert) != 0:
                                            sysdb.addfilelinebodies(bodytoinsert)
                                            timer.time('bodyinsert', len(bodytoinsert))
                                            bodytoinsert = []
                                        if len(timestamptoinsert) != 0:
                                            sysdb.addfilelinetimestamps(timestamptoinsert)
                                            timer.time('timestampinsert', len(timestamptoinsert))
                                            timestamptoinsert = []
                                        conn.commit()
                                        sys.stderr.write('.')
                                        sys.stderr.flush()
                                
                                conn.commit()
                                if len(systoinsert) != 0:
                                    sysdb.addfilelinesystems(systoinsert)
                                    timer.time('sysinsert', len(systoinsert))
                                    systoinsert = []
                                if len(stntoinsert) != 0:
                                    sysdb.addfilelinestations(stntoinsert)
                                    timer.time('stninsert', len(stntoinsert))
                                    stntoinsert = []
                                if len(bodytoinsert) != 0:
                                    sysdb.addfilelinebodies(bodytoinsert)
                                    timer.time('bodyinsert', len(bodytoinsert))
                                    bodytoinsert = []
                                if len(timestamptoinsert) != 0:
                                    sysdb.addfilelinetimestamps(timestamptoinsert)
                                    timer.time('timestampinsert', len(timestamptoinsert))
                                    timestamptoinsert = []

                                conn.commit()

                                sys.stderr.write('\n')
                                sysdb.updatelinecount(fileinfo.id, linecount)
                else:
                    if market and (fileinfo.linecount is None or (reprocess == True and fileinfo.linecount != fileinfo.stnlinecount)):
                        fn = eddndir + '/' + filename
                        if os.path.exists(fn):
                            sys.stderr.write('{0}\n'.format(fn))
                            with bz2.BZ2File(fn, 'r') as f:
                                stnlines = sysdb.getstationfilelines(fileinfo.id)
                                tslines = sysdb.gettimestampfilelines(fileinfo.id)
                                linecount = 0
                                stntoinsert = []
                                timestamptoinsert = []
                                timer.time('load')
                                for lineno, line in enumerate(f):
                                    if (lineno + 1) not in stnlines:
                                        timer.time('read')
                                        try:
                                            msg = json.loads(line)
                                            body = msg['message']
                                            hdr = msg['header']
                                            sysname = body['systemName']
                                            stationname = body['stationName']
                                            marketid = body['marketId'] if 'marketId' in body else None
                                            timestamp = body['timestamp'] if 'timestamp' in body else None
                                            gwtimestamp = hdr['gatewayTimestamp'] if 'gatewayTimestamp' in hdr else None
                                        except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                                            print('Error: {0}'.format(sys.exc_info()[0]))
                                            timer.time('error')
                                            pass
                                        else:
                                            if marketid is not None and (marketid <= 0 or marketid > 1 << 32):
                                                marketid = None
                                            sqltimestamp = timestamptosql(timestamp)
                                            sqlgwtimestamp = timestamptosql(gwtimestamp)
                                            timer.time('parse')
                                            if sqltimestamp is not None:
                                                if (lineno + 1) not in tslines and sqltimestamp is not None and sqlgwtimestamp is not None:
                                                    timestamptoinsert += [(fileinfo.id, lineno + 1, (sqltimestamp - tsbasedate).total_seconds(), (sqlgwtimestamp - tsbasedate).total_seconds() * 10000000.0)]
                                                
                                                station = sysdb.getstation(timer, stationname, sysname, marketid, sqltimestamp)
                                                timer.time('stnquery')

                                                if station is not None:
                                                    stntoinsert += [(fileinfo.id, lineno + 1, station)]
                                                else:
                                                    #import pdb; pdb.set_trace()
                                                    #sysdb.addfilelinestationunresolved(fileinfo.id, lineno + 1, line.strip(), sysname, None, stationname, marketid, sqltimestamp)
                                                    pass
                                    linecount += 1
                                    if (linecount % 1000) == 0:
                                        conn.commit()
                                        if len(stntoinsert) != 0:
                                            sysdb.addfilelinestations(stntoinsert)
                                            timer.time('stninsert', len(stntoinsert))
                                            stntoinsert = []
                                        if len(timestamptoinsert) != 0:
                                            sysdb.addfilelinetimestamps(timestamptoinsert)
                                            timer.time('timestampinsert', len(timestamptoinsert))
                                            timestamptoinsert = []
                                        conn.commit()
                                        sys.stderr.write('.')
                                        sys.stderr.flush()
                                
                                conn.commit()
                                if len(stntoinsert) != 0:
                                    sysdb.addfilelinestations(stntoinsert)
                                    timer.time('stninsert', len(stntoinsert))
                                    stntoinsert = []
                                if len(timestamptoinsert) != 0:
                                    sysdb.addfilelinetimestamps(timestamptoinsert)
                                    timer.time('timestampinsert', len(timestamptoinsert))
                                    timestamptoinsert = []
                                conn.commit()
                                sys.stderr.write('\n')
                                sysdb.updatelinecount(fileinfo.id, linecount)
                    conn.commit()
                    timer.time('commit')

        if args.edsmsys:
            sys.stderr.write('Processing EDSM systems\n')
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
                        timer.time('error')
                        pass
                    else:
                        sqltimestamp = timestamptosql(timestamp)
                        sqlts = int((sqltimestamp - tsbasedate).total_seconds())
                        timer.time('parse')
                        (sysid, ts) = sysdb.findedsmsysid(edsmsysid)
                        timer.time('sysquery')
                        if not sysid or ts != sqlts:
                            starpos = [ math.floor(v * 32 + 0.5) / 32.0 for v in starpos ]
                            system = sysdb.getsystem(timer, sysname, starpos[0], starpos[1], starpos[2], sysaddr)
                            timer.time('sysquery', 0)

                            if system is not None:
                                sysdb.updateedsmsysid(edsmsysid, system.id, sqltimestamp)
                            timer.time('edsmupdate')
                            w += 1

                    if ((i + 1) % 1000) == 0:
                        conn.commit()
                        sys.stderr.write('.' if w == 0 else '*')
                        sys.stderr.flush()
                        w = 0

                        if ((i + 1) % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(i + 1))
                            sys.stderr.flush()
                        timer.time('commit')
                            
            sys.stderr.write('  {0}\n'.format(i + 1))
            sys.stderr.flush()
            conn.commit()
            timer.time('commit')
        
        if args.edsmbodies:
            sys.stderr.write('Processing EDSM bodies\n')
            with bz2.BZ2File(edsmbodiesfile, 'r') as f:
                w = 0
                for i, line in enumerate(f):
                    try:
                        msg = json.loads(line)
                        edsmbodyid = msg['id']
                        bodyid = msg['bodyId']
                        bodyname = msg['name']
                        edsmsysid = msg['systemId']
                        sysname = msg['systemName']
                        timestamp = msg['updateTime'].replace(' ', 'T')
                        periapsis = msg['argofPeriapsis'] if 'argOfPeriapsis' in msg else None
                        semimajor = msg['semiMajorAxis'] if 'semiMajorAxis' in msg else None
                        bodytype = msg['type']
                        subtype = msg['subType']
                    except (OverflowError,ValueError,TypeError,json.JSONDecodeError):
                        sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[0]))
                        timer.time('error')
                        pass
                    else:
                        sqltimestamp = timestamptosql(timestamp)
                        timer.time('parse')
                        sysid = sysdb.findedsmsysid(edsmsysid)
                        if sysid:
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
                                
                                scanbody = sysdb.getbody(timer, bodyname, sysname, bodyid, system, body, sqltimestamp)

                                if scanbody:
                                    sysdb.updateedsmbodyid(scanbody.id, edsmbodyid, sqltimestamp)
                                
                                timer.time('bodyquery')

                    if ((i + 1) % 1000) == 0:
                        conn.commit()
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if ((i + 1) % 64000) == 0:
                            sys.stderr.write('  {0}\n'.format(i + 1))
                            sys.stderr.flush()

            sys.stderr.write('  {0}\n'.format(i + 1))
            sys.stderr.flush()
            conn.commit()
            timer.time('commit')

                            
    finally:
        timer.printstats()

if __name__ == '__main__':
    main()
