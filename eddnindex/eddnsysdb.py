import os
import os.path
import sys
import json
import math
from functools import lru_cache
from datetime import datetime, timedelta
import time
import urllib.request
import urllib.error
from typing import Set, Tuple, List, Dict, Union, Any

import numpy
import numpy.core.records

from . import edtslookup
import edts.edtslib.system as edtslib_system
import edts.edtslib.id64data as edtslib_id64data

from . import mysqlutils as mysql
from .types import EDDNSystem, EDDNBody, EDDNFaction, EDDNFile, EDSMFile, EDDNRegion, EDDNStation, EDSMBody
from .timer import Timer
from . import constants
from .util import timestamptosql
from .config import Config

class EDDNSysDB(object):
    conn: mysql.DBConnection
    regions: Dict[str, EDDNRegion]
    regionaddrs: Dict[int, EDDNRegion]
    namedsystems: Dict[str, Union[EDDNSystem, List[EDDNSystem]]]
    namedbodies: Dict[str, Union[EDDNBody, List[EDDNBody]]]
    parentsets: Dict[Tuple[int, str], int]
    bodydesigs: Dict[str, int]
    software: Dict[str, int]
    factions: Dict[str, EDDNFaction]
    edsmsysids: Union[numpy.ndarray[Any, numpy.dtype[numpy.intc, numpy.intc, numpy.intc, numpy.byte, numpy.byte, numpy.byte, numpy.byte]], None]
    edsmbodyids: Union[numpy.ndarray[Any, numpy.dtype[numpy.intc, numpy.intc, numpy.intc]], None]
    eddbsysids: Union[numpy.ndarray[Any, numpy.dtype[numpy.intc, numpy.intc, numpy.intc]], None]
    edsmsyscachefile: str
    knownbodiessheeturi: str
    edsmbodycachefile: str

    def __init__(self, conn: mysql.DBConnection, loadedsmsys: bool, loadedsmbodies: bool, loadeddbsys: bool, config: Config):
        self.conn = conn
        self.regions = {}
        self.regionaddrs = {}
        self.namedsystems = {}
        self.namedbodies = {}
        self.parentsets = {}
        self.bodydesigs = {}
        self.software = {}
        self.factions = {}
        self.knownbodies = {}
        self.edsmsysids = None
        self.edsmbodyids = None
        self.eddbsysids = None
        self.edsmsyscachefile = config.edsmsyscachefile
        self.knownbodiessheeturi = config.knownbodiessheeturi
        self.edsmbodycachefile = config.edsmbodycachefile

        try:
            timer = Timer()
            self.loadregions(conn, timer)
            self.loadnamedsystems(conn, timer)
            self.loadnamedbodies(conn, timer)
            self.loadparentsets(conn, timer)
            self.loadsoftware(conn, timer)
            self.loadbodydesigs(conn, timer)
            self.loadfactions(conn, timer)
            self.loadknownbodies(timer)

            if loadedsmsys or loadedsmbodies:
                self.loadedsmsystems(conn, timer)

            if loadedsmbodies:
                self.loadedsmbodies(conn, timer)
            
            if loadeddbsys:
                self.loadeddbsystems(conn, timer)

        finally:
            timer.printstats()

    def loadedsmsystems(self, conn: mysql.DBConnection, timer: Timer):
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT MAX(EdsmId) FROM Systems_EDSM')
        row = c.fetchone()
        maxedsmsysid = row[0]

        timer.time('sql')

        if maxedsmsysid:
            sys.stderr.write('Loading EDSM System IDs\n')
            if os.path.exists(self.edsmsyscachefile):
                with open(self.edsmsyscachefile, 'rb') as f:
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
                with open(self.edsmsyscachefile + '.tmp', 'wb') as f:
                    self.edsmsysids.tofile(f)
                os.rename(self.edsmsyscachefile + '.tmp', self.edsmsyscachefile)

    def loadeddbsystems(self, conn: mysql.DBConnection, timer: Timer):
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

    def loadedsmbodies(self, conn: mysql.DBConnection, timer: Timer):
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT MAX(EdsmId) FROM SystemBodies_EDSM')
        row = c.fetchone()
        maxedsmbodyid = row[0]

        timer.time('sql')

        if maxedsmbodyid:
            sys.stderr.write('Loading EDSM Body IDs\n')
            if os.path.exists(self.edsmbodycachefile):
                with open(self.edsmbodycachefile, 'rb') as f:
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

    def loadparentsets(self, conn: mysql.DBConnection, timer: Timer):
        sys.stderr.write('Loading Parent Sets\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, BodyID, ParentJson FROM ParentSets')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlparents', len(rows))
        for row in rows:
            self.parentsets[(int(row[1]),row[2])] = int(row[0])
        timer.time('loadparents', len(rows))

    def loadsoftware(self, conn: mysql.DBConnection, timer: Timer):
        sys.stderr.write('Loading Software\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, Name FROM Software')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlsoftware', len(rows))
        for row in rows:
            self.software[row[1]] = int(row[0])
        timer.time('loadsoftware', len(rows))

    def loadbodydesigs(self, conn: mysql.DBConnection, timer: Timer):
        sys.stderr.write('Loading Body Designations\n')
        c = mysql.makestreamingcursor(conn)
        c.execute('SELECT Id, BodyDesignation FROM SystemBodyDesignations WHERE IsUsed = 1')
        timer.time('sql')
        rows = c.fetchall()
        timer.time('sqlbodydesigs', len(rows))
        for row in rows:
            self.bodydesigs[row[1]] = int(row[0])
        timer.time('loadbodydesigs', len(rows))

    def loadnamedbodies(self, conn: mysql.DBConnection, timer: Timer):
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

    def loadnamedsystems(self, conn: mysql.DBConnection, timer: Timer):
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

    def loadregions(self, conn: mysql.DBConnection, timer: Timer):
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

    def loadfactions(self, conn: mysql.DBConnection, timer: Timer):
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

    def loadknownbodies(self, timer: Timer):
        sys.stderr.write('Loading Known Bodies\n')
        knownbodies = {}

        with urllib.request.urlopen(self.knownbodiessheeturi) as f:
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

    def findmodsysaddr(self, part, modsysaddr: int, sysname: str, starpos: Tuple[float, float, float], start: int, end: int, search):
        arr = part.view(numpy.ndarray)
        sysaddr = self.modsysaddrtosysaddr(modsysaddr)
        xstart = arr[start:end].searchsorted(numpy.array(search, arr.dtype)) + start
        if xstart >= start and xstart < end and arr[xstart][0] == modsysaddr:
            return part[xstart:min(xstart+10,end)]

        #sys.stderr.write('Cannot find system {0} [{1}] ({2},{3},{4})\n'.format(sysname, sysaddr, starpos[0], starpos[1], starpos[2]))
        #sys.stderr.writelines(['{0}\n'.format(s) for s in ])
        #raise ValueError('Unable to find system')
        return None

    def _namestr(self, name: Union[str, bytes]):
        if type(name) is str:
            return name
        else:
            return name.decode('utf-8')

    def _findsystem(self, cursor: Union[List[EDDNSystem],List[Tuple[int, int, Union[str, bytes], float, float, float]]], sysname: str, starpos: Tuple[float, float, float], sysaddr: int, syslist: Set[EDDNSystem]):
        rows = list(cursor)
        systems: List[EDDNSystem]

        if len(rows) > 0 and type(rows[0]) is EDDNSystem:
            systems = rows
        else:
            systems = set([ EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 and row[4] != 0 and row[5] != 0) for row in rows ])

        if starpos is not None or sysaddr is not None:
            matches: Set[EDDNSystem] = set()
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

    def sysaddrtomodsysaddr(self, sysaddr: int) -> int:
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

    def modsysaddrtosysaddr(self, modsysaddr: int) -> int:
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

    def findsystemsbyname(self, sysname: str) -> List[EDDNSystem]:
        systems: List[EDDNSystem] = []

        if sysname in self.namedsystems:
            systems = self.namedsystems[sysname]
            if type(systems) is not list:
                systems = [systems]
            systems = [ s for s in systems ]

        pgsysmatch = constants.pgsysre.match(sysname)
        ri = None
        modsysaddr = None

        if pgsysmatch:
            regionname: str = pgsysmatch[1]
            mid1_2: str = pgsysmatch[2].upper()
            sizecls: str = pgsysmatch[3].lower()
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

    def getrejectdata(self, sysname: str, sysaddr: int, systems: Union[List[EDDNSystem], None]):
        id64name = None
        nameid64 = None
        pgsysmatch = constants.pgsysre.match(sysname)
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
            regionname: str = pgsysmatch[1]
            mid1_2: str = pgsysmatch[2].upper()
            sizecls: str = pgsysmatch[3].lower()
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
    def getsystem(self, timer: Timer, sysname: str, x: float, y: float, z: float, sysaddr: int) -> Union[Tuple[EDDNSystem, None, None], Tuple[None, str, dict]]:
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
        pgsysmatch = constants.pgsysre.match(sysname)
        ri = None
        modsysaddr = None

        if pgsysmatch:
            regionname: str = pgsysmatch[1]
            mid1_2: str = pgsysmatch[2].upper()
            sizecls: str = pgsysmatch[3].lower()
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
        edtsid64 = edtslookup.find_edts_system_id64(sysname, sysaddr, starpos)

        if edtsid64 is not None:
            timer.time('sysqueryedts', 0)
            modsysaddr = self.sysaddrtomodsysaddr(edtsid64)
            c = self.conn.cursor()
            c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE ModSystemAddress = %s', (modsysaddr,))
            system = self._findsystem(c, sysname, starpos, sysaddr, systems)
            timer.time('sysselectmaddr')

            if system is not None:
                timer.time('sysqueryedts')
                return (system, None, None)

        timer.time('sysqueryedts', 0)
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

    def getstation(self, 
                   timer: Timer,
                   name: str,
                   sysname: str,
                   marketid: Union[int, None],
                   timestamp: datetime,
                   system: Union[EDDNSystem, None] = None,
                   stationtype: Union[str, None] = None,
                   bodyname: Union[str, None] = None,
                   bodyid: Union[int, None] = None,
                   bodytype: Union[str, None] = None,
                   eventtype: Union[str, None] = None,
                   test: bool = False) -> Union[Tuple[EDDNStation, None, None], Tuple[None, str, Union[List[dict], None]]]:
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

        if (stationtype is not None and stationtype == 'FleetCarrier') or constants.carriernamere.match(name):
            sysid = None
            sysname = ''
            bodyname = None
            bodytype = None
            bodyid = None
            stationtype = 'FleetCarrier'

        stationtype_location = None

        if eventtype is not None and eventtype == 'Location' and stationtype is not None and stationtype == 'Bernal' and timestamp > constants.ed332date:
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
                validuntil = constants.ed330date
            elif (marketid is not None and marketid >= 3789600000) or stationtype == 'OnFootSettlement':
                validfrom = constants.ed400date
            elif (marketid is not None and marketid >= 3700000000) or stationtype == 'FleetCarrier':
                validfrom = constants.ed370date
            elif stationtype in ['CraterPort', 'CraterOutpost']:
                validfrom = constants.ed330date
            elif stationtype == 'Ocellus':
                validfrom = constants.ed332date
                stationtype_location = 'Bernal'
            elif stationtype == 'Bernal' and timestamp < constants.ed332date:
                validuntil = constants.ed332date
            elif stationtype == 'Megaship' and marketid is not None and marketid >= 3400000000:
                validfrom = constants.megashipweek0 + timedelta(weeks = math.floor((timestamp - constants.megashipweek0).total_seconds() / 86400 / 7), hours = -2)
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

    def insertbodyparents(self, timer: Timer, scanbodyid: int, system: EDDNSystem, bodyid: int, parents: List[Dict]):
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

    def insertsoftware(self, softwarename: str):
        if softwarename not in self.software:
            c = self.conn.cursor()
            c.execute('INSERT INTO Software (Name) VALUES (%s)', (softwarename,))
            self.software[softwarename] = c.lastrowid

    def insertedsmfile(self, filename: str):
        c = self.conn.cursor()
        c.execute('INSERT INTO EDSMFiles (FileName) VALUES (%s)', (filename,))
        return c.lastrowid

    def getbody(self, timer: Timer, name: str, sysname: str, bodyid: int, system, body, timestamp):
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
            match = constants.pgbodyre.match(desig)

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
            return (EDDNBody(row[0], name, sysname, system.id, row[4] or bodyid, None, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 0), None, None)
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
                return (EDDNBody(cursor.lastrowid, name, sysname, system.id, bodyid, None, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 0), None, None)
                
            if (not ispgname and constants.pgsysre.match(name)) or desigid is None:
                allrows = []
                cursor = self.conn.cursor()
                cursor.execute('SELECT Id, BodyName, SystemName, SystemId, BodyId, BodyCategoryDescription, ArgOfPeriapsis, ValidFrom, ValidUntil, IsRejected FROM SystemBodyNames sb WHERE sb.CustomName = %s', (name,))
                allrows += cursor.fetchall()
                pgsysbodymatch = constants.pgsysbodyre.match(name)
                dupsystems = []

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
                    if len(dupsystems) > 0:
                        return (
                            None,
                            'Procgen body in wrong system',
                            [{
                                'System': sysname,
                                'Body': name,
                                'Systems': [{
                                    'id': s.id,

                                } for s in dupsystems]
                            }]
                        )
                    return (
                        None,
                        'Procgen body in wrong system',
                        [{'System': sysname, 'Body': name}])
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

            return (EDDNBody(rowid, name, sysname, system.id, bodyid, None, (body['Periapsis'] if 'Periapsis' in body else None), None, None, 1), None, None)

    def getfaction(self, timer: Timer, name: str, government: str, allegiance: str):
        factions = None

        if government[:12] == '$government_' and government[-1] == ';':
            government = government[12:-1]

        if name in self.factions:
            factions = self.factions[name]
            if type(factions) is not list:
                factions = [factions]
            for faction in factions:
                if faction.government == government and (allegiance is None or faction.allegiance == allegiance):
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

    def updatestation(self, station: EDDNStation, **kwargs):
        station = station._replace(**kwargs)

        c = self.conn.cursor()
        c.execute('UPDATE Stations SET MarketId = %s, SystemId = %s, StationType = %s, Body = %s, BodyID = %s WHERE Id = %s', (station.marketid, station.systemid, station.type, station.body, station.bodyid, station.id))

        return station

    def getsystembyid(self, sysid: int) -> EDDNSystem:
        c = self.conn.cursor()
        c.execute('SELECT ns.Id, ns.SystemAddress, ns.Name, ns.X, ns.Y, ns.Z FROM SystemNames ns WHERE Id = %s', (sysid,))
        row = c.fetchone()

        if row:
            return EDDNSystem(row[0], row[1], self._namestr(row[2]), row[3] / 32.0 - 49985, row[4] / 32.0 - 40985, row[5] / 32.0 - 24105, row[3] != 0 and row[4] != 0 and row[5] != 0)
        else:
            return None

    def getbodiesfromedsmbyid(self, edsmid: int, timer: Timer) -> List[EDSMBody]:
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

    def updatesystemfromedsmbyid(self, edsmid: int, timer: Timer, rejectout) -> bool:
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
            sqlts = int((sqltimestamp - constants.tsbasedate).total_seconds())
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

    def findedsmsysid(self, edsmid: int) -> Tuple[int, int, bool, list]:
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

    def findedsmbodyid(self, edsmid: int) -> Tuple[int, int, list]:
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
        with open(self.edsmsyscachefile + '.tmp', 'wb') as f:
            self.edsmsysids.tofile(f)
        os.rename(self.edsmsyscachefile + '.tmp', self.edsmsyscachefile)

    def saveedsmbodycache(self):
        with open(self.edsmbodycachefile + '.tmp', 'wb') as f:
            self.edsmbodyids.tofile(f)
        os.rename(self.edsmbodycachefile + '.tmp', self.edsmbodycachefile)

    def updateedsmsysid(self, edsmid: int, sysid: int, ts: Union[int, datetime], hascoords: bool, ishidden: bool, isdeleted: bool):
        if type(ts) is datetime:
            ts = int((ts - constants.tsbasedate).total_seconds())

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

    
    def updateedsmbodyid(self, bodyid: int, edsmid: int, ts: datetime):
        ts = int((ts - constants.tsbasedate).total_seconds())
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
    
    def updateedsmstationid(self, edsmid: int, stationid: int, ts: datetime):
        c = self.conn.cursor()
        c.execute('INSERT INTO Stations_EDSM SET EdsmStationId = %s, Id = %s, Timestamp = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, Timestamp = %s',
                  (edsmid, stationid, ts, stationid, ts))
    
    def findeddbsysid(self, eddbid: int):
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

    def updateeddbsysid(self, eddbid: int, sysid: int, ts: datetime):
        c = self.conn.cursor()
        c.execute('INSERT INTO Systems_EDDB SET EddbId = %s, Id = %s, TimestampSeconds = %s ' +
                  'ON DUPLICATE KEY UPDATE Id = %s, TimestampSeconds = %s',
                  (eddbid, sysid, ts, sysid, ts))
    
    def addfilelinestations(self, linelist: List[Tuple[int, int, EDDNStation]]):
        values = [(fileid, lineno, station.id) for fileid, lineno, station in linelist]
        self.conn.cursor().executemany('INSERT INTO FileLineStations (FileId, LineNo, StationId) VALUES (%s, %s, %s)', values)

    def addfilelineinfo(self, linelist: List[Tuple[int, int, datetime, datetime, int, int, int, int, float, bool, bool, bool]]):
        self.conn.cursor().executemany(
            'INSERT INTO FileLineInfo ' +
            '(FileId, LineNo, Timestamp, GatewayTimestamp, SoftwareId, SystemId, BodyId, LineLength, DistFromArrivalLS, HasBodyId, HasSystemAddress, HasMarketId) VALUES ' +
            '(%s,     %s,     %s,        %s,               %s,         %s,       %s,     %s,         %s,                %s,        %s,               %s)',
            linelist
        )

    def addfilelinefactions(self, linelist: List[Tuple[int, int, EDDNFaction, int]]):
        values = [(fileid, lineno, faction.id, entrynum) for fileid, lineno, faction, entrynum in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO FileLineFactions ' +
            '(FileId, LineNo, FactionId, EntryNum) VALUES ' +
            '(%s,     %s,     %s,        %s)',
            values
        )

    def addfilelineroutesystems(self, linelist: List[Tuple[int, int, EDDNSystem, int]]):
        values = [(fileid, lineno, system.id, entrynum) for fileid, lineno, system, entrynum in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO FileLineNavRoutes ' +
            '(FileId, LineNo, SystemId, EntryNum) VALUES ' +
            '(%s,     %s,     %s,       %s)',
            values
        )

    def addedsmfilelinebodies(self, linelist: List[Tuple[int, int, int]]):
        values = [(fileid, lineno, edsmbodyid) for fileid, lineno, edsmbodyid in linelist]
        self.conn.cursor().executemany(
            'INSERT INTO EDSMFileLineBodies ' +
            '(FileId, LineNo, EdsmBodyId) VALUES ' +
            '(%s,     %s,     %s)',
            values
        )

    def getstationfilelines(self, fileid: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, StationId FROM FileLineStations WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor }

    def getinfofilelines(self, fileid: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, Timestamp, SystemId, BodyId FROM FileLineInfo WHERE FileId = %s', (fileid,))

        return { row[0]: (row[1], row[2], row[3]) for row in cursor }

    def getinfosystemfilelines(self, fileid: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, SystemId FROM FileLineInfo WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor if row[1] is not None }

    def getinfobodyfilelines(self, fileid: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, BodyId FROM FileLineInfo WHERE FileId = %s', (fileid,))

        return { row[0]: row[1] for row in cursor if row[1] is not None }

    def getfactionfilelines(self, fileid: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, FactionId FROM FileLineFactions WHERE FileId = %s', (fileid,))

        lines = {}
        for row in cursor:
            if row[0] not in lines:
                lines[row[0]] = []
            lines[row[0]] += [row[1]]

        return lines

    def getnavroutefilelines(self, fileid: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute('SELECT LineNo, EntryNum, SystemId FROM FileLineNavRoutes WHERE FileId = %s', (fileid,))

        lines = {}
        for row in cursor:
            lines[(row[0], row[1])] = row[2]

        return lines

    def getedsmbodyfilelines(self, fileid: int):
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
                stnlinecounts[row[0]] if row[0] in stnlinecounts else 0,
                infolinecounts[row[0]] if row[0] in infolinecounts else 0,
                factionlinecounts[row[0]] if row[0] in factionlinecounts else 0,
                navroutelinecounts[row[0]] if row[0] in navroutelinecounts else 0,
                row[5],
                row[6],
                row[7],
                row[8]
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

    def updatefileinfo(self, fileid: int, linecount: int, totalsize: int, comprsize: int, poplinecount: int, stnlinecount: int, navroutesystemcount: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute(
            'UPDATE Files SET LineCount = %s, CompressedSize = %s, UncompressedSize = %s, PopulatedLineCount = %s, StationLineCount = %s, NavRouteSystemCount = %s WHERE Id = %s',
            (linecount, comprsize, totalsize, poplinecount, stnlinecount, navroutesystemcount, fileid)
        )

    def updateedsmfileinfo(self, fileid: int, linecount: int, totalsize: int, comprsize: int):
        cursor = mysql.makestreamingcursor(self.conn)
        cursor.execute(
            'UPDATE EDSMFiles SET LineCount = %s, CompressedSize = %s, UncompressedSize = %s WHERE Id = %s',
            (linecount, comprsize, totalsize, fileid)
        )

