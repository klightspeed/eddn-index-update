import os
import os.path
import sys
import json
import bz2
import gzip
import math
from datetime import datetime, timedelta
import csv
from typing import Callable

from . import config
from .types import EDDNFile, EDSMFile, ProcessorArgs, Writable
from .timer import Timer
from . import constants
from .eddnsysdb import EDDNSysDB
from .util import timestamptosql
from . import mysqlutils as mysql
from .timer import Timer
from .rejectdata import EDDNRejectData

def edsmmissingbodies(sysdb: EDDNSysDB, timer: Timer, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing EDSM missing bodies\n')
    w = 0
    wg = 0
    w2 = 0
    from timeit import default_timer
    tstart = default_timer()

    fn = 'fetchbodies-{0}.jsonl'.format(datetime.utcnow().isoformat())
    fileid = sysdb.insertedsmfile(fn)

    timer.time('bodyquery')

    with open(os.path.join(config.edsmbodiesdir, fn), 'w', encoding='utf-8') as f:
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
                    sysdb.updateedsmbodyid(0, i, constants.timestamp_base_date)
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
                        periapsis = msg['argOfPeriapsis'] if 'argOfPeriapsis' in msg else None
                        semimajor = msg['semiMajorAxis'] if 'semiMajorAxis' in msg else None
                        bodytype = msg['type']
                        subtype = msg['subType']
                        sqltimestamp = timestamptosql(timestamp)
                        sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
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

def edsmbodies(sysdb: EDDNSysDB, filename: str, fileinfo: EDSMFile, reprocess: bool, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    fn = None

    if fileinfo.date is not None:
        fn = os.path.join(config.edsmbodiesdir, fileinfo.date.isoformat()[:7], filename)

    dumpfile = os.path.join(config.edsmdumpdir, filename)
    if os.path.exists(dumpfile):
        fn = dumpfile

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
                            periapsis = msg['argOfPeriapsis'] if 'argOfPeriapsis' in msg else None
                            semimajor = msg['semiMajorAxis'] if 'semiMajorAxis' in msg else None
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
                            sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
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

def edsmstations(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing EDSM stations\n')
    with gzip.open(config.edsmstationsfile, 'r') as f:
        stations = json.load(f)
        w = 0
        for i, msg in enumerate(stations):
            timer.time('read')
            try:
                edsmstationid = msg['id']
                marketid = msg['marketId']
                stationname = msg['name']
                stntype = msg['type']
                stntype = constants.EDSMStationTypes[stntype] if stntype in constants.EDSMStationTypes else stntype
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
                sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
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

def edsmsystems(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing EDSM systems\n')
    for i, rec in enumerate(sysdb.edsmsysids):
        if rec[1] == i and rec[5] == 0:
            rec.processed -= 1

    with bz2.BZ2File(config.edsmsysfile, 'r') as f:
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
                sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
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

def edsmsystemswithoutcoords(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing EDSM systems without coords\n')
    with bz2.BZ2File(config.edsmsyswithoutcoordsfile, 'r') as f:
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
                sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
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

def edsmsystemswithoutcoordsprepurge(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing pre-purge EDSM systems without coords\n')
    with bz2.BZ2File(config.edsmsyswithoutcoordsprepurgefile, 'r') as f:
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
                sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
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

def edsmhiddensystems(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing EDSM hidden systems\n')
    with bz2.BZ2File(config.edsmhiddensysfile, 'r') as f:
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

def edsmdeletedsystems(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
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

def eddbsystems(sysdb: EDDNSysDB, timer: Timer, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    sys.stderr.write('Processing EDDB systems\n')
    with bz2.open(config.eddbsysfile, 'rt', encoding='utf8') as f:
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

def eddnjournalfile(sysdb: EDDNSysDB, timer: Timer, filename: str, fileinfo: EDDNFile, reprocess: bool, reprocessall: bool, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    #if fileinfo.eventtype in ('Location'):
    #    continue
    if (fileinfo.linecount is None
        or fileinfo.populatedlinecount is None
        or (fileinfo.stationlinecount is None and fileinfo.eventtype in ('Docked', 'Location', 'CarrierJump'))
        or (reprocessall == True and fileinfo.eventtype == 'Scan' and fileinfo.date >= constants.ed_3_0_0_date.date())
        or (reprocess == True
            and (fileinfo.linecount != fileinfo.infolinecount
                 or (fileinfo.eventtype in ('Docked', 'Location', 'CarrierJump') and fileinfo.stnlinecount != fileinfo.stationlinecount)
                 or fileinfo.populatedlinecount != fileinfo.factionlinecount))):
        fn = os.path.join(config.eddndir, fileinfo.date.isoformat()[:7], filename)
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
                            eventtype = body['event'] if 'event' in body else None

                            if 'StarSystem' in body:
                                sysname = body['StarSystem']
                            elif 'System' in body:
                                sysname = body['System']
                            elif 'SystemName' in body:
                                sysname = body['SystemName']
                            else:
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
                            factions = body['Factions'] if 'Factions' in body else None
                            sysfaction = body['SystemFaction'] if 'SystemFaction' in body else (body['Faction'] if 'Faction' in body else None)
                            sysgovern = body['SystemGovernment'] if 'SystemGovernment' in body else (body['Government'] if 'Government' in body else None)
                            sysalleg = body['SystemAllegiance'] if 'SystemAllegiance' in body else (body['Allegiance'] if 'Allegiance' in body else '')
                            stnfaction = body['StationFaction'] if 'StationFaction' in body else None
                            stngovern = body['StationGovernment'] if 'StationGovernment' in body else None
                            timestamp = body['timestamp'] if 'timestamp' in body else None
                            gwtimestamp = hdr['gatewayTimestamp'] if 'gatewayTimestamp' in hdr else None
                            software = hdr['softwareName'] if 'softwareName' in hdr else None
                            distfromstar = body['DistanceFromArrivalLS'] if 'DistanceFromArrivalLS' in body else None
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
                                    if (lineno + 1) not in stnlines and sqltimestamp is not None and not (sqltimestamp >= constants.ed_3_0_3_date and sqltimestamp < constants.ed_3_0_4_date and not config.allow303bodies):
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

                                    if (lineno + 1) not in infolines and sqltimestamp is not None and not (sqltimestamp >= constants.ed_3_0_3_date and sqltimestamp < constants.ed_3_0_4_date and not config.allow303bodies):
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
                                    elif (reprocessall == True or (lineno + 1) not in infolines) and sqltimestamp is not None and not (sqltimestamp >= constants.ed_3_0_3_date and sqltimestamp < constants.ed_3_0_4_date and not config.allow303bodies):
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
                                                    'Allegiance': faction['Allegiance'] if 'Allegiance' in faction else None,
                                                    'EntryNum': n
                                                }]
                                                linefactions += [(n, sysdb.getfaction(timer, faction['Name'], faction['Government'], faction['Allegiance'] if 'Allegiance' in faction else None))]
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
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, poplinecount, stnlinecount, 0)

def eddnjournalroute(sysdb: EDDNSysDB, timer: Timer, filename: str, fileinfo: EDDNFile, reprocess: bool, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    #if fileinfo.eventtype in ('Location'):
    #    continue
    if (fileinfo.linecount is None
        or (reprocess == True and fileinfo.linecount != fileinfo.infolinecount)
        or (reprocess == True and fileinfo.routesystemcount != fileinfo.navroutesystemcount)):
        fn = os.path.join(config.eddndir, fileinfo.date.isoformat()[:7], filename)
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
                        timestamp = body['timestamp'] if 'timestamp' in body else None
                        route = list(body['Route'])
                        gwtimestamp = hdr['gatewayTimestamp'] if 'gatewayTimestamp' in hdr else None
                        software = hdr['softwareName'] if 'softwareName' in hdr else None
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
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, 0, 0, routesystemcount)

def eddnmarketfile(sysdb: EDDNSysDB, timer: Timer, filename: str, fileinfo: EDDNFile, reprocess: bool, rejectout: Writable, updatetitleprogress: Callable[[str], None]):
    if (fileinfo.linecount is None
        or (reprocess == True
            and (fileinfo.linecount != fileinfo.stnlinecount
                 or fileinfo.linecount != fileinfo.infolinecount))):
        fn = os.path.join(config.eddndir, fileinfo.date.isoformat()[:7], filename)
        if os.path.exists(fn):
            sys.stderr.write('{0}\n'.format(fn))
            updatetitleprogress('{0}:{1}'.format(fileinfo.date.isoformat()[:10], filename.split('-')[0]))
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                stnlines = sysdb.getstationfilelines(fileinfo.id)
                infolines = sysdb.getinfofilelines(fileinfo.id)
                linecount = 0
                totalsize = 0
                stntoinsert = []
                infotoinsert = []
                timer.time('load')
                for lineno, line in enumerate(f):
                    if (reprocess == True and (lineno + 1) not in stnlines) or (lineno + 1) not in infolines:
                        timer.time('read')
                        msg = None
                        try:
                            msg = json.loads(line)
                            body = msg['message']
                            hdr = msg['header']
                            sysname = body['systemName']
                            stationname = body['stationName']
                            marketid = body['marketId'] if 'marketId' in body else None
                            timestamp = body['timestamp'] if 'timestamp' in body else None
                            gwtimestamp = hdr['gatewayTimestamp'] if 'gatewayTimestamp' in hdr else None
                            software = hdr['softwareName'] if 'softwareName' in hdr else None
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
                            if sqltimestamp is not None and sqlgwtimestamp is not None and sqltimestamp < sqlgwtimestamp + timedelta(days = 1):
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
                sysdb.commit()
                sys.stderr.write('\n')
                sysdb.updatefileinfo(fileinfo.id, linecount, totalsize, comprsize, 0, linecount, 0)
        sysdb.commit()
        timer.time('commit')

def main(args: ProcessorArgs, config: config.Config, timer: Timer, updatetitleprogress: Callable[[str], None]):
    conn = mysql.DBConnection(config)
    sysdb = EDDNSysDB(conn, args.edsmsys, args.edsmbodies or args.edsmmissingbodies, args.eddbsys, config, updatetitleprogress)
    timer.time('init')

    if not args.noeddn:
        rf = EDDNRejectData(config.eddn_reject_dir)
        sys.stderr.write('Retrieving EDDN files from DB\n')
        sys.stderr.flush()
        files = sysdb.geteddnfiles()
        timer.time('init', 0)
        sys.stderr.write('Processing EDDN files\n')
        sys.stderr.flush()
        if not args.nojournal:
            for filename, fileinfo in files.items():
                if fileinfo.eventtype is not None and fileinfo.eventtype != 'NavRoute':
                    eddnjournalfile(sysdb, timer, filename, fileinfo, args.reprocess, args.reprocessall, rf, updatetitleprogress)
        if args.navroute:
            for filename, fileinfo in files.items():
                if fileinfo.eventtype is not None and fileinfo.eventtype == 'NavRoute':
                    eddnjournalroute(sysdb, timer, filename, fileinfo, args.reprocess, rf, updatetitleprogress)
        if args.market:
            for filename, fileinfo in files.items():
                if fileinfo.eventtype is None:
                    eddnmarketfile(sysdb, timer, filename, fileinfo, args.reprocess, rf, updatetitleprogress)

    if args.edsmsys:
        with open(config.edsm_systems_reject_file, 'at') as rf:
            edsmsystems(sysdb, timer, rf, updatetitleprogress)
            edsmsystemswithoutcoords(sysdb, timer, rf, updatetitleprogress)
            #process.edsmsystemswithoutcoordsprepurge(sysdb, timer, rf)
            edsmhiddensystems(sysdb, timer, rf, updatetitleprogress)
            edsmdeletedsystems(sysdb, timer, rf, updatetitleprogress)

    if args.edsmbodies:
        with open(config.edsm_bodies_reject_file, 'at') as rf:
            sys.stderr.write('Retrieving EDSM body files from DB\n')
            sys.stderr.flush()
            files = sysdb.getedsmfiles()
            timer.time('init', 0)
            sys.stderr.write('Processing EDSM bodies files\n')
            sys.stderr.flush()

            for filename, fileinfo in files.items():
                edsmbodies(sysdb, filename, fileinfo, args.reprocess, timer, rf, updatetitleprogress)

    if args.edsmmissingbodies:
        edsmmissingbodies(sysdb, timer, updatetitleprogress)

    if args.edsmstations:
        with open(config.edsm_stations_reject_file, 'at') as rf:
            edsmstations(sysdb, timer, rf, updatetitleprogress)

    if args.eddbsys:
        with open(config.eddb_systems_reject_file, 'at') as rf:
            eddbsystems(sysdb, timer, rf, updatetitleprogress)

