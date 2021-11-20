import os
import os.path
import sys
import json
import bz2
import math
from datetime import timedelta
from typing import Callable

from ..config import Config
from ..types import EDDNFile, Writable
from ..eddnsysdb import EDDNSysDB
from ..util import timestamptosql
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            timer: Timer,
            filename: str,
            fileinfo: EDDNFile,
            reprocess: bool,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            config: Config
            ):
    # if fileinfo.eventtype in ('Location'):
    #     continue
    if (fileinfo.linecount is None
            or (reprocess is True and fileinfo.linecount != fileinfo.infolinecount)
            or (reprocess is True and fileinfo.routesystemcount != fileinfo.navroutesystemcount)):
        fn = os.path.join(config.eddn_dir, fileinfo.date.isoformat()[:7], filename)
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
                    except (OverflowError, ValueError, TypeError, json.JSONDecodeError):
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
                        reject_reason = None
                        reject_data = None
                        line_len = len(line)
                        line_routes = []

                        for n, system in enumerate(route):
                            try:
                                sysname = system['StarSystem']
                                starpos = system['StarPos']
                                sysaddr = system['SystemAddress']
                            except ValueError:
                                line_routes += [(None, n + 1, "Missing property", system)]
                            else:
                                starpos = [math.floor(v * 32 + 0.5) / 32.0 for v in starpos]
                                (system, sysRejectReason, sysRejectData) = sysdb.getsystem(
                                    timer,
                                    sysname,
                                    starpos[0],
                                    starpos[1],
                                    starpos[2],
                                    sysaddr
                                )

                                timer.time('sysquery')
                                line_routes += [(system, n + 1, sysRejectReason, sysRejectData)]

                        if (sqltimestamp is not None
                                and sqlgwtimestamp is not None
                                and sqltimestamp < sqlgwtimestamp + timedelta(days=1)):
                            if len(line_routes) < 2:
                                reject = True
                                reject_reason = 'Route too short'
                                reject_data = route
                            elif len([system for system, _, _, _ in line_routes if system is None]) != 0:
                                sys_rejects = [
                                    (system, rejectReason, rejectData, n)
                                    for system, n, rejectReason, rejectData in line_routes
                                    if system is None
                                ]

                                reject = True
                                reject_reason = 'One or more systems failed validation'
                                reject_data = [{
                                    'entrynum': n,
                                    'rejectReason': rejectReason,
                                    'rejectData': rejectData
                                } for _, rejectReason, rejectData, n in sys_rejects]

                            if reject:
                                msg['rejectReason'] = reject_reason
                                msg['rejectData'] = reject_data
                                rejectout.write(json.dumps(msg) + '\n')
                            else:
                                for system, n, _, _ in line_routes:
                                    if (lineno + 1, n) not in navroutelines:
                                        routesystemstoinsert += [(fileinfo.id, lineno + 1, system, n)]

                                if (lineno + 1) not in infolines:
                                    sysdb.insertsoftware(software)
                                    system, _, _, _ = line_routes[0]
                                    infotoinsert += [(
                                        fileinfo.id,
                                        lineno + 1,
                                        sqltimestamp,
                                        sqlgwtimestamp,
                                        sysdb.software[software],
                                        system.id,
                                        None,
                                        line_len,
                                        None,
                                        0,
                                        1,
                                        0
                                    )]

                        else:
                            msg['rejectReason'] = 'Timestamp error'
                            rejectout.write(json.dumps(msg) + '\n')

                        routesystemcount += len(line_routes)

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
