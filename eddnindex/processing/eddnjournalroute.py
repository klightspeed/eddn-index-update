import os
import os.path
import sys
import json
import bz2
import math
from datetime import datetime, timedelta
from typing import Callable, Tuple
from collections.abc import MutableSequence as List

from ..config import Config
from ..types import EDDNFile, EDDNSystem, Writable
from ..eddnsysdb import EDDNSysDB
from ..util import timestamp_to_datetime
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

    line_count = fileinfo.line_count
    info_file_line_count = fileinfo.info_file_line_count
    route_system_count = fileinfo.route_system_count
    nav_route_system_count = fileinfo.nav_route_system_count
    event_type = fileinfo.event_type
    date_str = fileinfo.date.isoformat()[:10]

    if (line_count is None
            or (reprocess is True
                and line_count != info_file_line_count)
            or (reprocess is True
                and route_system_count != nav_route_system_count)):
        fn = os.path.join(
            config.eddn_dir,
            fileinfo.date.isoformat()[:7],
            filename
        )

        if os.path.exists(fn):
            sys.stderr.write(f'{fn}\n')
            updatetitleprogress(f'{date_str}:{event_type}')
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                infolines = sysdb.getinfofilelines(fileinfo.id)
                navroutelines = sysdb.getnavroutefilelines(fileinfo.id)
                linecount = 0
                routesystemcount = 0
                totalsize = 0
                timer.time('load')
                infotoinsert: List[Tuple[
                    int, int, datetime, datetime, int, int,
                    int, int, float, int, int, int
                ]] = []
                routesystemstoinsert: List[Tuple[
                    int, int, EDDNSystem, int
                ]] = []
                for lineno, line in enumerate(f):
                    process_line(
                        sysdb,
                        timer,
                        fileinfo,
                        rejectout,
                        infolines,
                        navroutelines,
                        routesystemcount,
                        infotoinsert,
                        routesystemstoinsert,
                        lineno,
                        line
                    )

                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        commit(
                            sysdb,
                            timer,
                            infotoinsert,
                            routesystemstoinsert
                        )

                        routesystemstoinsert = []
                        infotoinsert = []
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write(f'  {lineno + 1}\n')
                            sys.stderr.flush()

                commit(
                    sysdb,
                    timer,
                    infotoinsert,
                    routesystemstoinsert
                )

                sys.stderr.write(f'  {linecount}\n')
                sys.stderr.flush()
                sysdb.updatefileinfo(
                    fileinfo.id,
                    linecount,
                    totalsize,
                    comprsize,
                    0,
                    0,
                    routesystemcount
                )


def commit(sysdb: EDDNSysDB,
           timer: Timer,
           infotoinsert: List[Tuple[
                    int, int, datetime, datetime, int, int,
                    int, int, float, int, int, int
                ]],
           routesystemstoinsert: List[Tuple[int, int, EDDNSystem, int]]
           ):
    sysdb.commit()
    if len(infotoinsert) != 0:
        sysdb.addfilelineinfo(infotoinsert)
        timer.time('infoinsert', len(infotoinsert))
    if len(routesystemstoinsert) != 0:
        sysdb.addfilelineroutesystems(routesystemstoinsert)
        timer.time('routesysteminsert', len(routesystemstoinsert))
    sysdb.commit()


def process_line(sysdb,
                 timer,
                 fileinfo,
                 rejectout,
                 infolines,
                 navroutelines,
                 routesystemcount,
                 infotoinsert,
                 routesystemstoinsert,
                 lineno,
                 line
                 ):
    timer.time('read')

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
    else:
        sqltimestamp = timestamp_to_datetime(timestamp)
        sqlgwtimestamp = timestamp_to_datetime(gwtimestamp)
        timer.time('parse')
        line_len = len(line)
        line_routes = []

        for n, system in enumerate(route):
            try_add_system(
                sysdb,
                timer,
                line_routes,
                n,
                system
            )

        if (sqltimestamp is not None
                and sqlgwtimestamp is not None
                and sqltimestamp < sqlgwtimestamp + timedelta(days=1)):
            process_route(
                sysdb,
                fileinfo,
                rejectout,
                infolines,
                navroutelines,
                infotoinsert,
                routesystemstoinsert,
                lineno,
                msg,
                route,
                software,
                sqltimestamp,
                sqlgwtimestamp,
                line_len,
                line_routes
            )

        else:
            msg['rejectReason'] = 'Timestamp error'
            rejectout.write(json.dumps(msg) + '\n')

        routesystemcount += len(line_routes)


def process_route(sysdb,
                  fileinfo,
                  rejectout,
                  infolines,
                  navroutelines,
                  infotoinsert,
                  routesystemstoinsert,
                  lineno,
                  msg,
                  route,
                  software,
                  sqltimestamp,
                  sqlgwtimestamp,
                  line_len,
                  line_routes
                  ):
    reject = False
    reject_reason = None
    reject_data = None

    if len(line_routes) < 2:
        reject = True
        reject_reason = 'Route too short'
        reject_data = route
    elif len([s for s, _, _, _ in line_routes if s is None]) != 0:
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
                routesystemstoinsert += [(
                    fileinfo.id,
                    lineno + 1,
                    system,
                    n
                )]

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


def try_add_system(sysdb, timer, line_routes, n, system):
    try:
        sysname = system['StarSystem']
        starpos = system['StarPos']
        sysaddr = system['SystemAddress']
    except ValueError:
        line_routes.append((None, n + 1, "Missing property", system))
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
        line_routes.append((
                    system,
                    n + 1,
                    sysRejectReason,
                    sysRejectData
                ))
