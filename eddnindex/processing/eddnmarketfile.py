import os
import os.path
import sys
import json
import bz2
from datetime import datetime, timedelta
from typing import Callable, Tuple
from collections.abc import MutableSequence as List

from ..config import Config
from ..types import EDDNFile, EDDNStation, Writable
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
    if (fileinfo.line_count is None
        or (reprocess is True
            and (fileinfo.line_count != fileinfo.station_file_line_count
                 or fileinfo.line_count != fileinfo.info_file_line_count))):
        fn = os.path.join(
            config.eddn_dir,
            fileinfo.date.isoformat()[:7],
            filename
        )

        if os.path.exists(fn):
            sys.stderr.write('{0}\n'.format(fn))
            date_str = fileinfo.date.isoformat()[:10]
            file_prefix = filename.split('-')[0]
            updatetitleprogress(f'{date_str}:{file_prefix}')
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size
            with bz2.BZ2File(fn, 'r') as f:
                stnlines = sysdb.getstationfilelines(fileinfo.id)
                infolines = sysdb.getinfofilelines(fileinfo.id)
                linecount = 0
                totalsize = 0
                stntoinsert: List[Tuple[int, int, EDDNStation]] = []
                infotoinsert: List[Tuple[
                    int, int, datetime, datetime, int, int,
                    int, int, float, int, int, int
                ]] = []
                timer.time('load')
                for lineno, line in enumerate(f):
                    process_line(
                        sysdb,
                        timer,
                        fileinfo,
                        reprocess,
                        rejectout,
                        stnlines,
                        infolines,
                        stntoinsert,
                        infotoinsert,
                        lineno,
                        line
                    )

                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        commit(
                            sysdb,
                            timer,
                            stntoinsert,
                            infotoinsert
                        )

                        stntoinsert = []
                        infotoinsert = []
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write(f'  {lineno + 1}\n')
                            sys.stderr.flush()

                commit(
                    sysdb,
                    timer,
                    stntoinsert,
                    infotoinsert
                )

                sys.stderr.write(f'  {linecount}\n')

                sysdb.updatefileinfo(
                    fileinfo.id,
                    linecount,
                    totalsize,
                    comprsize,
                    0,
                    linecount,
                    0
                )

        sysdb.commit()
        timer.time('commit')


def process_line(sysdb: EDDNSysDB,
                 timer: Timer,
                 fileinfo: EDDNFile,
                 reprocess: bool,
                 rejectout: Writable,
                 stnlines,
                 infolines,
                 stntoinsert: List[Tuple[int, int, EDDNStation]],
                 infotoinsert: List[Tuple[
                        int, int, datetime, datetime, int, int,
                        int, int, float, int, int, int
                    ]],
                 lineno: int,
                 line: bytes
                 ):
    if ((reprocess is True and (lineno + 1) not in stnlines)
            or (lineno + 1) not in infolines):
        timer.time('read')

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
        except (OverflowError,
                ValueError,
                TypeError,
                json.JSONDecodeError
                ):
            print('Error: {0}'.format(sys.exc_info()[1]))

            msg = {
                'rejectReason': 'Invalid',
                'exception': '{0}'.format(sys.exc_info()[1]),
                'rawmessage': line.decode('utf-8')
            }

            rejectout.write(json.dumps(msg) + '\n')
            timer.time('error')
        else:
            if (marketid is not None
                    and (marketid <= 0 or marketid > 1 << 32)):
                marketid = None
            sqltimestamp = timestamp_to_datetime(timestamp)
            sqlgwtimestamp = timestamp_to_datetime(gwtimestamp)
            timer.time('parse')

            if (sqltimestamp is not None
                    and sqlgwtimestamp is not None
                    and sqltimestamp < sqlgwtimestamp + timedelta(days=1)
                    and ((lineno + 1) not in stnlines
                         or (lineno + 1) not in infolines)):
                process_station(
                    sysdb,
                    timer,
                    fileinfo,
                    rejectout,
                    stnlines,
                    infolines,
                    stntoinsert,
                    infotoinsert,
                    lineno,
                    line,
                    msg,
                    body,
                    sysname,
                    stationname,
                    marketid,
                    software,
                    sqltimestamp,
                    sqlgwtimestamp
                )


def process_station(sysdb,
                    timer,
                    fileinfo,
                    rejectout,
                    stnlines,
                    infolines,
                    stntoinsert,
                    infotoinsert,
                    lineno,
                    line,
                    msg,
                    body,
                    sysname,
                    stationname,
                    marketid,
                    software,
                    sqltimestamp,
                    sqlgwtimestamp
                    ):
    (station, rejectReason, rejectData) = sysdb.getstation(
        timer,
        stationname,
        sysname,
        marketid,
        sqltimestamp,
        test=fileinfo.test
    )

    timer.time('stnquery')

    if station is not None:
        if (lineno + 1) not in stnlines:
            stntoinsert += [(
                fileinfo.id,
                lineno + 1,
                station
            )]

        if (lineno + 1) not in infolines:
            sysdb.insertsoftware(software)
            infotoinsert += [(
                fileinfo.id,
                lineno + 1,
                sqltimestamp,
                sqlgwtimestamp,
                sysdb.software[software],
                station.system_id,
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


def commit(sysdb: EDDNSysDB,
           timer: Timer,
           stntoinsert: List[Tuple[int, int, EDDNStation]],
           infotoinsert: List[Tuple[
                    int, int, datetime, datetime, int, int,
                    int, int, float, int, int, int
                ]]):
    sysdb.commit()
    if len(stntoinsert) != 0:
        sysdb.addfilelinestations(stntoinsert)
        timer.time('stninsert', len(stntoinsert))
    if len(infotoinsert) != 0:
        sysdb.addfilelineinfo(infotoinsert)
        timer.time('infoinsert', len(infotoinsert))
    sysdb.commit()
