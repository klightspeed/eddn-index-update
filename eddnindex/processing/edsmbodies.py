import os
import os.path
import sys
import json
import bz2
from typing import Any, Callable, Tuple
from collections.abc import MutableSequence as List

from ..types import EDSMFile, Writable
from .. import constants
from ..eddnsysdb import EDDNSysDB
from ..util import timestamp_to_datetime
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            filename: str,
            fileinfo: EDSMFile,
            reprocess: bool,
            timer: Timer,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            edsm_bodies_dir: str,
            edsm_dump_dir: str
            ):
    fn = None

    date = fileinfo.date
    compressed_size = fileinfo.compressed_size
    line_count = fileinfo.line_count
    body_line_count = fileinfo.body_line_count

    if date is not None:
        fn = os.path.join(
            edsm_bodies_dir,
            date.isoformat()[:7],
            filename
        )

    dumpfile = os.path.join(edsm_dump_dir, filename)
    if os.path.exists(dumpfile):
        fn = dumpfile

    if fn is not None and os.path.exists(fn):
        statinfo = os.stat(fn)
        comprsize = statinfo.st_size

        if ((date is None and comprsize != compressed_size)
                or line_count is None
                or (reprocess is True and line_count != body_line_count)):

            sys.stderr.write(
                'Processing EDSM bodies file '
                f'{filename} ({body_line_count} / {line_count})\n'
            )

            with bz2.BZ2File(fn, 'r') as f:
                lines = sysdb.getedsmbodyfilelines(fileinfo.id)
                linecount = 0
                totalsize = 0
                bodiestoinsert: List[Tuple[int, int, int]] = []
                timer.time('load')
                updatecache = False

                for lineno, line in enumerate(f):
                    updatecache |= process_line(
                        sysdb,
                        fileinfo,
                        timer,
                        rejectout,
                        lines,
                        bodiestoinsert,
                        lineno,
                        line
                    )

                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        commit(sysdb, timer, bodiestoinsert)

                        bodiestoinsert = []
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write(f'  {linecount}\n')
                            sys.stderr.flush()
                            updatetitleprogress(f'{filename}:{linecount}')

                            if updatecache:
                                sysdb.saveedsmbodycache()
                                updatecache = False

            commit(sysdb, timer, bodiestoinsert)

            sys.stderr.write(f'  {linecount}\n')
            sys.stderr.flush()
            updatetitleprogress(f'{filename}:{linecount}')

            sysdb.saveedsmbodycache()
            timer.time('commit')
            sysdb.updateedsmfileinfo(
                fileinfo.id,
                linecount,
                totalsize,
                comprsize
            )


def commit(sysdb: EDDNSysDB,
           timer: Timer,
           bodiestoinsert: List[Tuple[int, int, int]]
           ):
    sysdb.commit()
    if len(bodiestoinsert) != 0:
        sysdb.addedsmfilelinebodies(bodiestoinsert)
        timer.time('bodyinsert', len(bodiestoinsert))
    sysdb.commit()


def process_line(sysdb: EDDNSysDB,
                 fileinfo: EDSMFile,
                 timer: Timer,
                 rejectout: Writable,
                 lines,
                 bodiestoinsert: List[Tuple[int, int, int]],
                 lineno: int,
                 line: bytes
                 ):
    updatecache = False

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
        except (OverflowError,
                ValueError,
                TypeError,
                json.JSONDecodeError
                ):
            sys.stderr.write(
                f'Error: {sys.exc_info()[0]}\n'
            )

            rejectmsg = {
                'rejectReason': 'Invalid',
                'exception': f'{sys.exc_info()[1]}',
                'line': line.decode(
                    'utf-8',
                    'backslashreplace'
                )
            }

            rejectout.write(json.dumps(rejectmsg) + '\n')
            timer.time('error')
        else:
            sqltimestamp = timestamp_to_datetime(timestamp)
            tsdelta = sqltimestamp - constants.timestamp_base_date
            sqlts = int(tsdelta.total_seconds())
            timer.time('parse')
            reject = True
            reject_reason = None
            reject_data: Any = None
            (sysid, _, _, _) = sysdb.findedsmsysid(edsmsysid)
            (_, ts, _) = sysdb.findedsmbodyid(edsmbodyid)

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

                    (scanbody, reject_reason, reject_data) = sysdb.getbody(
                        timer,
                        bodyname,
                        sysname,
                        bodyid,
                        system,
                        body,
                        sqltimestamp
                    )

                    if scanbody:
                        sysdb.updateedsmbodyid(
                            scanbody.id,
                            edsmbodyid,
                            sqltimestamp
                        )

                        reject = False
                        updatecache = True

                    timer.time('bodyquery')

            if reject and reject_reason is not None:
                rejectmsg = {
                    'rejectReason': reject_reason,
                    'rejectData': reject_data,
                    'data': msg
                }
                rejectout.write(json.dumps(rejectmsg) + '\n')
    return updatecache
