import os
import os.path
import sys
import json
import bz2
from typing import Callable

from ..config import Config
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
            config: Config
            ):
    fn = None

    if fileinfo.date is not None:
        fn = os.path.join(config.edsm_bodies_dir, fileinfo.date.isoformat()[:7], filename)

    dumpfile = os.path.join(config.edsm_dump_dir, filename)
    if os.path.exists(dumpfile):
        fn = dumpfile

    if fn is not None and os.path.exists(fn):
        statinfo = os.stat(fn)
        comprsize = statinfo.st_size

        if ((fileinfo.date is None and comprsize != fileinfo.compressed_size)
                or fileinfo.line_count is None
                or (reprocess is True and fileinfo.line_count != fileinfo.body_line_count)):

            sys.stderr.write('Processing EDSM bodies file {0} ({1} / {2})\n'.format(
                filename,
                fileinfo.body_line_count,
                fileinfo.line_count
            ))

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
                        except (OverflowError, ValueError, TypeError, json.JSONDecodeError):
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
                            sqltimestamp = timestamp_to_datetime(timestamp)
                            sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
                            timer.time('parse')
                            reject = True
                            reject_reason = None
                            reject_data = None
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
                                        sysdb.updateedsmbodyid(scanbody.id, edsmbodyid, sqltimestamp)
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
                            updatetitleprogress('{0}:{1}'.format(filename, linecount))

                            if updatecache:
                                sysdb.saveedsmbodycache()
                                updatecache = False

            if len(bodiestoinsert) != 0:
                sysdb.addedsmfilelinebodies(bodiestoinsert)
                timer.time('bodyinsert', len(bodiestoinsert))
                bodiestoinsert = []

            sys.stderr.write('  {0}\n'.format(linecount))
            sys.stderr.flush()
            updatetitleprogress('{0}:{1}'.format(filename, linecount))
            sysdb.commit()
            sysdb.saveedsmbodycache()
            timer.time('commit')
            sysdb.updateedsmfileinfo(fileinfo.id, linecount, totalsize, comprsize)
