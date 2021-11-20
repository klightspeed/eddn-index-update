import sys
import json
import bz2
from typing import Callable

from ..config import Config
from ..types import Writable
from .. import constants
from ..eddnsysdb import EDDNSysDB
from ..util import timestamp_to_datetime
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            timer: Timer,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            config: Config
            ):
    sys.stderr.write('Processing pre-purge EDSM systems without coords\n')
    with bz2.BZ2File(config.edsm_systems_without_coords_pre_purge_file, 'r') as f:
        w = 0
        for i, line in enumerate(f):
            timer.time('read')
            try:
                msg = json.loads(line)
                edsmsysid = msg['id']
                sysaddr = msg['id64']
                sysname = msg['name']
                timestamp = msg['date'].replace(' ', 'T')
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
