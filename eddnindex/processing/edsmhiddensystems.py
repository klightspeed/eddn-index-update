import sys
import json
import bz2
from typing import Callable

from ..config import Config
from ..types import Writable
from ..eddnsysdb import EDDNSysDB
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            timer: Timer,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            config: Config
            ):
    sys.stderr.write('Processing EDSM hidden systems\n')
    with bz2.BZ2File(config.edsm_hidden_systems_file, 'r') as f:
        w = 0
        for i, line in enumerate(f):
            timer.time('read')
            try:
                msg = json.loads(line)
                edsmsysid = msg['id']
                sysname = msg['system']
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
