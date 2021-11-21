import sys
from typing import Callable, Optional
from collections.abc import MutableSequence as List
import urllib.request
import urllib.error
import json
import time
import math

from ..config import Config
from ..types import Writable
from ..eddnsysdb import EDDNSysDB
from ..timer import Timer
from ..util import timestamp_to_datetime


def updatesystemfromedsmbyid(sysdb: EDDNSysDB,
                             edsmid: int,
                             timer: Timer,
                             rejectout: Writable
                             ) -> bool:
    url = ('https://www.edsm.net/api-v1/system?'
           f'systemId={edsmid}'
           '&coords=1'
           '&showId=1'
           '&submitted=1'
           '&includeHidden=1')

    starpos: Optional[List[float]]

    try:
        while True:
            try:
                with urllib.request.urlopen(url) as f:
                    msg = json.load(f)
            except urllib.error.URLError:
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
                starpos = [coords['x'], coords['y'], coords['z']]
            else:
                starpos = None
        else:
            timer.time('edsmhttp')
            return False
    except (OverflowError, ValueError, TypeError, json.JSONDecodeError):
        (exctype, _, traceback) = sys.exc_info()
        sys.stderr.write('Error: {0}\n'.format(exctype))
        import pdb
        pdb.post_mortem(traceback)
        timer.time('error')
        return True
    else:
        timer.time('edsmhttp')
        sqltimestamp = timestamp_to_datetime(timestamp)
        (_, _, _, rec) = sysdb.findedsmsysid(edsmsysid)
        timer.time('sysquery')
        if starpos is not None:
            starpos = [math.floor(v * 32 + 0.5) / 32.0 for v in starpos]
            (system, rejectReason, rejectData) = sysdb.getsystem(
                timer,
                sysname,
                starpos[0],
                starpos[1],
                starpos[2],
                sysaddr
            )
        else:
            (system, rejectReason, rejectData) = sysdb.getsystem(
                timer,
                sysname,
                None,
                None,
                None,
                sysaddr
            )

        timer.time('sysquery', 0)

        if system is not None:
            rec = sysdb.updateedsmsysid(
                edsmsysid,
                system.id,
                sqltimestamp,
                starpos is not None,
                False,
                False
            )

        else:
            rejectmsg = {
                'rejectReason': rejectReason,
                'rejectData': rejectData,
                'data': msg
            }
            rejectout.write(json.dumps(rejectmsg) + '\n')

        timer.time('edsmupdate')

        if rec is not None:
            rec[6] = 7

        return True


def process(sysdb: EDDNSysDB,
            timer: Timer,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            _: Config
            ):
    sys.stderr.write('Processing EDSM deleted systems\n')
    w = 0
    w2 = 0
    i = 0
    from timeit import default_timer
    tstart = default_timer()

    # for row in sysdb.edsmsysids:
    #     row[5] = 0
    #
    # sysdb.saveedsmsyscache()

    for i, row in enumerate(sysdb.edsmsysids):
        if row[1] == i and row[6] <= 0 and row[5] == 0:
            sys.stderr.write('{0:10d}'.format(row[1]) + '\b' * 10)
            sys.stderr.flush()
            rec = row

            if not updatesystemfromedsmbyid(sysdb, row[1], timer, rejectout):
                rec = sysdb.updateedsmsysid(
                    row[1],
                    row[0],
                    row[2],
                    False,
                    False,
                    True
                )

            rec.processed = 7

            w += 1
            w2 += 1

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
