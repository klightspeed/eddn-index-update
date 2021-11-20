import sys
import json
import bz2
import math
import csv
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
    sys.stderr.write('Processing EDDB systems\n')
    with bz2.open(config.eddb_systems_file, 'rt', encoding='utf8') as f:
        csvreader = csv.DictReader(f)
        w = 0
        for i, msg in enumerate(csvreader):
            timer.time('read')
            try:
                eddbsysid = int(msg['id'])
                sysname = msg['name']
                starpos = [float(msg['x']), float(msg['y']), float(msg['z'])]
                timestamp = int(msg['updated_at'])
            except (OverflowError, ValueError, TypeError):
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
                    starpos = [math.floor(v * 32 + 0.5) / 32.0 for v in starpos]
                    (system, rejectReason, rejectData) = sysdb.getsystem(
                        timer,
                        sysname,
                        starpos[0],
                        starpos[1],
                        starpos[2],
                        None
                    )

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
