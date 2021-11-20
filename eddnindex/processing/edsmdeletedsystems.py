import sys
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
    sys.stderr.write('Processing EDSM deleted systems\n')
    w = 0
    w2 = 0
    i = 0
    from timeit import default_timer
    tstart = default_timer()

    '''
    for row in sysdb.edsmsysids:
        row[5] = 0
    
    sysdb.saveedsmsyscache()
    '''

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
                import pdb
                pdb.set_trace()

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
