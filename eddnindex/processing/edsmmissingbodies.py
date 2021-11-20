import os
import os.path
import sys
import json
from datetime import datetime
from typing import Callable

from ..config import Config
from .. import constants
from ..eddnsysdb import EDDNSysDB
from ..util import timestamptosql
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            timer: Timer,
            updatetitleprogress: Callable[[str], None],
            config: Config
            ):
    sys.stderr.write('Processing EDSM missing bodies\n')
    w = 0
    wg = 0
    w2 = 0
    from timeit import default_timer
    tstart = default_timer()

    fn = 'fetchbodies-{0}.jsonl'.format(datetime.utcnow().isoformat())
    fileid = sysdb.insertedsmfile(fn)

    timer.time('bodyquery')

    with open(os.path.join(config.edsm_bodies_dir, fn), 'w', encoding='utf-8') as f:
        linecount = 0
        totalsize = 0
        updatecache = False

        for i in range(148480000 - 256000, len(sysdb.edsmbodyids) - 2097152):
            row = sysdb.edsmbodyids[i]

            if row[1] == 0:
                sys.stderr.write('{0:10d}'.format(i) + '\b' * 10)
                sys.stderr.flush()

                bodies = sysdb.getbodiesfromedsmbyid(i, timer)

                if len(bodies) == 0:
                    sysdb.updateedsmbodyid(0, i, constants.timestamp_base_date)
                else:
                    bodiestoinsert = []
                    for msg in bodies:
                        line = json.dumps(msg)
                        f.write(line + '\n')
                        f.flush()
                        linecount += 1
                        totalsize += len(line) + 1
                        bodiestoinsert += [(fileid, linecount + 1, msg['id'])]
                        wg += 1
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
                        sqltimestamp = timestamptosql(timestamp)
                        sqlts = int((sqltimestamp - constants.timestamp_base_date).total_seconds())
                        timer.time('parse')
                        (sysid, _, _, _) = sysdb.findedsmsysid(edsmsysid)
                        (sysbodyid, ts, rec) = sysdb.findedsmbodyid(edsmbodyid)
                        scanbodyid = -1

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

                                (scanbody, rejectReason, rejectData) = sysdb.getbody(
                                    timer,
                                    bodyname,
                                    sysname,
                                    bodyid,
                                    system,
                                    body,
                                    sqltimestamp
                                )

                                if scanbody:
                                    scanbodyid = scanbody.id

                                timer.time('bodyquery')

                        sysdb.updateedsmbodyid(scanbodyid, edsmbodyid, sqltimestamp)

                    sysdb.addedsmfilelinebodies(bodiestoinsert)
                    timer.time('bodyinsert', len(bodiestoinsert))
                    sysdb.updateedsmfileinfo(fileid, linecount, totalsize, totalsize)

                w += 1
                w2 += 1

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write(('.' if w == 0 else (':' if wg == 0 else '#')) + (' ' * 10) + ('\b' * 10))
                sys.stderr.flush()

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                    updatetitleprogress('EDSMBodyM:{0}'.format(i + 1))

                if w2 >= 10:
                    sysdb.saveedsmbodycache()
                    w2 = 0

                timer.time('commit')

                w = 0
                wg = 0

                # if default_timer() - tstart > 18 * 60 * 60:
                #     break

        sys.stderr.write('  {0}\n'.format(i + 1))
        sys.stderr.flush()
        sysdb.commit()
        sysdb.saveedsmbodycache()
        timer.time('commit')


