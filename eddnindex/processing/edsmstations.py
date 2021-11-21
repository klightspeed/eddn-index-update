import sys
import json
import gzip
from typing import Callable, Any
from collections.abc import MutableMapping as Dict

from ..types import EDSMStation, Writable
from .. import constants
from ..eddnsysdb import EDDNSysDB
from ..util import timestamp_to_datetime
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            timer: Timer,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            edsm_stations_file: str
            ):
    sys.stderr.write('Processing EDSM stations\n')
    with gzip.open(edsm_stations_file, 'r') as f:
        stations = json.load(f)
        w = 0
        for i, msg in enumerate(stations):
            process_station(
                sysdb,
                timer,
                rejectout,
                msg
            )

            if ((i + 1) % 1000) == 0:
                sysdb.commit()
                sys.stderr.write('.' if w == 0 else '*')
                sys.stderr.flush()
                w = 0

                if ((i + 1) % 64000) == 0:
                    sys.stderr.write('  {0}\n'.format(i + 1))
                    sys.stderr.flush()
                timer.time('commit')

    sys.stderr.write('  {0}\n'.format(i + 1))
    sys.stderr.flush()
    sysdb.commit()
    timer.time('commit')


def process_station(sysdb: EDDNSysDB,
                    timer: Timer,
                    rejectout: Writable,
                    msg: EDSMStation
                    ):
    timer.time('read')

    rejectmsg: Dict[str, Any]

    try:
        edsmstationid = msg['id']
        marketid = msg['marketId']
        stationname = msg['name']
        stntype = msg['type']
        stntype = constants.EDSMStationTypes.get(stntype) or stntype
        edsmsysid = msg['systemId']
        sysname = msg['systemName']
        timestamp = msg['updateTime']['information'].replace(' ', 'T')
    except (OverflowError,
            ValueError,
            TypeError,
            json.JSONDecodeError
            ):
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
        sqltimestamp = timestamp_to_datetime(timestamp)
        timer.time('parse')
        (sysid, ts, hascoord, rec) = sysdb.findedsmsysid(edsmsysid)
        timer.time('sysquery')
        reject = True
        reject_reason = None
        reject_data = None

        if sysid:
            system = sysdb.getsystembyid(sysid)
            timer.time('sysquery')

            if system is not None:
                if stationname is not None and stationname != '':
                    (station, reject_reason, reject_data) = sysdb.getstation(
                        timer,
                        stationname,
                        sysname,
                        marketid,
                        sqltimestamp,
                        system,
                        stntype
                    )

                    timer.time('stnquery')
                    if station is not None:
                        sysdb.updateedsmstationid(
                            edsmstationid,
                            station.id,
                            sqltimestamp
                        )
                        reject = False
                else:
                    reject_reason = 'No station name'
            else:
                reject_reason = 'System not found'

        if reject:
            rejectmsg = {
                'rejectReason': reject_reason,
                'rejectData': reject_data,
                'data': msg
            }
            rejectout.write(json.dumps(rejectmsg) + '\n')
