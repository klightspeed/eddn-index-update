import os
import os.path
import sys
import json
import bz2
import math
from datetime import datetime, timedelta
from typing import Any, Callable, List, Tuple

from ..config import Config
from ..types import EDDNFaction, EDDNFile, EDDNStation, Writable
from .. import constants
from ..constants import ed_3_0_3_date, ed_3_0_4_date
from ..eddnsysdb import EDDNSysDB
from ..util import timestamp_to_datetime
from ..timer import Timer


def process(sysdb: EDDNSysDB,
            timer: Timer,
            filename: str,
            fileinfo: EDDNFile,
            reprocess: bool,
            reprocessall: bool,
            rejectout: Writable,
            updatetitleprogress: Callable[[str], None],
            config: Config
            ):
    # if fileinfo.eventtype in ('Location'):
    #     continue

    line_count = fileinfo.line_count
    populated_line_count = fileinfo.populated_line_count
    station_line_count = fileinfo.station_line_count
    event_type = fileinfo.event_type
    date = fileinfo.date
    info_file_line_count = fileinfo.info_file_line_count
    station_file_line_count = fileinfo.station_file_line_count
    faction_file_line_count = fileinfo.faction_file_line_count
    date_str = fileinfo.date.isoformat()[:10]

    if (line_count is None
            or populated_line_count is None
            or (station_line_count is None
                and event_type in ('Docked', 'Location', 'CarrierJump'))
            or (reprocessall is True and event_type == 'Scan'
                and date >= constants.ed_3_0_0_date.date())
            or (reprocess is True
                and (line_count != info_file_line_count
                     or (event_type in ('Docked', 'Location', 'CarrierJump')
                         and station_file_line_count != station_line_count)
                     or populated_line_count != faction_file_line_count))):
        fn = os.path.join(
            config.eddn_dir,
            fileinfo.date.isoformat()[:7],
            filename
        )

        if os.path.exists(fn):
            sys.stderr.write(f'{fn}\n')
            updatetitleprogress(f'{date_str}:{event_type}')
            statinfo = os.stat(fn)
            comprsize = statinfo.st_size

            with bz2.BZ2File(fn, 'r') as f:
                stnlines = sysdb.getstationfilelines(fileinfo.id)
                infolines = sysdb.getinfofilelines(fileinfo.id)
                factionlines = sysdb.getfactionfilelines(fileinfo.id)
                linecount = 0
                poplinecount = 0
                stnlinecount = 0
                totalsize = 0
                timer.time('load')
                stntoinsert: List[Tuple[int, int, EDDNStation]] = []
                infotoinsert: List[Tuple[
                    int, int, datetime, datetime, int, int,
                    int, int, float, int, int, int
                ]] = []
                factionstoinsert: List[Tuple[int, int, EDDNFaction, int]] = []
                for lineno, line in enumerate(f):
                    process_line(
                        sysdb,
                        timer,
                        fileinfo,
                        reprocessall,
                        rejectout,
                        config,
                        stnlines,
                        infolines,
                        factionlines,
                        poplinecount,
                        stnlinecount,
                        stntoinsert,
                        infotoinsert,
                        factionstoinsert,
                        lineno,
                        line,
                        event_type
                    )

                    linecount += 1
                    totalsize += len(line)

                    if (linecount % 1000) == 0:
                        commit(
                            sysdb,
                            timer,
                            stntoinsert,
                            infotoinsert,
                            factionstoinsert
                        )

                        stntoinsert = []
                        infotoinsert = []
                        factionstoinsert = []
                        sys.stderr.write('.')
                        sys.stderr.flush()

                        if (linecount % 64000) == 0:
                            sys.stderr.write(f'  {lineno + 1}\n')
                            sys.stderr.flush()

                commit(
                    sysdb,
                    timer,
                    stntoinsert,
                    infotoinsert,
                    factionstoinsert
                )

                sys.stderr.write(f'  {linecount}\n')
                sys.stderr.flush()

                sysdb.updatefileinfo(
                    fileinfo.id,
                    linecount,
                    totalsize,
                    comprsize,
                    poplinecount,
                    stnlinecount,
                    0
                )


def commit(sysdb: EDDNSysDB,
           timer: Timer,
           stntoinsert: List[Tuple[int, int, EDDNStation]],
           infotoinsert: List[Tuple[
                    int, int, datetime, datetime, int, int,
                    int, int, float, int, int, int
                ]],
           factionstoinsert: List[Tuple[int, int, EDDNFaction, int]]
           ):
    sysdb.commit()
    if len(stntoinsert) != 0:
        sysdb.addfilelinestations(stntoinsert)
        timer.time('stninsert', len(stntoinsert))
    if len(infotoinsert) != 0:
        sysdb.addfilelineinfo(infotoinsert)
        timer.time('infoinsert', len(infotoinsert))
    if len(factionstoinsert) != 0:
        sysdb.addfilelinefactions(factionstoinsert)
        timer.time('factioninsert', len(factionstoinsert))
    sysdb.commit()


def process_line(sysdb: EDDNSysDB,
                 timer: Timer,
                 fileinfo: EDDNFile,
                 reprocessall: bool,
                 rejectout: Writable,
                 config: Config,
                 stnlines,
                 infolines,
                 factionlines,
                 poplinecount: int,
                 stnlinecount: int,
                 stntoinsert: List[Tuple[int, int, EDDNStation]],
                 infotoinsert: List[Tuple[
                        int, int, datetime, datetime, int, int,
                        int, int, float, int, int, int
                    ]],
                 factionstoinsert: List[Tuple[int, int, EDDNFaction, int]],
                 lineno: int,
                 line: bytes,
                 event_type: str
                 ):
    if ((lineno + 1) not in infolines
            or (reprocessall is True and event_type == 'Scan')):
        timer.time('read')
        msg = None
        try:
            msg = json.loads(line)
            body = msg['message']
            hdr = msg['header']
            eventtype = body.get('event')

            if 'StarSystem' in body:
                sysname = body['StarSystem']
            elif 'System' in body:
                sysname = body['System']
            elif 'SystemName' in body:
                sysname = body['SystemName']
            else:
                sysname = body['StarSystem']

            starpos = body['StarPos']
            sysaddr = body.get('SystemAddress')
            stationname = body.get('StationName')
            marketid = body.get('MarketID')
            stationtype = body.get('StationType')
            bodyname = body.get('Body')
            bodyid = body.get('BodyID')
            bodytype = body.get('BodyType')
            scanbodyname = body.get('BodyName')
            parents = body.get('Parents')
            factions = body.get('Factions')
            sysfaction = (body.get('SystemFaction')
                          or body.get('Faction'))
            sysgovern = (body.get('SystemGovernment')
                         or body.get('Government'))
            sysalleg = (body.get('SystemAllegiance')
                        or body.get('Allegiance')
                        or '')
            stnfaction = body.get('StationFaction')
            stngovern = body.get('StationGovernment')
            timestamp = body.get('timestamp')
            gwtimestamp = hdr.get('gatewayTimestamp')
            software = hdr.get('softwareName')
            distfromstar = body.get('DistanceFromArrivalLS')
        except (OverflowError, ValueError, TypeError, json.JSONDecodeError):
            sys.stderr.write('Error: {0}\n'.format(sys.exc_info()[1]))
            msg = {
                'rejectReason': 'Invalid',
                'exception': '{0}'.format(sys.exc_info()[1]),
                'rawmessage': line.decode('utf-8')
            }
            rejectout.write(json.dumps(msg) + '\n')
            timer.time('error')
        else:
            if marketid is not None and (marketid <= 0 or marketid > 1 << 32):
                marketid = None
            sqltimestamp = timestamp_to_datetime(timestamp)
            sqlgwtimestamp = timestamp_to_datetime(gwtimestamp)
            timer.time('parse')
            linelen = len(line)

            if (factions is not None
                    or sysfaction is not None
                    or stnfaction is not None):
                poplinecount += 1

            if (stationname is not None
                    or marketid is not None):
                stnlinecount += 1

            if (sqltimestamp is not None
                    and sqlgwtimestamp is not None
                    and sqltimestamp < sqlgwtimestamp + timedelta(days=1)):
                process_event(
                    sysdb, timer, fileinfo, reprocessall, rejectout,
                    config, stnlines, infolines, factionlines,
                    stntoinsert, infotoinsert, factionstoinsert,
                    lineno, msg, body, eventtype, sysname, starpos,
                    sysaddr, stationname, marketid, stationtype,
                    bodyname, bodyid, bodytype, scanbodyname,
                    parents, factions, sysfaction, sysgovern,
                    sysalleg, stnfaction, stngovern, software,
                    distfromstar, sqltimestamp, sqlgwtimestamp,
                    linelen
                )
            else:
                msg['rejectReason'] = 'Timestamp error'
                rejectout.write(json.dumps(msg) + '\n')


def process_event(sysdb: EDDNSysDB,
                  timer: Timer,
                  fileinfo: EDDNFile,
                  reprocessall: bool,
                  rejectout: Writable,
                  config: Config,
                  stnlines,
                  infolines,
                  factionlines,
                  stntoinsert: List[Tuple[int, int, EDDNStation]],
                  infotoinsert: List[Tuple[
                        int, int, datetime, datetime, int, int,
                        int, int, float, int, int, int
                    ]],
                  factionstoinsert: List[Tuple[int, int, EDDNFaction, int]],
                  lineno, msg, body, eventtype, sysname, starpos,
                  sysaddr, stationname, marketid, stationtype,
                  bodyname, bodyid, bodytype, scanbodyname, parents,
                  factions, sysfaction, sysgovern, sysalleg,
                  stnfaction, stngovern, software, distfromstar,
                  sqltimestamp, sqlgwtimestamp, linelen
                  ):
    starpos = [math.floor(v * 32 + 0.5) / 32.0 for v in starpos]
    reject_data: Any

    (system, reject_reason, reject_data) = sysdb.getsystem(
        timer,
        sysname,
        starpos[0],
        starpos[1],
        starpos[2],
        sysaddr
    )

    timer.time('sysquery')
    if system is not None:
        systemid = system.id
        if ((lineno + 1) not in stnlines
                and sqltimestamp is not None
                and not (ed_3_0_3_date <= sqltimestamp < ed_3_0_4_date
                         and not config.allow_3_0_3_bodies)):
            if stationname is not None and stationname != '':
                (station, reject_reason, reject_data) = sysdb.getstation(
                    timer,
                    stationname,
                    sysname,
                    marketid,
                    sqltimestamp,
                    system,
                    stationtype,
                    bodyname,
                    bodyid,
                    bodytype,
                    eventtype,
                    fileinfo.test
                )

                timer.time('stnquery')

                if station is not None:
                    stntoinsert += [(fileinfo.id, lineno + 1, station)]
                else:
                    reject = True
            elif (bodyname is not None and bodytype in ['Station']):
                (station, reject_reason, reject_data) = sysdb.getstation(
                    timer,
                    bodyname,
                    sysname,
                    None,
                    sqltimestamp,
                    system=system,
                    bodyid=bodyid,
                    eventtype=eventtype,
                    test=fileinfo.test
                )

                timer.time('stnquery')

                if station is not None:
                    stntoinsert += [(fileinfo.id, lineno + 1, station)]
                else:
                    reject = True

        if ((lineno + 1) not in infolines
                and sqltimestamp is not None
                and not (ed_3_0_3_date <= sqltimestamp < ed_3_0_4_date
                         and not config.allow_3_0_3_bodies)):
            if scanbodyname is not None:
                (scanbody, reject_reason, reject_data) = sysdb.getbody(
                    timer,
                    scanbodyname,
                    sysname,
                    bodyid,
                    system,
                    body,
                    sqltimestamp
                )

                if scanbody is not None:
                    sysdb.insertbodyparents(
                        timer,
                        scanbody.id,
                        system,
                        bodyid,
                        parents
                    )

                    sysbodyid = scanbody.id
                else:
                    reject = True
                timer.time('bodyquery')
            elif bodyname is not None and bodytype not in ['Station']:
                (scanbody, reject_reason, reject_data) = sysdb.getbody(
                    timer,
                    bodyname,
                    sysname,
                    bodyid,
                    system,
                    {},
                    sqltimestamp
                )

                if scanbody is not None:
                    sysbodyid = scanbody.id
                else:
                    reject = True
                timer.time('bodyquery')
        elif ((reprocessall is True or (lineno + 1) not in infolines)
                and sqltimestamp is not None
                and not (ed_3_0_3_date <= sqltimestamp < ed_3_0_4_date
                         and not config.allow_3_0_3_bodies)
              ):
            if scanbodyname is not None:
                (scanbody, reject_reason, reject_data) = sysdb.getbody(
                    timer,
                    scanbodyname,
                    sysname,
                    bodyid,
                    system,
                    body,
                    sqltimestamp
                )

                if scanbody is not None:
                    sysbodyid = scanbody.id
                    sysdb.insertbodyparents(
                        timer,
                        scanbody.id,
                        system,
                        bodyid,
                        parents
                    )
                else:
                    reject = True

                timer.time('bodyquery')

            elif bodyname is not None and bodytype not in ['Station']:
                (scanbody, reject_reason, reject_data) = sysdb.getbody(
                    timer,
                    bodyname,
                    sysname,
                    bodyid,
                    system,
                    {},
                    sqltimestamp
                )

                if scanbody is not None:
                    sysbodyid = scanbody.id
                else:
                    reject = True

                timer.time('bodyquery')

        if (lineno + 1) not in factionlines and not reject:
            linefactions = []
            linefactiondata = []
            if factions is not None:
                for n, faction in enumerate(factions):
                    linefactiondata += [{
                        'Name': faction['Name'],
                        'Government': faction['Government'],
                        'Allegiance': faction.get('Allegiance'),
                        'EntryNum': n
                    }]

                    linefactions += [(
                        n,
                        sysdb.getfaction(
                            timer,
                            faction['Name'],
                            faction['Government'],
                            faction.get('Allegiance'))
                        )
                    ]

            if sysfaction is not None:
                if type(sysfaction) is dict and 'Name' in sysfaction:
                    sysfaction = sysfaction['Name']
                linefactiondata += [{
                    'Name': sysfaction,
                    'Government': sysgovern,
                    'Allegiance': sysalleg,
                    'EntryNum': -1
                }]

                linefactions += [(
                    -1,
                    sysdb.getfaction(
                        timer,
                        sysfaction,
                        sysgovern,
                        sysalleg
                    )
                )]

            if stnfaction is not None:
                if type(stnfaction) is dict and 'Name' in stnfaction:
                    stnfaction = stnfaction['Name']
                if stnfaction != 'FleetCarrier':
                    linefactiondata += [{
                        'Name': stnfaction,
                        'Government': stngovern,
                        'EntryNum': -2
                    }]

                    linefactions += [(
                        -2,
                        sysdb.getfaction(
                            timer,
                            stnfaction,
                            stngovern,
                            None
                        )
                    )]

            if len(linefactions) != 0:
                if len([fid for n, fid in linefactions if fid is None]) != 0:
                    reject = True
                    reject_reason = 'Faction not found'
                    reject_data = linefactiondata
                else:
                    for n, faction in linefactions:
                        factionstoinsert += [(
                            fileinfo.id,
                            lineno + 1,
                            faction,
                            n
                        )]

            timer.time('factionupdate')

        if reject:
            msg['rejectReason'] = reject_reason
            msg['rejectData'] = reject_data
            rejectout.write(json.dumps(msg) + '\n')
        else:
            if (lineno + 1) not in infolines:
                sysdb.insertsoftware(software)
                infotoinsert.append((
                    fileinfo.id,
                    lineno + 1,
                    sqltimestamp,
                    sqlgwtimestamp,
                    sysdb.software[software],
                    systemid,
                    sysbodyid,
                    linelen,
                    distfromstar,
                    1 if 'BodyID' in body else 0,
                    1 if 'SystemAddress' in body else 0,
                    1 if 'MarketID' in body else 0
                ))

    else:
        msg['rejectReason'] = reject_reason
        msg['rejectData'] = reject_data
        rejectout.write(json.dumps(msg) + '\n')
