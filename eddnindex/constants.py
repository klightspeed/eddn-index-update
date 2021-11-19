import re
from datetime import datetime

reverse_procgen_sysname_re = re.compile(
    '''
      ^
       ([0-9]+)(|-[0-9]+)([a-h])[ ]
       ([A-Z])-([A-Z])([A-Z])[ ]
       ((rotceS|noigeR)[ ][A-Za-z0-9.\' -]+
        |[1-6][a-z]+[A-Z]
        |ZCI
        |[a-z]+[A-Z](|[ ][a-z]+[A-Z])
       )$
    ''', re.VERBOSE)
procgen_sysname_re = re.compile('^([A-Za-z0-9.()\' -]+?) ([A-Z][A-Z]-[A-Z]) ([a-h])(?:([0-9]+)-|)([0-9]+)$')
procgen_body_name_re = re.compile(
    '''
      ^
      (?:|[ ](?P<stars>A?B?C?D?E?F?G?H?I?J?K?L?M?N?O?))
      (?:
       |[ ](?P<nebula>Nebula)
       |[ ](?P<belt>[A-Z])[ ]Belt(?:|[ ]Cluster[ ](?P<cluster>[1-9][0-9]?))
       |[ ]Comet[ ](?P<stellarcomet>[1-9][0-9]?)
       |[ ](?P<planet>[1-9][0-9]?(?:[+][1-9][0-9]?)*)
        (?:
         |[ ](?P<planetring>[A-Z])[ ]Ring
         |[ ]Comet[ ](?P<planetcomet>[1-9][0-9]?)
         |[ ](?P<moon1>[a-z](?:[+][a-z])*)
          (?:
           |[ ](?P<moon1ring>[A-Z])[ ]Ring
           |[ ]Comet[ ](?P<moon1comet>[1-9][0-9]?)
           |[ ](?P<moon2>[a-z](?:[+][a-z])*)
            (?:
             |[ ](?P<moon2ring>[A-Z])[ ]Ring
             |[ ]Comet[ ](?P<moon2comet>[1-9][0-9]?)
             |[ ](?P<moon3>[a-z])
            )
          )
        )
      )
      $
   ''', re.VERBOSE)
procgen_sys_body_name_re = re.compile(
    '''
      ^
      (?P<sysname>.+?)
      (?P<desig>|[ ](?P<stars>A?B?C?D?E?F?G?H?I?J?K?L?M?N?O?))
      (?:
       |[ ](?P<nebula>Nebula)
       |[ ](?P<belt>[A-Z])[ ]Belt(?:|[ ]Cluster[ ](?P<cluster>[1-9][0-9]?))
       |[ ]Comet[ ](?P<stellarcomet>[1-9][0-9]?)
       |[ ](?P<planet>[1-9][0-9]?(?:[+][1-9][0-9]?)*)
        (?:
         |[ ](?P<planetring>[A-Z])[ ]Ring
         |[ ]Comet[ ](?P<planetcomet>[1-9][0-9]?)
         |[ ](?P<moon1>[a-z](?:[+][a-z])*)
          (?:
           |[ ](?P<moon1ring>[A-Z])[ ]Ring
           |[ ]Comet[ ](?P<moon1comet>[1-9][0-9]?)
           |[ ](?P<moon2>[a-z](?:[+][a-z])*)
            (?:
             |[ ](?P<moon2ring>[A-Z])[ ]Ring
             |[ ]Comet[ ](?P<moon2comet>[1-9][0-9]?)
             |[ ](?P<moon3>[a-z])
            )
          )
        )
      )
      $
   ''', re.VERBOSE)


timestamp_re = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-5][0-9]:[0-5][0-9])')
carrier_name_re = re.compile('^[A-Z0-9]{3}-[A-Z0-9]{3}$')

timestamp_base_date = datetime.strptime('2014-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
megaship_week_0 = datetime.strptime('2016-10-20 07:00:00', '%Y-%m-%d %H:%M:%S')
ed_3_0_0_date = datetime.strptime('2018-02-27 15:00:00', '%Y-%m-%d %H:%M:%S')
ed_3_0_3_date = datetime.strptime('2018-03-19 10:00:00', '%Y-%m-%d %H:%M:%S')
ed_3_0_4_date = datetime.strptime('2018-03-27 16:00:00', '%Y-%m-%d %H:%M:%S')
ed_3_3_0_date = datetime.strptime('2018-12-11 16:00:00', '%Y-%m-%d %H:%M:%S')
ed_3_3_2_date = datetime.strptime('2019-01-17 10:00:00', '%Y-%m-%d %H:%M:%S')
ed_3_7_0_date = datetime.strptime('2020-06-09 10:00:00', '%Y-%m-%d %H:%M:%S')
ed_4_0_0_date = datetime.strptime('2021-05-19 10:00:00', '%Y-%m-%d %H:%M:%S')

EDSMStationTypes = {
    'Asteroid base': 'AsteroidBase',
    'Coriolis Starport': 'Coriolis',
    'Mega ship': 'MegaShip',
    'Ocellus Starport': 'Ocellus',
    'Orbis Starport': 'Orbis',
    'Outpost': 'Outpost',
    'Planetary Outpost': 'CraterOutpost',
    'Planetary Port': 'CraterPort',
    'Odyssey Settlement': 'OnFootSettlement',
}
