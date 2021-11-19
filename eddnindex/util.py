from typing import Union
from datetime import datetime

def timestamptosql(timestamp: Union[str, None]):
    if timestamp is None:
        return None
    else:
        if timestamp[-1] == 'Z':
            timestamp = timestamp[:-1]
        if len(timestamp) == 26 and timestamp[19] == '.':
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
        else:
            return datetime.strptime(timestamp[:19], '%Y-%m-%dT%H:%M:%S')

