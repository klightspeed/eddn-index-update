from typing import Union
from datetime import datetime


def timestamp_to_datetime(timestamp: Union[str, None]):
    if timestamp is None:
        return None
    else:
        if timestamp[-1] == 'Z':
            timestamp = timestamp[:-1]
        if len(timestamp) == 26 and timestamp[19] == '.':
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
        else:
            return datetime.strptime(timestamp[:19], '%Y-%m-%dT%H:%M:%S')


def id64_to_modsysaddr(sysaddr: int) -> int:
    sz = sysaddr & 7
    sx = 7 - sz
    z0 = (sysaddr >> 3) & (0x3FFF >> sz)
    y0 = (sysaddr >> (10 + sx)) & (0x1FFF >> sz)
    x0 = (sysaddr >> (16 + sx * 2)) & (0x3FFF >> sz)
    seq = (sysaddr >> (23 + sx * 3)) & 0xFFFF
    sb = 0x7F >> sz
    x1 = x0 & sb
    x2 = x0 >> sx
    y1 = y0 & sb
    y2 = y0 >> sx
    z1 = z0 & sb
    z2 = z0 >> sx
    return ((z2 << 53)
            | (y2 << 47)
            | (x2 << 40)
            | (sz << 37)
            | (z1 << 30)
            | (y1 << 23)
            | (x1 << 16)
            | seq)


def modsysaddr_to_id64(modsysaddr: int) -> int:
    z2 = (modsysaddr >> 53) & 0x7F
    y2 = (modsysaddr >> 47) & 0x3F
    x2 = (modsysaddr >> 40) & 0x7F
    sz = (modsysaddr >> 37) & 7
    z1 = (modsysaddr >> 30) & 0x7F
    y1 = (modsysaddr >> 23) & 0x7F
    x1 = (modsysaddr >> 16) & 0x7F
    seq = modsysaddr & 0xFFFF
    sx = 7 - sz
    x0 = x1 + (x2 << sx)
    y0 = y1 + (y2 << sx)
    z0 = z1 + (z2 << sx)
    return (sz
            | (z0 << 3)
            | (y0 << (10 + sx))
            | (x0 << (16 + sx * 2))
            | (seq << (23 + sx * 3)))


def from_db_string(name: Union[str, bytes]):
    if type(name) is str:
        return name
    else:
        return name.decode('utf-8')
