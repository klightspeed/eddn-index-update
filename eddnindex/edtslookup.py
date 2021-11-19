import edts.edtslib.system as edtslib_system
import edts.edtslib.id64data as edtslib_id64data
from typing import Tuple, Union

def find_edts_system_id64(sysname: str, sysaddr: int, starpos: Tuple[float, float, float]) -> Union[int, None]:
    edtsid64 = None
    edtssys = edtslib_system.from_name(sysname, allow_known = False, allow_id64data = False)

    if edtssys is not None:
        edtsid64 = edtssys.id64
    else:
        edtsid64 = edtslib_id64data.get_id64(sysname, starpos)

    if sysaddr is None and edtsid64 is not None:
        return edtsid64
    elif sysaddr is not None and (edtsid64 is None or edtsid64 == sysaddr):
        return sysaddr
    else:
        return None