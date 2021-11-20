from typing import List, Tuple, Union

try:
    from edts.edtslib.system import from_name
    from edts.edtslib.id64data import get_id64
except ImportError:
    def from_name(sysname: str,
                  allow_known: bool,
                  allow_id64data: bool
                  ) -> None:
        return None

    def get_id64(sysname: str,
                 starpos: Tuple[float, float, float]
                 ) -> Union[int, None]:
        return None


def find_edts_system_id64(sysname: str,
                          sysaddr: int,
                          starpos: Union[Tuple[float, float, float],
                                         List[float],
                                         None
                                         ]
                          ) -> Union[int, None]:
    edtssys = from_name(sysname, allow_known=False, allow_id64data=False)

    if edtssys is not None:
        edtsid64 = edtssys.id64
    else:
        edtsid64 = get_id64(sysname, starpos)

    if sysaddr is None and edtsid64 is not None:
        return edtsid64
    elif sysaddr is not None and (edtsid64 is None or edtsid64 == sysaddr):
        return sysaddr
    else:
        return None
