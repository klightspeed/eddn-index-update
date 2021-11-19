from functools import lru_cache
import os.path
import json

class EDDNRejectData(object):
    def __init__(self, rejectdir):
        self.rejectdir = rejectdir

    @lru_cache(maxsize=256)
    def open(self, filename: str):
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return open(filename, 'at', encoding='utf-8')

    def write(self, jsonstr: str):
        j = json.loads(jsonstr)
        rejectfile = self.rejectdir

        if 'rejectReason' in j and j['rejectReason'] is not None:
            reason = j['rejectReason']
            if reason.startswith('Unable to resolve system '):
                reason = 'Unable to resolve system'

            rejectfile += '/' + reason
        else:
            rejectfile += '/None'

        if 'header' in j and 'gatewayTimestamp' in j['header']:
            date = j['header']['gatewayTimestamp'][:10]
            rejectfile += '/' + date

        rejectfile += '.jsonl'

        outfile = self.open(rejectfile)
        outfile.write(jsonstr)
        outfile.flush()

