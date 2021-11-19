from timeit import default_timer as timer
import sys
from typing import Dict


class Timer(object):
    tstart: float
    timers: Dict[str, float]
    counts: Dict[str, int]

    def __init__(self):
        self.tstart = timer()
        self.timers = {}
        self.counts = {}

    def time(self, name: str, count: int = 1):
        tend = timer()

        if name not in self.timers:
            self.timers[name] = 0
            self.counts[name] = 0

        self.timers[name] += tend - self.tstart
        self.counts[name] += count
        self.tstart = tend

    def printstats(self):
        sys.stderr.write('\nTimes taken:\n')
        for name, time in sorted(self.timers.items()):
            count = self.counts[name]
            sys.stderr.write(
                '  {0}: {1}s / {2} ({3}ms/iteration)\n'.format(
                    name, time, count, time * 1000 / (count or 1)
                )
            )
