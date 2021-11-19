from setproctitle import getproctitle, setproctitle

proctitleprogresspos = None

def updatetitleprogress(progress):
    global proctitleprogresspos

    title = getproctitle()

    if proctitleprogresspos is None:
        proctitleprogresspos = title.find('--processtitleprogress')

    if proctitleprogresspos > 0:
        title = title[0:proctitleprogresspos] + '[{0:20.20s}]'.format(progress) + title[proctitleprogresspos + 22:]
        setproctitle(title)
