from setproctitle import getproctitle, setproctitle

process_title_progress_pos = None


def update_title_progress(progress):
    global process_title_progress_pos

    title = getproctitle()

    if process_title_progress_pos is None:
        process_title_progress_pos = title.find('--processtitleprogress')

    if process_title_progress_pos > 0:
        title = (title[0:process_title_progress_pos] +
                 '[{0:20.20s}]'.format(progress) +
                 title[process_title_progress_pos + 22:])
        setproctitle(title)
