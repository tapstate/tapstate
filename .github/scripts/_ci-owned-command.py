#!/usr/bin/env python3
"""Hold a worker's session identity until its parent releases or stops the group."""
import os
import select
import signal
import subprocess
import sys
import time


def main():
    assert os.getpgrp() == os.getpid() and os.getsid(0) == os.getpid(), 'owned session identity required'
    status_fd, release_fd = int(sys.argv[1]), int(sys.argv[2])
    assert sys.argv[3] == '--' and len(sys.argv) > 4
    # Caught handlers reset to defaults when the actual command execs. The leader
    # stays alive through group interrupts so its identity cannot be reused.
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, lambda *_: None)
    def abandoned():
        os.killpg(os.getpgrp(), signal.SIGINT)
        time.sleep(7.5)
        os.killpg(os.getpgrp(), signal.SIGTERM)
        time.sleep(2.5)
        os.killpg(os.getpgrp(), signal.SIGKILL)
    command = subprocess.Popen(sys.argv[4:])
    while command.poll() is None:
        ready, _, _ = select.select([release_fd], [], [], 0.2)
        if ready and os.read(release_fd, 1) == b'':
            # Parent disappeared while work was active. Stop this session only.
            abandoned()
    try:
        os.write(status_fd, (str(command.returncode) + '\n').encode())
    except BrokenPipeError:
        abandoned()
    # Even after the command exits, descendants can remain. Keep the group ID
    # reserved while the parent checks obsolescence/interruption and cleans up.
    if os.read(release_fd, 1) != b'R':
        abandoned()
    try:
        os.write(status_fd, b'READY\n')
    except BrokenPipeError:
        abandoned()
    os.close(status_fd)
    # The parent acknowledges status, then stops this still-reserved group.
    # Never exit between that acknowledgement and descendant cleanup.
    os.read(release_fd, 1)
    abandoned()
    return 1


if __name__ == '__main__':
    sys.exit(main())
