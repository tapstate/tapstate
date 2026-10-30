#!/usr/bin/env python3
"""Bound an owned provider command's inherited output, including descendants."""
import os
from pathlib import Path
import resource
import selectors
import subprocess
import sys

LOG_LIMIT_EXIT = 122


def main():
    if len(sys.argv) < 7 or sys.argv[5] != '--':
        return 125
    stdout, stderr = map(Path, sys.argv[1:3])
    log_limit, file_limit = map(int, sys.argv[3:5])
    if not 0 < log_limit <= file_limit <= 64 * 1024 * 1024:
        return 125
    if any(not path.is_file() or path.is_symlink() for path in (stdout, stderr)):
        return 125
    # The hard limit survives caller/collector loss and is inherited by all children.
    resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))
    process, streams = None, []
    selector = selectors.DefaultSelector()
    try:
        process = subprocess.Popen(sys.argv[6:], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=dict(os.environ))
        for incoming, path in ((process.stdout, stdout), (process.stderr, stderr)):
            outgoing = path.open('wb', buffering=0)
            streams.append(outgoing)
            selector.register(incoming, selectors.EVENT_READ, [outgoing, 0])
        # Wait for inherited pipes too: an exit-zero direct child is not EOF while
        # its descendants can still write. Only this collector owns log file FDs.
        while selector.get_map():
            for key, unused in selector.select(timeout=0.2):
                raw = os.read(key.fileobj.fileno(), 65536)
                if not raw:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                outgoing, size = key.data
                allowed = min(len(raw), max(0, log_limit - size))
                if allowed:
                    outgoing.write(raw[:allowed])
                    key.data[1] += allowed
                if allowed != len(raw):
                    return LOG_LIMIT_EXIT
        code = process.wait()
        return code if code >= 0 else 128 - code
    finally:
        selector.close()
        for stream in streams:
            stream.close()
        if process is not None:
            for stream in (process.stdout, process.stderr):
                stream.close()
            if process.poll() is None:
                process.kill()
                process.wait(timeout=2.5)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (KeyboardInterrupt, OSError, ValueError, subprocess.TimeoutExpired):
        sys.exit(125)
