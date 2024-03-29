from typing import Generator, Tuple, Optional
from pathlib import Path
from contextlib import closing
import stat
import datetime
import subprocess
import re
import errno
import os

# Returns an iterator to the file's lines.
# If not able to retrieve a next line for 1 second, yields ''.
# Remember to close it after usage, since it keeps the file opened.
# For example, use "with contextlib.closing(tail(...)) as t: ..."
def tail(path: str) -> Generator[str, None, None]:
    with subprocess.Popen(['tail', '-F', path, '-n', '+1'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as f:
        out = f.stdout
        if out is None:
            return

        #p = select.poll()
        #p.register(out)
        #while True:
        #    if p.poll(1000):
        #        yield out.readline().decode('utf-8')
        #    else:
        #        yield ''
        while True:
            yield out.readline().decode('utf-8')

def wait_for_init(scylla_log_lines: Generator[str, None, None]) -> None:
    for l in scylla_log_lines:
        ms = re.match(r".*Scylla.*initialization completed.*", l)
        if ms:
            return

def wait_for_init_path(scylla_log: Path) -> None:
    with closing(tail(str(scylla_log.resolve()))) as t:
        wait_for_init(t)

def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
    return True

def write_executable_script(path: Path, body: str) -> None:
    with open(path, 'w') as f:
        f.write(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
