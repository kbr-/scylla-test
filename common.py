from typing import Iterator, Tuple, Optional
from pathlib import Path
from contextlib import closing
import datetime
import subprocess
import re

def log(*args):
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), *args, flush=True)

# Returns an iterator to the file's lines.
# If not able to retrieve a next line for 1 second, yields ''.
# Remember to close it after usage, since it keeps the file opened.
# For example, use "with contextlib.closing(tail(...)) as t: ..."
def tail(path: str) -> Iterator[str]:
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

def wait_for_init(scylla_log_lines: Iterator[str]) -> None:
    for l in scylla_log_lines:
        #log(l)
        ms = re.match(r".*Scylla.*initialization completed.*", l)
        if ms:
            return

def wait_for_init_path(scylla_log: Path) -> None:
    with closing(tail(str(scylla_log.resolve()))) as t:
        wait_for_init(t)
