from threading import Thread
from contextlib import closing
from dataclasses import replace
from node_config import *
from pathlib import Path
from typing import Iterator, Tuple
import time
import subprocess
import select
import datetime
import yaml
import stat
import libtmux
import re

@dataclass(frozen=True)
class SeastarOpts:
    smp: int = 3
    max_io_requests: int = 4

@dataclass(frozen=True)
class ScyllaOpts:
    developer_mode: bool = False
    skip_gossip_wait: bool = False

@dataclass(frozen=True)
class RunOpts(SeastarOpts, ScyllaOpts):
    pass

@dataclass(frozen=True)
class LocalNodeEnv:
    cfg: NodeConfig
    opts: RunOpts

def mk_run_script(opts: RunOpts, scylla_path: str) -> str:
    return """#!/bin/bash
{path} \\
    --smp {smp} \\
    --max-io-requests {max_io_requests} \\
    --developer-mode={developer_mode} \\
    {skip_gossip_wait} \\
    2>&1 | tee scyllalog
""".format(
        path = scylla_path,
        smp = opts.smp,
        max_io_requests = opts.max_io_requests,
        developer_mode = opts.developer_mode,
        skip_gossip_wait = '--skip-wait-for-gossip-to-settle 0' if opts.skip_gossip_wait else '')

# IPs start from 127.0.0.{start}
def mk_dev_cluster_env(start: int, num_nodes: int) -> List[LocalNodeEnv]:
    assert start + num_nodes <= 256
    assert num_nodes > 0

    ips = [f'127.0.0.{i}' for i in range(start, num_nodes + start)]
    envs = [LocalNodeEnv(
                cfg = NodeConfig(ip_addr = i, seed_ip_addr = ips[0]),
                opts = RunOpts(developer_mode = True))
            for i in ips]

    # Optimization to start first node faster
    envs[0] = LocalNodeEnv(cfg = envs[0].cfg, opts = replace(envs[0].opts, skip_gossip_wait = True))

    return envs

# Returns an iterator to the file's lines.
# If not able to retrieve a next line for 1 second, yields ''.
# Remember to close it after usage, since it keeps the file opened.
# For example, use "with contextlib.closing(tail(...)) as t: ..."
def tail(path: str) -> Iterator[str]:
    with subprocess.Popen(['tail', '-F', path, '-n', '+1'], stdout=subprocess.PIPE) as f:
        out = f.stdout
        if out is None:
            return

        p = select.poll()
        p.register(out)
        while True:
            if p.poll(1000):
                yield out.readline().decode('utf-8')
            else:
                yield ''

def wait_for_init(scylla_log_lines: Iterator[str]) -> None:
    for l in scylla_log_lines:
        #print(l)
        ms = re.match(r".*Scylla.*initialization completed.*", l)
        if ms:
            return

def wait_for_init_path(scylla_log: Path) -> None:
    with closing(tail(str(scylla_log.resolve()))) as t:
        wait_for_init(t)

class TmuxNode:
    # name: str
    # path: Path
    # window: libtmux.Window

    # invariant: `window` has a single pane with initially bash running, with 'path' as cwd

    # Create a directory for the node with configuration and run script,
    # create a tmux window, but don't start the node yet
    def __init__(self, base_path: Path, env: LocalNodeEnv, sess: libtmux.Session):
        self.name = env.cfg.ip_addr
        self.path = base_path / self.name

        self.path.mkdir(parents=True)

        script_path = self.path / 'run.sh'
        with open(script_path, 'w') as f:
            f.write(mk_run_script(env.opts, scylla_path))
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)

        conf_path = self.path / 'conf'
        conf_path.mkdir(parents=True)
        with open(conf_path / 'scylla.yaml', 'w') as f:
            yaml.dump(mk_node_cfg(cfg_tmpl, env.cfg), f)

        self.window = sess.new_window(
            window_name = env.cfg.ip_addr, start_directory = self.path, attach = False)

    # Start node and wait for initialization.
    # Assumes that `start` wasn't called yet.
    def start(self) -> None:
        self.window.panes[0].send_keys('./run.sh')

        log_file = self.path / 'scyllalog'
        print('Waiting for node', self.name, 'to initialize...')
        while not log_file.is_file():
            time.sleep(1)
        wait_for_init_path(log_file)
        print('Node', self.name, 'initialized.')

if __name__ == "__main__":
    scylla_path = '/home/kbraun/dev/scylla/build/dev/scylla' # TODO
    cfg_tmpl: dict = load_cfg_template()

    run_id: str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    print('run ID:', run_id)

    runs_path = Path.cwd() / 'runs'
    run_path = runs_path / run_id
    run_path.mkdir(parents=True)

    latest_run_path = runs_path / 'latest'
    latest_run_path.unlink(missing_ok = True)
    latest_run_path.symlink_to(run_path, target_is_directory = True)

    serv = libtmux.Server()
    sess = serv.new_session(session_name = f'scylla-test-{run_id}')

    master_envs = mk_dev_cluster_env(start = 10, num_nodes = 3)
    master_nodes = [TmuxNode(run_path, e, sess) for e in master_envs]

    replica_envs = mk_dev_cluster_env(start = 13, num_nodes = 1)
    replica_nodes = [TmuxNode(run_path, e, sess) for e in replica_envs]

    # Unnecessary default-created window
    sess.windows[0].kill_window()

    print('tmux session name:', f'scylla-test-{run_id}')

    def start_cluster(nodes: List[TmuxNode]):
        for n in nodes:
            n.start()
    start_master = Thread(target=start_cluster, args=[master_nodes])
    start_replica = Thread(target=start_cluster, args=[replica_nodes])
    start_master.start()
    start_replica.start()
    start_master.join()
    start_replica.join()

    print('tmux session name:', f'scylla-test-{run_id}')

