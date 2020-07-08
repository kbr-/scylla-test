from threading import Thread
from contextlib import closing, ExitStack
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
import os
import signal
import argparse
import random

from cassandra.cluster import Cluster

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

def mk_run_script(opts: RunOpts, scylla_path: Path) -> str:
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

def log(*args):
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), flush=True, *args)

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

class TmuxNode:
    # name: str
    # path: Path
    # window: libtmux.Window
    # env: LocalNodeEnv

    # invariant: `window` has a single pane with initially bash running, with 'path' as cwd

    def ip(self) -> str:
        return self.env.cfg.ip_addr

    # Create a directory for the node with configuration and run script,
    # create a tmux window, but don't start the node yet
    def __init__(self, base_path: Path, env: LocalNodeEnv, sess: libtmux.Session, scylla_path: Path):
        self.name = env.cfg.ip_addr
        self.path = base_path / self.name
        self.env = env

        self.path.mkdir(parents=True)

        script_path = self.path / 'run.sh'
        with open(script_path, 'w') as f:
            f.write(mk_run_script(self.env.opts, scylla_path))
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)

        conf_path = self.path / 'conf'
        conf_path.mkdir(parents=True)
        with open(conf_path / 'scylla.yaml', 'w') as f:
            yaml.dump(mk_node_cfg(cfg_tmpl, self.env.cfg), f)

        self.window = sess.new_window(
            window_name = self.name, start_directory = self.path, attach = False)

    # Start node and wait for initialization.
    # Assumes that `start` wasn't called yet.
    def start(self) -> None:
        self.window.panes[0].send_keys('ulimit -Sn $(ulimit -Hn)')
        self.window.panes[0].send_keys('ulimit -Sn')
        self.window.panes[0].send_keys('./run.sh')

        log_file = self.path / 'scyllalog'
        log('Waiting for node', self.name, 'to initialize...')
        while not log_file.is_file():
            time.sleep(1)
        wait_for_init_path(log_file)
        log('Node', self.name, 'initialized.')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--scylla-path', type=Path, required=True)
    parser.add_argument('--replicator-path', type=Path, required=True)
    parser.add_argument('--migrate-path', type=Path, required=True)
    parser.add_argument('--gemini', default=False, action='store_true')
    parser.add_argument('--gemini-seed', type=int)
    args = parser.parse_args()

    scylla_path = args.scylla_path.resolve()
    use_gemini = args.gemini
    log('Scylla: {}\nuse_gemini: {}'.format(
        scylla_path, use_gemini))

    gemini_seed = args.gemini_seed
    if gemini_seed is None:
        gemini_seed = random.randint(1, 1000)
    if gemini_seed < 1:
        print('Wrong gemini seed. Use a positive number.')
        exit(1)

    if use_gemini:
        log(f'gemini seed: {gemini_seed}')

    cfg_tmpl: dict = load_cfg_template()

    run_id: str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log('run ID:', run_id)

    runs_path = Path.cwd() / 'runs'
    run_path = runs_path / run_id
    run_path.mkdir(parents=True)

    latest_run_path = runs_path / 'latest'
    latest_run_path.unlink(missing_ok = True)
    latest_run_path.symlink_to(run_path, target_is_directory = True)

    serv = libtmux.Server()
    tmux_sess = serv.new_session(session_name = f'scylla-test-{run_id}', start_directory = run_path)

    master_envs = mk_dev_cluster_env(start = 10, num_nodes = 2)
    master_nodes = [TmuxNode(run_path, e, tmux_sess, scylla_path) for e in master_envs]

    log('tmux session name:', f'scylla-test-{run_id}')

    def start_cluster(nodes: List[TmuxNode]):
        for n in nodes:
            n.start()
    start_master = Thread(target=start_cluster, args=[master_nodes[:-1]])
    start_master.start()
    start_master.join()

    MASTER_RF = 1
    #Hardcoded in gemini:
    KS_NAME = 'ks1'
    
    cm = Cluster([n.ip() for n in master_nodes[:-1]])

    w = tmux_sess.windows[0]
    w.panes[0].send_keys('tail -F stressor.log -n +1')

    with ExitStack() as stack:
        stressor_log = stack.enter_context(open(run_path / 'stressor.log', 'w'))

        log('Starting stressor')
        if use_gemini:
            stressor_proc = stack.enter_context(subprocess.Popen([
                'gemini',
                '-d', '--duration', '3m', '--warmup', '0', '-c', '1', '-m', 'write', '--non-interactive', '--cql-features', 'basic',
                '--max-mutation-retries', '100', '--max-mutation-retries-backoff', '100ms',
                '--replication-strategy', f"{{'class': 'SimpleStrategy', 'replication_factor': '{MASTER_RF}'}}",
                '--test-cluster={}'.format(master_nodes[0].ip()),
                '--seed', str(gemini_seed),
                '--verbose',
                '--level', 'info'
            ], stdout=stressor_log, stderr=subprocess.STDOUT))
        else:
            stressor_proc = stack.enter_context(subprocess.Popen([
                'cassandra-stress',
                "user no-warmup profile=profile.yaml ops(update=1) cl=QUORUM duration=3m",
                "-port jmx=6868", "-mode cql3", "native", "-rate threads=1", "-log level=verbose interval=5", "-errors retries=999 ignore",
                "-node {}".format(master_nodes[0].ip())],
                stdout=stressor_log, stderr=subprocess.STDOUT))

        log('Letting stressor run for a while...')
        # Sleep long enough for stressor to create the keyspace
        time.sleep(10)

        log('Fetching schema definitions from master cluster.')
        cm.control_connection_timeout = 20
        with cm.connect() as _:
            cm.refresh_schema_metadata(max_schema_agreement_wait=10)
            ks = cm.metadata.keyspaces[KS_NAME]
            ut_ddls = [t[1].as_cql_query() for t in ks.user_types.items()]
            table_ddls = []
            for name, table in ks.tables.items():
                table_ddls.append(table.as_cql_query())

        log('User types:\n{}'.format('\n'.join(ut_ddls)))
        log('Table definitions:\n{}'.format('\n'.join(table_ddls)))

        log('Letting stressor run for a while...')
        time.sleep(15)

        log('Bootstrapping new node')
        master_nodes[-1].start()

        log('Waiting for stressor to finish...')
        stressor_proc.wait()
        log('Gemini return code:', stressor_proc.returncode)

    log('tmux session name:', f'scylla-test-{run_id}')
