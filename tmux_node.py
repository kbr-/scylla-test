from pathlib import Path
import stat
import libtmux
import time
import os
import signal
from dataclasses import replace

from node_config import *
from common import *

@dataclass(frozen=True)
class SeastarOpts:
    smp: int = 3
    max_io_requests: int = 4
    overprovisioned: bool = False

@dataclass(frozen=True)
class ScyllaOpts:
    developer_mode: bool = False
    skip_gossip_wait: bool = False
    stall_notify_ms: Optional[int] = None

@dataclass(frozen=True)
class RunOpts(SeastarOpts, ScyllaOpts):
    pass

@dataclass(frozen=True)
class LocalNodeEnv:
    cfg: NodeConfig
    opts: RunOpts

def mk_run_script(opts: RunOpts, scylla_path: Path) -> str:
    return """#!/bin/bash
set -m
{path} \\
    --smp {smp} \\
    --max-io-requests {max_io_requests} \\
    --developer-mode={developer_mode} \\
    {overprovisioned} \\
    {skip_gossip_wait} \\
    {stall_notify_ms} \\
    2>&1 | tee scyllalog &
echo $! > scylla.pid
fg
""".format(
        path = scylla_path,
        smp = opts.smp,
        max_io_requests = opts.max_io_requests,
        developer_mode = opts.developer_mode,
        skip_gossip_wait = '--skip-wait-for-gossip-to-settle 0' if opts.skip_gossip_wait else '',
        overprovisioned = '--overprovisioned' if opts.overprovisioned else '',
        stall_notify_ms = '--blocked-reactor-notify-ms {}'.format(opts.stall_notify_ms) if opts.stall_notify_ms else '')

# IPs start from 127.0.0.{start}
def mk_dev_cluster_env(start: int, num_nodes: int, smp: int = 3, overprovisioned: bool = False,
        stall_notify_ms: Optional[int] = None) -> List[LocalNodeEnv]:
    assert start + num_nodes <= 256
    assert num_nodes > 0

    ips = [f'127.0.0.{i}' for i in range(start, num_nodes + start)]
    envs = [LocalNodeEnv(
                cfg = NodeConfig(ip_addr = i, seed_ip_addr = ips[0]),
                opts = RunOpts(developer_mode = True, smp = smp, overprovisioned = overprovisioned,
                    stall_notify_ms = stall_notify_ms))
            for i in ips]

    # Optimization to start first node faster
    envs[0] = LocalNodeEnv(cfg = envs[0].cfg, opts = replace(envs[0].opts, skip_gossip_wait = True))

    return envs

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
    def __init__(self, cfg_tmpl: dict, base_path: Path, env: LocalNodeEnv, sess: libtmux.Session, scylla_path: Path):
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
        self.running = False

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

        self.running = True
        with open(self.path / 'scylla.pid') as pidfile:
            self.pid = int(pidfile.read())

    def pause(self) -> None:
        os.kill(self.pid, signal.SIGSTOP)

    def unpause(self) -> None:
        os.kill(self.pid, signal.SIGCONT)
