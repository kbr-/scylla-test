from pathlib import Path
import libtmux
import time
import os
import signal
import logging
from dataclasses import replace

from lib.node_config import *
from lib.common import *

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
class ClusterConfig:
    ring_delay_ms: int
    hinted_handoff_enabled: bool
    enable_rbo: bool
    first_node_skip_gossip_settle: bool

@dataclass(frozen=True)
class LocalNodeEnv:
    cfg: NodeConfig
    opts: RunOpts

def mk_run_script(opts: RunOpts, scylla_path: Path) -> str:
    return """#!/bin/bash
set -m
({path} \\
    --smp {smp} \\
    --max-io-requests {max_io_requests} \\
    --developer-mode={developer_mode} \\
    {overprovisioned} \\
    {skip_gossip_wait} \\
    {stall_notify_ms} \\
    2>&1 & echo $! >&3) 3>scylla.pid | tee scyllalog &
""".format(
        path = scylla_path,
        smp = opts.smp,
        max_io_requests = opts.max_io_requests,
        developer_mode = opts.developer_mode,
        skip_gossip_wait = '--skip-wait-for-gossip-to-settle 0' if opts.skip_gossip_wait else '',
        overprovisioned = '--overprovisioned' if opts.overprovisioned else '',
        stall_notify_ms = '--blocked-reactor-notify-ms {}'.format(opts.stall_notify_ms) if opts.stall_notify_ms else '')

def mk_kill_script() -> str:
    return """#!/bin/bash
kill $(cat scylla.pid)
"""

# IPs start from 127.0.0.{start}
def mk_cluster_env(start: int, num_nodes: int, opts: RunOpts, cluster_cfg: ClusterConfig) -> List[LocalNodeEnv]:
    assert start + num_nodes <= 256
    assert num_nodes > 0

    ips = [f'127.0.0.{i}' for i in range(start, num_nodes + start)]
    envs = [LocalNodeEnv(
                cfg = NodeConfig(
                    ip_addr = i,
                    seed_ip_addr = ips[0],
                    ring_delay_ms = cluster_cfg.ring_delay_ms,
                    hinted_handoff_enabled = cluster_cfg.hinted_handoff_enabled,
                    enable_rbo = cluster_cfg.enable_rbo),
                opts = opts)
            for i in ips]

    # Optimization to start first node faster
    if cluster_cfg.first_node_skip_gossip_settle:
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
    def __init__(self, logger: logging.Logger, cfg_tmpl: dict, base_path: Path, env: LocalNodeEnv, sess: libtmux.Session, scylla_path: Path):
        self.logger = logger
        self.name = env.cfg.ip_addr
        self.path = base_path / self.name
        self.env = env
        self.no_kills = 0

        self.path.mkdir(parents=True)
        self.__write_run_script(scylla_path)
        self.__write_kill_script()

        kill_script_path = self.path

        conf_path = self.path / 'conf'
        conf_path.mkdir(parents=True)
        with open(conf_path / 'scylla.yaml', 'w') as f:
            yaml.dump(mk_node_cfg(cfg_tmpl, self.env.cfg), f)

        self.window = sess.new_window(
            window_name = self.name, start_directory = self.path, attach = False)

        self.window.panes[0].send_keys('ulimit -Sn $(ulimit -Hn)')
        self.window.panes[0].send_keys('ulimit -Sn')

    # Start node and wait for initialization.
    # Assumes that the node is not running.
    def start(self) -> None:
        self.window.panes[0].send_keys('./run.sh')
        log_file = self.path / 'scyllalog'
        self.__log(f'Waiting for node {self.name} to start...')
        while not log_file.is_file():
            time.sleep(1)
        wait_for_init_path(log_file)
        self.__log(f'Node {self.name} started.')

        with open(self.path / 'scylla.pid') as pidfile:
            self.pid = int(pidfile.read())

    def stop(self) -> None:
        self.__log(f'Killing node {self.name} with SIGTERM...')
        os.kill(self.pid, signal.SIGTERM)
        while is_running(self.pid):
            time.sleep(1)

    def restart(self) -> None:
        self.stop()
        self.start()

    def hard_stop(self) -> None:
        self.__log(f'Killing node {self.name} with SIGKILL...')
        os.kill(self.pid, signal.SIGKILL)
        while is_running(self.pid):
            time.sleep(1)

    def hard_restart(self) -> None:
        self.hard_stop()
        self.start()

    def pause(self) -> None:
        os.kill(self.pid, signal.SIGSTOP)

    def unpause(self) -> None:
        os.kill(self.pid, signal.SIGCONT)

    def reset_scylla_path(self, scylla_path: Path) -> None:
        self.__write_run_script(scylla_path)

    # Precondition: self.path directory exists
    def __write_run_script(self, scylla_path: Path) -> None:
        write_executable_script(
            path = self.path / 'run.sh',
            body = mk_run_script(self.env.opts, scylla_path)
        )

    # Precondition: self.path directory exists
    def __write_kill_script(self) -> None:
        write_executable_script(
            path = self.path / 'kill.sh',
            body = mk_kill_script()
        )

    def __log(self, *args, **kwargs) -> None:
        self.logger.info(*args, **kwargs)
