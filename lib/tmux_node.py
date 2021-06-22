from pathlib import Path
from typing import Final
import libtmux # type: ignore
import time
import os
import signal
import logging
from dataclasses import replace

from lib.common import wait_for_init_path, is_running, write_executable_script
from lib.node_config import RunOpts, ClusterConfig, NodeConfig
from lib.local_node import LocalNodeEnv, LocalNode
from lib.node import Node

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

def mk_hard_kill_script() -> str:
    return """#!/bin/bash
kill -9 $(cat scylla.pid)
"""

class TmuxNode(Node):
    # invariant: `self.window: Final[libtmux.Window]` has a single pane with initially bash running, with 'path' as cwd

    # Create a directory for the node with configuration and run script,
    # create a tmux window, but don't start the node yet
    def __init__(self, logger: logging.Logger, cfg_tmpl: dict, base_path: Path, env: LocalNodeEnv, sess: libtmux.Session, scylla_path: Path):
        self.__node: Final[LocalNode] = LocalNode(cfg_tmpl, base_path, env.cfg)
        self.__logger: Final[logging.Logger] = logger
        self.__opts: RunOpts = env.opts

        self.__write_run_script(scylla_path)
        self.__write_kill_script()
        self.__write_hard_kill_script()

        self.window: Final[libtmux.Window] = sess.new_window(
            window_name = self.__node.name, start_directory = self.__node.path, attach = False)

        self.window.panes[0].send_keys('ulimit -Sn $(ulimit -Hn)')
        self.window.panes[0].send_keys('ulimit -Sn')

    # Start node and wait for initialization.
    # Assumes that the node is not running.
    def start(self) -> None:
        self.window.panes[0].send_keys('./run.sh')
        log_file = self.__node.path / 'scyllalog'
        self.__log(f'Waiting for node {self.__node.name} to start...')
        while not log_file.is_file():
            time.sleep(1)
        wait_for_init_path(log_file)
        self.__log(f'Node {self.__node.name} started.')

        with open(self.__node.path / 'scylla.pid') as pidfile:
            self.pid = int(pidfile.read())

    def stop(self) -> None:
        self.__log(f'Killing node {self.__node.name} with SIGTERM...')
        os.kill(self.pid, signal.SIGTERM)
        while is_running(self.pid):
            time.sleep(1)

    def restart(self) -> None:
        self.stop()
        self.start()

    def hard_stop(self) -> None:
        self.__log(f'Killing node {self.__node.name} with SIGKILL...')
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

    def ip(self) -> str:
        return self.__node.ip()

    def get_node_config(self) -> NodeConfig:
        return self.__node.get_node_config()

    def reset_node_config(self, cfg: NodeConfig) -> None:
        self.__node.reset_node_config(cfg)

    def reset_scylla_binary(self, binary_path: Path) -> None:
        self.__write_run_script(binary_path)

    # Precondition: self.path directory exists
    def __write_run_script(self, scylla_path: Path) -> None:
        write_executable_script(
            path = self.__node.path / 'run.sh',
            body = mk_run_script(self.__opts, scylla_path)
        )

    # Precondition: self.path directory exists
    def __write_kill_script(self) -> None:
        write_executable_script(
            path = self.__node.path / 'kill.sh',
            body = mk_kill_script()
        )

    # Precondition: self.path directory exists
    def __write_hard_kill_script(self) -> None:
        write_executable_script(
            path = self.__node.path / 'hard-kill.sh',
            body = mk_hard_kill_script()
        )

    def __log(self, *args, **kwargs) -> None:
        self.__logger.info(*args, **kwargs)
