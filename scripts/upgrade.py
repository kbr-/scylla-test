from pathlib import Path
from typing import Optional, List, Dict, Sequence
from threading import Thread
from dataclasses import dataclass, field, replace
import argparse
import itertools
import operator
import libtmux # type: ignore
import os
import sys
import random
import logging

from lib.node_config import RunOpts, ClusterConfig
from lib.local_node import mk_cluster_env
from lib.tmux_node import TmuxNode
from lib.node import Node

def create_cluster(
        logger: logging.Logger,
        run_path: Path, sess: libtmux.Session, scylla_path: Path,
        ip_start: int, num_nodes: int, opts: RunOpts, cluster_cfg: ClusterConfig) -> Sequence[Node]:
    envs = mk_cluster_env(ip_start, num_nodes, opts, cluster_cfg)
    nodes = [TmuxNode(logger, run_path, e, sess, scylla_path) for e in envs]
    return nodes

@dataclass(frozen=True)
class TestConfig:
    sess: libtmux.Session
    scylla_path_1: Path
    scylla_path_2: Path
    run_path: Path
    num_nodes: int
    num_shards: int
    overprovisioned: bool
    stall_notify_ms: int
    ring_delay_ms: int
    enable_rbo: bool
    interactive: bool
    first_node_skip_gossip_settle: bool
    experimental_1: List[str]
    experimental_2: List[str]

def upgrade_test(cfg: TestConfig) -> None:
    cfg.run_path.mkdir(parents=True)
    logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(message)s",
        handlers = [
            logging.FileHandler(cfg.run_path / 'run.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger()
    logger.info(
f"""current session: {cfg.sess}
Scylla binary before upgrade: {cfg.scylla_path_1}
Scylla binary after upgrade: {cfg.scylla_path_2}
run path: {cfg.run_path}
number of nodes: {cfg.num_nodes}
number of shards: {cfg.num_shards}
overprovisioned: {cfg.overprovisioned}
ring_delay_ms: {cfg.ring_delay_ms}
enable_rbo: {cfg.enable_rbo}
stall_notify_ms: {cfg.stall_notify_ms}
""")

    opts = replace(RunOpts(),
            developer_mode = True,
            smp = cfg.num_shards,
            overprovisioned = cfg.overprovisioned,
            stall_notify_ms = cfg.stall_notify_ms)

    cluster_cfg = ClusterConfig(
        ring_delay_ms = cfg.ring_delay_ms,
        hinted_handoff_enabled = False,
        enable_rbo = cfg.enable_rbo,
        first_node_skip_gossip_settle = cfg.first_node_skip_gossip_settle,
        experimental = cfg.experimental_1
    )

    ip_start = 1
    logger.info('Creating cluster...')
    c = create_cluster(logger, cfg.run_path, cfg.sess, cfg.scylla_path_1, ip_start, cfg.num_nodes, opts, cluster_cfg)

    if cfg.interactive:
        input('Press Enter to boot the cluster')

    logger.info('Waiting for the cluster to boot...')
    for n in c:
        n.start()

    node_map: Dict[int, str] = {i: c[i].ip() for i in range(len(c))}
    logger.info(f'Node map: {node_map}')

    ord: Optional[List[int]] = None
    if cfg.interactive:
        inp = input('Provide rolling upgrade order as a space-separated list of integers (keys in the node map)'
                    ' or leave input empty and press Enter for random order:\n')
        while True:
            if inp:
                try:
                    ord = list(map(int, inp.split()))
                except Exception as e:
                    inp = input(f'Non-empty input provided, but could not parse as list of ints: "{e}". Try again:\n')
                else:
                    if set(ord) - set(node_map.keys()):
                        inp = input('Provided list contains extra keys. Try again:\n')
                    elif set(node_map.keys()) - set(ord):
                        inp = input('Provided list does not contain all keys. Try again:\n')
                    else:
                        break
            else:
                ord = None
                break

    if not ord:
        ord = list(node_map.keys())
        random.shuffle(ord)

    logger.info(f'Rolling upgrade order: {[c[i].ip() for i in ord]}')

    assert ord and set(ord) == set(node_map.keys())

    for i in ord:
        n = c[i]

        if cfg.interactive:
            inp = input(f'Press Enter to upgrade node {n.ip()}.')

        logger.info(f'Stopping node {n.ip()}...')
        n.stop()

        logger.info(f'Resetting Scylla binary path for node {n.ip()} to {cfg.scylla_path_2}.')
        n.reset_scylla_binary(cfg.scylla_path_2)

        if cfg.experimental_2 != cfg.experimental_1:
            logger.info(f'Resetting experimental setting from {cfg.experimental_1} to {cfg.experimental_2}')
            n.reset_node_config(replace(n.get_node_config(), experimental = cfg.experimental_2))

        logger.info(f'Restarting node {n.ip()}...')
        n.start()

        logger.info(f'Node {n.ip()} upgraded.')

    logger.info(f'Upgrade finished.')
