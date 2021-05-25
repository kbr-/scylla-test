from typing import Optional
from threading import Thread
import argparse
import itertools
import operator
import libtmux
import os
import sys

from lib.tmux_node import *
from lib.tmux import *

def create_cluster(
        logger: logging.Logger,
        cfg_tmpl: dict, run_path: Path, sess: libtmux.Session, scylla_path: Path,
        ip_start: int, num_nodes: int, opts: RunOpts, cluster_cfg: ClusterConfig) -> List[TmuxNode]:
    envs = mk_cluster_env(ip_start, num_nodes, opts, cluster_cfg)
    nodes = [TmuxNode(logger, cfg_tmpl, run_path, e, sess, scylla_path) for e in envs]
    return nodes

def boot(nodes: List[TmuxNode]) -> Thread:
    def start():
        for n in nodes:
            n.start()
    start_thread = Thread(target=start)
    start_thread.start()
    return start_thread

@dataclass(frozen=True)
class TestConfig:
    sess: libtmux.Session
    scylla_path: Path
    run_path: Path
    num_nodes: List[int] = field(default_factory=lambda:[3])
    num_shards: int = 3
    overprovisioned: bool = True
    stall_notify_ms: Optional[int] = 10
    ring_delay_ms: int = 4000
    enable_rbo: bool = False
    first_node_skip_gossip_settle: bool = True
    experimental: List[str] = field(default_factory=list)
    start_clusters: bool = True

def boot_clusters(cfg: TestConfig):
    if any(n <= 0 for n in cfg.num_nodes):
        print('Cluster sizes must be positive')
        exit(1)
    if cfg.num_shards <= 0:
        print('Number of shards must be positive')
        exit(1)
    if cfg.stall_notify_ms and cfg.stall_notify_ms <= 0:
        print('stall-notify-ms must be positive')
        exit(1)
    if cfg.ring_delay_ms < 1:
        print('ring-delay-ms must be positive')
        exit(1)

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

    logger.info("Current session: {}".format(cfg.sess))
    logger.info('Scylla: {}\nRun path: {}\nNum nodes: {}\nNum shards: {}\nOverprovisioned: {}{}\nStart clusters: {}'.format(
        cfg.scylla_path, cfg.run_path, cfg.num_nodes, cfg.num_shards, cfg.overprovisioned,
        '\nstall_notify_ms: {}'.format(cfg.stall_notify_ms) if cfg.stall_notify_ms else '',
        cfg.start_clusters))

    opts = replace(RunOpts(),
            developer_mode = True,
            smp = cfg.num_shards,
            overprovisioned = cfg.overprovisioned,
            stall_notify_ms = cfg.stall_notify_ms)

    cluster_cfg = ClusterConfig(
        ring_delay_ms = cfg.ring_delay_ms,
        hinted_handoff_enabled = False,
        enable_rbo = cfg.enable_rbo,
        first_node_skip_gossip_settle = cfg.first_node_skip_gossip_settle
    )

    cfg_tmpl: dict = load_cfg_template()

    ip_starts = itertools.accumulate([1] + cfg.num_nodes, operator.add)
    logger.info('Creating {} clusters...'.format(len(cfg.num_nodes)))
    cs = [create_cluster(logger, cfg_tmpl, cfg.run_path, cfg.sess, cfg.scylla_path, ip_start, num, opts, cluster_cfg)
            for ip_start, num in zip(ip_starts, cfg.num_nodes)]

    if cfg.start_clusters:
        ts = [boot(c) for c in cs]
        logger.info('Waiting for clusters to boot...')
        for t in ts: t.join()
