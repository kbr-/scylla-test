from typing import Optional
from threading import Thread
import argparse
import itertools
import operator
import libtmux
import os
import sys

from tmux_node import *
from tmux import *

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

if __name__ == "__main__":
    s = libtmux.Server()
    sess = current_session(s)
    if not sess:
        print('Must run in a tmux session.')
        exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument('--scylla-path', type=Path, required=True)
    parser.add_argument('--runs-root', type=Path)
    parser.add_argument('--run-path', type=Path)
    parser.add_argument('--num-nodes', nargs='+', type=int)
    parser.add_argument('--num-shards', type=int)
    parser.add_argument('--overprovisioned', action='store_true')
    parser.add_argument('--stall-notify-ms', type=int)
    parser.add_argument('--no-boot', action='store_true')
    parser.add_argument('--ring-delay-ms', type=int, default=3000)
    parser.add_argument('--enable-rbo', default=False, action='store_true')
    args = parser.parse_args()

    scylla_path: Path = args.scylla_path.resolve()
    num_nodes: List[int] = args.num_nodes
    num_shards: int = args.num_shards if args.num_shards else 3
    overprovisioned: bool = bool(args.overprovisioned)
    stall_notify_ms: int = args.stall_notify_ms
    start_clusters: bool = not args.no_boot
    ring_delay_ms: int = args.ring_delay_ms
    enable_rbo : bool = args.enable_rbo
    if any(n <= 0 for n in num_nodes):
        print('Cluster sizes must be positive')
        exit(1)
    if num_shards <= 0:
        print('Number of shards must be positive')
        exit(1)
    if stall_notify_ms and stall_notify_ms <= 0:
        print('stall-notify-ms must be positive')
        exit(1)
    if ring_delay_ms < 1:
        print('ring-delay-ms must be positive')
        exit(1)
    if bool(args.run_path) == bool(args.runs_root):
        print('pass either run-path or runs-root, but not both')
        exit(1)

    if args.runs_root:
        runs_root: Path = args.runs_root.resolve()
        run_id: str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        run_path: Path = runs_root / run_id
    else:
        assert args.run_path
        run_path = args.run_path.resolve()

    run_path.mkdir(parents=True)
    logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(message)s",
        handlers = [
            logging.FileHandler(run_path / 'run.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger()

    logger.info("Current session: {}".format(sess))
    logger.info('Scylla: {}\nRun path: {}\nNum nodes: {}\nNum shards: {}\nOverprovisioned: {}{}\nStart clusters: {}'.format(
        scylla_path, run_path, num_nodes, num_shards, overprovisioned,
        '\nstall_notify_ms: {}'.format(stall_notify_ms) if stall_notify_ms else '',
        start_clusters))

    opts = replace(RunOpts(),
            developer_mode = True,
            smp = num_shards,
            overprovisioned = overprovisioned,
            stall_notify_ms = stall_notify_ms)

    cluster_cfg = ClusterConfig(
        ring_delay_ms = ring_delay_ms,
        hinted_handoff_enabled = False,
        enable_rbo = enable_rbo
    )

    cfg_tmpl: dict = load_cfg_template()

    ip_starts = itertools.accumulate([1] + num_nodes, operator.add)
    logger.info('Creating {} clusters...'.format(len(num_nodes)))
    cs = [create_cluster(logger, cfg_tmpl, run_path, sess, scylla_path, ip_start, num, opts, cluster_cfg)
            for ip_start, num in zip(ip_starts, num_nodes)]

    if start_clusters:
        ts = [boot(c) for c in cs]
        logger.info('Waiting for clusters to boot...')
        for t in ts: t.join()
