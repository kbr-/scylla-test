from typing import Optional
from threading import Thread
import argparse
import itertools
import operator
import libtmux
import os
import sys

from tmux_node import *

def current_session(serv: libtmux.Server) -> Optional[libtmux.Session]:
    tty = os.ttyname(sys.stdout.fileno())
    for sess in serv.list_sessions():
        for w in sess.list_windows():
            for p in w.list_panes():
                if p.get('pane_tty') == tty:
                    return sess
    return None

def boot_cluster(
        cfg_tmpl: dict, run_path: Path, sess: libtmux.Session, scylla_path: Path,
        ip_start: int, num_nodes: int, smp: int, overprovisioned: bool) -> Tuple[List[TmuxNode], Thread]:
    envs = mk_dev_cluster_env(ip_start, num_nodes, smp, overprovisioned)
    nodes = [TmuxNode(cfg_tmpl, run_path, e, sess, scylla_path) for e in envs]
    def start_cluster():
        for n in nodes:
            n.start()
    start_thread = Thread(target=start_cluster)
    start_thread.start()
    return nodes, start_thread

if __name__ == "__main__":
    s = libtmux.Server()
    sess = current_session(s)
    if not sess:
        print('Must run in a tmux session.')
        exit(1)

    log("Current session: {}".format(sess))

    parser = argparse.ArgumentParser()
    parser.add_argument('--scylla-path', type=Path, required=True)
    parser.add_argument('--run-path', type=Path, required=True)
    parser.add_argument('--num-nodes', nargs='+', type=int)
    parser.add_argument('--num-shards', type=int)
    parser.add_argument('--overprovisioned', action='store_true')
    args = parser.parse_args()

    scylla_path = args.scylla_path.resolve()
    run_path = args.run_path.resolve()
    num_nodes = args.num_nodes
    num_shards = args.num_shards if args.num_shards else 3
    overprovisioned = bool(args.overprovisioned)
    if any(n <= 0 for n in num_nodes):
        print('Cluster sizes must be positive')
        exit(1)
    if num_shards <= 0:
        print('Number of shards must be positive')
        exit(1)

    log('Scylla: {}\nRun path: {}\nNum nodes: {}\nNum shards: {}\nOverprovisioned: {}'.format(
        scylla_path, run_path, num_nodes, num_shards, overprovisioned))

    cfg_tmpl: dict = load_cfg_template()

    ip_starts = itertools.accumulate([1] + num_nodes, operator.add)
    log('Booting {} clusters...'.format(len(num_nodes)))
    ts = [boot_cluster(cfg_tmpl, run_path, sess, scylla_path, ip_start, num, num_shards, overprovisioned)[1]
            for ip_start, num in zip(ip_starts, num_nodes)]

    log('Waiting for clusters to boot...')
    for t in ts: t.join()
