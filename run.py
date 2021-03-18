from threading import Thread
from contextlib import closing, ExitStack
from dataclasses import replace
from pathlib import Path
from typing import Iterator, Tuple, Optional
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
import queue
import itertools

from cassandra.cluster import Cluster

from node_config import *
from tmux_node import *

def cdc_opts(mode: str):
    if mode == 'preimage':
        return "{'enabled': true, 'preimage': 'full'}"
    if mode == 'postimage':
        return "{'enabled': true, 'postimage': true}"
    return "{'enabled': true}"

class PauseNemesis:
    def __init__(self, n: TmuxNode):
        self.n = n
        self.q: Optional[queue.Queue] = None
        self.t: Optional[Thread] = None

    def _nemesis_thread(self, q: queue.Queue) -> None:
        while True:
            time.sleep(5)
            if not q.empty():
                break
            log('Nemesis: pausing {}'.format(self.n.ip()))
            self.n.pause()
            time.sleep(3 + random.randrange(0,2))
            if not q.empty():
                break
            log('Nemesis: unpausing {}'.format(self.n.ip()))
            self.n.unpause()
        log('Nemesis: asked to finish, unpausing {}'.format(self.n.ip()))
        self.n.unpause()

    def start(self) -> None:
        if self.t:
            return

        self.q = queue.Queue()
        self.t = Thread(target=PauseNemesis._nemesis_thread, args=[self, self.q])
        self.t.start()

    def stop(self) -> None:
        if not self.t:
            return

        assert self.q
        self.q.put(None)
        self.t.join()
        self.q = None
        self.t = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--scylla-path', type=Path, required=True)
    parser.add_argument('--replicator-path', type=Path, required=True)
    parser.add_argument('--migrate-path', type=Path, required=True)
    parser.add_argument('--gemini', default=False, action='store_true')
    parser.add_argument('--gemini-seed', type=int)
    parser.add_argument('--gemini-concurrency', type=int, default=5)
    parser.add_argument('--cql', default=False, action='store_true')
    parser.add_argument('--cqlsh-path', type=Path)
    parser.add_argument('--single', default=False, action='store_true')
    parser.add_argument('--mode', default='delta', choices=['delta','preimage','postimage'])
    parser.add_argument('--no-bootstrap-node', default=False, action='store_true')
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--with-pauses', default=False, action='store_true')
    parser.add_argument('--ring_delay_ms', type=int, default=3000)
    args = parser.parse_args()

    scylla_path = args.scylla_path.resolve()
    replicator_path = args.replicator_path.resolve()
    migrate_path = args.migrate_path.resolve()
    use_gemini = args.gemini
    use_cql = args.cql
    if use_cql:
        cqlsh_path = args.cqlsh_path.resolve()
    bootstrap_node = not args.no_bootstrap_node
    duration = args.duration
    with_pauses = args.with_pauses
    gemini_concurrency = args.gemini_concurrency
    ring_delay_ms = args.ring_delay_ms
    log('Scylla: {}\nReplicator: {}\nMigrate: {}\nuse_gemini: {}\nuse_cql: {}\nbootstrap_node: {}\nduration: {}s\npauses: {}\nring_delay_ms: {}'.format(
        scylla_path, replicator_path, migrate_path, use_gemini, use_cql, bootstrap_node, duration, with_pauses, ring_delay_ms))

    gemini_seed = args.gemini_seed
    if gemini_seed is None:
        gemini_seed = random.randint(1, 1000)
    if gemini_seed < 1:
        print('Wrong gemini seed. Use a positive number.')
        exit(1)
    if duration < 1:
        print('Wrong duration. Use a positive number.')
        exit(1)
    if gemini_concurrency < 1:
        print('Wrong gemini_concurrency. Use a positive number.')
        exit(1)
    if ring_delay_ms < 1:
        print('ring_delay_ms must be positive')
        exit(1)

    if use_gemini:
        log(f'gemini seed: {gemini_seed}\ngemini concurrency: {gemini_concurrency}')

    num_master_nodes = 1 if args.single else 3
    mode = args.mode

    if mode != 'delta' and not use_gemini and not use_cql:
        print('preimage/postimage supported with gemini only')
        exit(1)

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

    master_envs = mk_cluster_env(start = 10, num_nodes = int(bootstrap_node) + num_master_nodes,
            opts = replace(RunOpts(), developer_mode = True, overprovisioned = True), ring_delay_ms = ring_delay_ms)
    master_nodes = [TmuxNode(cfg_tmpl, run_path, e, tmux_sess, scylla_path) for e in master_envs]

    new_node = None
    if bootstrap_node:
        new_node = master_nodes[-1]
        master_nodes = master_nodes[:-1]

    nemeses = None
    if with_pauses:
        nemeses = [PauseNemesis(n) for n in master_nodes]

    replica_envs = mk_cluster_env(start = 20, num_nodes = 1,
            opts = replace(RunOpts(), developer_mode = True, overprovisioned = True), ring_delay_ms = ring_delay_ms)
    replica_nodes = [TmuxNode(cfg_tmpl, run_path, e, tmux_sess, scylla_path) for e in replica_envs]

    log('tmux session name:', f'scylla-test-{run_id}')

    def start_cluster(nodes: List[TmuxNode]):
        for n in nodes:
            n.start()
    start_master = Thread(target=start_cluster, args=[master_nodes])
    start_replica = Thread(target=start_cluster, args=[replica_nodes])
    start_master.start()
    start_replica.start()
    start_master.join()
    start_replica.join()

    #Hardcoded in gemini:
    KS_NAME = 'ks1'
    TABLE_NAMES = ['table1']

    if use_cql:
        TABLE_NAMES = [os.path.splitext(f)[0] for f in os.listdir('./cql/') if os.path.isfile(os.path.join('./cql/', f))]
    
    cm = Cluster([n.ip() for n in master_nodes])
    cr = Cluster([n.ip() for n in replica_nodes])

    w = tmux_sess.windows[0]
    w.split_window(start_directory = run_path, attach = False)
    w.split_window(start_directory = run_path, attach = False)
    w.select_layout('even-vertical')
    w.panes[0].send_keys('tail -F stressor.log -n +1')
    w.panes[1].send_keys('tail -F replicator.log -n +1')
    w.panes[2].send_keys('tail -F migrate.log -n +1')

    log('Waiting for the latest CDC generation to start...')
    time.sleep(15)

    with ExitStack() as stack:
        stressor_log = stack.enter_context(open(run_path / 'stressor.log', 'w'))
        repl_log = stack.enter_context(open(run_path / 'replicator.log', 'w'))
        migrate_log = stack.enter_context(open(run_path / 'migrate.log', 'w'))

        log('Starting stressor')
        if use_gemini:
            stressor_proc = stack.enter_context(subprocess.Popen([
                'gemini',
                '-d', '--duration', '{}s'.format(duration), '--warmup', '0',
                '-c', '{}'.format(gemini_concurrency),
                '-m', 'write',
                '--non-interactive',
                '--cql-features', 'basic',
                '--max-mutation-retries', '100', '--max-mutation-retries-backoff', '100ms',
                '--replication-strategy', f"{{'class': 'SimpleStrategy', 'replication_factor': '{num_master_nodes}'}}",
                '--table-options', "cdc = {}".format(cdc_opts(mode)),
                '--test-cluster={}'.format(master_nodes[0].ip()),
                '--seed', str(gemini_seed),
                '--verbose',
                '--level', 'info',
                '--use-server-timestamps',
                '--test-host-selection-policy', 'token-aware'
            ], stdout=stressor_log, stderr=subprocess.STDOUT))
        elif use_cql:
            stressor_proc = stack.enter_context(subprocess.Popen([
                './run_cqlsh.sh',
                cqlsh_path,
                master_nodes[0].ip()
            ], stdout=stressor_log, stderr=subprocess.STDOUT))
        else:
            prof_file = 'cdc_replication_profile_single.yaml' if args.single else 'cdc_replication_profile.yaml'
            stressor_proc = stack.enter_context(subprocess.Popen([
                'cassandra-stress',
                "user no-warmup profile={} ops(update=1) cl=QUORUM duration={}s".format(prof_file, duration),
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
                if name.endswith('_scylla_cdc_log'):
                    continue
                if 'cdc' in table.extensions:
                    del table.extensions['cdc']
                table_ddls.append(table.as_cql_query())

        log('User types:\n{}'.format('\n'.join(ut_ddls)))
        log('Table definitions:\n{}'.format('\n'.join(table_ddls)))

        log('Letting stressor run for a while...')
        time.sleep(5)

        log('Creating schema on replica cluster.')
        with cr.connect() as sess:
            sess.execute(f"create keyspace if not exists {KS_NAME}"
                          " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
            for stmt in ut_ddls + table_ddls:
                sess.execute(stmt)

        log('Letting stressor run for a while...')
        time.sleep(5)

        log('Starting replicator')
        repl_proc = stack.enter_context(subprocess.Popen([
            'java', '-cp', replicator_path, 'com.scylladb.cdc.replicator.Main',
            '-k', KS_NAME, '-t', ','.join(TABLE_NAMES), '-s', master_nodes[0].ip(), '-d', replica_nodes[0].ip(), '-cl', 'one',
            '-m', mode],
            stdout=repl_log, stderr=subprocess.STDOUT))

        if nemeses:
            log('Starting pause nemeses')
            for n in nemeses:
                n.start()

        log('Letting stressor run for a while...')
        time.sleep(15)

        if new_node:
            if nemeses:
                log('Stopping pause nemeses before bootstrapping a node')
                for n in nemeses:
                    n.stop()

            log('Bootstrapping new node')
            new_node.start()

            if nemeses:
                log('Restarting pause nemeses after bootstrapping a node')
                for n in nemeses:
                    n.start()

        log('Waiting for stressor to finish...')
        stressor_proc.wait()
        log('Gemini return code:', stressor_proc.returncode)

        log('Letting replicator run for a while (60s)...')
        time.sleep(60)

        if nemeses:
            log('Stopping pause nemeses')
            for n in nemeses:
                n.stop()

        log('Waiting for replicator to finish...')
        repl_proc.send_signal(signal.SIGINT)
        repl_proc.wait()
        log('Replicator return code:', repl_proc.returncode)

        log('Comparing table contents using scylla-migrate...')
        migrate_res = subprocess.run(
            '{} check --master-address {} --replica-address {}'
            ' --ignore-schema-difference {} {}'.format(
                migrate_path, master_nodes[0].ip(), replica_nodes[0].ip(),
                '--no-writetime' if mode == 'postimage' else '',
                ' '.join(map(lambda e: e[0] + '.' + e[1], zip(itertools.repeat(KS_NAME), TABLE_NAMES)))),
            shell = True, stdout = migrate_log, stderr = subprocess.STDOUT)
        log('Migrate return code:', migrate_res.returncode)

    with open(run_path / 'migrate.log', 'r') as f:
        ok = 'Consistency check OK.\n' in (line for line in f)

    if mode == 'preimage':
        with open(run_path / 'replicator.log', 'r') as f:
            ok = ok and not ('Inconsistency detected.\n' in (line for line in f))

    if ok:
        log('Consistency OK')
    else:
        log('Inconsistency detected')

    log('tmux session name:', f'scylla-test-{run_id}')
