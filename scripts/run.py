from threading import Thread
from contextlib import closing, ExitStack
from dataclasses import replace
from pathlib import Path
from typing import Iterator, Tuple, Optional, Union
import time
import subprocess
import select
import datetime
import yaml
import stat
import libtmux # type: ignore
import re
import os
import signal
import argparse
import random
import queue
import itertools
import logging
import sys

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT # type: ignore
from cassandra import policies # type: ignore

from lib.node_config import *
from lib.tmux_node import *
from lib.node import mk_cluster_env

def cdc_opts(mode: str):
    if mode == 'preimage':
        return "{'enabled': true, 'preimage': 'full'}"
    if mode == 'postimage':
        return "{'enabled': true, 'postimage': true}"
    return "{'enabled': true}"

class PauseNemesis:
    def __init__(self, logger: logging.Logger, n: TmuxNode):
        self.logger = logger
        self.n = n
        self.q: Optional[queue.Queue] = None
        self.t: Optional[Thread] = None

    def log(self, *args, **kwargs) -> None:
        self.logger.info(*args, **kwargs)

    def _nemesis_thread(self, q: queue.Queue) -> None:
        while True:
            time.sleep(5)
            if not q.empty():
                break
            self.log('Nemesis: pausing {}'.format(self.n.node.ip()))
            self.n.pause()
            time.sleep(3 + random.randrange(0,2))
            if not q.empty():
                break
            self.log('Nemesis: unpausing {}'.format(self.n.node.ip()))
            self.n.unpause()
        self.log('Nemesis: asked to finish, unpausing {}'.format(self.n.node.ip()))
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

class RestartNemesis:
    def __init__(self, logger: logging.Logger, ns: List[TmuxNode], hard: bool):
        self.logger = logger
        self.ns = ns
        self.hard = hard
        self.q: Optional[queue.Queue] = None
        self.t: Optional[Thread] = None

    def log(self, *args, **kwargs) -> None:
        self.logger.info(*args, **kwargs)

    def _nemesis_thread(self, q: queue.Queue) -> None:
        while True:
            time.sleep(8 + random.randrange(0, 5))
            if not q.empty():
                break
            n = random.choice(self.ns)
            self.log('Nemesis: {}restarting {}'.format('hard ' if self.hard else '', n.node.ip()))
            if self.hard:
                n.hard_restart()
            else:
                n.restart()
        self.log('Restart Nemesis: asked to finish')

    def start(self) -> None:
        if self.t:
            return

        self.q = queue.Queue()
        self.t = Thread(target=RestartNemesis._nemesis_thread, args=[self, self.q])
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
    parser.add_argument('--with-restarts', default=False, action='store_true')
    parser.add_argument('--ring_delay_ms', type=int, default=3000)
    parser.add_argument('--enable-rbo', default=False, action='store_true')
    args = parser.parse_args()

    scylla_path: Path = args.scylla_path.resolve()
    replicator_path: Path = args.replicator_path.resolve()
    migrate_path: Path = args.migrate_path.resolve()
    use_gemini: bool = args.gemini
    use_cql: bool = args.cql
    if use_cql:
        cqlsh_path: Path = args.cqlsh_path.resolve()
    bootstrap_node: bool = not args.no_bootstrap_node
    duration: int = args.duration
    with_pauses: bool = args.with_pauses
    with_restarts: bool = args.with_restarts
    gemini_concurrency: int = args.gemini_concurrency
    ring_delay_ms: int = args.ring_delay_ms
    enable_rbo : bool = args.enable_rbo

    gemini_seed: int = args.gemini_seed
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

    num_master_nodes = 1 if args.single else 3
    mode = args.mode

    if mode != 'delta' and not use_gemini and not use_cql:
        print('preimage/postimage supported with gemini only')
        exit(1)

    cluster_cfg = ClusterConfig(
        ring_delay_ms = ring_delay_ms,
        hinted_handoff_enabled = False,
        enable_rbo = enable_rbo,
        first_node_skip_gossip_settle = True,
    )

    cfg_tmpl: dict = load_cfg_template()

    run_id: str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    runs_path = Path.cwd() / 'runs'
    run_path = runs_path / run_id
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

    gemini_log = f"""
    gemini seed: {gemini_seed}
    gemini concurrency: {gemini_concurrency}""" if use_gemini else ""

    logger.info(f"""
    scylla: {scylla_path}
    replicator: {replicator_path}
    migrate: {migrate_path}
    use_gemini: {use_gemini}
    use_cql: {use_cql}
    bootstrap_node: {bootstrap_node}
    duration: {duration}
    pauses: {with_pauses}
    restrats: {with_restarts}
    ring_delay_ms: {ring_delay_ms}"""
    f"{gemini_log}"
    f"""
    run ID: {run_id}""")

    latest_run_path = runs_path / 'latest'
    latest_run_path.unlink(missing_ok = True)
    latest_run_path.symlink_to(run_path, target_is_directory = True)

    serv = libtmux.Server()
    session_name = f'scylla-test-{run_id}'
    tmux_sess = serv.new_session(session_name = session_name, start_directory = run_path)

    master_envs = mk_cluster_env(start = 10, num_nodes = int(bootstrap_node) + num_master_nodes,
            opts = replace(RunOpts(), developer_mode = True, overprovisioned = True), cluster_cfg = cluster_cfg)
    master_nodes = [TmuxNode(logger, cfg_tmpl, run_path, e, tmux_sess, scylla_path) for e in master_envs]

    new_node = None
    if bootstrap_node:
        new_node = master_nodes[-1]
        master_nodes = master_nodes[:-1]

    nemeses: Optional[Union[List[PauseNemesis], List[RestartNemesis]]] = None
    # FIXME: make them composable
    if with_pauses:
        nemeses = [PauseNemesis(logger, n) for n in master_nodes]
    if with_restarts:
        nemeses = [RestartNemesis(logger, master_nodes, True)]

    replica_envs = mk_cluster_env(start = 20, num_nodes = 1,
            opts = replace(RunOpts(), developer_mode = True, overprovisioned = True), cluster_cfg = cluster_cfg)
    replica_nodes = [TmuxNode(logger, cfg_tmpl, run_path, e, tmux_sess, scylla_path) for e in replica_envs]

    logger.info(f'tmux session name: {session_name}')

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
    
    profile = ExecutionProfile(load_balancing_policy = policies.TokenAwarePolicy(policies.DCAwareRoundRobinPolicy()))
    cm = Cluster([n.node.ip() for n in master_nodes], protocol_version = 4, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    cr = Cluster([n.node.ip() for n in replica_nodes], protocol_version = 4, execution_profiles={EXEC_PROFILE_DEFAULT: profile})

    w = tmux_sess.windows[0]
    w.split_window(start_directory = run_path, attach = False)
    w.split_window(start_directory = run_path, attach = False)
    w.select_layout('even-vertical')
    w.panes[0].send_keys('tail -F stressor.log -n +1')
    w.panes[1].send_keys('tail -F replicator.log -n +1')
    w.panes[2].send_keys('tail -F migrate.log -n +1')

    logger.info('Waiting for the latest CDC generation to start...')
    time.sleep(15)

    with ExitStack() as stack:
        stressor_log = stack.enter_context(open(run_path / 'stressor.log', 'w'))
        repl_log = stack.enter_context(open(run_path / 'replicator.log', 'w'))
        migrate_log = stack.enter_context(open(run_path / 'migrate.log', 'w'))

        logger.info('Starting stressor')
        if use_gemini:
            stressor_proc = stack.enter_context(subprocess.Popen([
                'gemini',
                '--duration', '{}s'.format(duration), '--warmup', '0',
                '-c', '{}'.format(gemini_concurrency),
                '-m', 'write',
                '--non-interactive',
                '--cql-features', 'basic',
                '--max-mutation-retries', '100', '--max-mutation-retries-backoff', '100ms',
                '--replication-strategy', f"{{'class': 'SimpleStrategy', 'replication_factor': '{num_master_nodes}'}}",
                '--table-options', "cdc = {}".format(cdc_opts(mode)),
                '--test-cluster={}'.format(master_nodes[0].node.ip()),
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
                master_nodes[0].node.ip()
            ], stdout=stressor_log, stderr=subprocess.STDOUT))
        else:
            prof_file = 'cdc_replication_profile_single.yaml' if args.single else 'cdc_replication_profile.yaml'
            stressor_proc = stack.enter_context(subprocess.Popen([
                'cassandra-stress',
                "user no-warmup profile={} ops(update=1) cl=QUORUM duration={}s".format(prof_file, duration),
                "-port jmx=6868", "-mode cql3", "native", "-rate threads=1", "-log level=verbose interval=5", "-errors retries=999 ignore",
                "-node {}".format(master_nodes[0].node.ip())],
                stdout=stressor_log, stderr=subprocess.STDOUT))

        logger.info('Letting stressor run for a while...')
        # Sleep long enough for stressor to create the keyspace
        time.sleep(10)

        logger.info('Fetching schema definitions from master cluster.')
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

        logger.info('User types:\n{}'.format('\n'.join(ut_ddls)))
        logger.info('Table definitions:\n{}'.format('\n'.join(table_ddls)))

        logger.info('Letting stressor run for a while...')
        time.sleep(5)

        logger.info('Creating schema on replica cluster.')
        with cr.connect() as sess:
            sess.execute(f"create keyspace if not exists {KS_NAME}"
                          " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
            for stmt in ut_ddls + table_ddls:
                sess.execute(stmt)

        logger.info('Letting stressor run for a while...')
        time.sleep(5)

        logger.info('Starting replicator')
        repl_proc = stack.enter_context(subprocess.Popen([
            'java', '-cp', replicator_path, 'com.scylladb.cdc.replicator.Main',
            '-k', KS_NAME, '-t', ','.join(TABLE_NAMES), '-s', master_nodes[0].node.ip(), '-d', replica_nodes[0].node.ip(), '-cl', 'one',
            '-m', mode],
            stdout=repl_log, stderr=subprocess.STDOUT))

        if nemeses:
            logger.info('Starting nemeses')
            for n in nemeses:
                n.start()

        logger.info('Letting stressor run for a while...')
        time.sleep(15)

        if new_node:
            if nemeses:
                logger.info('Stopping nemeses before bootstrapping a node')
                for n in nemeses:
                    n.stop()

            logger.info('Bootstrapping new node')
            new_node.start()

            if nemeses:
                logger.info('Restarting nemeses after bootstrapping a node')
                for n in nemeses:
                    n.start()

        logger.info('Waiting for stressor to finish...')
        stressor_proc.wait()
        logger.info(f'Stressor return code: {stressor_proc.returncode}')

        logger.info('Letting replicator run for a while (90s)...')
        time.sleep(90)

        if nemeses:
            logger.info('Stopping nemeses')
            for n in nemeses:
                n.stop()

        #logger.info('Letting replicator run for a while (240s)...')
        #time.sleep(240)

        logger.info('Waiting for replicator to finish...')
        repl_proc.send_signal(signal.SIGINT)
        repl_proc.wait()
        logger.info(f'Replicator return code: {repl_proc.returncode}')

        logger.info('Comparing table contents using scylla-migrate...')
        migrate_res = subprocess.run(
            '{} check --master-address {} --replica-address {}'
            ' --ignore-schema-difference {} {}'.format(
                migrate_path, master_nodes[0].node.ip(), replica_nodes[0].node.ip(),
                '--no-writetime' if mode == 'postimage' else '',
                ' '.join(map(lambda e: e[0] + '.' + e[1], zip(itertools.repeat(KS_NAME), TABLE_NAMES)))),
            shell = True, stdout = migrate_log, stderr = subprocess.STDOUT)
        logger.info(f'Migrate return code: {migrate_res.returncode}')

    with open(run_path / 'migrate.log', 'r') as f:
        ok = 'Consistency check OK.\n' in (line for line in f)

    if mode == 'preimage':
        with open(run_path / 'replicator.log', 'r') as f:
            ok = ok and not ('Inconsistency detected.\n' in (line for line in f))

    if ok:
        logger.info('Consistency OK')
    else:
        logger.info('Inconsistency detected')

    logger.info(f'tmux session name: {session_name}')
