from pathlib import Path
from typing import Final, List, Union, IO, Any, Optional, Tuple
from os import PathLike
from threading import Thread
from queue import Queue
import logging
import resource
import subprocess
import re
import signal

from lib.node import Node
from lib.node_config import NodeConfig, RunOpts
from lib.local_node import LocalNode

def set_max_soft_fd_limit() -> None:
    (_, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

# TODO error handling...
class SubprocessNode(Node):
    def __init__(self,
            logger: logging.Logger,
            base_path: Path, # TODO define meaning of base_path
            binary_path: Path,
            cfg: NodeConfig,
            opts: RunOpts):
        self.__node: Final[LocalNode] = LocalNode(base_path, cfg)
        self.__logger: Final[logging.Logger] = logger
        self.__opts: RunOpts = opts
        self.__binary_path: Path = binary_path
        self.__log_file: Final[Path] = self.__node.path / 'scyllalog'
        self.__process: Optional[Tuple[subprocess.Popen, Thread]] = None

    def start(self) -> None:
        assert not self.__process
        # TODO do some locking to protect from concurrent executions?

        args: List[Union[str, PathLike]] = [
            self.__binary_path,

            '--workdir', 'workdir',
            '--smp', f'{self.__opts.smp}',
            '--max-io-requests', f'{self.__opts.max_io_requests}',
        ]

        if self.__opts.developer_mode:
            args.append('--developer-mode=yes')
        if self.__opts.skip_gossip_wait:
            args.extend(['--skip-wait-for-gossip-to-settle', '0'])
        if self.__opts.overprovisioned:
            args.append('--overprovisioned')
        if self.__opts.stall_notify_ms:
            args.extend(['--blocked-reactor-notify-ms', f'{self.__opts.stall_notify_ms}'])

        p = subprocess.Popen(args,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                universal_newlines=True, bufsize=1,
                cwd=self.__node.path,
                preexec_fn=set_max_soft_fd_limit)

        def readline_thread(stdout: IO[Any], log_file: Path, q: Queue[()]):
            with open(log_file, 'a') as f:
                with stdout as pipe:
                    for l in iter(pipe.readline, ''):
                        f.write(l)
                        print(l, end='')
                        ms = re.match(r".*Scylla.*initialization completed.*", l)
                        if ms:
                            q.put(())
                            break
                    for l in iter(pipe.readline, ''):
                        f.write(l)
                        print(l, end='')

        assert p.stdout
        q: Queue[()] = Queue()
        t = Thread(target=readline_thread, args=[p.stdout, self.__log_file, q], daemon=True)
        t.start()

        # wait for initialization to complete TODO: timeout?
        self.__log(f'Waiting for node {self.ip()} to start...')
        q.get()

        self.__log(f'Node {self.ip()} initialized.')
        self.__process = (p, t)

    def stop(self) -> None:
        if not self.__process:
            return

        self.__log(f'Killing node {self.ip()} with SIGTERM...')
        (p, t) = self.__process
        p.terminate()
        # TODO probably need hard kill after timeout; also specify this in the interface
        # or simply make `stop` take timeout?
        t.join()
        assert p.stdout
        p.stdout.close()
        p.wait()

        self.__process = None

    def hard_stop(self) -> None:
        if not self.__process:
            return

        self.__log(f'Killing node {self.ip()} with SIGKILL...')
        # TODO: copypasta
        (p, t) = self.__process
        p.kill()
        t.join()
        assert p.stdout
        p.stdout.close()
        p.wait()
        self.__process = None

    def pause(self) -> None:
        if not self.__process:
            return

        self.__log(f'Pausing {self.ip()}...')
        (p, _) = self.__process
        p.send_signal(signal.SIGSTOP)

    def unpause(self) -> None:
        if not self.__process:
            return

        self.__log(f'Unpausing {self.ip()}...')
        (p, _) = self.__process
        p.send_signal(signal.SIGCONT)

    def ip(self) -> str:
        return self.__node.get_node_config().ip_addr

    def get_node_config(self) -> NodeConfig:
        return self.__node.get_node_config()

    def reset_node_config(self, cfg: NodeConfig) -> None:
        self.__node.reset_node_config(cfg)

    def reset_scylla_binary(self, binary_path: Path) -> None:
        self.__binary_path = binary_path

    def __log(self, *args, **kwargs) -> None:
        self.__logger.info(*args, **kwargs)
