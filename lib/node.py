from pathlib import Path
from typing import Optional, List, Final
from dataclasses import dataclass, field, replace
import yaml

from lib.node_config import NodeConfig, mk_node_cfg

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
    experimental: List[str] = field(default_factory=list)

@dataclass(frozen=True)
class LocalNodeEnv:
    cfg: NodeConfig
    opts: RunOpts

# TODO: this is test specific?
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
                    enable_rbo = cluster_cfg.enable_rbo,
                    experimental = cluster_cfg.experimental),
                opts = opts)
            for i in ips]

    # Optimization to start first node faster
    if cluster_cfg.first_node_skip_gossip_settle:
        envs[0] = LocalNodeEnv(cfg = envs[0].cfg, opts = replace(envs[0].opts, skip_gossip_wait = True))

    return envs

# TODO: better name, specification?
# this encapsulates the "directory" of a node; where the configuration files are, paths, the node's "name", ip, ...
class Node:
    def __init__(self, cfg_tmpl: dict, base_path: Path, env: LocalNodeEnv, scylla_path: Path):
        self.name: Final[str] = env.cfg.ip_addr
        self.path: Final[Path] = base_path / self.name
        self.conf_path: Final[Path] = self.path / 'conf'
        self.cfg_tmpl: Final[dict] = cfg_tmpl
        self.env = env

        self.path.mkdir(parents=True)
        self.conf_path.mkdir(parents=True)
        self.__write_conf()

    def ip(self) -> str:
        return self.env.cfg.ip_addr

    def get_node_config(self) -> NodeConfig:
        return self.env.cfg

    def reset_node_config(self, cfg: NodeConfig) -> None:
        self.env = replace(self.env, cfg = cfg)
        self.__write_conf()

    # Precondition: self.conf_path exists, self.env assigned
    def __write_conf(self) -> None:
        with open(self.conf_path / 'scylla.yaml', 'w') as f:
            yaml.dump(mk_node_cfg(self.cfg_tmpl, self.env.cfg), f)
