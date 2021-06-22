from pathlib import Path
from typing import Optional, List, Final
from dataclasses import dataclass, field, replace
import yaml

from lib.node_config import NodeConfig, RunOpts, ClusterConfig, mk_node_cfg

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
class LocalNode:
    def __init__(self, base_path: Path, cfg: NodeConfig):
        self.name: Final[str] = cfg.ip_addr
        self.path: Final[Path] = base_path / self.name
        self.conf_path: Final[Path] = self.path / 'conf'
        self.cfg: NodeConfig = cfg

        self.path.mkdir(parents=True)
        self.conf_path.mkdir(parents=True)
        self.__write_conf()

    def ip(self) -> str:
        return self.cfg.ip_addr

    def get_node_config(self) -> NodeConfig:
        return self.cfg

    def reset_node_config(self, cfg: NodeConfig) -> None:
        self.cfg = cfg
        self.__write_conf()

    # Precondition: self.conf_path exists, self.cfg assigned
    def __write_conf(self) -> None:
        with open(self.conf_path / 'scylla.yaml', 'w') as f:
            yaml.dump(mk_node_cfg(self.cfg), f)
