from dataclasses import dataclass, field
from typing import List, Optional
from importlib import resources
import yaml

@dataclass(frozen=True)
class NodeConfig:
    ip_addr: str
    seed_ip_addr: str
    ring_delay_ms: int
    hinted_handoff_enabled: bool
    enable_rbo: bool
    experimental: List[str] = field(default_factory=list)

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
    extra: str = ''

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

def __load_cfg_template() -> dict:
    with resources.open_binary('resources', 'scylla.yaml') as f:
        return yaml.load(f, Loader=yaml.FullLoader)

# TODO: is this OK?
cfg_template: dict = __load_cfg_template()

def mk_node_cfg(cfg: NodeConfig) -> dict:
    d = dict(cfg_template, **{
            'listen_address': cfg.ip_addr,
            'rpc_address': cfg.ip_addr,
            'api_address': cfg.ip_addr,
            'prometheus_address': cfg.ip_addr,
            'seed_provider': [{
                'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                'parameters': [{
                    'seeds': '{}'.format(','.join([cfg.seed_ip_addr]))
                    }]
                }],
            'ring_delay_ms': cfg.ring_delay_ms,
            'hinted_handoff_enabled': cfg.hinted_handoff_enabled,
            'enable_repair_based_node_ops': cfg.enable_rbo
        })
    if cfg.experimental:
        d = dict(d, **{
            'experimental_features': cfg.experimental
        })
    return d
