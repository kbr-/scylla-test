from dataclasses import dataclass, field
from typing import List
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

def load_cfg_template() -> dict:
    with resources.open_binary('resources', 'scylla.yaml') as f:
        return yaml.load(f, Loader=yaml.FullLoader)

def mk_node_cfg(tmpl: dict, cfg: NodeConfig) -> dict:
    d = dict(tmpl, **{
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
