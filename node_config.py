from dataclasses import dataclass
from typing import List
import yaml

@dataclass(frozen=True)
class NodeConfig:
    ip_addr: str
    seed_ip_addr: str
    ring_delay_sec: int = 5

def load_cfg_template() -> dict:
    with open('resources/scylla.yaml') as f:
        return yaml.load(f, Loader=yaml.FullLoader)

def mk_node_cfg(tmpl: dict, cfg: NodeConfig) -> dict:
    return dict(tmpl, **{
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
            'ring_delay_ms': cfg.ring_delay_sec * 1000
        })

#print(yaml.dump(mk_node_cfg(NodeConfig(seed_ip_addr='127.0.0.1', ip_addr='127.0.0.1'))))
