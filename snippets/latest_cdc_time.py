import sys
from cassandra import ConsistencyLevel # type: ignore
from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import Cluster # type: ignore

ip = str(sys.argv[1])

c = Cluster([ip])
s = c.connect()

res = s.execute(SimpleStatement('select "cdc$time" from ks.tb_scylla_cdc_log', consistency_level=ConsistencyLevel.QUORUM))
print(max(res))
