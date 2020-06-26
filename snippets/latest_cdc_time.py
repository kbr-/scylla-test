import sys
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster

ip = str(sys.argv[1])

c = Cluster([ip])
s = c.connect()

res = s.execute(SimpleStatement('select "cdc$time" from ks.tb_scylla_cdc_log', consistency_level=ConsistencyLevel.QUORUM))
print(max(res))
