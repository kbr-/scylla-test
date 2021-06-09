#!/usr/bin/python3

import sys
from cassandra import ConsistencyLevel # type: ignore
from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import Cluster # type: ignore

ip = str(sys.argv[1])
sid = str(sys.argv[2])

if sid.startswith('0x'):
    sid = sid[2:]

c = Cluster([ip])
s = c.connect()

res = s.execute(SimpleStatement('select * from system_distributed.cdc_streams', consistency_level=ConsistencyLevel.QUORUM))
res = sorted(res, key=lambda r: r[0])
for r in res:
    print(r[0], len(r[2]))

for r in res:
    if sid in (s.hex() for s in r[2]):
        print(sid, 'is in', r[0])
        break
