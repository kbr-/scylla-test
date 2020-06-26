#!/usr/bin/python3

from datetime import datetime
from uuid import UUID
import sys

def get_timestamp(u):
    return int((UUID(u).time - 0x01b21dd213814000)/10)

t = get_timestamp(sys.argv[1])
print(t)
print(datetime.utcfromtimestamp(t/1000000).strftime('%Y-%m-%d %H:%M:%S'))
