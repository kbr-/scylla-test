#!/bin/bash

CQLSH=$1
CLUSTER_IP=$2

$CQLSH $CLUSTER_IP -e"CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;"

for CQL_FILE in cql/*.cql
do
	CQL_FILENAME=$(basename $CQL_FILE)
	CQL_TESTNAME="${CQL_FILENAME%.*}"
	echo $CQL_TESTNAME

	$CQLSH $CLUSTER_IP -f $CQL_FILE 2>&1 &
done 
