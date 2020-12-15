CREATE TABLE ks1.simple_partition_deletes (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};
INSERT INTO ks1.simple_partition_deletes (pk, ck, v1, v2) VALUES ('simple_partition_deletes', 1, 2, 'abc');
INSERT INTO ks1.simple_partition_deletes (pk, ck, v1, v2) VALUES ('simple_partition_deletes', 2, 3, 'def');
DELETE FROM ks1.simple_partition_deletes WHERE pk = 'simple_partition_deletes';

// Insert one more row, just to check if replication works at all
INSERT INTO ks1.simple_partition_deletes (pk, ck, v1, v2) VALUES ('simple_partition_deletes', 4, 5, 'def');