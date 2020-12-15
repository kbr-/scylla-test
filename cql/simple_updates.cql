CREATE TABLE ks1.simple_updates (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};
UPDATE ks1.simple_updates SET v1 = 1 WHERE pk = 'simple_updates' AND ck = 1;
UPDATE ks1.simple_updates SET v2 = 'abc' WHERE pk = 'simple_updates' AND ck = 2;
UPDATE ks1.simple_updates SET v1 = 5, v2 = 'def' WHERE pk = 'simple_updates' AND ck = 3;
