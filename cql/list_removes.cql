CREATE TABLE ks1.list_removes (pk text, ck int, v list<int>, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.list_removes (pk, ck, v) VALUES ('list_removes', 1, [1, 2, 3]);
UPDATE ks1.list_removes SET v = v + [4, 5, 6] WHERE pk = 'list_removes' AND ck = 1;
UPDATE ks1.list_removes SET v = v - [1, 2, 3] WHERE pk = 'list_removes' AND ck = 1;