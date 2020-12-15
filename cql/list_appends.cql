CREATE TABLE ks1.list_appends (pk text, ck int, v list<int>, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.list_appends (pk, ck, v) VALUES ('list_appends', 1, [1, 2, 3]);
UPDATE ks1.list_appends SET v = v + [4, 5, 6] WHERE pk = 'list_appends' AND ck = 1;
UPDATE ks1.list_appends SET v = [-2, -1, 0] + v WHERE pk = 'list_appends' AND ck = 1;