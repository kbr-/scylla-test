CREATE TABLE ks1.set_appends (pk text, ck int, v set<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.set_appends (pk, ck, v) VALUES ('set_appends', 1, {1, 2, 3, 4});
UPDATE ks1.set_appends SET v = v + {5, 6} WHERE pk = 'set_appends' AND ck = 1;
UPDATE ks1.set_appends SET v = v + {5, 6} WHERE pk = 'set_appends' AND ck = 2;
