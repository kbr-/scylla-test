CREATE TABLE ks1.map_appends (pk text, ck int, v map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.map_appends (pk, ck, v) VALUES ('map_appends', 1, {1: 2, 3: 4});
UPDATE ks1.map_appends SET v = v + {5: 6} WHERE pk = 'map_appends' AND ck = 1;
UPDATE ks1.map_appends SET v = v + {5: 6} WHERE pk = 'map_appends' AND ck = 2;
