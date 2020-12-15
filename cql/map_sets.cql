CREATE TABLE ks1.map_sets (pk text, ck int, v map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.map_sets (pk, ck, v) VALUES ('map_sets', 1, {1: 2, 3: 4, 5: 6});
UPDATE ks1.map_sets SET v[1] = 42 WHERE pk = 'map_sets' AND ck = 1;
UPDATE ks1.map_sets SET v[3] = null WHERE pk = 'map_sets' AND ck = 1;
UPDATE ks1.map_sets SET v[3] = 123 WHERE pk = 'map_sets' AND ck = 1;
UPDATE ks1.map_sets SET v[5] = 321 WHERE pk = 'map_sets' AND ck = 2;
