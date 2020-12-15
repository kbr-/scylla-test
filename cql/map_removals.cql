CREATE TABLE ks1.map_removals (pk text, ck int, v map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.map_removals (pk, ck, v) VALUES ('map_removals', 1, {1: 2, 3: 4});
UPDATE ks1.map_removals SET v = v - {1} WHERE pk = 'map_removals' AND ck = 1;
UPDATE ks1.map_removals SET v = v - {1138} WHERE pk = 'map_removals' AND ck = 2;
