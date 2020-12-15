CREATE TABLE ks1.map_overwrites (pk text, ck int, v map<int, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.map_overwrites (pk, ck, v) VALUES ('map_overwrites', 1, {1: 2, 3: 4});
INSERT INTO ks1.map_overwrites (pk, ck, v) VALUES ('map_overwrites', 1, {5: 6, 7: 8});
INSERT INTO ks1.map_overwrites (pk, ck, v) VALUES ('map_overwrites', 2, {9: 10, 11: 12});
INSERT INTO ks1.map_overwrites (pk, ck, v) VALUES ('map_overwrites', 2, null);
INSERT INTO ks1.map_overwrites (pk, ck, v) VALUES ('map_overwrites', 3, {13: 14, 15: 16});
UPDATE ks1.map_overwrites SET v = null WHERE pk = 'map_overwrites' AND ck = 3;
