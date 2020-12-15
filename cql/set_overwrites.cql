CREATE TABLE ks1.set_overwrites (pk text, ck int, v set<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.set_overwrites (pk, ck, v) VALUES ('set_overwrites', 1, {1, 2, 3, 4});
INSERT INTO ks1.set_overwrites (pk, ck, v) VALUES ('set_overwrites', 1, {4, 5, 6, 7});
INSERT INTO ks1.set_overwrites (pk, ck, v) VALUES ('set_overwrites', 2, {8, 9, 10, 11});
INSERT INTO ks1.set_overwrites (pk, ck, v) VALUES ('set_overwrites', 2, null);
INSERT INTO ks1.set_overwrites (pk, ck, v) VALUES ('set_overwrites', 3, {12, 13, 14, 15});
UPDATE ks1.set_overwrites SET v = null WHERE pk = 'set_overwrites' AND ck = 3;
