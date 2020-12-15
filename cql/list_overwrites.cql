CREATE TABLE ks1.list_overwrites (pk text, ck int, v list<int>, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.list_overwrites (pk, ck, v) VALUES ('list_overwrites', 1, [1, 2, 3]);
INSERT INTO ks1.list_overwrites (pk, ck, v) VALUES ('list_overwrites', 1, [4, 5, 6, 7]);
INSERT INTO ks1.list_overwrites (pk, ck, v) VALUES ('list_overwrites', 2, [6, 5, 4, 3, 2, 1]);
INSERT INTO ks1.list_overwrites (pk, ck, v) VALUES ('list_overwrites', 2, null);
INSERT INTO ks1.list_overwrites (pk, ck, v) VALUES ('list_overwrites', 3, [1, 11, 111]);
UPDATE ks1.list_overwrites SET v = [2, 22, 222] WHERE pk = 'list_overwrites' AND ck = 3;
