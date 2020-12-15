CREATE TABLE ks1.tuple_updates (pk text, ck int, v tuple<int, text>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 1, (7, 'abc'));
INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 2, (9, 'def'));
INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 3, (11, 'ghi'));
INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 4, (13, 'jkl'));
INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 5, (15, 'mno'));
INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 6, (17, 'pqr'));
INSERT INTO ks1.tuple_updates (pk, ck, v) VALUES ('tuple_updates', 7, (19, 'stu'));
UPDATE ks1.tuple_updates SET v = (111, 'zyx') WHERE pk = 'tuple_updates' AND ck = 1;
UPDATE ks1.tuple_updates SET v = null WHERE pk = 'tuple_updates' AND ck = 2;
INSERT INTO ks1.tuple_updates (pk, ck) VALUES ('tuple_updates', 3);
UPDATE ks1.tuple_updates SET v = (null, null) WHERE pk = 'tuple_updates' AND ck = 4;
UPDATE ks1.tuple_updates SET v = (null, 'asdf') WHERE pk = 'tuple_updates' AND ck = 5;
UPDATE ks1.tuple_updates SET v = (123, null) WHERE pk = 'tuple_updates' AND ck = 6;
UPDATE ks1.tuple_updates SET v = (null, '') WHERE pk = 'tuple_updates' AND ck = 7;
