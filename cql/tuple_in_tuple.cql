CREATE TABLE ks1.tuple_in_tuple (pk text, ck int, v tuple<tuple<int, text>, int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.tuple_in_tuple (pk, ck, v) VALUES ('tuple_in_tuple', 1, ((1, 'abc'), 7));
INSERT INTO ks1.tuple_in_tuple (pk, ck, v) VALUES ('tuple_in_tuple', 2, ((3, 'def'), 9));
INSERT INTO ks1.tuple_in_tuple (pk, ck, v) VALUES ('tuple_in_tuple', 3, ((3, 'ghi'), 9));
INSERT INTO ks1.tuple_in_tuple (pk, ck, v) VALUES ('tuple_in_tuple', 4, ((3, 'jkl'), 9));
UPDATE ks1.tuple_in_tuple SET v = ((100, 'zyx'), 111) WHERE pk = 'tuple_in_tuple' AND ck = 1;
UPDATE ks1.tuple_in_tuple SET v = null WHERE pk = 'tuple_in_tuple' AND ck = 2;
UPDATE ks1.tuple_in_tuple SET v = ((200, null), 999) WHERE pk = 'tuple_in_tuple' AND ck = 3;
UPDATE ks1.tuple_in_tuple SET v = ((300, ''), 333) WHERE pk = 'tuple_in_tuple' AND ck = 4;
