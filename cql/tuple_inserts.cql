CREATE TABLE ks1.tuple_inserts (pk text, ck int, v tuple<int, text>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.tuple_inserts (pk, ck, v) VALUES ('tuple_inserts', 1, (7, 'abc'));
INSERT INTO ks1.tuple_inserts (pk, ck, v) VALUES ('tuple_inserts', 2, (9, 'def'));
INSERT INTO ks1.tuple_inserts (pk, ck, v) VALUES ('tuple_inserts', 2, null);
