CREATE TABLE ks1.simple_inserts (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};
INSERT INTO ks1.simple_inserts (pk, ck, v1, v2) VALUES ('simple_inserts', 1, 2, 'abc');
INSERT INTO ks1.simple_inserts (pk, ck, v1) VALUES ('simple_inserts', 2, 3);
INSERT INTO ks1.simple_inserts (pk, ck, v2) VALUES ('simple_inserts', 2, 'def');
