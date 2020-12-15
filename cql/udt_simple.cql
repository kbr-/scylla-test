CREATE TYPE ks1.udt_simple_udt (a int, b int, c text);
CREATE TABLE ks1.udt_simple (pk text, ck int, v frozen<ks1.udt_simple_udt>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 1, (2, 3, 'abc'));
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 2, {a: 6, c: 'zxcv'});
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 3, (9, 4, 'def'));
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 4, (123, 321, 'ghi'));
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 5, (333, 222, 'jkl'));
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 6, (432, 678, 'mno'));
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 7, (765, 345, 'pqr'));
UPDATE ks1.udt_simple SET v.b = 41414 WHERE pk = 'udt_simple' AND ck = 2;
UPDATE ks1.udt_simple SET v = null WHERE pk = 'udt_simple' AND ck = 3;
UPDATE ks1.udt_simple SET v = {b: 123456, c: 'tyu'} WHERE pk = 'udt_simple' AND ck = 4;
INSERT INTO ks1.udt_simple (pk, ck, v) VALUES ('udt_simple', 5, (999, 888, 'zxc'));
UPDATE ks1.udt_simple SET v.c = null WHERE pk = 'udt_simple' AND ck = 6;
UPDATE ks1.udt_simple SET v = {a: 923, b: 123456, c: ''} WHERE pk = 'udt_simple' AND ck = 7;
