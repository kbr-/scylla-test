CREATE TABLE ks1.simple_row_deletes (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};
INSERT INTO ks1.simple_row_deletes (pk, ck, v1, v2) VALUES ('simple_row_deletes', 1, 2, 'abc');
INSERT INTO ks1.simple_row_deletes (pk, ck, v1, v2) VALUES ('simple_row_deletes', 2, 3, 'def');
DELETE FROM ks1.simple_row_deletes WHERE pk = 'simple_row_deletes' AND ck = 1;
