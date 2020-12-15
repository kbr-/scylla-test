CREATE TABLE ks1.blobs (pk text, ck int, v blob, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.blobs (pk, ck, v) VALUES ('blobs', 1, 0x1234);
INSERT INTO ks1.blobs (pk, ck, v) VALUES ('blobs', 2, 0x);
INSERT INTO ks1.blobs (pk, ck, v) VALUES ('blobs', 3, null);
INSERT INTO ks1.blobs (pk, ck, v) VALUES ('blobs', 4, 0x4321);
INSERT INTO ks1.blobs (pk, ck, v) VALUES ('blobs', 5, 0x00);
UPDATE ks1.blobs SET v = null WHERE pk = 'blobs' AND ck = 4;
UPDATE ks1.blobs SET v = 0x WHERE pk = 'blobs' AND ck = 5;