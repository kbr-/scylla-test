CREATE TABLE ks1.set_removals (pk text, ck int, v set<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};

INSERT INTO ks1.set_removals (pk, ck, v) VALUES ('set_removals', 1, {1, 2, 3, 4});
UPDATE ks1.set_removals SET v = v - {1, 3} WHERE pk = 'set_removals' AND ck = 1;
UPDATE ks1.set_removals SET v = v - {1138} WHERE pk = 'set_removals' AND ck = 2;
