Install tmux, e.g. Fedora:
```
sudo dnf install tmux
```

Clone and build CDC replicator:
```
git clone https://github.com/haaawk/scylla-cdc-java/
(cd scylla-cdc-java && mvn package)
```
the replicator will be available at
```
./scylla-cdc-java/scylla-cdc-replicator/target/scylla-cdc-replicator-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

Clone and build scylla-migrate:
```
git clone https://github.com/scylladb/scylla-migrate.git
(cd scylla-migrate && mvn package)
```
the tool will be available at
```
./scylla-migrate/target/scylla-migrate
```

Preferably create a virtual env:
```
python3 -m venv env
```
Install requirements:
```
. env/bin/activate
pip install -r requirements.txt
```

Run the test
```
. env/bin/activate
python3 run.py \
    --scylla-path path/to/scylla/bin \
    --migrate-path path/to/scylla/migrate \
    --replicator-path path/to/cdc/replicator
    --language java
```

The tmux session name in which the test runs will be printed, e.g.:
```
2020-06-26 16:58:36 tmux session name: scylla-test-2020-06-26_16-58-36
```
a directory will be created with node configs and logs:
```
$ ls runs/2020-06-26_16-58-36/
127.0.0.10  127.0.0.11  127.0.0.20  cs.log  migrate.log  replicator.log
```
the `runs/latest` symlink points to the directory corresponding to the latest run.

You can attach the tmux session in which the test runs:
```
tmux a -t scylla-test-2020-06-26_16-58-36
```
the first window shows the tool logs, and consecutive windows have Scylla instances running (you can scroll between windows using `C-b n` and `C-b p` and switch panes within a window using `C-b {arrow}`, where `{arrow}` is an arrow key). You can stop a node and then restart it with `run.sh` in the appropriate directory. `scyllalog` contains the node's logs.

If you want to run the test again, stop the previous nodes first: the test uses hardcoded IPs.

### Useful snippets
Some useful snippets in the `snippets` directory:
- `which_gen.py`:  print CDC generation timestamps, their sizes, and point the one which contains the given stream.
 ```
 $ python3 snippets/which_gen.py 127.0.0.10 0x813aaaaaaaaaaaab70ed0d48264a3cad
2020-06-26 14:58:58.449000 768
2020-06-26 14:59:49.826000 1536
813aaaaaaaaaaaab70ed0d48264a3cad is in 2020-06-26 14:58:58.449000
```
- `latest_cdc_time.py`: give the greatest `cdc$time` in `ks.tb_scylla_cdc_log`
```
$ python3 snippets/latest_cdc_time.py 127.0.0.10
Row(cdc_time=UUID('b74b2a80-b7bd-11ea-eb9f-b0c9b6407d33'))
```
- `get_time.py`: convert `timeuuid` to timestamp (in microseconds) and print as UTC date-time
```
$ python3 snippets/get_time.py b74b2a80-b7bd-11ea-eb9f-b0c9b6407d33
1593183604008000
2020-06-26 15:00:04
```
