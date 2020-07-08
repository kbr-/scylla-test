The runner assumes you have working `cassandra-stress` or `gemini` (depending on what you use) in your `$PATH`.

The runner needs tmux. For Fedora:
```
sudo dnf install tmux
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
I may have forgotten about some requirements; if the runner complains, just install them and report an issue.

Run the test
```
. env/bin/activate
python3 run.py \
    --scylla-path path/to/scylla/bin \
    --gemini \
    --gemini-seed 94
```
if you skip the `--gemini` flag, the runner will use `cassandra-stress` to generate workload.
For `cassandra-stress` you can modify `profile.yaml` as you like.

The `--gemini-seed` flag modifies how `gemini` runs. If you skip it, the runner will generate the seed randomly.

The tmux session name in which the test runs will be printed, e.g.:
```
2020-06-26 16:58:36 tmux session name: scylla-test-2020-06-26_16-58-36
```
a directory will be created with node configs and logs:
```
$ ls runs/2020-06-26_16-58-36/
127.0.0.10  127.0.0.11  127.0.0.20  stressor.log
```
the `runs/latest` symlink points to the directory corresponding to the latest run.

You can attach the tmux session in which the test runs:
```
tmux a -t scylla-test-2020-06-26_16-58-36
```
the first window shows the tool logs, and consecutive windows have Scylla instances running (you can scroll between windows using `C-b n` and `C-b p` and switch panes within a window using `C-b {arrow}`, where `{arrow}` is an arrow key). You can stop a node and then restart it with `run.sh` in the appropriate directory. `scyllalog` contains the node's logs.

If you want to run the test again, stop the previous nodes first: the test uses hardcoded IPs.

IMPORTANT: the runner doesn't check if something goes wrong during the test, so it may get stuck e.g. waiting for a node to bootstrap.
In this case just attach the tmux session and watch what happened.
