import socket
import stat
import time

import pytest

from svc_backline_qpm import executors


def _fake_binary(tmp_path, body):
	path = tmp_path / "catalyst-executor"
	path.write_text("#!/bin/sh\n" + body + "\n", encoding="utf-8")
	path.chmod(path.stat().st_mode | stat.S_IEXEC)
	return str(path)


def test_start_waits_for_ready_and_stop_kills_the_group(tmp_path):
	binary = _fake_binary(tmp_path, 'echo "$@"; echo "Listening on 0.0.0.0:1"; sleep 60')
	running = executors.start(tmp_path, ["libdecoder.so"], binary=binary,
				   log_dir=tmp_path)
	host, port = running.address.rsplit(":", 1)
	assert host == socket.gethostname() and int(port) > 0
	log = open(running.log_path).read()
	assert f"--bind=0.0.0.0:{port}" in log
	assert f"--plugin={tmp_path / 'libdecoder.so'}" in log
	assert running.process.poll() is None
	executors.stop(running)
	assert running.process.poll() is not None


def test_start_fails_when_the_executor_exits(tmp_path):
	binary = _fake_binary(tmp_path, "exit 3")
	with pytest.raises(executors.ExecutorStartError, match="did not become ready"):
		executors.start(tmp_path, [], binary=binary, log_dir=tmp_path)


def test_start_times_out_and_cleans_up(tmp_path):
	binary = _fake_binary(tmp_path, "sleep 60")
	t0 = time.monotonic()
	with pytest.raises(executors.ExecutorStartError):
		executors.start(tmp_path, [], binary=binary, log_dir=tmp_path,
				timeout_s=0.5)
	assert time.monotonic() - t0 < 10
