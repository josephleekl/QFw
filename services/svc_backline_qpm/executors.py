# catalyst-executor processes owned by the Backline QPM, one per reservation.
#
# The executor hosts the reservation's controller and decoder code (dispatched
# by the client over ORC) in one process, so memcpy can connect them. Remote
# Backline nodes name libraries by bare filename, so the executor runs with
# Catalyst's lib/ as working directory and on LD_LIBRARY_PATH.

import os
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

RUNTIME_PLUGINS = ("librt_transport.so", "librt_capi.so")
# First line catalyst-executor prints once bound (as matched by
# catalyst.executor.utils.OutputPatterns).
_READY = "Listening on"


class ExecutorStartError(RuntimeError):
	pass


@dataclass
class Running:
	address: str
	process: object
	log_path: str


def _free_port():
	with socket.socket() as s:
		s.bind(("", 0))
		return s.getsockname()[1]


def start(catalyst_lib, plugins, binary=None, log_dir=None, timeout_s=30.0):
	lib = Path(catalyst_lib)
	port = _free_port()
	log_dir = Path(log_dir or os.environ.get("QFW_LOG_DIR") or
		       tempfile.gettempdir())
	log_path = str(log_dir / f"backline-executor-{port}.log")
	argv = [binary or str(lib / "catalyst-executor"),
		f"--bind=0.0.0.0:{port}"]
	argv += [f"--plugin={lib / p}" for p in plugins]
	env = dict(os.environ)
	env["LD_LIBRARY_PATH"] = os.pathsep.join(
		p for p in (str(lib), os.environ.get("LD_LIBRARY_PATH")) if p)
	with open(log_path, "w") as log:
		proc = subprocess.Popen(argv, cwd=lib, env=env,
					stdin=subprocess.DEVNULL, stdout=log,
					stderr=subprocess.STDOUT,
					start_new_session=True)
	running = Running(f"{socket.gethostname()}:{port}", proc, log_path)
	deadline = time.monotonic() + timeout_s
	while time.monotonic() < deadline and proc.poll() is None:
		if _READY in Path(log_path).read_text(errors="replace"):
			return running
		time.sleep(0.1)
	stop(running)
	raise ExecutorStartError(
		f"catalyst-executor did not become ready; see {log_path}")


def stop(running):
	# The executor forks a child per connection, so stop the whole group.
	proc = running.process
	if proc is None or proc.poll() is not None:
		return
	try:
		os.killpg(proc.pid, signal.SIGTERM)
		proc.wait(timeout=5)
	except ProcessLookupError:
		pass
	except subprocess.TimeoutExpired:
		os.killpg(proc.pid, signal.SIGKILL)
		proc.wait()
