"""Dispatch under concurrent callers, openQSE/QFw#91.

Every async_run, every read_cq and every provider completion dispatches, so
under load several threads do at once. These tests force the interleavings
that used to break: two threads dispatching one selected task, a task ended
while another thread dispatches it, a failure that belonged to another
task, and two threads draining the out-of-resources queue. The QPM has one
host slot, as the fake IQM QPM does, so a slot that is lost stops it.
"""

import threading

import pytest

import util.qpm.util_qpm as util_qpm
from tests.mock.fakes import FakeSchedulerContext
from tests.mock.test_qpm_scheduler import FakeAdmissionContext, FakeQRC
from util.qpm.controller import (
	QPM_TASK_FAILED,
	QPM_TASK_QUEUED,
	QPMTaskNotActive,
	_clear_target_controllers_for_tests,
)
from util.qpm.util_qpm import UTIL_QPM


class OneSlotQPM(UTIL_QPM):
	def __init__(self, target_id="dispatch-races"):
		self.fake_qrc = FakeQRC()
		super().__init__(
			self.fake_qrc,
			max_ppn=1,
			target_id=target_id,
			admission_context_factory=FakeAdmissionContext,
			scheduler_context_factory=FakeSchedulerContext,
		)


def _setup(monkeypatch):
	_clear_target_controllers_for_tests()
	FakeAdmissionContext.usage_status = "accepted"
	monkeypatch.setenv("QFW_QPM_ASSIGNED_HOSTS", "localhost:1")
	monkeypatch.delenv(
		"QFW_QPM_COMPLETION_TERMINAL_RESERVATION_RETENTION_SECONDS",
		raising=False)
	monkeypatch.setattr(util_qpm, "qpm_initialized", True)


def _run(qpm):
	return qpm.async_run({
		"qasm": "OPENQASM 2.0;",
		"num_qubits": 2,
		"reservation_id": "1",
	})


def _finish(qpm, cid, drain=True):
	"""What a provider's thread does once its job is done: give back the
	slot, which dispatches what waits, then publish the result."""
	circuit = qpm.circuits[cid]
	circuit.set_launching()
	circuit.set_running()
	circuit.set_exec_done()
	result = {
		"cid": cid,
		"qtask_id": circuit.info["qtask_id"],
		"rc": 0,
		"result": {"00": 1},
	}
	free = circuit.free_resources if drain else qpm.free_resources
	free(circuit, result=dict(result))
	qpm.controller.publish_completion(dict(result))


def _in_thread(fn):
	"""Run fn on another thread to the end, returning what it returns
	and raising what it raises."""
	box = {}

	def body():
		try:
			box["value"] = fn()
		except BaseException as error:  # noqa: BLE001
			box["error"] = error

	thread = threading.Thread(target=body)
	thread.start()
	thread.join()
	if "error" in box:
		raise box["error"]
	return box.get("value")


def _outcome(qpm, cid):
	return qpm.controller.task_status_for_cid(
		cid, reservation_id="1")["outcome"]


def _one_running_one_queued(qpm):
	"""The first job holds the slot and the one place at the provider
	that the QPM's default dispatch depth allows. The second waits in
	the scheduler's queue until the first finishes."""
	first = _run(qpm)
	second = _run(qpm)
	runtime = qpm.controller.task_for_cid(second["cid"])
	assert qpm.fake_qrc.async_cids == [first["cid"]]
	assert runtime.state == QPM_TASK_QUEUED
	assert qpm.free_hosts == {"localhost": 0}
	return first, second


def test_a_selected_task_goes_to_one_thread_at_a_time(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	# The first job gives back its slot without a drain, so nothing has
	# selected the second yet.
	_finish(qpm, first["cid"], drain=False)

	mine = qpm.controller.select_qtask_for_dispatch()
	theirs = _in_thread(qpm.controller.select_qtask_for_dispatch)

	assert mine.cid == second["cid"]
	# Another thread leaves it to this one instead of dispatching it too.
	assert theirs is None
	# This thread selects it again on its way through, and once it gives
	# the task back, another thread can take it.
	assert qpm.controller.select_qtask_for_dispatch() is mine
	qpm.controller.release_dispatch_claim(mine.qtask_id)
	assert _in_thread(qpm.controller.select_qtask_for_dispatch) is mine


def test_a_task_cancelled_before_it_takes_its_slot_never_runs(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	select = qpm.controller.select_qtask_for_dispatch
	calls = []

	def select_then_cancel():
		runtime = select()
		# The first select picks the task to dispatch. The second is
		# inside the dispatch, before the task takes its slot. Another
		# thread cancels the task there.
		calls.append(runtime)
		if len(calls) == 2:
			_in_thread(lambda: qpm.controller.cancel_task(
				cid=second["cid"], reservation_id="1",
				reason="caller"))
		return runtime

	monkeypatch.setattr(
		qpm.controller, "select_qtask_for_dispatch", select_then_cancel)
	# The first job finishes on its provider's thread, which frees the
	# slot and dispatches what waits.
	_in_thread(lambda: _finish(qpm, first["cid"]))

	assert len(calls) >= 2
	assert qpm.fake_qrc.async_cids == [first["cid"]]
	assert _outcome(qpm, second["cid"]) == "CANCELLED"
	assert qpm.free_hosts == {"localhost": 1}


def test_a_task_cancelled_after_it_takes_its_slot_gives_it_back(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	attach = qpm.controller.attach_provider_credential

	def cancel_then_attach(circuit):
		# The task holds the slot now and has not reached its provider.
		# Another thread cancels it here.
		if circuit.get_cid() == second["cid"]:
			_in_thread(lambda: qpm.controller.cancel_task(
				cid=second["cid"], reservation_id="1",
				reason="caller"))
		return attach(circuit)

	monkeypatch.setattr(
		qpm.controller, "attach_provider_credential",
		cancel_then_attach)
	_in_thread(lambda: _finish(qpm, first["cid"]))

	assert qpm.fake_qrc.async_cids == [first["cid"]]
	assert _outcome(qpm, second["cid"]) == "CANCELLED"
	assert qpm.free_hosts == {"localhost": 1}


def test_another_tasks_failure_stays_with_that_task(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	qpm.fake_qrc.async_error = RuntimeError("provider refused the job")

	# The first job's provider thread frees the slot and dispatches the
	# second, whose provider refuses it. That failure is the second
	# job's, so the first job's thread still publishes its own result.
	_in_thread(lambda: _finish(qpm, first["cid"]))
	completion = qpm.read_cq(cid=first["cid"], reservation_id="1")

	assert completion["completion_ready"] is True
	assert completion["cid"] == first["cid"]
	assert completion["rc"] == 0
	assert completion["result"] == {"00": 1}
	assert _outcome(qpm, second["cid"]) == "FAILED"
	assert qpm.free_hosts == {"localhost": 1}


def test_read_cq_is_not_failed_by_queued_work(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	qpm.fake_qrc.async_error = RuntimeError("provider refused the job")
	# The slot comes back without a drain, so the caller's read_cq is
	# what dispatches the second job.
	_finish(qpm, first["cid"], drain=False)

	completion = qpm.read_cq(cid=first["cid"], reservation_id="1")

	assert completion["completion_ready"] is True
	assert completion["cid"] == first["cid"]
	assert completion["rc"] == 0
	assert completion["result"] == {"00": 1}
	assert qpm.fake_qrc.async_cids == [first["cid"], second["cid"]]
	assert _outcome(qpm, second["cid"]) == "FAILED"
	assert qpm.free_hosts == {"localhost": 1}


def test_a_failed_dispatch_fails_one_task_not_the_queue(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	third = _run(qpm)
	# A fault every job would meet, as when the scheduler holds the QPU
	# busy. The drain fails the job it tried and stops there, so the
	# fault does not fail every job behind it.
	qpm.fake_qrc.async_error = RuntimeError("provider refused the job")

	_in_thread(lambda: _finish(qpm, first["cid"]))

	assert qpm.fake_qrc.async_cids == [first["cid"], second["cid"]]
	assert _outcome(qpm, second["cid"]) == "FAILED"
	assert qpm.controller.task_for_cid(third["cid"]).state == (
		QPM_TASK_QUEUED)
	assert qpm.free_hosts == {"localhost": 1}


def test_a_task_another_thread_failed_stays_failed(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)
	qpm.fake_qrc.async_error = RuntimeError("provider refused the job")
	fail = qpm.fail_provider_submission

	def fail_then_defer(circuit, error):
		runtime = fail(circuit, error)
		# The task's own caller, out of resources a moment before,
		# defers it here: after the dispatching thread failed it and
		# before that thread gives back its slot.
		_in_thread(lambda: qpm.defer_local_retry(circuit.get_cid()))
		return runtime

	monkeypatch.setattr(qpm, "fail_provider_submission", fail_then_defer)
	_in_thread(lambda: _finish(qpm, first["cid"]))

	assert _outcome(qpm, second["cid"]) == "FAILED"
	assert qpm.free_hosts == {"localhost": 1}


def test_a_refused_start_keeps_its_task_from_other_threads(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first, second = _one_running_one_queued(qpm)

	def refuse(task_id):
		raise RuntimeError("failed to mark task started: rc=-7")

	monkeypatch.setattr(
		qpm.controller.scheduler_context, "task_started", refuse)
	fail = qpm.fail_provider_submission
	seen = []

	def look_then_fail(circuit, error):
		# Between the scheduler's refusal and the failure being
		# recorded, another thread looks for work to dispatch.
		select = qpm.controller.select_qtask_for_dispatch
		seen.append(_in_thread(select))
		return fail(circuit, error)

	monkeypatch.setattr(qpm, "fail_provider_submission", look_then_fail)
	_in_thread(lambda: _finish(qpm, first["cid"]))

	# The task was still the dispatching thread's, so there was nothing
	# for the other to take.
	assert seen == [None]
	assert _outcome(qpm, second["cid"]) == "FAILED"
	assert qpm.controller.capacity_holds == {}
	assert qpm.free_hosts == {"localhost": 1}


def test_an_ended_task_is_not_authorized_again(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	_, second = _one_running_one_queued(qpm)
	circuit = qpm.circuits[second["cid"]]
	qpm.controller.cancel_task(
		cid=second["cid"], reservation_id="1", reason="caller")

	# A thread that still has the circuit asks for capacity for it again.
	with pytest.raises(QPMTaskNotActive):
		qpm.controller.authorize_capacity_hold(circuit)

	assert second["qtask_id"] not in qpm.controller.capacity_holds
	assert _outcome(qpm, second["cid"]) == "CANCELLED"


def test_a_failure_after_the_slot_is_taken_gives_it_back(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	attach = qpm.controller.attach_provider_credential
	refuse = [True]

	def attach_or_refuse(circuit):
		if refuse:
			raise RuntimeError("no credential for the reservation")
		return attach(circuit)

	monkeypatch.setattr(
		qpm.controller, "attach_provider_credential", attach_or_refuse)
	response = _run(qpm)

	assert response["outcome"] == "FAILED"
	assert response["error"]["error"] == "no credential for the reservation"
	assert qpm.fake_qrc.async_cids == []
	assert qpm.free_hosts == {"localhost": 1}

	# With the slot back, the next job runs.
	refuse.clear()
	following = _run(qpm)
	assert qpm.fake_qrc.async_cids == [following["cid"]]


def test_a_task_retired_meanwhile_has_no_state_to_set(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	first = _run(qpm)
	qtask_id = first["qtask_id"]
	_finish(qpm, first["cid"])

	# A caller that looked the task up just before its provider finished
	# it, as async_run's retry path did, now finds nothing to change.
	assert qpm.controller.task_for_qtask_id(qtask_id) is None
	assert qpm.controller.set_task_state(qtask_id, QPM_TASK_FAILED) is None
	assert qpm.controller.record_timeout(qtask_id, reason="late") is None


def test_one_thread_drains_and_a_later_request_is_not_lost(monkeypatch):
	_setup(monkeypatch)
	qpm = OneSlotQPM()
	retry = qpm.controller.retry_pending_capacity
	entered = threading.Event()
	release = threading.Event()
	passes = []

	def first_pass_waits(*args, **kwargs):
		# Each drain pass starts here. The first waits while another
		# thread asks to drain.
		name = threading.current_thread().name
		passes.append((name, release.is_set()))
		if len(passes) == 1:
			entered.set()
			release.wait(5)
		return retry(*args, **kwargs)

	monkeypatch.setattr(
		qpm.controller, "retry_pending_capacity", first_pass_waits)
	drainer = threading.Thread(target=qpm.process_oor_queue, name="first")
	drainer.start()
	assert entered.wait(5)
	# The second caller leaves a request and returns, rather than drain
	# alongside the first.
	_in_thread(qpm.process_oor_queue)
	release.set()
	drainer.join(5)

	assert not drainer.is_alive()
	# The first thread makes the pass the second asked for, after its own.
	assert passes == [("first", False), ("first", True)]
