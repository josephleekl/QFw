import pytest

import util.qpm.util_qpm as util_qpm
from defw_exception import DEFwExecutionError
from tests.mock.fakes import FakeSchedulerContext
from tests.mock.test_fake_iqm_qpm import FakeAdmissionContext
from svc_backline_qpm.svc_qpm import BACKLINE_TARGET_ID, QPM
from svc_backline_qpm.svc_qrc import QRC
from util.qpm.controller import _clear_target_controllers_for_tests


INVENTORY = """
controller:
  - id: local-cpu-ctrl
    hardware: cpu
    devices:
      - null.qubit
    max_wires: 64
    transports:
      - memcpy
coprocessor:
  - id: local-cpu-coproc
    hardware: cpu
    transports:
      - memcpy
decoder:
  - id: steane-cpu
    code: steane
    hardware: cpu
    symbol: steane_coprocessor
    lib: ${CATALYST_LIB}/libsteane_coprocessor_cpu.so
"""


def _qpm(monkeypatch, tmp_path):
	_clear_target_controllers_for_tests()
	monkeypatch.setenv("QFW_QPM_ASSIGNED_HOSTS", "localhost:1")
	monkeypatch.setattr(util_qpm, "qpm_initialized", True)
	(tmp_path / "libsteane_coprocessor_cpu.so").touch()
	inventory = tmp_path / "inventory.yaml"
	inventory.write_text(INVENTORY, encoding="utf-8")
	monkeypatch.setenv("CATALYST_LIB", str(tmp_path))
	monkeypatch.setenv("QFW_BACKLINE_INVENTORY", str(inventory))
	return QPM(
		admission_context_factory=FakeAdmissionContext,
		scheduler_context_factory=FakeSchedulerContext,
	)


def _request(job="job-backline", **intent_overrides):
	intent = {
		"version": 1,
		"controller": {"role": "qpu_control", "kind": "cpu"},
		"coprocessors": [{"role": "qec_decoder", "kind": "cpu",
				  "decoder": "steane"}],
		"transport": {"preferred": "memcpy"},
		"qec": {"code": "steane"},
	}
	intent.update(intent_overrides)
	return {
		"owner": {"user": "backline-user"},
		"job_id": job,
		"scope_id": "allocation-1",
		"target_device_id": BACKLINE_TARGET_ID,
		"walltime_ns": 1_000_000_000,
		"task_class": {"qubit_count": 3, "depth": 3, "shots": 10},
		"resource_intent": intent,
	}


def test_reserve_returns_placement_and_get_reservation_carries_it(
		monkeypatch, tmp_path):
	qpm = _qpm(monkeypatch, tmp_path)

	decision = qpm.reserve(request=_request())

	assert decision["status"] == "accepted", decision
	placement = decision["placement"]
	assert placement["controller"]["device"] == {
		"name": "null.qubit", "wires": 3}
	assert placement["coprocessors"][0]["function"] == {
		"symbol": "steane_coprocessor",
		"lib_path": str(tmp_path / "libsteane_coprocessor_cpu.so"),
	}
	assert placement["transport"] == "memcpy"
	reservation = qpm.get_reservation(
		reservation_id=decision["reservation_id"])
	assert reservation["placement"] == placement
	# The intent is not forwarded into QFw's admission request.
	admission = qpm.controller.admission_context
	assert "resource_intent" not in admission.requests[-1]


def test_components_are_exclusive_until_release(monkeypatch, tmp_path):
	qpm = _qpm(monkeypatch, tmp_path)
	first = qpm.reserve(request=_request("job-a"))
	assert first["status"] == "accepted", first

	second = qpm.reserve(request=_request("job-b"))
	assert second["status"] == "rejected"
	assert second["reason"] == "capacity-exhausted"

	assert qpm.release(
		reservation_id=first["reservation_id"])["status"] == "accepted"
	assert "placement" not in qpm.get_reservation(
		reservation_id=first["reservation_id"])
	third = qpm.reserve(request=_request("job-c"))
	assert third["status"] == "accepted", third


def test_unsupported_intent_is_rejected_before_admission(
		monkeypatch, tmp_path):
	qpm = _qpm(monkeypatch, tmp_path)
	admission = qpm.controller.admission_context

	decision = qpm.reserve(
		request=_request(qec={"code": "surface_code"}))

	assert decision["status"] == "rejected"
	assert decision["reason"] == "unsupported-qec-code"
	assert admission.requests == []
	assert qpm.evaluate(request=_request(
		qec={"code": "surface_code"}))["reason"] == "unsupported-qec-code"


def test_reserve_without_intent_uses_the_service_default(
		monkeypatch, tmp_path):
	# The qfw-slurm gateway sends no resource_intent: the service name is the
	# intent, and the QPM applies its default.
	qpm = _qpm(monkeypatch, tmp_path)
	request = _request()
	del request["resource_intent"]

	decision = qpm.reserve(request=request)

	assert decision["status"] == "accepted", decision
	assert decision["placement"]["qec_code"] == "steane"


def test_query_advertises_backline_capabilities(monkeypatch, tmp_path):
	qpm = _qpm(monkeypatch, tmp_path)
	properties = qpm.query()["properties"]

	assert properties["provider"] == "backline"
	assert properties["qec_codes"] == ["steane"]
	assert properties["transports"] == ["memcpy"]
	assert properties["placement_version"] == 1


def test_qrc_refuses_execution():
	qrc = QRC()
	with pytest.raises(DEFwExecutionError, match="client-executed"):
		qrc.sync_run(None)
	with pytest.raises(DEFwExecutionError, match="client-executed"):
		qrc.async_run(None)
