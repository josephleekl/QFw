import copy
import logging
import os
import threading
from pathlib import Path

from .matcher import Reject, capabilities, load_inventory, match, rejected
from .svc_qrc import QRC
from util.qpm.admission import normalize_reservation_id
from util.qpm.util_circuit import set_max_qubits_pp
from util.qpm.util_qpm import UTIL_QPM

BACKLINE_PROVIDER = "backline"
BACKLINE_TARGET_ID = "backline-local"
DEFAULT_INVENTORY = Path(__file__).with_name("inventory.yaml")

# Applied when a reserve request carries no resource_intent, as from the
# qfw-slurm gateway: there the selected service name is the intent.
DEFAULT_INTENT = {
	"version": 1,
	"controller": {"role": "qpu_control"},
	"coprocessors": [{"role": "qec_decoder"}],
	"qec": {"code": "steane"},
}


def backline_profile(device_id, max_qubits):
	# Admission timing model. ponytail: placeholder costs from the fake IQM
	# profile; replace with measured compile and QEC-round times.
	return {
		"device_id": device_id,
		"external_device_id": BACKLINE_TARGET_ID,
		"max_qubits": max_qubits,
		"max_shots": 10_000,
		"time_span_ns": 60_000_000_000,
		"baseline": {
			"qubit_count": 3,
			"depth": 3,
			"one_q_gate_count": 1,
			"two_q_gate_count": 2,
			"measurement_count": 3,
			"shots": 10,
		},
		"one_q_gate_ns": 20,
		"two_q_gate_ns": 100,
		"measurement_ns": 1000,
		"one_q_gate_transfer_ns": 1,
		"two_q_gate_transfer_ns": 4,
		"measurement_transfer_ns": 10,
		"compile_ns": 1000,
		"control_overhead_ns": 200,
		"provider_overhead_ns": 300,
		"total_credits": 64,
		"device_rate": 512,
		"concurrent_jobs": 1,
		"default_ttl_ns": 60_000_000_000,
		"max_provider_queue_depth": 1,
	}


class QPM(UTIL_QPM):
	def __init__(self, start=True, admission_context_factory=None,
		     scheduler_context_factory=None):
		self.inventory = load_inventory(
			os.environ.get("QFW_BACKLINE_INVENTORY") or DEFAULT_INVENTORY)
		self._placements = {}
		self._placement_lock = threading.Lock()
		max_qubits = max(
			(c["max_wires"] for c in self.inventory.get("controller", [])),
			default=0)
		super().__init__(
			QRC(start=start),
			max_ppn=1,
			start=start,
			target_id=BACKLINE_TARGET_ID,
			admission_context_factory=admission_context_factory,
			scheduler_context_factory=scheduler_context_factory)
		set_max_qubits_pp(max_qubits)
		device_id = self.controller.canonicalize_external_id(
			"device_id", BACKLINE_TARGET_ID)
		self.configure_device_profile(
			profile=backline_profile(device_id, max_qubits))

	def query(self):
		from . import SERVICE_NAME, SERVICE_DESC, svc_info
		from api_qpm_common import QPMCapability, QPMType

		properties = dict(svc_info.get('properties', {}))
		properties.update(capabilities(self.inventory))
		properties.update({
			"provider": BACKLINE_PROVIDER,
			"target_id": BACKLINE_TARGET_ID,
			"device_id": BACKLINE_TARGET_ID,
			"execution": "client",
		})
		info = self.query_helper(
			QPMType.QPM_TYPE_SIMULATOR,
			QPMCapability.QPM_CAP_STATEVECTOR,
			SERVICE_NAME, SERVICE_DESC,
			properties=properties)
		logging.debug(f"Backline {SERVICE_DESC}: {info}")
		return info

	# ------------------------------------------------------------ admission

	def evaluate(self, token=None, request=None):
		if isinstance(request, dict):
			request, intent = self._split(request)
			try:
				match(self.inventory, intent, self._qubits(request),
				      self._busy())
			except Reject as r:
				return rejected(r.reason, str(r))
		return super().evaluate(token=token, request=request)

	def reserve(self, token=None, request=None):
		if not isinstance(request, dict):
			return super().reserve(token=token, request=request)
		request, intent = self._split(request)
		# Hold the lock across match and commit so two reserves cannot be
		# placed on the same inventory entry.
		with self._placement_lock:
			try:
				placement, entries = match(
					self.inventory, intent, self._qubits(request),
					self._busy())
			except Reject as r:
				return rejected(r.reason, str(r))
			decision = super().reserve(token=token, request=request)
			if decision.get("status") != "accepted":
				return decision
			rid = normalize_reservation_id(decision["reservation_id"])
			self._placements[rid] = (placement, entries)
		return dict(decision, placement=copy.deepcopy(placement))

	def release(self, token=None, reservation_id=None, reason=None):
		result = super().release(
			token=token, reservation_id=reservation_id, reason=reason)
		if result.get("status") == "accepted":
			with self._placement_lock:
				self._placements.pop(
					normalize_reservation_id(reservation_id), None)
		return result

	def get_reservation(self, token=None, reservation_id=None):
		reservation = super().get_reservation(
			token=token, reservation_id=reservation_id)
		held = self._placements.get(normalize_reservation_id(reservation_id))
		if held is not None and reservation.get("state") == "active":
			reservation["placement"] = copy.deepcopy(held[0])
		return reservation

	# ------------------------------------------------------------ internals

	@staticmethod
	def _split(request):
		request = dict(request)
		intent = request.pop("resource_intent", None)
		return request, DEFAULT_INTENT if intent is None else intent

	@staticmethod
	def _qubits(request):
		task_class = request.get("task_class")
		return task_class.get("qubit_count") if isinstance(
			task_class, dict) else None

	def _busy(self):
		# Entries held by reservations that are still active. Expired or
		# released ones are dropped here, so capacity frees lazily.
		busy = set()
		for rid in list(self._placements):
			state = super().get_reservation(reservation_id=rid).get("state")
			if state == "active":
				busy.update(self._placements[rid][1])
			else:
				self._placements.pop(rid, None)
		return busy
