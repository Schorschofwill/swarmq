"""Valkey-Keys und Pub/Sub-Kanaele aus einer Quelle (Plan 0.12.0, MR 0, Q4).

Neutral und rein: kein I/O, keine Imports aus Backend oder Broker (Q16), damit
``worker``, ``scheduler`` und ``client`` die Namen ebenso bilden koennen wie die
Valkey-Stores. Ausserhalb dieses Moduls steht kein ``sq:``-Literal im Code;
``test_architecture_guards.py`` prueft das.

Das Wire-Format ist durch ``tests/integration/backend/golden/keys.json`` und
``channels.txt`` festgeschrieben. Die Hash-Tags (``{sched}``, ``{<identity>}``,
``{<task_name>}``) legen zusammen genutzte Keys in einen Cluster-Slot.

``namespace`` ist vorbereitet (BZ-10); bis dahin gilt nur ``None``, der
heutige Stamm ``sq:``.
"""

from __future__ import annotations

from dataclasses import dataclass

_ROOT = "sq:"


@dataclass(frozen=True)
class KeySpace:
    namespace: str | None = None

    @property
    def _root(self) -> str:
        return _ROOT

    # -- Ergebnisse, Flags, Fortschritt -------------------------------------

    def result(self, task_id: str) -> str:
        return f"{self._root}result:{task_id}"

    def cancel(self, task_id: str) -> str:
        return f"{self._root}cancel:{task_id}"

    def unique(self, dedup_hash: str) -> str:
        return f"{self._root}unique:{dedup_hash}"

    def progress(self, task_id: str) -> str:
        return f"{self._root}progress:{task_id}"

    # -- Pub/Sub-Kanaele ------------------------------------------------------

    def notify_channel(self, task_id: str) -> str:
        return f"{self._root}notify:{task_id}"

    def progress_channel(self, task_id: str) -> str:
        return f"{self._root}progress:channel:{task_id}"

    def workflow_notify_channel(self, wf_id: str) -> str:
        return f"{self._root}wf:notify:{wf_id}"

    # -- Workflows ------------------------------------------------------------

    def workflow(self, wf_id: str) -> str:
        return f"{self._root}wf:{wf_id}"

    def workflow_cancel(self, wf_id: str) -> str:
        return f"{self._root}wf:cancel:{wf_id}"

    def chord(self, wf_id: str, chord_id: str) -> str:
        return f"{self._root}wf:chord:{wf_id}:{chord_id}"

    # -- Ausfuehrungsschutz ---------------------------------------------------

    def guard_run(self, identity: str) -> str:
        return f"{self._root}guard:{{{identity}}}:run"

    def guard_state(self, identity: str) -> str:
        return f"{self._root}guard:{{{identity}}}:state"

    # -- Locks und Rate-Limit -------------------------------------------------

    @property
    def lock_prefix(self) -> str:
        return f"{self._root}lock:"

    def lock(self, lock_name: str) -> str:
        return f"{self.lock_prefix}{lock_name}"

    def scheduler_lock(self) -> str:
        return self.lock("{sched}:leader")

    def rate_limit(self, task_name: str, window: int | str) -> str:
        return f"{self._root}rl:{{{task_name}}}:{window}"

    # -- Kontrollkanal --------------------------------------------------------

    def control_setpoint(self, group: str) -> str:
        return f"{self._root}control:setpoint:{group}"

    def control_status(self, group: str, supervisor_id: str) -> str:
        return f"{self._root}control:status:{group}:{supervisor_id}"

    # -- Scheduler ------------------------------------------------------------

    def sched_zset(self) -> str:
        return f"{self._root}{{sched}}:zset"

    def sched_defs(self) -> str:
        return f"{self._root}{{sched}}:defs"

    def sched_report_first_seen(self) -> str:
        return f"{self._root}{{sched}}:report:first_seen"

    def sched_report_last_seen(self) -> str:
        return f"{self._root}{{sched}}:report:last_seen"

    def sched_report_def(self) -> str:
        return f"{self._root}{{sched}}:report:def"

    def sched_report_known(self) -> str:
        """Set der je von einem Worker ab 0.12 gemeldeten Cron-Namen (BZ-11);
        ein Name faellt nur zusammen mit der Definition heraus (Q5)."""
        return f"{self._root}{{sched}}:report:known"

    def sched_leader_version(self) -> str:
        return f"{self._root}{{sched}}:leader_version"

    @property
    def sched_worker_version_prefix(self) -> str:
        return f"{self._root}{{sched}}:worker_version:"

    def sched_worker_version(self, worker_id: str) -> str:
        return f"{self.sched_worker_version_prefix}{worker_id}"

    def sched_disabled(self) -> str:
        return f"{self._root}{{sched}}:disabled"

    def sched_epoch(self) -> str:
        """Epoch des Datenbestands (BZ-04): ein Key ohne TTL (Q5-Ausnahme);
        fehlt er bei einem Prozess, der ihn kannte, hat Valkey Daten verloren."""
        return f"{self._root}{{sched}}:epoch"

    def sched_fired(self, schedfire_key: str) -> str:
        return f"{self._root}sched:fired:{schedfire_key}"

    def sched_inflight(self, schedfire_key: str) -> str:
        return f"{self._root}sched:inflight:{schedfire_key}"

    def sched_handover_latest(self) -> str:
        """Fester Handover-Marker ab 0.12 (BZ-07), Wert: Owner-Token."""
        return f"{self._root}{{sched}}:handover"

    def sched_handover(self, owner_token: str) -> str:
        return f"{self._root}sched:handover:{owner_token}"

    def sched_handover_pattern(self) -> str:
        """SCAN-Muster ueber alle Handover-Marker."""
        return self.sched_handover("*")
