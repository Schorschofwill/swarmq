"""R3.7 Progress tracking — typed client-side view of a task's HASH state.

The worker-side writer (``Task.update_progress``) stores progress as a
Valkey HASH under ``sq:progress:{task_id}``. Every field is stringified
so the data is inspectable via ``valkey-cli HGETALL`` without the
consumer needing a SwarmQ client. :class:`ProgressInfo` is the typed
wrapper the :class:`Client` surface returns to Python callers.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProgressInfo:
    """Client-side view of one task's progress HASH.

    Fields mirror the Valkey HASH layout but are typed for convenient
    Python consumption. ``percent`` is always computed from current +
    total at write time — storing it in the HASH lets operators run a
    one-field HGET without decoding the whole struct.

    ``started_at`` is an ISO-8601 string set when the worker begins
    execution; ``updated_at`` is refreshed on every surfaced update.
    ``eta_seconds`` is ``None`` when no meaningful estimate is
    possible: current == 0, total <= 0, or the final update
    (``current == total``). Gate completion on ``percent == 100.0``
    rather than ``eta_seconds is not None``.

    ``percent`` is stored as a 2-decimal-place rounded value. Callers
    re-deriving from current/total may see a difference of up to 0.005.
    """

    current: int
    total: int
    percent: float
    message: str
    updated_at: str
    started_at: str | None = None
    eta_seconds: int | None = None

    @classmethod
    def from_hash(
        cls, raw: dict[bytes, bytes] | dict[str, str],
    ) -> ProgressInfo | None:
        """Decode a Valkey HGETALL response into a ProgressInfo.

        Accepts both ``dict[bytes, bytes]`` (default valkey-py
        response) and ``dict[str, str]`` (already-decoded fixture
        data). Returns ``None`` for an empty mapping — the caller
        uses that to distinguish "no progress recorded" from
        "progress present".

        Raises ``ValueError`` when required fields are missing or
        non-parseable — pins the contract so silent decode drift is
        caught immediately in tests rather than surfacing as a
        ``None`` to a downstream consumer.
        """
        # Fuzz-Fund: ein Pub/Sub-Payload, der
        # als JSON-Skalar dekodiert (int/str/list/bool), erreichte
        # ``raw.items()`` und schlug als AttributeError durch —
        # watch_progress' Drop-Contract fängt nur ValueError. Nicht-
        # Mappings werden jetzt als ValueError abgewiesen.
        if not isinstance(raw, dict):
            if raw is None:
                return None
            raise ValueError(
                "progress payload must be a mapping, got "
                f"{type(raw).__name__}"
            )
        if not raw:
            return None

        # Normalise the dual-shape dict into a single str->str view.
        # Bytes-keyed entries (valkey-py default) and str-keyed entries
        # (test fixtures) are both accepted; both are decoded once here
        # so the rest of the method deals only with strs.
        normalised: dict[str, str] = {}
        for k, v in raw.items():
            k_str = k.decode("utf-8") if isinstance(k, bytes) else k
            v_str = v.decode("utf-8") if isinstance(v, bytes) else v
            normalised[k_str] = v_str

        def _decode(key: str) -> str | None:
            return normalised.get(key)

        current_raw = _decode("current")
        total_raw = _decode("total")
        percent_raw = _decode("percent")
        if current_raw is None or total_raw is None or percent_raw is None:
            raise ValueError(
                f"progress HASH missing required fields; got keys {list(raw)}"
            )

        eta_raw = _decode("eta_seconds")
        eta_seconds: int | None = (
            int(eta_raw) if eta_raw is not None and eta_raw != "" else None
        )

        return cls(
            current=int(current_raw),
            total=int(total_raw),
            percent=float(percent_raw),
            message=_decode("message") or "",
            updated_at=_decode("updated_at") or "",
            started_at=_decode("started_at") or None,
            eta_seconds=eta_seconds,
        )
