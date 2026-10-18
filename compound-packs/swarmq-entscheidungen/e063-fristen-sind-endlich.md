---
title: Frist-, TTL- und Zeitfelder der Konfiguration müssen endlich sein; inf und nan sind ein Konfigurationsfehler, unbegrenzt heißt none
applies_when:
  - ein neues Frist-, TTL- oder Zeitfeld kommt in SwarmQConfig oder als Env-Wert dazu
  - Änderungen an config_validation (_POSITIVE_FIELDS, _COUNT_FIELDS, _validate_bounds, MAX_DEADLINE_S) oder an retry_max_delay
  - eine Fehlersuche meldet, dass SWARMQ_CONSUMER_TIMEOUT_S=inf oder ein anderer unendlicher Wert mit Exit 78 abgelehnt wird
  - eine Fehlersuche meldet OverflowError oder Exit 1 statt 78 für einen sehr großen Konfigurationswert
  - ein neues Ganzzahl- oder Fristfeld kommt dazu (Obergrenze seines Ziels festlegen)
tags: [konfiguration, validierung, fristen, f3-04]
id: E-063
datum: 2026-10-15
status: gilt
quelle:
  - Orchestrator nach Gegenprüfung G1–G4 (Fehlersuche Runde 3, F3-04), Georg kann widersprechen
  - Commit 2962bf2 (Review MR 13, P2-3, lock_wait_max_s muss endlich sein)
  - src/swarmq/config_validation.py:155-160 (_validate_finite, Begründung ms-Umrechnung)
  - website/reference/configuration.md:49-56, 66, 72, 118, 129 (Must be > 0; none statt inf für unbegrenzt)
  - Georg 2026-10-17 (Fehlersuche Runde 4, Produktfrage A, PF-10, PF-11, F3-38)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-10, PF-11)
tests:
  - tests/unit/test_config_lock_wait.py::test_must_be_finite
  - tests/unit/test_config_stepdown.py::test_negative_or_infinite_is_rejected
  - tests/unit/test_logging_settings.py::TestValidation::test_non_positive_non_numeric_or_infinite_drain_timeout_is_a_configuration_error
  - tests/unit/test_befund_f3_04_finite_deadlines.py::test_deadline_field_must_be_finite_kwarg
  - tests/unit/test_befund_f3_04_finite_deadlines.py::test_deadline_field_must_be_finite_env
  - tests/unit/test_config_bounds.py::test_shutdown_timeout_with_310_digits_names_the_field
  - tests/unit/test_config_bounds.py::test_huge_retry_max_delay_is_rejected_before_the_derived_fields
  - tests/unit/test_config_bounds.py::test_lock_wait_max_s_far_above_the_bound_names_the_field
  - tests/unit/test_config_bounds.py::test_concurrency_above_the_amqp_short_is_rejected
  - tests/unit/test_config_bounds.py::test_count_field_above_float_max_names_the_field
  - tests/unit/test_config_bounds.py::test_deadline_exactly_at_the_bound_is_valid
  - tests/unit/test_config_bounds.py::test_deadline_just_above_the_bound_names_the_field
  - tests/unit/test_config_bounds.py::test_delivery_limit_above_the_amqp_long_long_names_the_field
  - tests/unit/test_config_bounds.py::test_delivery_limit_bound_from_env
  - tests/integration/broker/test_expiration_bounds_rabbitmq.py::test_expiration_one_millisecond_above_the_bound_is_rejected
  - tests/integration/backend/test_deadline_bounds_valkey.py::test_px_at_the_bound_is_accepted
---

**Entscheidung.** Jedes Frist-, TTL- und Zeitfeld der Konfiguration (alle `_POSITIVE_FIELDS`, dazu `retry_max_delay` und gleichartige Felder) muss endlich sein. `inf` und `nan` ergeben `ConfigurationError` (Exit 78), als Argument wie als Env-Wert. Wo „unbegrenzt“ vorgesehen ist, heißt das `none`, nicht `inf`. Neue Felder dieser Art bekommen die Prüfung mit.

**Begründung.** `inf` und `nan` bestehen die Prüfung `<= 0`. Die Werte werden in ganze Millisekunden umgerechnet oder an Broker und Valkey gegeben; `inf` warf dann `OverflowError` vor jedem Claim oder ließ `aio_pika.Message` scheitern (F3-04). Für `lock_wait_max_s` galt das schon seit Review MR 13 (2962bf2); die Regel verallgemeinert es.

**Bewusste Folgen.** `SWARMQ_..._S=inf` als Ersatz für „nie“ wird abgelehnt. Das ist kein Fehler; die Doku nennt `none`, wo es das gibt.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; Produktfrage A: PF-10, PF-11, F3-38): endlich und innerhalb der technischen Grenze des Ziels.** Jeder Wert der Konfiguration muss nicht nur endlich sein, sondern auch innerhalb der technischen Grenze seines Ziels liegen; sonst `ConfigurationError` mit dem Feldnamen (Exit 78, E-057), als Argument wie als Env-Wert. Grenzen: Fristen und TTLs als Millisekunden ≤ 2⁶³−1 (Valkey `PX`, gegen den Teststack prüfen, damit die Zahl in der Doku stimmt), `concurrency` ≤ 65535 (F3-38), Ganzzahlfelder ≤ `float_info.max` (sonst scheitert schon `math.isfinite`, PF-10). Vorbild ist `valid_drain_timeout` (`logging_settings.py:206-216`, `threading.TIMEOUT_MAX`), festgeschrieben in `tests/unit/test_logging_settings.py::TestValidation::test_non_positive_non_numeric_or_infinite_drain_timeout_is_a_configuration_error` (Fall `1e10`). Die Tests für die übrigen Felder sind mit dem Fix gekommen (0.13.0, U16: PF-10, PF-11, F3-38; siehe unten und `tests:`).

Umgesetzt in 0.13.0 (U16): Gemessen gegen den Teststack ist die kleinere Grenze die von RabbitMQ (4.3.6): Per-Message-`expiration` bis 315 360 000 000 ms (zehn Jahre), eine Millisekunde mehr schließt den Kanal mit `PRECONDITION_FAILED`; Valkey nimmt `PX` bis 2⁶³−1 minus der Unix-Zeit an. Darum gilt für alle Fristen und TTLs `MAX_DEADLINE_S = 315 360 000` s (`config_validation.py`), auch für `result_ttl` und `scheduler_publish_stepdown_s`. Zählfelder, die an RabbitMQ gehen, höchstens 2⁶³−1 (AMQP `long long`; `MAX_BROKER_COUNT` in `config_validation.py`): das betrifft `delivery_limit` (`x-delivery-limit`); RabbitMQ nimmt 2⁶³−1 an, 2⁶³ ließ schon pamqp mit `TypeError` scheitern (Exit 1 statt 78). Die übrigen Zählfelder gehen nicht an den Broker und behalten `float_info.max`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
