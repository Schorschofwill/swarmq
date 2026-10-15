---
title: Frist-, TTL- und Zeitfelder der Konfiguration müssen endlich sein; inf und nan sind ein Konfigurationsfehler, unbegrenzt heißt none
applies_when:
  - ein neues Frist-, TTL- oder Zeitfeld kommt in SwarmQConfig oder als Env-Wert dazu
  - Änderungen an config_validation (_POSITIVE_FIELDS, _validate_finite) oder an retry_max_delay
  - eine Fehlersuche meldet, dass SWARMQ_CONSUMER_TIMEOUT_S=inf oder ein anderer unendlicher Wert mit Exit 78 abgelehnt wird
tags: [konfiguration, validierung, fristen, f3-04]
id: E-063
datum: 2026-10-15
status: gilt
quelle:
  - Orchestrator nach Gegenprüfung G1–G4 (Fehlersuche Runde 3, F3-04), Georg kann widersprechen
  - Commit 2962bf2 (Review MR 13, P2-3, lock_wait_max_s muss endlich sein)
  - src/swarmq/config_validation.py:155-160 (_validate_finite, Begründung ms-Umrechnung)
  - website/reference/configuration.md:49-56, 66, 72, 118, 129 (Must be > 0; none statt inf für unbegrenzt)
tests: []
ohne_test: Test entsteht mit dem Fix in 0.12.3 (F3-04, tests/unit/test_befund_f3_04_finite_deadlines.py). Kandidaten ohne Marker für den bestehenden Teil sind tests/unit/test_config_lock_wait.py::test_must_be_finite, tests/unit/test_config_stepdown.py::test_negative_or_infinite_is_rejected und tests/unit/test_logging_settings.py::TestValidation::test_non_positive_non_numeric_or_infinite_drain_timeout_is_a_configuration_error.
---

**Entscheidung.** Jedes Frist-, TTL- und Zeitfeld der Konfiguration (alle `_POSITIVE_FIELDS`, dazu `retry_max_delay` und gleichartige Felder) muss endlich sein. `inf` und `nan` ergeben `ConfigurationError` (Exit 78), als Argument wie als Env-Wert. Wo „unbegrenzt“ vorgesehen ist, heißt das `none`, nicht `inf`. Neue Felder dieser Art bekommen die Prüfung mit.

**Begründung.** `inf` und `nan` bestehen die Prüfung `<= 0`. Die Werte werden in ganze Millisekunden umgerechnet oder an Broker und Valkey gegeben; `inf` warf dann `OverflowError` vor jedem Claim oder ließ `aio_pika.Message` scheitern (F3-04). Für `lock_wait_max_s` galt das schon seit Review MR 13 (2962bf2); die Regel verallgemeinert es.

**Bewusste Folgen.** `SWARMQ_..._S=inf` als Ersatz für „nie“ wird abgelehnt. Das ist kein Fehler; die Doku nennt `none`, wo es das gibt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
