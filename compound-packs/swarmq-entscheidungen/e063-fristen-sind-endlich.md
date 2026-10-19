---
title: Frist-, TTL- und Zeitfelder der Konfiguration müssen endlich sein; inf und nan sind ein Konfigurationsfehler, unbegrenzt heißt none
applies_when:
  - ein neues Frist-, TTL- oder Zeitfeld kommt in SwarmQConfig oder als Env-Wert dazu
  - Änderungen an config_validation (_POSITIVE_FIELDS, _COUNT_FIELDS, _validate_bounds, MAX_DEADLINE_S) oder an retry_max_delay
  - eine Fehlersuche meldet, dass SWARMQ_CONSUMER_TIMEOUT_S=inf oder ein anderer unendlicher Wert mit Exit 78 abgelehnt wird
  - eine Fehlersuche meldet OverflowError oder Exit 1 statt 78 für einen sehr großen Konfigurationswert
  - ein neues Ganzzahl- oder Fristfeld kommt dazu (Obergrenze seines Ziels festlegen)
  - Änderungen an den Fristfeldern von TaskInfo (result_ttl, max_age, unique_for, hard_timeout) oder am Aufruf-Parameter unique_for (Client, CLI)
  - Änderungen an soft_timeout oder hard_timeout in TaskInfo, an check_deadline_bound oder check_positive_seconds
  - eine Fehlersuche meldet, dass soft_timeout=0, hard_timeout=False oder ein negativer Timeout beim Import abgelehnt wird
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
  - 'Georg 2026-10-19 (Entscheidungsvorlage 0.13.0, Punkt 2: TaskInfo-Grenze in 0.13.0rc2; Punkt 4b/4c bestätigt)'
  - docs/befunde/entscheidungsvorlage-0-13-0-2026-10-19.md (Punkte 2, 4b, 4c)
  - 'Georg 2026-10-19 (Gegenprüfung 0.13.1, Punkt A: Untergrenze und Endlichkeit für soft_timeout und hard_timeout)'
  - docs/befunde/fehlersuche-runde-4-2026-10.md (Deferred und Verdachte, soft_timeout/hard_timeout)
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
  - tests/unit/test_taskinfo_deadline_bounds.py::test_result_ttl_just_above_the_bound_names_the_field
  - tests/unit/test_taskinfo_deadline_bounds.py::test_max_age_just_above_the_bound_names_the_field
  - tests/unit/test_taskinfo_deadline_bounds.py::test_hard_timeout_just_above_the_bound_names_the_field
  - tests/unit/test_taskinfo_deadline_bounds.py::test_unique_for_just_above_the_bound_names_the_field
  - tests/unit/test_taskinfo_deadline_bounds.py::test_deadline_exactly_at_the_bound_is_valid
  - tests/unit/test_taskinfo_deadline_bounds.py::test_float_just_above_the_bound_is_rejected_after_rounding_up
  - tests/unit/test_taskinfo_deadline_bounds.py::test_float_just_above_the_bound_is_rejected_without_rounding
  - tests/unit/test_taskinfo_deadline_bounds.py::test_non_finite_deadline_is_rejected
  - tests/unit/test_taskinfo_deadline_bounds.py::test_nan_soft_timeout_next_to_hard_timeout_is_rejected
  - tests/unit/test_taskinfo_deadline_bounds.py::test_soft_timeout_alone_must_be_a_positive_number
  - tests/unit/test_taskinfo_deadline_bounds.py::test_hard_timeout_must_be_positive
  - tests/unit/test_taskinfo_deadline_bounds.py::test_timedelta_timeout_names_the_field
  - tests/unit/test_taskinfo_deadline_bounds.py::test_false_timeout_is_zero_and_rejected
  - tests/unit/test_taskinfo_deadline_bounds.py::test_true_timeout_stays_one_second
  - tests/unit/test_taskinfo_deadline_bounds.py::test_soft_timeout_alone_has_no_upper_bound
  - tests/unit/test_taskinfo_deadline_bounds.py::test_smallest_positive_timeout_is_valid
  - tests/unit/client/test_call_unique_for_deadline_bound.py::test_schedule_rejects_unique_for_above_the_bound
  - tests/unit/client/test_call_unique_for_deadline_bound.py::test_schedule_many_rejects_unique_for_above_the_bound
  - tests/unit/client/test_call_unique_for_deadline_bound.py::test_schedule_accepts_unique_for_at_the_bound
  - tests/unit/client/test_call_unique_for_deadline_bound.py::test_schedule_many_accepts_unique_for_at_the_bound
  - tests/unit/test_cli.py::TestScheduleAddUniqueFor::test_unique_for_above_the_deadline_bound_is_a_usage_error
---

**Entscheidung.** Jedes Frist-, TTL- und Zeitfeld der Konfiguration (alle `_POSITIVE_FIELDS`, dazu `retry_max_delay` und gleichartige Felder) muss endlich sein. `inf` und `nan` ergeben `ConfigurationError` (Exit 78), als Argument wie als Env-Wert. Wo „unbegrenzt“ vorgesehen ist, heißt das `none`, nicht `inf`. Neue Felder dieser Art bekommen die Prüfung mit.

**Begründung.** `inf` und `nan` bestehen die Prüfung `<= 0`. Die Werte werden in ganze Millisekunden umgerechnet oder an Broker und Valkey gegeben; `inf` warf dann `OverflowError` vor jedem Claim oder ließ `aio_pika.Message` scheitern (F3-04). Für `lock_wait_max_s` galt das schon seit Review MR 13 (2962bf2); die Regel verallgemeinert es.

**Bewusste Folgen.** `SWARMQ_..._S=inf` als Ersatz für „nie“ wird abgelehnt. Das ist kein Fehler; die Doku nennt `none`, wo es das gibt.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; Produktfrage A: PF-10, PF-11, F3-38): endlich und innerhalb der technischen Grenze des Ziels.** Jeder Wert der Konfiguration muss nicht nur endlich sein, sondern auch innerhalb der technischen Grenze seines Ziels liegen; sonst `ConfigurationError` mit dem Feldnamen (Exit 78, E-057), als Argument wie als Env-Wert. Grenzen: Fristen und TTLs als Millisekunden ≤ 2⁶³−1 (Valkey `PX`, gegen den Teststack prüfen, damit die Zahl in der Doku stimmt), `concurrency` ≤ 65535 (F3-38), Ganzzahlfelder ≤ `float_info.max` (sonst scheitert schon `math.isfinite`, PF-10). Vorbild ist `valid_drain_timeout` (`logging_settings.py:206-216`, `threading.TIMEOUT_MAX`), festgeschrieben in `tests/unit/test_logging_settings.py::TestValidation::test_non_positive_non_numeric_or_infinite_drain_timeout_is_a_configuration_error` (Fall `1e10`). Die Tests für die übrigen Felder sind mit dem Fix gekommen (0.13.0, U16: PF-10, PF-11, F3-38; siehe unten und `tests:`).

Umgesetzt in 0.13.0 (U16): Gemessen gegen den Teststack ist die kleinere Grenze die von RabbitMQ (4.3.6): Per-Message-`expiration` bis 315 360 000 000 ms (zehn Jahre), eine Millisekunde mehr schließt den Kanal mit `PRECONDITION_FAILED`; Valkey nimmt `PX` bis 2⁶³−1 minus der Unix-Zeit an. Darum gilt für alle Fristen und TTLs `MAX_DEADLINE_S = 315 360 000` s (`config_validation.py`), auch für `result_ttl` und `scheduler_publish_stepdown_s`. Zählfelder, die an RabbitMQ gehen, höchstens 2⁶³−1 (AMQP `long long`; `MAX_BROKER_COUNT` in `config_validation.py`): das betrifft `delivery_limit` (`x-delivery-limit`); RabbitMQ nimmt 2⁶³−1 an, 2⁶³ ließ schon pamqp mit `TypeError` scheitern (Exit 1 statt 78). Die übrigen Zählfelder gehen nicht an den Broker und behalten `float_info.max`.

**Nachtrag 2026-10-19 (Georg, Entscheidungsvorlage 0.13.0, Punkte 2, 4b und 4c): dieselbe Grenze für `TaskInfo` und den Aufruf-Parameter `unique_for`.** Bestätigt sind die Entscheidungen des Orchestrators vom 2026-10-18: eine Grenze `MAX_DEADLINE_S` (zehn Jahre) für jede Frist und TTL, auch für Felder, die nur als Valkey-TTL enden (4b), und `delivery_limit` höchstens 2⁶³−1 mit `ConfigurationError` (Exit 78) darüber (4c). Neu (Folge-Lücke aus dem Abschluss-Review 0.13.0): `TaskInfo.result_ttl`, `max_age`, `unique_for` und `hard_timeout` lehnen Werte über `MAX_DEADLINE_S` mit `ValueError` ab, beim Import des Task-Moduls wie die übrigen `TaskInfo`-Prüfungen (E-065; Exit 1 nach E-057, nicht 78); bei `result_ttl` und `unique_for` wird ein `float` vorher aufgerundet (E-055, siehe Nachtrag unten). Dieselbe Grenze gilt für den Aufruf-Parameter `unique_for` von `schedule()` und `schedule_many()` (`ValueError` vor jedem Claim, Publish oder Speichern, wie E-056) und für `swarmq schedule add --unique-for` (Aufruffehler, Exit 2). Der genaue Grenzwert ist erlaubt. Gemeinsame Prüfung: `config_validation.check_deadline_bound`. Vorher scheiterte etwa `result_ttl=sys.maxsize` („für immer“) erst beim Speichern (`PX` über int64, `BackendResponseError`); der Worker stellte neu zu und der Task lief bei jeder Zustellung erneut bis zum Zustelllimit. Breaking gegenüber 0.13.0rc1 und deshalb in 0.13.0 (rc2), nicht in einem 0.13.x-Patch. `soft_timeout` bleibt ohne eigene Grenze; gesetzt neben `hard_timeout` ist es durch `hard_timeout > soft_timeout` begrenzt.

**Nachtrag 2026-10-19, Abschluss-Review 0.13.0rc2: `nan` und genaue Rundung.** Die Regel „inf und nan … abgelehnt“ gilt auch für die `TaskInfo`-Fristen: `check_deadline_bound` lehnt nicht endliche Werte mit `ValueError` ab („`<feld>` must be a finite number of seconds“). Vorher bestand `nan` den Vergleich mit der Grenze; `max_age=nan` ließ jede Zustellung als abgelaufen in die DLQ laufen, `hard_timeout=nan` und `-inf` wurden angenommen. `soft_timeout=nan` neben `hard_timeout` fällt unter `hard_timeout > soft_timeout` durch (der Vergleich ist für `nan` falsch). Aufgerundet (E-055) werden nur `result_ttl` und `unique_for`; `max_age` und `hard_timeout` werden so geprüft, wie sie übergeben sind. Eigene Grenzen für `soft_timeout` allein und eine Untergrenze für `hard_timeout` legt diese Regel nicht fest.

**Nachtrag 2026-10-19 (Georg, Gegenprüfung 0.13.1, Punkt A): Untergrenze und Endlichkeit für `soft_timeout` und `hard_timeout`.** Der Satz oben „Eigene Grenzen für `soft_timeout` allein und eine Untergrenze für `hard_timeout` legt diese Regel nicht fest“ ist damit ergänzt. `TaskInfo` lehnt in beiden Feldern beim Import des Task-Moduls ab: `nan`, `-inf`, Werte `<= 0` (`ValueError` „`<feld>` must be > 0 seconds“; für `hard_timeout` bei `nan`/`-inf` weiter „must be a finite number of seconds“) und Nicht-Zahlen wie `timedelta` (`TypeError` „`<feld>` must be a number of seconds“). Beides bleibt Exit 1 beim Import (E-057). `soft_timeout=inf` und große `soft_timeout`-Werte bleiben erlaubt („nie“ in `asyncio.wait_for`); für `hard_timeout` gilt weiter die Obergrenze `MAX_DEADLINE_S`. Die Relation `hard_timeout > soft_timeout` bleibt und wird vor der Untergrenze von `soft_timeout` geprüft, damit `soft_timeout=nan` neben `hard_timeout` die bisherige Meldung behält. Ort: `config_validation.check_deadline_bound(..., positive=True)` und `check_positive_seconds` (geprüft wird `not value > 0`, damit `nan` durchfällt).

Begründung: Keiner der abgelehnten Werte funktionierte. `soft_timeout=nan` zerstört die Heap-Ordnung der Timer des Event-Loops und kann ihn abstürzen lassen (`select(nan)` wirft `ValueError`, `run_forever` bricht ab). `soft_timeout <= 0` und `hard_timeout <= 0` brechen jeden Lauf sofort ab; ab `hard_timeout=-30` mit `lock` ist die Lock-TTL (`hard_timeout + 30 s`) `<= 0`, und die Nachricht kreist bis zum Zustelllimit.

`bool`: E-055 lässt `bool` unverändert (`True` wirkt als 1 s); das gilt auch hier. `False` ist der Wert 0 und fällt deshalb unter die Untergrenze, wie schon `unique_for=False` (E-056) und `max_age=False`. Das ist mit E-055 vereinbar: E-055 regelt nur, dass `bool` nicht umgewandelt wird, nicht, dass jeder `bool` gültig ist. „Kein Timeout“ heißt `None`; `hard_timeout=False` brach bisher jeden Lauf sofort ab, weil der Worker nur `is not None` prüft.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
