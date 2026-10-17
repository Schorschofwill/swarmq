# Fuzz-Ziele (Spur F, U16)

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R20, R21, KTD8, KTD9, Einheit U16. Stand: swarmq 0.12.3 (Branch `test/fehlersuche-runde-4`), Hypothesis 6.168.0, croniter 6.2.4.

Dieser Bericht enthält Verdachte, keine Befunde. Jeder Verdacht geht in U17 durch die Gegenprüfung nach `docs/befunde/README.md`, bevor er ins Register kommt.

## Ergebnis in Kürze

- Sechs neue Fuzz-Dateien unter `tests/fuzz/` mit 27 Tests zu je 500 Beispielen. Alle laufen ohne Dienste.
- `just fuzz` fährt jetzt das Verzeichnis `tests/fuzz` ohne `test_external_publish_fuzz.py`: 31 Tests (mit `test_parser_fuzz.py`), grün nach den Review-Fixes (frisches `.hypothesis/` je Lauf): `CI=1 uv run just fuzz` (derandomisiert) 31 passed in 26,8 s; `SWARMQ_TEST_SEED=1|2|3|5|9 uv run just fuzz` je 31 passed in 23,5 bis 27,0 s. Zusätzlich 40 Hypothesis-Seeds (`--hypothesis-seed=1..40`, `-n 8`) ohne Fehlschlag.
- Zehn Verdachte R4F-01 bis R4F-10. Jeder ist in der Strategie mit einem Kommentar `F4-nn` ausgeklammert; das geschrumpfte Beispiel steht als `@example` im Kommentar daneben.
- Ohne Treffer blieben `bounded_header_text`, `retry_count_of`, die nachsichtigen Zähler-Leser (`prior_deferrals`, `broker_redelivered`, `redeliver_pause_s`), der Konfliktpfad des Lock-Gates, `execution_identity`, `error_info` und die DST-Regel von `next_fire_times`.

## Vorgehen

Jede Datei entstand zuerst mit einem Ziel und einer zu engen Erwartung, und jede schlug aus dem erwarteten Grund fehl:

| Datei | Erster roter Lauf | Grund |
|---|---|---|
| `test_header_text_fuzz.py` | Ergebnis == Eingabe | `'\ud800'` wird zu `'\\ud800'` escapt (gewollt) |
| `test_counter_headers_fuzz.py` | `retry_count_of` liefert immer 0 | `raw=1` ergibt 1 |
| `test_error_info_fuzz.py` | nur `builtins.*` | dynamische Unterklassen liegen im Testmodul |
| `test_config_env_fuzz.py` | keine Ausnahme erlaubt | `SWARMQ_CONCURRENCY=':'` ergibt `ConfigurationError` |
| `test_cron_fuzz.py` | nur `CroniterBadCronError` erlaubt | `cron='L'` ergibt den `ValueError` von `TaskInfo` |
| `test_lock_template_fuzz.py` | keine Ausnahme erlaubt | `template='{'` ergibt `ValueError` |

Danach wurden die Dateien auf die zugesagte Allow-List erweitert. Jeder Treffer wurde zusätzlich ohne Ausklammerung gegengeprüft: Eine Kopie der Datei mit aufgehobenem Filter lief rot und lieferte das geschrumpfte Beispiel. Die Kopien sind gelöscht.

Alle Einstellungen folgen `test_parser_fuzz.py`: `max_examples=500`, `deadline=None`, Marker `fuzz` über `tests/fuzz/conftest.py`. Ein Hänger fällt über `pytest.mark.timeout(60)` je Test auf. Eine Hypothesis-Deadline wurde bewusst nicht gesetzt, weil sie unter Last als `Flaky` scheitern kann (Testdeterminismus). Das Profil `quick` aus der Verification des Plans gibt es nur in `tests/property/conftest.py`, für `tests/fuzz` ist es nicht registriert.

## Laufzeiten

`SWARMQ_TEST_SEED=7 uv run just fuzz --durations=0`, Summe je Datei (Setup und Call):

| Datei | Tests | Laufzeit |
|---|---|---|
| `test_counter_headers_fuzz.py` | 8 | 4,1 s |
| `test_cron_fuzz.py` | 5 | 3,7 s |
| `test_lock_template_fuzz.py` | 2 | 2,1 s |
| `test_config_env_fuzz.py` | 4 | 1,8 s |
| `test_error_info_fuzz.py` | 4 | 1,5 s |
| `test_header_text_fuzz.py` | 4 | 0,7 s |
| `test_parser_fuzz.py` (Bestand) | 4 | 0,5 s |

Gesamt mit pytest-Start: 15 bis 27 s je nach Last (Läufe oben). Das Ziel lag bei unter 2 min.

**Hinweis:** Der Kommentar in `.gitlab-ci.yml` (Z. 170-172) beschreibt die Fuzz-Suite als „sub-second“. Das gilt nur noch für `test_parser_fuzz.py`; die CI-Datei bleibt in dieser Runde unverändert.

## Ziele

### 1. `header_text.bounded_header_text` (`test_header_text_fuzz.py`)

- **Strategien:** alle Codepoints einschließlich einzelner Surrogates (`st.characters(exclude_categories=())`), kurz (bis 64 Zeichen) und um die Grenze. Für die Grenze wird ein Baustein mit 1 bis 8 Zeichen so oft wiederholt, dass der escapte Text zwischen 64 Byte unter und 24 KiB über der Grenze liegt. Explizit geprüft: leer, `"cannot open \udcff.txt"`, genau 8192 und 8193 Byte.
- **Zusagen und Quelle:** Ergebnis höchstens `HEADER_TEXT_LIMIT_BYTES` (8 KiB) und streng UTF-8-kodierbar; kurzer Text bleibt bis auf escapte Surrogates gleich; langer Text behält ein Präfix und ein Suffix des escapten Texts und trägt den Kürzungsvermerk; der Helfer ist idempotent. Quelle: Docstring und Modulkopf von `src/swarmq/header_text.py` (BZ-17) und `tests/unit/test_header_text.py`.
- **Allow-List:** leer.
- **Ergebnis:** kein Treffer.

### 2. Zähler-Header (`test_counter_headers_fuzz.py`)

- **Strategien:** Header-Werte nach den AMQP-Feldtypen, die pamqp liefert (`pamqp.common.FieldValue`): `None`, `bool`, `int` beliebiger Größe (dazu 0, ±1, 2³¹, 2⁶³, 2⁷⁰, 10⁴⁰⁰), `float` mit `inf`/`nan`/`1e308`, endliches `Decimal`, `str`, numerische Zeichenketten wie aus der RabbitMQ-UI (`"3"`, `" 7 "`, `"١٢"`, `"1_000"`, `"1e400"`, `"9"*5000`), `bytes`, `datetime`, Listen und Dicts. Skalare überwiegen.
- **Ziele (nur öffentlich):** `execution_guard.retry_count_of`; `gates.prior_deferrals`, `gates.broker_redelivered`, `redelivery.redeliver_pause_s`; `DeliveryCountGate.apply`; `LockGate.apply` im Konfliktpfad mit Zustands-Headern der Lock-Wartezeit; `lock.wait_generation`; `identity.execution_identity` (`x-death`, Base, Replay-ID). Die privaten Funktionen `_as_int`, `_count` und `_as_count` werden nicht aufgerufen.
- **Zusagen und Quelle:** E-051 sagt: Retry-Zähler streng (nur `int` > 0), Rate-Limit- und ähnliche Zähler nachsichtig (`int()`; Unlesbares und `inf` zählen 0, negative 0). Ein unlesbarer Zähler zählt wie keiner, „die Nachricht wird normal weiterverarbeitet statt unquittiert hängen zu bleiben“. Für den Lock-Konflikt gilt: Rückstellung mit 100 ms bis 60 s (`LOCK_CONFLICT_BACKOFF_*`) oder Dead-Letter am Ende der Wartezeit. `execution_identity` deckelt n auf 12 Stellen und die Replay-ID auf 64 Zeichen (`identity.py`).
- **Allow-List:** leer.
- **Ergebnis:** ein Treffer, R4F-01. Alle anderen Leser halten E-051 ein, auch für `inf`, `nan`, `True`, `"3"`, `-2` und 2⁷⁰.

### 3. `worker/error_info` (`test_error_info_fuzz.py`)

- **Strategien:** alle eingebauten Exceptions, die sich mit einem Argument bauen lassen, und dynamische Unterklassen mit beliebigem Namen. Variiert werden `__module__` (auch `None`), `__qualname__`, `__slots__ = ()`, `__str__`/`__repr__`/`__getattr__`, die werfen, und nicht serialisierbare Attribute (Lambda, `threading.Lock`, Generator, rekursive Liste, `object()`). Dazu `RemoteTaskError` mit beliebigem `remote_type` und `gate_error_class` mit beliebigem Text.
- **Zusagen und Quelle:** `error_class_of` liefert `modul.qualname` als `str`, bei `RemoteTaskError` den `remote_type` (`"unknown"` ergibt `None`) und für markierte Worker-Fehler `None`. `mark_worker_error` gibt dasselbe Objekt zurück. Quelle: Docstrings in `src/swarmq/worker/error_info.py` (Plan 2026-10-12-1105, MR 3, T12/T17).
- **Allow-List:** leer.
- **Ergebnis:** kein Treffer. Wie robust die Fehlertexte sind (`str(exc)` wirft), liegt außerhalb dieses Moduls; siehe offene Punkte.

### 4. `SwarmQConfig()` unter `SWARMQ_*` (`test_config_env_fuzz.py`)

- **Strategien:** Je Beispiel werden 1 bis 6 Variablen aus `SwarmQConfig._ENV_OVERRIDES` gesetzt (alle 70 Namen, gelesen aus der Klasse, damit keine fehlt), alle übrigen per `MonkeyPatch.context` entfernt. Die Werte: `inf`, `nan`, `1e400`, Leerraum, Unicode-Ziffern (`٣`), `1_000`, `0x10`, `none`, Moduswörter, Logger-Level-Listen, Zonennamen, `\udcff` (surrogateescape), `"9"*5000`, Gleitkommazahlen als `repr`, Ganzzahlen bis 10³⁰⁰ und beliebiger Text. Eigene Tests gibt es für `SWARMQ_SCHEDULER_TIMEZONE` (Pfadteile bis 255 Zeichen), für `inf`/`nan` in jedem float-Feld und für die Umrechnung angenommener Fristen in Millisekunden.
- **Zusagen und Quelle:** Ein ungültiger Wert ist ein `ConfigurationError` (Exit 78, E-057), der die Variable oder das Feld nennt (E-063: „Meldung nennt das ursächliche Feld“). Plan U16 verlangt: nie roh `ValueError`/`OverflowError`. `inf`/`nan` ergeben `ConfigurationError` (E-063, E-055 Nachtrag). Angenommene Fristen lassen sich in ganze Millisekunden umrechnen (E-063, Begründung).
- **Allow-List:** `ConfigurationError`.
- **Ergebnis:** drei Treffer, R4F-02, R4F-03 und R4F-04. Alle übrigen Werte, auch `inf`/`nan` in allen 29 float-Variablen, ergeben einen `ConfigurationError` mit Namen.

### 5. Cron (`test_cron_fuzz.py`)

- **Strategien:** Standard-Ausdrücke nach crontab(5) (Zahlen im Bereich, `*`, Bereiche, Schritte ≥ 1, Listen, Monats- und Tagesnamen in beliebiger Schreibweise). Dazu Standard-Ausdrücke mit einem ersetzten Feld (`?`, `L`, `W`, `#`, `H`, `1W`, `5L`, `mon#2`, `H(0-5)`, `%s`, Zahlen von −2 bis 100, Schritte 0 bis 70) und Ausdrücke mit 1 bis 9 Feldern aus solchen Token. Getrennt wird mit Leerzeichen, Tab oder NBSP. Weiter: `@`-Aliase mit Leerraum und anderer Schreibweise sowie beliebiger Text. Zehn Zonen, darunter Lord Howe (30-min-DST), Dublin (negative DST), Santiago, St. John's, Casablanca und Chatham. Startzeiten von 2025 bis 2035; für DST Tage im März, April, September, Oktober und November.
- **Zusagen und Quelle:** E-065 sagt: 5 Felder, `L`/`W`/`#` abgelehnt, ohne nächsten Termin `ValueError` („never fires“), jeder Fehler ein `ValueError`. Standard-Syntax nach crontab(5) wird angenommen (`website/reference/configuration.md:471` „Standard 5-field cron expression“; crontab(5): Namen „case does not matter“). `next_fire_times` liefert streng monotone Termine nach dem Start, bei jedem Aufruf dieselben. Nach KTD2 (Docstring von `next_fire_times`) feuert ein Cron mit festem Stundenfeld in der Rückstellung nur im ersten Durchlauf.
- **Allow-List:** `ValueError` (die croniter-Fehler erben davon) für `TaskInfo(cron=…)`. Für `next_fire_times` mit angenommenem Ausdruck ist sie leer.
- **Ergebnis:** drei Treffer, R4F-05, R4F-06 und R4F-07 (bestätigt F3-35). Ein vierter Fehlschlag bei `--hypothesis-seed=19` (`30 * * APR 0-1`, Lord Howe) war ein Testfehler: Python vergleicht zwei aware datetimes mit demselben `ZoneInfo` nach Wanduhrzeit ohne `fold`, 01:30 (+11) und 01:30 (+10:30) galten als gleich. Die Monotonie wird jetzt über Zeitstempel geprüft; `next_fire_times` war korrekt. `0 0 30 2 *` ergibt wie zugesagt `ValueError` „never fires“. Die DST-Regel hält: Kein Cron mit festem Stundenfeld erzeugt in einer der zehn Zonen eine doppelte Wanduhrzeit, die Termine steigen streng.

### 6. Lock-Templates (`test_lock_template_fuzz.py`)

- **Strategien:** Templates aus 1 bis 4 Stücken: Platzhalter (`{key}`, `{other}`, `{key!r}`, `{key[id]}`, `{key:>3}`, `{key:d}`, `{key:.2f}`, `{missing}`, `{}`, `{0}`, `{{`, `}}`, `{`, `}`, `{key:{other}}`) und Literale (`""`, `"user-"`, `"tenant:"`, `" "`, `"sq:lock:"`, `"{sched}"`). Argumente wie aus dem Nachrichtenrumpf: `None`, `bool`, `int`, `float`, Text (auch `""`, Leerraum, `​`, `{`, `}`, `{sched}`), Listen und Dicts. Ziele sind `render_lock_name` und `LockGate.apply` mit einem Fake-Store, der jeden Namen protokolliert.
- **Zusagen und Quelle:** `render_lock_name` wirft bei einem Render-Fehler `ValueError` („caller routes the message to DLQ“, Docstring). Ein leerer oder nur aus Leerraum bestehender Name erreicht `acquire` nie, sondern geht an den Dead-Letter des Workers (Kommentar „Empty-rendered lock guard“ in `gates/lock.py`, Modulkopf KTD3; Hintergrund `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`). Ein Lock-Name aus Nutzdaten belegt keinen internen Key (`KeySpace.scheduler_lock`, `locking.SCHEDULER_LOCK_KEY`).
- **Allow-List:** `ValueError` für `render_lock_name`; für `LockGate.apply` ist sie leer.
- **Ergebnis:** drei Treffer, R4F-08, R4F-09 und R4F-10. Leere und reine Leerraum-Renders (`"{key}"` mit `"   "`, `"{key}{other}"` mit `""`, `""`) gehen wie zugesagt an den Dead-Letter; `acquire` sieht sie nie. Dass `​` nicht als Leerraum gilt, ist kein Verstoß: der Name ist ein eigener Key.

## Verdachte

Konfidenz bezieht sich darauf, wie sicher das Verhalten im Code belegt ist. Realismus schätzt, wie wahrscheinlich die Eingabe im Betrieb vorkommt.

### R4F-01 – `DeliveryCountGate` wirft `OverflowError` bei `x-delivery-count`/`x-acquired-count` = `inf`

- **Fundstelle:** `src/swarmq/worker/gates/delivery_count.py:46-54` (`_count`: `int(raw)` fängt nur `TypeError`/`ValueError`).
- **Szenario:** Eine Zustellung trägt `x-acquired-count: inf` (AMQP-Double; auch `-inf`). `DeliveryCountGate.apply` wirft `OverflowError: cannot convert float infinity to integer`. Das Gate liegt vor dem `try/finally` (`pipeline._pre_pipeline_gates`), die Ausnahme endet in `settle.contain_delivery_errors` als `delivery_handler_failed`. Kein Broker-Verb, die Nachricht bleibt unbestätigt bis `consumer_timeout` oder bis der Kanal schließt. Laut E-054 behält RabbitMQ 4 einen vom Publisher gesetzten `x-delivery-count` bei der ersten Zustellung bei. Ein Wert, den der Publisher setzt, kommt also an. Auf Classic-Queues bleibt der Header bei jeder Neuzustellung gleich; daraus wird eine Schleife, die das Zustelllimit nicht beendet, weil gerade das Gate dafür scheitert. Das Schleifenverhalten ist aus dem Code abgeleitet, nicht gegen einen Broker gefahren.
- **Beleg:** `tests/fuzz/test_counter_headers_fuzz.py::test_delivery_count_gate_never_raises` ohne F4-01 rot; geschrumpft `@example(delivered=None, acquired=float("inf"), limit=0)`.
- **Quelle der Erwartung:** E-051 („Ein Zähler-Header, der sich nicht als Zähler lesen lässt, zählt wie kein Header; die Nachricht wird normal weiterverarbeitet“; `applies_when` nennt „andere Zähler-Header“). Review U17 hat dieselbe Lücke in `_as_count` und `lock._as_int` geschlossen (`OverflowError` im `except`), `_count` blieb offen.
- **Register-Abgleich:** E-051 (gilt, wird verletzt), E-054 (Kontext). Keine Entscheidung deckt das Verhalten.
- **Einstufung:** FEHLER (E-051).
- **Konfidenz:** hoch. **Realismus:** gering (ein Publisher muss einen Double `inf` setzen).
- **Ausklammerung:** F4-01, `±inf` ausgeklammert (`nan` bleibt drin, `int(nan)` wirft `ValueError` und wird gefangen).

### R4F-02 – Zeitzone mit Pfadteil über 255 Byte ergibt `OSError` statt `ConfigurationError`/`ValueError`

- **Fundstelle:** `src/swarmq/config.py:70-86` (`normalize_timezone` fängt `ZoneInfoNotFoundError, ValueError`). Auch `src/swarmq/scheduler/cron_time.py:31-33` (`_zone`).
- **Szenario:** `SWARMQ_SCHEDULER_TIMEZONE=aaaa…` (256 Zeichen) ergibt `OSError: [Errno 36] File name too long` aus `zoneinfo`. Statt `ConfigurationError` (Exit 78) endet der Worker mit Exit 1, und der Supervisor startet neu. Dieselbe Ursache gilt für `TaskInfo(cron=…, timezone="a"*256)` (`OSError` statt des zugesagten `ValueError`, Exit 1 bleibt nach E-057) und für `next_fire_times(…, timezone="a"*256)`. Der Leader fängt das in `scheduler/sync.py` über `except Exception`.
- **Beleg:** `tests/fuzz/test_config_env_fuzz.py::test_scheduler_timezone_is_a_zone_or_a_configuration_error` ohne F4-02 (Pfadteile bis 300 Zeichen) rot; geschrumpft `@example(env={"SWARMQ_SCHEDULER_TIMEZONE": "a" * 256})`. Gegenprobe für `TaskInfo` und `next_fire_times` per Skript.
- **Quelle der Erwartung:** E-057 (Konfigurationsfehler ergeben Exit 78), E-065 (falscher Wert in `TaskInfo` ergibt `ValueError`), Docstring von `normalize_timezone` („sonst `ValueError` mit Feld- und Zonennamen“).
- **Register-Abgleich:** E-057, E-065; nichts entschieden.
- **Einstufung:** FEHLER (E-057: Exit 1 statt 78 für `SWARMQ_SCHEDULER_TIMEZONE`; bei `TaskInfo` nur kosmetisch, Exit 1 bleibt).
- **Konfidenz:** hoch. **Realismus:** sehr gering.
- **Ausklammerung:** F4-02, Pfadteile der Zeitzone höchstens 255 Byte, im Zeitzonen-Test über die Strategie und im gemischten Env-Test über den Filter `_zone_parts_fit`.

### R4F-03 – Ganzzahlfeld größer `float_info.max` ergibt `OverflowError` statt `ConfigurationError`

- **Fundstelle:** `src/swarmq/config_validation.py:187` (`_validate_finite`: `math.isfinite(value)` auf einem `int`).
- **Szenario:** `SWARMQ_SHUTDOWN_TIMEOUT` (ebenso `SWARMQ_DELIVERY_LIMIT`, `SWARMQ_LOG_QUEUE_SIZE`, `SWARMQ_MAX_MESSAGE_BODY_BYTES`, `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS`, `SWARMQ_MALLOC_*`, `SWARMQ_COMPUTE_THREAD_LIMIT`) mit 310 Stellen ergibt `OverflowError: int too large to convert to float`. Es folgen Exit 1 und Neustarts statt Exit 78.
- **Beleg:** `tests/fuzz/test_config_env_fuzz.py::test_env_gives_a_valid_config_or_a_configuration_error` ohne F4-03 (Ganzzahlen bis 400 Stellen) rot; geschrumpft `@example(env={"SWARMQ_SHUTDOWN_TIMEOUT": "1" + "0" * 309})`.
- **Quelle der Erwartung:** E-057, E-063 (Prüfung `_validate_finite` soll Konfigurationsfehler liefern), Plan U16 Approach 3 („nie `ValueError`/`OverflowError` roh“).
- **Register-Abgleich:** E-057, E-063; nichts entschieden.
- **Einstufung:** FEHLER (E-057, E-063).
- **Konfidenz:** hoch. **Realismus:** sehr gering.
- **Ausklammerung:** F4-03, Ganzzahlen höchstens 301 Stellen.

### R4F-04 – Endliche Frist über ~1,8·10³⁰⁵ s wird angenommen und läuft bei der ms-Umrechnung über

- **Fundstelle:** `src/swarmq/config_validation.py:187` (`_validate_finite`, nur `isfinite`). Folgestellen: unter anderem `src/swarmq/worker/gates/lock.py:353` (`int(lock_wait_max_s * 1000)`), `worker/gates/execution_guard.py:125, 430`, `scheduler/leader_version.py:113`, `scheduler/prune.py:125`.
- **Szenario:** `SWARMQ_LOCK_WAIT_MAX_S=1e306` (ebenso `SWARMQ_BACKEND_DEFER_MAX_DELAY=1.797e305`, `SWARMQ_EXECUTION_GUARD_TTL_S`, …) besteht die Prüfung. `wert * 1000` ist `inf`, `int()` davon wirft `OverflowError` zur Laufzeit, beim Lock-Gate bei jedem Konflikt. Die Ausnahme läuft ins `finally` der Pipeline ohne Broker-Verb. Nach demselben Muster lehnt Valkey schon deutlich kleinere Werte (über ~9,2·10¹⁵ s, `PX` > int64) mit einer Fehlerantwort ab. Das ist nicht gefahren, siehe offene Punkte.
- **Beleg:** `tests/fuzz/test_config_env_fuzz.py::test_accepted_deadlines_convert_to_milliseconds` ohne F4-04 rot; geschrumpft `@example(env={"SWARMQ_BACKEND_DEFER_MAX_DELAY": "1.797693134862316e+305"})`, auch `{"SWARMQ_LOCK_WAIT_MAX_S": "1e306"}`.
- **Quelle der Erwartung:** E-063, Begründung: „Die Werte werden in ganze Millisekunden umgerechnet …; `inf` warf dann `OverflowError`“. Wörtlich verlangt E-063 nur Endlichkeit, und die ist erfüllt. Einstufung deshalb als „Entscheidung überdenken?“ mit dem neuen Argument, dass endliche Werte denselben Fehler auslösen, den die Regel verhindern soll.
- **Register-Abgleich:** E-063 (wörtlich erfüllt, Zweck verfehlt).
- **Einstufung:** ENTSCHEIDUNG, „überdenken?“ (E-063).
- **Konfidenz:** hoch für den Überlauf, mittel für die Folgen an den genannten Stellen (nur gelesen). **Realismus:** sehr gering.
- **Ausklammerung:** F4-04, Gleitkomma-Zeichenketten höchstens `1e300`.

### R4F-05 – croniters Zufallsfeld `R` wird angenommen; jeder neue Termin würfelt neu

- **Fundstelle:** `src/swarmq/task.py:37, 225-251` (`_FORBIDDEN_CRON_CHARS = [LW#]`, kein `R`; auch kleines `r` wird angenommen: `TaskInfo(cron="r * * * *")`). `src/swarmq/scheduler/cron_time.py:89` erzeugt je Berechnung einen neuen `croniter`.
- **Szenario:** `TaskInfo(cron="R * * * *")` wird angenommen (auch `0 R * * *`, `R R R R R`). croniter 6.2.4 ersetzt `R` bei jeder Instanz neu per `random.randint` (`croniter.py:1434`). `next_fire_times("R * * * *", t)` ergab achtmal hintereinander die Minuten 42, 0, 8, 52, 20, 11, 3, 15, und `next_fire_ms("R R * * *", None, ms)` ergab 49 verschiedene Werte in 50 Aufrufen. Der Leader rechnet den nächsten Termin nach jedem Fire neu (`iter_fire_ms`/`next_fire_ms`). Ein „einmal je Stunde zu zufälliger Minute“ kann deshalb innerhalb derselben Stunde erneut feuern oder ausfallen. Die Fire-Frequenz ist nicht mehr durch den Ausdruck bestimmt.
- **Beleg:** `tests/fuzz/test_cron_fuzz.py::test_accepted_cron_has_increasing_repeatable_fire_times` (Wiederholbarkeit) ohne F4-05 rot (Seed 2 von 3); geschrumpft `@example(cron="0 0 1 R 0")`, von Hand minimal `"R * * * *"`.
- **Quelle der Erwartung:** E-065 („Ein Cron ist ein Standard-Ausdruck mit 5 Feldern; erweiterte Syntax … wird abgelehnt“). `R` ist eine croniter-Erweiterung, keine Standard-Syntax.
- **Register-Abgleich:** E-065 nennt nur `L`/`W`/`#`; `R` ist nicht entschieden. Kein Treffer in E-003, E-011, E-014, E-046, E-049.
- **Einstufung:** LÜCKE (E-065 regelt nur `L`/`W`/`#`).
- **Konfidenz:** hoch. **Realismus:** gering bis mittel (`R`/`H` kennt man aus Jenkins; `H` lehnt croniter ohne `hash_id` ab, `R` nicht).
- **Ausklammerung:** F4-05, `R` nicht im Token-Alphabet.

### R4F-06 – Großgeschriebene Standard-Namen mit `L`/`W` gelten als erweiterte Syntax

- **Fundstelle:** `src/swarmq/task.py:37` (`re.compile(r"[LW#]")` ohne Wortgrenze, Groß/Klein unterschieden).
- **Szenario:** `0 0 * * WED`, `0 0 * * Wed`, `0 0 * JUL *`, `0 0 * APR-JUL *`, `@HOURLY`, `@DAILY`, `@WEEKLY`, `@YEARLY` werden mit „cron uses unsupported extended syntax (L/W/#)“ abgelehnt. `0 0 * * wed`, `0 0 * jul *`, `0 0 * * MON` und `0 0 * * SAT` werden angenommen. `next_fire_times` nimmt alle an; `TaskInfo` und Leader messen also verschieden. Der Fehler fällt beim Import auf (E-065), es geht nichts still verloren. Die Meldung führt aber in die Irre, und die Ablehnung trifft Standard-Syntax.
- **Beleg:** `tests/fuzz/test_cron_fuzz.py::test_standard_cron_is_accepted_or_never_fires` ohne F4-06 rot; geschrumpft `@example(cron="* * * * WED")`.
- **Quelle der Erwartung:** crontab(5) („Names can also be used for the ‘month’ and ‘day of week’ fields … (case does not matter)“), `website/reference/configuration.md:471`.
- **Register-Abgleich:** E-065, `applies_when`: „eine Fehlersuche meldet, dass eine Cron-Angabe mit L, W oder # beim Import abgelehnt wird“. Damit ist das entschieden, also höchstens „Entscheidung überdenken?“. Neues Argument: Die Regel zielt auf die Sonderzeichen `L`/`W`/`#` und trifft nebenbei Standard-Namen und Aliase in Großschreibung, die keine erweiterte Syntax sind.
- **Einstufung:** ENTSCHEIDUNG, „überdenken?“ (E-065, `applies_when`).
- **Konfidenz:** hoch. **Realismus:** mittel (`MON-FRI` geht, `WED` und `JUL` nicht).
- **Ausklammerung:** F4-06, Namen mit `l`/`w` nur klein.

### R4F-07 – 6- und 7-feldrige Ausdrücke werden angenommen (bestätigt F3-35, neues Argument)

- **Fundstelle:** `src/swarmq/task.py:225-251` (`_validate_scheduling`, keine Feldzahlprüfung), croniter 6.2.4 (6. Feld = Sekunden am Ende, 7. Feld = Jahr).
- **Szenario:** Ein Ausdruck im Quartz-/Spring-Stil (Sekunden vorn) wie `0 0 9 * * *`, gemeint als „täglich 09:00:00“, wird angenommen. croniter liest ihn als Minute 0, Stunde 0, Tag 9, Sekunde `*`: Er feuert am 9. jedes Monats 60-mal in der Minute 00:00 (`2026-11-09 00:00:00`, `:01`, `:02`, …) und nie um 9 Uhr. Ebenso feuert `0 0 * * * *` täglich 60-mal um Mitternacht. F3-35 hält fest: „Schädlich ist das nur im Randfall mit Jahresfeld.“ Das neue Argument: Auch 6 Felder schaden, still und mit 60 Fires je Termin.
- **Beleg:** `tests/fuzz/test_cron_fuzz.py::test_accepted_cron_has_five_fields` ohne F4-07 rot; geschrumpft `@example(cron="0 0 * * * *")`, `* * * * * *`. Gegenprobe `0 0 9 * * *` per Skript.
- **Quelle der Erwartung:** E-065 („Ein Cron ist ein Standard-Ausdruck mit 5 Feldern“), `configuration.md:471`.
- **Register-Abgleich:** E-065 nennt F3-35 ausdrücklich als offen.
- **Einstufung:** ENTSCHEIDUNG offen (F3-35) mit neuem Argument, kein neuer Fehler.
- **Konfidenz:** hoch. **Realismus:** mittel (wer von Spring oder Quartz kommt, schreibt 6 Felder).
- **Ausklammerung:** F4-07, der Test auf genau 5 Felder erzeugt keine 6/7 Felder.

### R4F-08 – Lock-Template mit Index- oder Attributzugriff wirft `IndexError`/`AttributeError` am Lock-Gate vorbei

- **Fundstelle:** `src/swarmq/worker/gates/lock.py:143-164` (`render_lock_name` fängt nur `TypeError, KeyError`), `:209-227` (`apply` fängt nur `ValueError`).
- **Szenario:** `TaskInfo(lock="{items[0]}")` mit `items=[]` ergibt `IndexError`, `lock="{user.id}"` mit `user=None` oder einem Dict (JSON) ergibt `AttributeError`. Statt Dead-Letter mit `x-swarmq-lock-render-error` (zugesagt) wirft `LockGate.apply`. Im Hauptteil der Pipeline (`pipeline.py:643-647`) steht das Outcome dann auf `Terminal.release_in_finally()`. Das `finally` gibt das Unique-Flag frei und finalisiert einen Schedfire-Claim terminal, ein Broker-Verb fehlt. Die Nachricht bleibt unbestätigt bis `consumer_timeout`, danach wiederholt sich das bis zum Zustelllimit. Die Folgen sind aus dem Code gelesen; belegt ist das Werfen.
- **Beleg:** `tests/fuzz/test_lock_template_fuzz.py` (beide Tests) ohne F4-08 rot; geschrumpft `@example(template="{key[0]}", key=[], other=None)` (`IndexError`) und `@example(template="{key.id}", key=None, other=None)` (`AttributeError`).
- **Quelle der Erwartung:** Docstring `render_lock_name` („Raises ValueError … on render failure — caller routes the message to DLQ“), Modulkopf `gates/lock.py` (Render-Fehler gehen an das Dead-Letter-Ziel, KTD3), `website/guides/locking-and-dedup.md:174-178` („the render fails and the task is routed to the DLQ with an `x-swarmq-lock-render-error` header“).
- **Register-Abgleich:** E-043 berührt nur den Ack-Fehler nach dem DLQ-Publish eines Render-Fehlers; nichts entschieden.
- **Einstufung:** FEHLER (Docstring und Guide).
- **Konfidenz:** hoch für das Werfen, mittel für die Folgen. **Realismus:** mittel (`{order[0]}` und `{payload.id}` sind naheliegende Templates; leere Listen und fehlende Attribute kommen aus Nutzdaten).
- **Ausklammerung:** F4-08, keine `[<int>]`- und `.attr`-Platzhalter.

### R4F-09 – Lock-Name aus Nutzdaten kann den Key des Scheduler-Leader-Locks belegen

- **Fundstelle:** `src/swarmq/keyspace.py:121-125` (`lock(name)` und `scheduler_lock()` = `lock("{sched}:leader")` im selben Präfix), `src/swarmq/worker/gates/lock.py:229-249` (nur Leer-Prüfung).
- **Szenario:** Eine Task mit `lock="{resource}"` bekommt `resource="{sched}:leader"`. `LockGate.apply` ruft `acquire("{sched}:leader", …)` auf, also den Key `sq:lock:{sched}:leader` des Scheduler-Leaders. Hält der Leader ihn, wartet die Task bis `lock_wait_max_s` und geht in die DLQ. Ist er frei (Leaderwechsel), hält die Task ihn für die Lock-TTL (vom Keeper verlängert), und kein Scheduler kann Leader werden. Ein Producer kann so die Cron-Ausführung blockieren. Der Leader erwirbt seinen Lock über denselben `LockManager.acquire` mit `SCHEDULER_LOCK_NAME = "{sched}:leader"` und `max_concurrency=1` (`scheduler/loop.py:413-418`, `locking.py:91`); Task-Mutex und Leader-Lock sind also derselbe Key mit demselben Skript.
- **Beleg:** `tests/fuzz/test_lock_template_fuzz.py::test_lock_gate_never_acquires_an_empty_or_internal_name` ohne F4-09 rot (Seeds 1 und 2 von 3); geschrumpft `@example(template="{key}", key="{sched}:leader", other=None)`.
- **Quelle der Erwartung:** `keyspace.py` (Namensraum-Trennung, Hash-Tags nur für swarmq-eigene Keys), `locking.py:95-113` (`SCHEDULER_LOCK_KEY` als festes Leader-Lock). Eine ausdrückliche Zusage, dass Nutzer-Locks interne Keys nicht treffen, gibt es nicht.
- **Register-Abgleich:** E-012 (Namensraum nur Valkey) berührt es nicht; nichts entschieden.
- **Einstufung:** LÜCKE (keine Regel trennt Nutzer-Locks von internen Lock-Namen).
- **Konfidenz:** hoch (gleicher Key, gleiches `acquire`). **Realismus:** gering (Template muss mit einem Platzhalter beginnen, der Producer muss den Wert kennen).
- **Ausklammerung:** F4-09, der Wert `"{sched}:leader"` kommt in den Argumenten nicht vor.

### R4F-10 – Formatbreite aus Nutzdaten allokiert Gigabytes oder wirft `MemoryError`

- **Fundstelle:** `src/swarmq/worker/gates/lock.py:162` (`lock_template.format_map(bound)`), `:143-164` (nur `TypeError`/`KeyError` werden zu `ValueError`).
- **Szenario:** Ein Template mit verschachtelter Formatbreite wie `{key:{other}}` übernimmt die Breite aus den Argumenten. `other=1e7` ergibt einen Lock-Namen von 10 MB, `other=98077398153.0` ein `MemoryError` beim Rendern. Statt Dead-Letter mit `x-swarmq-lock-render-error` wirft `LockGate.apply`; die Folgen wie R4F-08. Zusätzlich droht vor dem `MemoryError` eine Allokation im GB-Bereich im Worker-Prozess.
- **Beleg:** `tests/fuzz/test_lock_template_fuzz.py` (Review-Lauf) rot; geschrumpft `@example(template="{key:{other}}", key=0.0, other=98077398153.0)`. Gegenprobe per Skript: `other=1e7` liefert 10 000 000 Zeichen, `other=98077398153.0` `MemoryError`.
- **Quelle der Erwartung:** wie R4F-08: Docstring `render_lock_name`, `website/guides/locking-and-dedup.md:174-178`.
- **Register-Abgleich:** nichts entschieden.
- **Einstufung:** FEHLER (Docstring und Guide).
- **Konfidenz:** hoch. **Realismus:** sehr gering (das Template muss die Breite aus einem Argument nehmen).
- **Ausklammerung:** F4-10, Argumente für `other` ohne Ziffernfolge ab fünf Stellen.

## Abgleich

- **Entscheidungsregister:** Geprüft gegen E-051, E-054, E-055, E-056, E-057, E-060, E-062, E-063, E-064, E-065 (Volltext) sowie per Suche über Titel, `applies_when` und Tags nach Render/Template, Delivery-Count, Zeitzone, Cron, Overflow und Leader-Lock. E-056, E-060, E-062 und E-064 betreffen keinen Treffer. Einstufungen: FEHLER R4F-01, -02, -03, -08, -10; LÜCKE R4F-05, -09; ENTSCHEIDUNG „überdenken?“ R4F-04 (E-063), R4F-06 (E-065); offene ENTSCHEIDUNG R4F-07 (F3-35).
- **Keine Tests mit `entscheidung`-Marker** wurden geändert. Unter `src/swarmq/` wurde nichts geändert.
- **Erlaubte Abweichungen (KTD3/KTD5)** betreffen die Spuren Z und I, nicht die Fuzz-Ziele.

## Abweichungen vom Plan

1. **Cron-Prüfung liegt nicht in `scheduler/definition.py`.** R20 nennt „Cron-Validierung in `scheduler/definition.py`“. Dort gibt es nur `parse_schedfire_key`. Die Prüfung steht in `TaskInfo._validate_scheduling` (`src/swarmq/task.py:225`) und beim Leader in `scheduler/sync.py:_validated`. Gefuzzt wurden `TaskInfo` und `next_fire_times`.
2. **`test_external_publish_fuzz.py` lief bei keinem Rezept.** R21 sagt, die Datei „bleibt bei `just chaos`“. Sie trug aber nur den Auto-Marker `fuzz` (Verzeichnis), nicht `chaos`; `just chaos` wählt `-m chaos` und sammelte sie nicht. Nachgezogen: `pytestmark` der Datei trägt jetzt zusätzlich `pytest.mark.chaos`. `pytest --collect-only -m chaos tests/fuzz` sammelt ihre 2 Tests, `just fuzz` schließt sie weiter per `--ignore` aus (31 Tests gesammelt).
3. **Profil `quick`:** Die Verification nennt „Laufzeit unter 2 min gesamt im Profil `quick`“. Ein Hypothesis-Profil gibt es nur für `tests/property`. Die Fuzz-Dateien setzen `max_examples=500` je Test wie `test_parser_fuzz.py` (KTD8).
4. **Zähler-Leser über öffentliche Helfer:** Der nachsichtige Rate-Limit-Zähler wird über `prior_deferrals`, `broker_redelivered` und `redeliver_pause_s` geprüft. Diese teilen sich `_as_count` mit dem Rate-Limit-Gate (`gates/rate_limit.py:88`). Das Rate-Limit-Gate selbst wurde nicht gefuzzt.
5. **`tests/unit/test_justfile.py`:** `test_all_holds_the_lock_once_across_every_suite` erwartete den alten Fuzz-Aufruf `tests/fuzz/test_parser_fuzz.py`. Die Erwartung lautet jetzt `tests/fuzz` und prüft zusätzlich `--ignore tests/fuzz/test_external_publish_fuzz.py` (kein `entscheidung`-Marker).
6. **Lesen von `SwarmQConfig._ENV_OVERRIDES`:** Der Env-Fuzz liest die Liste der Variablen aus der Klasse, damit alle 70 vorkommen. Geprüft wird nur das Verhalten der Konstruktion, nicht die Liste.

## Offene Punkte

- **Fundwahrscheinlichkeit:** R4F-05 (Seed 2 von 3) und R4F-09 (Seeds 1 und 2 von 3) fand die ungefilterte Strategie nicht bei jedem Seed. R4F-02 fand zuerst ein eigener Test für die Zeitzone; im Review zeigte sich, dass auch der gemischte Env-Test ihn über `_number_text`/`_specials` erreicht. Der Filter `_zone_parts_fit` klammert ihn jetzt dort ebenfalls aus. R4F-10 fand erst der Review-Lauf. Die Treffer sind trotzdem belegt, weil die geschrumpften Beispiele deterministisch scheitern.
- **int64-Grenze der Fristen:** R4F-04 deckt nur den float-Überlauf. Werte ab ~9,2·10¹⁵ s überschreiten `PX`/`EX` von Valkey und sollten gegen den Teststack geprüft werden (Spur B oder ein Integrationstest).
- **Fehlertexte:** `str(exc)` einer Task-Exception, deren `__str__` wirft, fällt nicht unter `error_info`. Ob Ergebnis- und Logpfad das abfangen, ist hier nicht geprüft.
- **Unicode in Cron-Ausdrücken:** `٠ * * * *` (arabisch-indische Null) und NBSP als Trenner werden angenommen und verhalten sich wie `0 * * * *`. Das ist kein Verstoß gegen eine Zusage und kein Verdacht. Es ist nur vermerkt.
- **Lint und Typen:** `uv run just lint` und `uv run just types` sind für die sechs Dateien und das `justfile` grün (`ruff format --check`, `ruff check`, `mypy tests/fuzz`). Beide Rezepte schlagen derzeit nur an Dateien anderer Einheiten fehl, die parallel im Worktree entstehen: `tests/unit/test_stateful_driver.py` (I001 und `arg-type`) und `tests/support/stateful.py` (`explicit-override`).
