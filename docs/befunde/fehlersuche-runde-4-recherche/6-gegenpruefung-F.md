# Gegenprüfung Spur F (Fuzzing), Verdachte R4F-01 bis R4F-10

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R1, R4, Einheit U17. Eingabe: `5-fuzz.md` (Finder U16). Stand: swarmq 0.12.3, Branch `test/fehlersuche-runde-4` (HEAD `e92699f`), croniter 6.2.4, Python 3.14. Ablauf nach `docs/befunde/README.md`. Der Prüfer hat keinen der Verdachte gefunden. Er hat weder `src/` noch `tests/` geändert.

## Ergebnis in Kürze

| Verdacht | Finder | Gegenprüfung | Gewicht | Fix-Art |
|---|---|---|---|---|
| R4F-01 `DeliveryCountGate` mit `inf` | FEHLER | **FEHLER** (Zustimmung) | P3 | Code |
| R4F-02 Zeitzone > 255 Byte → `OSError` | FEHLER | **FEHLER** (Zustimmung, stärkere Quelle) | P3 | Code |
| R4F-03 Ganzzahl > `float_info.max` → `OverflowError` | FEHLER | **LÜCKE** (Widerspruch) | P4 | Register, dann Code |
| R4F-04 endliche Frist ≥ 1,8·10³⁰⁵ s | ENTSCHEIDUNG „überdenken?“ | **LÜCKE** (Widerspruch) | P3 | Register (Nachtrag E-063), dann Code |
| R4F-05 croniter-`R` angenommen | LÜCKE | **LÜCKE** (Zustimmung) | P3 | Register, dann Code |
| R4F-06 `WED`/`JUL`/`@HOURLY` abgelehnt | ENTSCHEIDUNG „überdenken?“ | **FEHLER** (Widerspruch) | P3 | Code + Nachtrag E-065 |
| R4F-07 6/7 Felder angenommen | „ENTSCHEIDUNG offen“ | **LÜCKE**, bestehende F3-35, kein neuer Eintrag (Kategorie korrigiert) | F3-35 von P3 auf P2 heben | Register, dann Code |
| R4F-08 Lock-Template `IndexError`/`AttributeError` | FEHLER | **FEHLER** (Zustimmung) | P2 | Code |
| R4F-09 Nutzer-Lock trifft Leader-Lock | LÜCKE | **LÜCKE** (Zustimmung) | P3 | Register, dann Code |
| R4F-10 Formatbreite aus Nutzdaten | FEHLER | **NICHT HALTBAR** (Widerspruch); der Fix für R4F-08 deckt es mit ab | – | – |

Bilanz: 4 FEHLER (R4F-01, -02, -06, -08), 5 LÜCKEN (R4F-03, -04, -05, -09 neu; R4F-07 = F3-35), 1 NICHT HALTBAR (R4F-10). Keine ENTSCHEIDUNG.

Berührte offene LÜCKEN aus R4: **F3-35** (R4F-07 direkt, R4F-05 als Teil derselben Syntaxfrage) und **F3-38** (R4F-03 und R4F-04: dieselbe Klasse „absurde Zahlenwerte in der Konfiguration“). Keine der übrigen (FS-25, FS-26, FS-29, FS-32, F3-32 bis F3-34, F3-36, F3-37, F3-41).

## Reproduktion

Alle Beispiele laufen gegen den unmutierten Code im Worktree, ohne Dienste. Werkzeug: `uv run python <skript>` mit dem Skript des früheren Reviewers (`/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad/repro.py`) und drei eigenen Einzeilern (`tz.py`, `r4.py` im selben Scratchpad sowie `python -c`).

| Verdacht | Eingabe | Ergebnis |
|---|---|---|
| R4F-01 | `DeliveryCountGate.apply`, `x-acquired-count: inf` | `OverflowError: cannot convert float infinity to integer`; `nan` ergibt `Continue` |
| R4F-02 | `SWARMQ_SCHEDULER_TIMEZONE="a"*256`, `TaskInfo(timezone="a"*256)`, `next_fire_times(…, "a"*256)`, `"Europe/"+"x"*300` | jeweils `OSError [Errno 36] File name too long`; `"a"*255`, `America`, `Europe/Berlin/`, `\x00`, `Europe\Berlin` ergeben den zugesagten `ValueError` |
| R4F-03 | `SWARMQ_SHUTDOWN_TIMEOUT`/`SWARMQ_DELIVERY_LIMIT` = `"1"+"0"*309`, ebenso `shutdown_timeout=10**400` als Argument | `OverflowError: int too large to convert to float` |
| R4F-04 | `SWARMQ_LOCK_WAIT_MAX_S=1e306`, `SWARMQ_BACKEND_DEFER_MAX_DELAY=1.797693134862316e+305`, `SWARMQ_EXECUTION_GUARD_TTL_S=1e306` | angenommen; `int(wert*1000)` wirft `OverflowError`. `1e300` und `1e16` angenommen, ms-Umrechnung gelingt |
| R4F-05 | `TaskInfo(cron="R * * * *")`, `"r * * * *"`, `"0 0 1 R 0"` | angenommen; `next_fire_times("R * * * *")` sechsmal: Minuten 45, 44, 23, 53, 39, 1; `"0 0 1 R 0"` zehnmal: 8 verschiedene Termine |
| R4F-06 | `0 0 * * WED`, `Wed`, `0 0 * JUL *`, `@HOURLY`, `@DAILY`, `@Weekly` | `ValueError: cron uses unsupported extended syntax (L/W/#)`; `0 0 * * wed`, `MON` angenommen; `next_fire_times("0 0 * * WED")` liefert 2026-01-07 |
| R4F-07 | `TaskInfo(cron="0 0 9 * * *")`, `"0 0 * * * * 2030"` | angenommen; Termine `2026-11-09 00:00:00`, `:01`, `:02`; `0 0 * * * *` ab 2026-10-17 00:00:01 sekündlich |
| R4F-08 | `render_lock_name("{items[0]}", …, items=[])`, `"{user.id}"` mit `None` und mit `{"id": 1}` | `IndexError`, `AttributeError`, `AttributeError`; `"{user[0]}"` mit Dict ergibt den zugesagten `ValueError` |
| R4F-09 | `KeySpace().lock(render_lock_name("{user}", …, user="{sched}:leader"))` | `sq:lock:{sched}:leader` == `KeySpace().scheduler_lock()` |
| R4F-10 | `"{key:{other}}"`, `other=1e7` / `98077398153.0` | 10 000 000 Zeichen / `MemoryError` (Unterklasse von `Exception`) |

Alle zehn Treffer sind damit unabhängig bestätigt. Die Folgen nach dem Werfen sind aus dem Code gelesen und nicht gegen einen Broker gefahren.

## Je Verdacht

### R4F-01 – `DeliveryCountGate` wirft `OverflowError` bei `inf`: FEHLER, P3 (Zustimmung)

**Entscheidungs-Abgleich.** E-051 trägt im Titel „Kaputte Zähler-Header zählen wie keine“. `applies_when` nennt „Änderungen am Lesen von x-swarmq-retry-count, x-swarmq-rate-limit-count **oder anderen Zähler-Headern**“. Die Entscheidung ist allgemein formuliert: „Ein Zähler-Header, der sich nicht als Zähler lesen lässt, zählt wie kein Header; die Nachricht wird normal weiterverarbeitet statt unquittiert hängen zu bleiben.“ `x-delivery-count` und `x-acquired-count` sind Zähler-Header. Dass der Broker sie setzt, ändert daran nichts, weil der Code (`_count`, Docstring „fehlend oder unlesbar -> `None`“) sie schon heute nachsichtig liest und nur den Fall `inf` auslässt. Commit `06958c8` („Zähler-Helfer gegen inf/bool fest“) hat `OverflowError` in `gates/__init__.py:231` (`_as_count`), `lock.py:122` (`_as_int`) und `redelivery.py:34` aufgenommen, `delivery_count.py:53` aber nicht (`git log -S` auf `def _count` zeigt nur `47aed19`, BZ-29). Die Regel gilt also an drei Stellen und fehlt an der vierten. Keine Entscheidung deckt das `OverflowError`, kein Test mit `entscheidung`-Marker berührt `_count`.

**Kann der Broker `inf` liefern?** Nein. RabbitMQ setzt beide Zähler als Ganzzahl. Ein `inf` kommt nur von einem Publisher, der einen AMQP-Double setzt. E-054 belegt, dass das ankommt: „RabbitMQ 4 behält einen vom Publisher gesetzten `x-delivery-count` bei der ersten Zustellung bei“ (der Anlass für FS-10). Ein Replay per Shovel kopiert einen solchen Header ebenso. Die RabbitMQ-UI kommt als Quelle nicht in Frage, weil JSON kein `Infinity` kennt. Ein Text `"inf"` ergibt `ValueError` und wird gefangen.

**Folge und Realismus.** Auf Quorum-Queues (Voreinstellung, `config.py:244`) schreibt der Broker den Zähler bei der nächsten Zustellung nach E-054 mit einer Ganzzahl neu. Der Schaden ist dann eine einmalige Hängepartie bis `consumer_timeout`. Der Kanal schließt dabei und nimmt andere unbestätigte Nachrichten mit. Auf Classic-Queues bleibt der Header bestehen. Dort wiederholt sich die Schleife ohne Ende, wobei Classic-Queues laut `configuration.md:46` ohnehin keine Zustellgrenze haben. Der Auslöser ist ein absichtlich oder fehlerhaft gesetzter Double. Realismus gering, daher P3.

**Register-Zeile.** `F4-nn | P3 | DeliveryCountGate wirft OverflowError, wenn x-delivery-count oder x-acquired-count ±inf ist (vom Publisher gesetzt, E-054); die Nachricht bleibt bis consumer_timeout unbestätigt. E-051: unlesbarer Zähler zählt wie keiner. | Code: OverflowError in delivery_count._count fangen wie in _as_count (06958c8); Nachweis-Test über DeliveryCountGate.apply mit inf.`

### R4F-02 – Zeitzone mit Pfadteil über 255 Byte: FEHLER, P3 (Zustimmung, stärkere Quelle)

**Entscheidungs-Abgleich.** Kein Eintrag regelt Zeitzonen-Ausnahmen (Suche über `timezone`, `zone`, `zoneinfo` in Titel, `applies_when` und Tags: nur E-065 nennt `TaskInfo`-Felder allgemein). E-057 zählt die Klassen auf, die 78 ergeben („`ConfigurationError`, jeder `ImportError` … und jeder `SyntaxError`“). Der Exit-Code ist dort also an `ConfigurationError` gebunden und nicht an „jede Ausnahme beim Konfigurieren“. Genau deshalb trägt der Verdacht: Für diesen Wert ist ein `ConfigurationError` zugesagt.

**Quelle der Erwartung.** Sie ist ausdrücklicher als beim Finder:
- `website/reference/configuration.md:123` (`SWARMQ_SCHEDULER_TIMEZONE`): „An unknown zone is a configuration error.“
- `configuration.md:482` (`TaskInfo.timezone`): „an unknown zone raises `ValueError`“, gedeckt durch E-065 („ein falscher Wert wirft `ValueError` beim Import“).
- Docstring `normalize_timezone` (`config.py:71-77`): „sonst `ValueError` mit Feld- und Zonennamen“. `config.py:703-706` macht daraus den `ConfigurationError`, fängt aber nur `ValueError`.

**Realismus.** Ein Name mit einem Pfadteil über 255 Byte ist sehr selten. Er ist aber nicht ganz ausgeschlossen: Ein Template-Fehler im Deployment kann einen langen Wert (JSON, Zertifikat, Secret) in die falsche Variable schreiben. Alle kurzen Fehleingaben ergeben den zugesagten `ValueError` (Gegenprobe oben). Folge für `SWARMQ_SCHEDULER_TIMEZONE`: Exit 1 mit Neustarts statt 78, also das Gegenteil von E-057 („ein deterministisch kaputtes Deployment soll nicht fünfmal neu starten“). Bei `TaskInfo` bleibt es bei Exit 1 (E-057), nur die Ausnahmeklasse ist falsch. Beim Leader fängt `scheduler/sync.py` über `except Exception`. P3.

**Register-Zeile.** `F4-nn | P3 | Eine Zeitzone mit einem Pfadteil über 255 Byte ergibt OSError statt ValueError/ConfigurationError (normalize_timezone, TaskInfo.timezone, next_fire_times); SWARMQ_SCHEDULER_TIMEZONE endet mit Exit 1 und Neustarts statt 78. configuration.md:123/482, Docstring normalize_timezone. | Code: OSError in normalize_timezone (und cron_time._zone) wie ZoneInfoNotFoundError behandeln.`

### R4F-03 – Ganzzahlfeld größer `float_info.max`: LÜCKE, P4 (Widerspruch zum Finder)

**Entscheidungs-Abgleich.** E-063 verlangt Endlichkeit für Fristen, TTLs und Zeitfelder („`inf` und `nan` ergeben `ConfigurationError`“). Eine Ganzzahl mit 310 Stellen ist endlich. Nach dem Wortlaut der Doku ist sie sogar gültig: `configuration.md:31-33` sagt „must be a finite number“, `SWARMQ_SHUTDOWN_TIMEOUT` (`:43`) lehnt nur negative Werte ab. Eine Zusage, dass solche Werte abgelehnt werden, gibt es nicht. Die einzige Quelle für „nie roh `OverflowError`“ ist Plan R4/U16 Approach 3, ein Testplan und keine Produktzusage. E-057 bindet Exit 78 an `ConfigurationError` (siehe R4F-02). Die Frage, ob der Wert ein Konfigurationsfehler ist, bleibt damit offen.

**Präzedenz.** F3-38 ist derselbe Fall: „Ist `concurrency > 65535` ein Konfigurationsfehler (Exit 78)? Heute wirft pamqp beim Start einen `TypeError`.“ Runde 3 hat ihn als LÜCKE P4 geführt: „Höchstens Härtung (a); der Auslöser ist praktisch ausgeschlossen.“ Status offen. Zwischen 65 536 Verbindungs-Slots und 10³⁰⁹ Sekunden gibt es keinen sachlichen Unterschied, der hier einen FEHLER und dort eine LÜCKE rechtfertigt. Anders als bei R4F-02 erzeugt ein Template-Fehler keine 310-stellige Zahl. Der Auslöser ist ausschließlich absichtlich.

**Warum nicht NICHT HALTBAR.** Der Validator selbst stürzt ab (`_validate_finite`, `config_validation.py:193`, `math.isfinite` auf einem `int`). Der Kommentar `:106-107` sagt, die Prüfung laufe „vor jeder Relationsprüfung, damit die Meldung das ursächliche Feld nennt (E-063)“. Die Härtung ist billig. Wie bei F3-38 bleibt die Frage deshalb offen und wird nicht verworfen.

**Produktfrage (gebündelt mit R4F-04 und F3-38).** Siehe „Produktfrage A“ unten.

**Register-Zeile.** `F4-nn | P4 | Ein Ganzzahlfeld der Konfiguration über float_info.max (SWARMQ_SHUTDOWN_TIMEOUT, SWARMQ_DELIVERY_LIMIT, … mit 310 Stellen, auch als Argument) lässt _validate_finite mit OverflowError scheitern; Exit 1 statt 78. Keine Zusage, ob der Wert gültig ist (wie F3-38). | Register: Produktfrage A; danach Code.`

### R4F-04 – Endliche Frist mit Überlauf bei der ms-Umrechnung: LÜCKE, P3 (Widerspruch zum Finder)

**Entscheidungs-Abgleich.** Der Finder stuft als ENTSCHEIDUNG „überdenken?“ ein, weil E-063 wörtlich nur Endlichkeit verlangt. Das trägt nicht. Nach `docs/befunde/README.md` heißt ENTSCHEIDUNG, dass das Verhalten „bewusst so festgelegt“ wurde. E-063 hat eine Untergrenze der Gültigkeit festgelegt (endlich) und über Obergrenzen nichts gesagt. Die „Bewussten Folgen“ nennen nur „`SWARMQ_..._S=inf` als Ersatz für ‚nie‘ wird abgelehnt“. Dass `1e306` angenommen wird und zur Laufzeit wirft, ist nirgends hingenommen. Die Begründung von E-063 nennt genau diesen Schaden als Grund der Regel: „Die Werte werden in ganze Millisekunden umgerechnet …; `inf` warf dann `OverflowError` vor jedem Claim.“ Die Regel verfehlt also ihren Zweck in einem Bereich, den sie nie betrachtet hat. Das ist eine LÜCKE mit Bezug E-063 und keine entschiedene Folge.

**Präzedenz im Code.** `logging_settings.py:206-216` (`valid_drain_timeout`, Plan 2026-10-06-1811, KTD11) begrenzt `SWARMQ_LOG_DRAIN_TIMEOUT` bereits auf `threading.TIMEOUT_MAX`: „`inf` und alles über `TIMEOUT_MAX` ließen `Thread.join` mit `OverflowError` scheitern … Die Grenze ist die technische, keine willkürliche.“ Dokumentiert ist das in `cli.md:113` („at most `threading.TIMEOUT_MAX`, otherwise exit `78`“). Für ein Feld hat das Projekt die Frage also schon mit „technische Obergrenze → 78“ beantwortet, für die übrigen Fristen nicht.

**Realismus.** Für den float-Überlauf (≥ ~1,8·10³⁰⁵) sehr gering. Höher liegt der Bereich darunter, der im Bericht als offener Punkt steht: Wer „praktisch nie“ will und `inf` abgelehnt bekommt, schreibt etwa `1e18`. Ab ~9,2·10¹⁵ s überschreitet `PX`/`EX` int64. Die ms-Umrechnung gelingt dann (Gegenprobe `1e16` → `10000000000000000000`), Valkey antwortet aber mit einem Fehler. Das ist nicht gefahren und gehört in Spur B. Folgen an den Fundstellen (`lock.py:353`, `execution_guard.py:125, 193, 293-319, 430`, `leader_version.py:113`, `prune.py:125`) sind gelesen, nicht ausgelöst. P3 wegen des Bezugs auf eine bestehende Regel und des ausgeprägten „großer Wert statt `inf`“-Musters.

**Register-Zeile.** `F4-nn | P3 | Eine endliche Frist ab ~1,8·10³⁰⁵ s (SWARMQ_LOCK_WAIT_MAX_S, SWARMQ_BACKEND_DEFER_MAX_DELAY, SWARMQ_EXECUTION_GUARD_TTL_S, …) besteht E-063 und wirft bei der ms-Umrechnung OverflowError zur Laufzeit, beim Lock-Gate bei jedem Konflikt; ab ~9,2·10¹⁵ s droht eine Valkey-Fehlerantwort (ungeprüft). E-063 regelt keine Obergrenze; valid_drain_timeout hat sie (TIMEOUT_MAX). | Register: Produktfrage A (Nachtrag E-063); danach Code.`

### R4F-05 – croniter-Zufallsfeld `R`: LÜCKE, P3 (Zustimmung)

**Entscheidungs-Abgleich.** E-065: „Ein Cron ist ein Standard-Ausdruck mit 5 Feldern; erweiterte Syntax (`L`, `W`, `#`) wird abgelehnt.“ Die Klammer zählt auf, was abgelehnt wird. `R` (und `r`) steht nicht darin. `applies_when` nennt „ein Plan will die Cron-Syntax erweitern (Sekunden- oder Jahresfeld, L/W/#)“, `R` dagegen nicht. Der Code-Kommentar `task.py:34-36` nennt nur „L (last), W (nearest weekday), # (nth weekday of month)“. `R` wurde also nie betrachtet. Gegen FEHLER spricht die Präzedenz F3-35: Dass croniter mehr annimmt als die 5 Standardfelder, hat Runde 3 trotz „Standard 5-field“ in `configuration.md:471` als LÜCKE geführt. „Nicht-Standard wird stillschweigend angenommen“ ist seitdem die offene Produktfrage. `R` gehört in dieselbe Frage.

**Realismus.** Gering bis mittel. Jenkins-Nutzer kennen `H`, croniter lehnt `H` ohne `hash_id` ab. `R` liest sich wie „zufällige, aber feste Minute“, verhält sich aber anders. Der Leader rechnet nach jedem Fire neu (`cron_time.py:89`), und jede Instanz würfelt neu (Gegenprobe: 8 verschiedene Termine in 10 Aufrufen). Die Fire-Frequenz folgt damit nicht mehr dem Ausdruck.

**Produktfrage (gebündelt mit R4F-07/F3-35).** Siehe „Produktfrage B“.

**Register-Zeile.** `F4-nn | P3 | TaskInfo nimmt croniters Zufallsfeld R/r an (R * * * *); jede Neuberechnung würfelt neu, ein „einmal je Stunde“ kann in derselben Stunde doppelt feuern oder ausfallen. E-065 nennt nur L/W/#. | Register: Produktfrage B; danach Code.`

### R4F-06 – Großgeschriebene Standard-Namen mit `L`/`W`: FEHLER, P3 (Widerspruch zum Finder)

**Entscheidungs-Abgleich.** Der Finder verweist auf `applies_when` von E-065: „eine Fehlersuche meldet, dass eine Cron-Angabe mit L, W oder # beim Import abgelehnt wird“. Diese Zeile sagt, wann man E-065 nachlesen muss. Was gilt, steht im Entscheidungstext. Dort steht die Ablehnung von `WED`/`JUL` **nicht**: „Ein Cron ist ein Standard-Ausdruck mit 5 Feldern; erweiterte Syntax (`L`, `W`, `#`) wird abgelehnt.“ `0 0 * * WED` ist ein Standard-Ausdruck und keine erweiterte Syntax. Der Code-Kommentar an der Fundstelle (`task.py:34-36`) erklärt die Absicht eindeutig über die Operatoren: „L (last), W (nearest weekday), # (nth weekday of month)“. Die Tests mit Marker E-065 prüfen nur Operatoren (`0 3 L * *`, `0 3 15W * *`, `0 3 * * 1#2`, `tests/unit/test_task.py:353-368`). Eine operatorgenaue Prüfung ließe alle drei rot-grün bestehen. Die Meldung „unsupported extended syntax (L/W/#) … use a standard 5-field expression“ ist für `0 0 * * WED` sachlich falsch.

**Quelle der Erwartung.** `configuration.md:471`: „Standard 5-field cron expression with a next fire time.“ crontab(5) zu den Namen: „case does not matter“. Dazu kommt die gleiche Regel an anderer Stelle: `next_fire_times` und `swarmq schedule add --cron` nehmen `WED` an. `TaskInfo` misst also anders als der Leader. Ein Restrisiko bleibt: Liest Georg „Cron mit W“ in den Bewussten Folgen buchstäblich, wäre es ENTSCHEIDUNG. Dafür spricht nichts in Kommentar, Tests oder Doku.

**Realismus.** Mittel. `MON-FRI` geht, `WED` und `JUL` nicht, `wed` geht. Der Fehler fällt laut beim Import auf (E-065), es geht nichts still verloren. Daher P3.

**Register-Zeile.** `F4-nn | P3 | TaskInfo lehnt Standard-Namen mit L/W in Groß- oder gemischter Schreibweise (WED, Wed, JUL, @HOURLY, @DAILY, @WEEKLY) als „extended syntax (L/W/#)“ ab; kleingeschrieben werden sie angenommen, next_fire_times und schedule --cron nehmen sie an. configuration.md:471, crontab(5), Kommentar task.py:34. | Code: L/W/# nur als Operator erkennen (Namen vorher entfernen oder tokenweise prüfen); Register: Nachtrag E-065 „gemeint sind die Operatoren, nicht Buchstaben in Namen“.`

### R4F-07 – 6- und 7-feldrige Ausdrücke: LÜCKE = F3-35, kein neuer Eintrag (Kategorie korrigiert)

**Stand im Register.** F3-35 ist eine offene LÜCKE (P3): „Sollen 6- und 7-feldrige Cron-Ausdrücke abgelehnt werden? … Schädlich ist das nur im Randfall mit Jahresfeld. | (a) In TaskInfo auf 5 Felder begrenzen; (b) zulassen und dokumentieren. | Keine Festlegung.“ Status „offen (kein Fix in 0.12.3); Bezug im Register: E-065“ (`fehlersuche-runde-3-2026-10.md:84`). E-065 schließt mit „Offen ist F3-35“. Einen Nachtrag gibt es nicht (`grep F3-35` über Register, Pläne und Befunde). Plan Runde 3 R9 und `fix-r-learnings.md:127` halten ausdrücklich fest, dass F3-35 nicht mitentschieden wurde. „ENTSCHEIDUNG offen“ ist keine der vier Einstufungen. Richtig ist LÜCKE, geführt unter der bestehenden ID F3-35, ohne neue F4-ID.

**Neues Argument (bestätigt).** Die Annahme „schädlich nur mit Jahresfeld“ ist falsch. croniter liest das 6. Feld als Sekunden **am Ende**. Ein Quartz-/Spring-Ausdruck mit Sekunden vorn (`0 0 9 * * *`, gemeint „täglich 09:00“) feuert am 9. jedes Monats sekündlich in 00:00:00 bis 00:00:59 und nie um 9 Uhr. `0 0 * * * *` feuert täglich 60-mal (Gegenprobe oben). Das passiert still und mit Faktor 60. Wer von Spring oder Quartz kommt, schreibt so etwas leicht. Empfehlung: F3-35 von P3 auf **P2** heben und die Begründung im Register ersetzen.

**Register-Zeile.** Keine neue Zeile. Nachtrag in F3-35: `Neues Argument (Runde 4, R4F-07): auch 6 Felder schaden – Sekundenfeld am Ende, „0 0 9 * * *“ feuert am 9. sekündlich um 00:00 statt täglich 09:00; Gewicht P2.` Produktfrage B.

### R4F-08 – Lock-Template mit Index- oder Attributzugriff: FEHLER, P2 (Zustimmung)

**Entscheidungs-Abgleich.** Kein Eintrag über Render, Template, Lock-Name (Suche über Titel, `applies_when`, Tags). E-043 betrifft nur den Ack nach erfolgreichem DLQ-Publish. E-070 und E-071 (Rate-Limit vor Lock, Leftover-Locks) und E-007 (zeitbasierte Wartezeit) gelten nicht für den Render-Pfad.

**Quelle der Erwartung.** Mehrere Stellen sagen dasselbe:
- Docstring `render_lock_name` (`lock.py:143-151`): „Raises `ValueError` … on render failure — caller routes the message to DLQ with an explanatory header.“ Der Klammerzusatz „(with a wrapped TypeError / KeyError as cause)“ beschreibt die Ursache. Er grenzt nicht ein, was als Render-Fehler gilt.
- Modulkopf KTD3 (`lock.py:22-24`): „Render-Fehler, leerer Render und das Ende der Wartezeit gehen an das Dead-Letter-Ziel.“
- Kommentar in `apply` (`lock.py:205-208`): „A render failure is a user configuration bug — route to DLQ … do NOT requeue.“
- Guide `website/guides/locking-and-dedup.md:174-180`, Abschnitt „Lock-render failures“: „the render fails and the task is routed to the DLQ with an `x-swarmq-lock-render-error` header … a broken lock template is a bug — you want it loud.“ Das Beispiel dort (fehlender kwarg) zählt nicht abschließend auf.
- `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md:37, 51-55` (ADV-009): Ein dauerhafter Render-Fehler darf nicht „loop forever without counter or cap“.

**Folge (gelesen).** `apply` fängt nur `ValueError`. Die Ausnahme verlässt das Gate im Hauptteil (`pipeline.py:643-647`), das Outcome steht auf `Terminal.release_in_finally()`. Das `finally` gibt das Unique-Flag frei und finalisiert einen Schedfire-Claim, ein Broker-Verb fehlt. Dann folgen `consumer_timeout`, Kanalabbruch (mit den anderen unbestätigten Nachrichten des Kanals) und Neuzustellung. Erst das Delivery-Count-Gate schickt die Nachricht nach `delivery_limit` Runden mit der irreführenden Meldung `WorkerHungError` in die DLQ. Genau das wollte ADV-009 verhindern.

**Realismus.** Mittel bis hoch für `AttributeError`. kwargs kommen JSON-dekodiert an, ein Objekt ist also immer ein `dict`. Ein Template `"{user.id}"` scheitert deshalb bei **jeder** Nachricht dieses Tasks, nicht nur im Randfall. `"{items[0]}"` scheitert bei leerer Liste. P2: Der Weg ist deterministisch, betrifft alle Nachrichten des Tasks und verursacht Doppelfreigabe sowie Kanalabbrüche. Daten gehen nicht verloren, die Nachricht landet am Ende in der DLQ.

**Register-Zeile.** `F4-nn | P2 | render_lock_name wandelt nur TypeError/KeyError in ValueError; ein Template mit Index- oder Attributzugriff („{items[0]}“ mit [], „{user.id}“ mit JSON-Dict oder None) wirft IndexError/AttributeError an LockGate.apply vorbei. Statt DLQ mit x-swarmq-lock-render-error bleibt die Nachricht bis consumer_timeout unbestätigt (Kanalabbruch), das Unique-Flag ist frei, erst das Zustelllimit beendet die Schleife mit WorkerHungError. Docstring, KTD3, locking-and-dedup.md:174, ADV-009. | Code: in render_lock_name jede Exception aus format_map (mindestens LookupError, AttributeError, ValueError, MemoryError) als ValueError melden.`

### R4F-09 – Nutzer-Lock trifft den Leader-Lock: LÜCKE, P3 (Zustimmung)

**Entscheidungs-Abgleich.** E-012 trennt Apps über den Namensraum (`<ns>:sq:`). Innerhalb einer App trennt er nichts, und `applies_when` nennt nur Apps auf einem Valkey. Der Test `test_befund_bz10_shared_valkey.py::test_same_lock_name_in_two_apps_does_not_block_the_other_app` belegt nur die Trennung zwischen Apps. `KeySpace.lock(name)` und `scheduler_lock()` = `lock("{sched}:leader")` (`keyspace.py:121-125`) teilen bewusst ein Präfix. `locking.py:91-109` begründet den Leader-Namen mit dem Hash-Tag (`sched_commit.lua`, Cluster-Slot), nicht mit einer Trennung von Nutzer-Locks. Weder E-022 noch E-070 oder E-071 noch eine andere Regel sagt, ob Nutzer-Lock-Namen interne Namen treffen dürfen.

**Quelle der Erwartung.** Eine Zusage fehlt. Es gibt nur zwei Analogien. (1) `lock.py:229-236` und `locking-and-dedup.md:182-186`: Ein leerer Render „would collapse to the bare Valkey namespace key `sq:lock:` and collide with every other empty-locked task“. Kollisionen mit fremden Schlüsseln gelten dort also als Fehler (ADV-010). (2) Der Bericht `5-fuzz.md` zitiert dafür `keyspace.py`. Dort steht aber nichts über Nutzer-Locks. FEHLER wäre ohne Zusage nicht begründet. LÜCKE ist richtig.

**Sicherheitsfrage.** Ein Treffer setzt voraus, dass das Template mit einem Platzhalter **beginnt** (`lock="{tenant}"`; `"user-{id}"` kann nicht treffen) und der Producer den Wert `{sched}:leader` kennt. Ein literal `TaskInfo(lock="{sched}:leader")` rendert mit `KeyError` und geht in die DLQ. Nur `"{{sched}}:leader"` träfe, und das wäre der Entwickler selbst. Stammt der Wert aus Endnutzer-Eingaben (Tenant-ID aus einer HTTP-Anfrage), kann ein Fremder die Cron-Ausführung blockieren. Das gilt für die Dauer eines Laufs (TTL `hard_timeout + 30 s`, vom Keeper verlängert), wenn der Lock gerade frei ist. Hält der Leader ihn, wartet nur die eine Nachricht bis `lock_wait_max_s` und geht in die DLQ. Ein dokumentiertes Vertrauensmodell für Nutzdaten fehlt (`grep -i trust website/` ohne Treffer). Ob ein Producer-Wert als vertrauenswürdig gilt, ist Teil der Produktfrage. Realismus gering, daher P3.

**Produktfrage C.** Siehe unten.

**Register-Zeile.** `F4-nn | P3 | Ein Lock-Name aus Nutzdaten kann interne Lock-Namen belegen: lock="{resource}" mit resource="{sched}:leader" ergibt den Key sq:lock:{sched}:leader des Scheduler-Leaders (gleiches LockManager.acquire). Ist der Lock frei, kann für die Laufzeit des Tasks kein Leader gewählt werden. Keine Regel trennt Nutzer-Locks von internen Namen (E-012 trennt nur Apps). | Register: Produktfrage C; danach Code.`

### R4F-10 – Formatbreite aus Nutzdaten: NICHT HALTBAR (Widerspruch zum Finder)

**Begründung.** Der Treffer ist echt (10-MB-Name, `MemoryError`). Das Szenario verlangt aber ein Template, dessen Format-Spezifikation selbst aus einem Task-Argument kommt (`"{key:{other}}"`). So schreibt niemand einen Lock-Namen: Ein Lock-Name soll eine Ressource benennen und nicht formatieren. Keine Doku zeigt Format-Spezifikationen in Lock-Templates (`configuration.md:473` nur `"user-{user_id}"`). Eine feste Breite im Template (`{key:>1000000000}`) ist ein Fehler des Entwicklers und kein Weg über Nutzdaten. Nach README fällt ein „Szenario unrealistisch“ unter NICHT HALTBAR. Ein eigener Befund mit Nachweis-Test lohnt sich nicht.

**Hinweis für R4F-08.** `MemoryError` ist eine Unterklasse von `Exception`. Fängt der Fix für R4F-08 breit, landet auch dieser Fall in der DLQ. Die Allokation davor (im Extremfall GB-Bereich) bleibt. Wer das abfangen will, begrenzt die Länge des gerenderten Namens (etwa auf 1 KiB, `ValueError` → DLQ). Das wäre eine Härtung und kein Befund. Im Fuzz-Kommentar F4-10 sollte die Ausklammerung bleiben, mit Verweis auf diese Einstufung.

## Produktfragen

### A – Obergrenze numerischer Konfigurationswerte (R4F-03, R4F-04, offen F3-38)

Was soll mit endlichen, aber technisch nicht verarbeitbaren Werten geschehen? Betroffen sind Ganzzahlen über `float_info.max` (R4F-03), Fristen, deren ms-Wert `inf` wird (R4F-04) oder int64 übersteigt (ungeprüft), und `concurrency > 65535` (F3-38).

| Option | Inhalt |
|---|---|
| (a) | Eine Regel für alle: Jeder Wert über der technischen Grenze seines Ziels ist ein `ConfigurationError`, der das Feld nennt (Exit 78). Fristen: ms-Wert ≤ 2⁶³−1 (Valkey `PX`). Zähler und Größen: wie bisher plus kein Überlauf in der Prüfung. `concurrency` ≤ 65535. Vorbild ist `valid_drain_timeout` (TIMEOUT_MAX). |
| (b) | Nur den Absturz der Prüfung beheben (R4F-03: `isfinite` nur auf `float`), Obergrenzen offenlassen. |
| (c) | Nichts tun; dokumentieren, dass Werte jenseits technischer Grenzen nicht unterstützt sind. |

Empfehlung **(a)** als Nachtrag zu E-063 („endlich **und** innerhalb der technischen Grenze des Ziels“), mit F3-38 zusammen entschieden. Das kostet wenig, ein Muster existiert bereits, und die Regel beendet die Klasse statt drei Einzelfälle zu flicken. Die int64-Grenze vorher gegen den Teststack prüfen (Spur B), damit die Zahl in der Doku stimmt.

### B – Welche Cron-Syntax nimmt swarmq an? (R4F-05, R4F-07 = F3-35)

| Option | Inhalt |
|---|---|
| (a) | Positivliste nach crontab(5): genau 5 Felder, Zahlen, `*`, `-`, `,`, `/`, Monats- und Tagesnamen ohne Rücksicht auf Groß- und Kleinschreibung, `@`-Aliase. Alles andere (`L`, `W`, `#`, `R`, `H`, 6/7 Felder) ist `ValueError`. |
| (b) | Negativliste erweitern: `R`/`r` und 6/7 Felder zusätzlich ablehnen. |
| (c) | Zulassen und dokumentieren (F3-35 Option b), auch `R`. |

Empfehlung **(a)**. Sie beantwortet F3-35, R4F-05 und R4F-06 in einer Regel. Sie macht „Standard 5-field“ aus `configuration.md:471` prüfbar und verhindert, dass die nächste croniter-Erweiterung wieder still durchrutscht. Dieselbe Prüfung gehört an `swarmq schedule add --cron` (siehe Nebenbeobachtung N1). Neue Regel als Nachtrag zu E-065, festschreibende Tests für `R`, `0 0 9 * * *` und `0 0 * * WED`.

### C – Dürfen Nutzer-Lock-Namen interne Lock-Namen treffen? (R4F-09)

| Option | Inhalt |
|---|---|
| (a) | Getrennte Präfixe: Nutzer-Locks unter `sq:lock:u:` (oder Leader-Lock aus dem Lock-Präfix herausnehmen). Das ist BREAKING für laufende Locks, die beim Update kurz doppelt gehalten werden können, und braucht einen Migrationshinweis. |
| (b) | Reservierte Namen: Ein gerenderter Name, der mit `{sched}` beginnt oder einem internen Namen gleicht, geht wie ein Render-Fehler in die DLQ (`x-swarmq-lock-render-error`). |
| (c) | Dokumentieren: Lock-Templates sollen mit einem Literal beginnen; Nutzdaten gelten als vertrauenswürdig. |

Empfehlung **(b)**. Sie ist klein, nicht BREAKING und folgt dem Muster der Leerprüfung (`lock.py:229-250`). Dazu ein Satz zu (c) im Guide („start a templated lock with a literal prefix“). (a) nur, wenn weitere interne Lock-Namen dazukommen.

## Nebenbeobachtung (nicht eingestuft, neuer Verdacht für die Orchestrierung)

**N1 – `swarmq schedule add --cron` umgeht die Cron-Prüfung von `TaskInfo`.** `cli/schedule_cmd.py:139-153` prüft nur, ob `next_fire_ms` einen Termin liefert. `0 3 L * *` (1793415600000), `R * * * *` und `0 0 9 * * *` werden angenommen. `cli.md:313` nennt „`--cron` (5-field expression)“. E-065 regelt nur `TaskInfo`. Ob die Ablehnung von `L`/`W`/`#` auch für dynamische Termine gilt, ist nicht entschieden. Vorschlag: als eigener Verdacht gegenprüfen lassen und mit Produktfrage B zusammen entscheiden.

## Abgleich

- **Register:** E-007, E-011, E-012, E-022, E-043, E-051, E-054, E-055, E-057, E-063, E-065, E-070 und E-071 im Volltext oder per Titel und `applies_when`. Dazu `grep` über `compound-packs/swarmq-entscheidungen/` nach Render, Template, Zeitzone, Cron, Overflow, Leader-Lock und Zähler.
- **Befundregister:** `fehlersuche-runde-3-2026-10.md` (F3-31, F3-35, F3-38, Abschnitt „Entscheidungen außerhalb des Registers“ Zeile 136 Cron-Syntax), `fehlersuche-runde-2-2026-10.md` (offene LÜCKEN).
- **Pläne:** Plan R4 (R1, R4, R20, KTD8), Plan Runde 3 R9, `fehlersuche-runde-3-recherche/fix-r-learnings.md:127`.
- **docs/solutions:** `nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md` (ADV-009/010).
- **Website:** `configuration.md:31-33, 43, 46, 64, 123, 471-482`, `cli.md:32-43, 113, 313`, `locking-and-dedup.md:174-186`, `errors.md:22, 63-66`.
- **git:** `git log -S` zu `_FORBIDDEN_CRON_CHARS` (`a1a8c99`), `def _count` (`47aed19`), `OverflowError` in den Zähler-Helfern (`06958c8`).
- **Tests mit `entscheidung`-Marker:** E-065 (`test_task.py:353-368`), E-051 (`test_befund_fs19_counter_headers.py`), E-063 (`test_befund_f3_04_finite_deadlines.py` u. a.). Keiner widerspricht einem der vorgeschlagenen Nachweis-Tests für R4F-01, -02, -06 und -08.
