---
title: Das Ersatz-Event log_record_unformattable trägt die Vorlage msg und die Argumente einzeln lesbar (str, sonst Typname); formatiert wird weiter nur im Listener
applies_when:
  - Änderungen an _SafeProcessorFormatter oder am Ersatz-Event log_record_unformattable (src/swarmq/logging_format.py)
  - eine Fehlersuche oder ein Lastlauf meldet log_record_unformattable aus aio_pika.tools (OneShotCallback) und fragt, was die ursprüngliche Meldung war
  - ein Plan will fremde Records schon im Event-Loop formatieren, damit die Meldung lesbar bleibt
tags: [logging, aio-pika]
id: E-090
datum: 2026-10-31
status: gilt
quelle:
  - "Georg 2026-10-31: „K5 überdenken?“ freigegeben, Entscheidung nach bestem Wissen autonom"
  - docs/befunde/fehlersuche-runde-4-2026-10.md (V0160-K5, Gegenprüfung 0.16.0)
  - docs/plans/2026-09-08-001-feat-debug-backlog-loadsuite-plan.md (B11, R27, KTD9)
  - docs/plans/2026-10-06-0831-refactor-logging-haertung-plan.md (R6, R8)
  - website/reference/logging.md (log_record_unformattable)
tests:
  - tests/unit/test_logging_formatter.py::test_substitute_event_keeps_template_and_readable_args
  - tests/unit/test_logging_formatter.py::test_substitute_event_keeps_mapping_args_by_name
  - tests/unit/test_logging_formatter.py::test_substitute_event_names_the_type_of_an_unprintable_template
---

**Entscheidung.** Scheitert die Formatierung eines Records (`msg % args`), rendert swarmq wie bisher das Ersatz-Event `log_record_unformattable` mit `logger`, `level` und `error` (R27). Zusätzlich trägt es `msg`, die unformatierte Vorlage, und `args`, die Argumente einzeln als `str()`: bei Tupel-Argumenten eine Liste, bei Mapping-Argumenten (`%(name)s`) ein Mapping, ohne Argumente eine leere Liste. Wirft `str()` eines Arguments oder der Vorlage, steht dort der Typname in spitzen Klammern (`<OneShotCallback>`). Formatiert wird weiterhin nur im Listener-Thread, nie im Event-Loop (R6).

**Begründung.** Bei jeder Broker-Trennung loggt aio-pika je offenem `RobustChannel` `"Callback %r error: %s: %s"` mit dem `OneShotCallback`, dem Ausnahmetyp und dem Text, und löscht danach `callback`; das `__repr__` wirft dann (aio-pika 10.0.4 bis 10.2.1). Das Ersatz-Event verlor bisher die ganze Meldung, auch Ausnahmetyp und Text, die sich sehr wohl darstellen lassen (6-h-Lauf an 0.16.0rc1: 16-mal, V0160-K5). Früh formatieren (im aufrufenden Thread) widerspräche R6 und kostete jeden Record im Event-Loop; die Argumente einzeln im Listener zu lesen, kostet nur den seltenen Fehlerfall.

**Bewusste Folgen.**
- `args` zeigt `str()`, nicht die Konvertierung der Vorlage (`%r`); ein String-Argument erscheint ohne Anführungszeichen.
- Ein Argument, dessen `str()` gelingt, dessen `repr()` aber wirft, erscheint lesbar, obwohl `%r` an ihm scheiterte.
- Die Zeile wird nicht in eine fertige Meldung zurückübersetzt; wer sie lesen will, setzt `args` in `msg` ein.

**Überdenken nur mit neuem Argument.** Behebt aio-pika das `__repr__` von `OneShotCallback`, entfällt der häufigste Anlass, die Felder bleiben für andere Bibliotheken.
