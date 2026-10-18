---
title: Der Verbindungsaufbau zum Backend (PING beim Verbinden) läuft unter einer Gesamtfrist und endet mit BackendConnectionError; die Wiederholpolitik zur Laufzeit bleibt
applies_when:
  - Änderungen an ValkeyBackend.connect (backend/valkey/connection.py), an socket_connect_timeout oder an der Retry-Politik des Valkey-Clients
  - eine Fehlersuche meldet, dass ein Worker gegen einen Host, der Pakete verwirft, minutenlang im Start hängt
  - ein Plan will Verbindungs- und Befehls-Wiederholung trennen (PF-16 b)
tags: [backend, valkey, verbindung, start, frist, pf-16, f4-12, f4-13, f4-14]
id: E-075
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-16, Option a)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-16, R4M-D-02b; Vor dem Fix Punkt 11)
  - Commit 111983a (socket_connect_timeout, bound every hang path)
  - docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-47 (Befehls-Retry-Politik)
ohne_test: Test folgt mit dem Fix (Bündel 3 des Fix-Plans 0.13 mit F4-12, F4-13, F4-14). Heute hat connect() keine Gesamtfrist (gemessen rund 2,9 min gegen einen Host, der Pakete verwirft); ein grüner Test ist erst nach dem Fix möglich. Die Nachweise F4-12 und F4-13 decken die Fehlerklasse und das Rücksetzen ab, nicht die Frist.
---

**Entscheidung.** `ValkeyBackend.connect()` führt den `PING` beim Verbinden unter einer Gesamtfrist aus (Größenordnung 15–30 s, konfigurierbar; Name und Default legt der Fix-Plan fest). Reißt die Frist, wirft `connect()` `BackendConnectionError` (und schließt den eigenen Client, F4-13). Die Wiederholpolitik zur Laufzeit bleibt unverändert (`Retry(ExponentialBackoff(cap=2, base=0.05), retries=5)`). Ob Verbindungs- und Befehls-Wiederholung getrennt werden (PF-16 b), ist nicht entschieden: Prüfauftrag im Fix-Plan mit Toxiproxy-Test (`timeout`-Toxic), dabei F3-33 mitbetrachten.

**Begründung.** valkey-py kopiert die Retry-Politik des Clients in jede Verbindung und wiederholt dort den Verbindungsaufbau, während der Client den Befehl wiederholt: 31 Verbindungsversuche, mit 5 s Connect-Timeout rund 2,9 min bis zur Ausnahme. Unter K8s bestimmt die Startdauer Readiness und CrashLoop-Takt; 111983a wollte eine Grenze, keine Dauer von Minuten.

**Bewusste Folgen.** Ein Worker, dessen Valkey beim Start länger als die Frist nicht antwortet, endet mit `BackendConnectionError` und wird neu gestartet, statt zu warten. Zur Laufzeit kann ein Befehl ohne `op_timeout` bei einer Netzpartition weiter so lange blockieren wie heute.

**Überdenken nur mit neuem Argument.** Offen: PF-16 (b) (Trennung der Wiederholungen, betrifft auch die Laufzeit).
