---
title: already_exists in der JSON-Ausgabe von swarmq schedule add heißt „unter dem Namen existiert ein Eintrag“; bei einem Dedup-Duplikat ist es false, den Grund nennt duplicate_of
applies_when:
  - Änderungen an _render_add_result oder _run_schedule_add (cli/schedule_cmd.py)
  - eine Fehlersuche meldet, dass schedule add --format json bei einem Duplikat already_exists false meldet
  - Änderungen an der Doku der JSON-Felder von schedule add (website/reference/cli.md)
tags: [cli, schedule, json, ausgabevertrag, unique, pf-7]
id: E-076
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-7, Option a)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-7, Verdacht R4M-F-02)
  - Commit 8971cbc (already_exists = not inserted), Commit 0a2a8b6 (duplicate_of)
tests:
  - tests/unit/test_cli_schedule_add_contract.py::test_inserted_schedule_reports_already_exists_false
  - tests/unit/test_cli_schedule_add_contract.py::test_taken_name_reports_already_exists_true
---

**Entscheidung.** In `swarmq schedule add --format json` heißt `already_exists`: Unter dem Namen existiert ein Eintrag (der Grund, warum nichts gespeichert wurde). Bei einem Dedup-Duplikat (`--unique-for`, Claim gehört einem anderen Schedule) ist `already_exists` `false`; den Grund nennt `duplicate_of`. `inserted` bleibt unverändert. Das Feld wird in `cli.md` dokumentiert, mit Changelog-Hinweis. Eine eigene Regel zur CLI-Ausgabe gab es bisher nicht (E-062 regelt nur `--check`, E-058 nur die Meldung „already exists“ bei gleichem Namen).

**Begründung.** Agenten lesen die JSON-Ausgabe als Vertrag (R3.10 P3 #18). `already_exists = not inserted` (8971cbc) stammt aus der Zeit vor `duplicate_of` (0a2a8b6) und meldet heute `true`, obwohl unter dem Namen nichts gespeichert ist.

**Bewusste Folgen.** Ein Aufrufer, der `already_exists` als „nicht gespeichert“ gelesen hat, muss `inserted` lesen. Festgeschrieben ist heute der Fall ohne Duplikat (neu angelegt, Name belegt); der Test für den Duplikatfall folgt mit dem Fix (PF-7, eine Zeile in `_render_add_result`).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
