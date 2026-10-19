---
title: "Spike PF-16 (b): Verbindungs-Retry vom Befehls-Retry des Valkey-Clients trennen"
date: 2026-10-18
category: spikes
module: backend/valkey
problem_type: spike
component: backend
status: findings-captured
applies_when:
  - ein Plan will die Wiederholung des TCP-Aufbaus von der Befehls-Wiederholung trennen (PF-16 b)
  - Änderungen an der Retry-Politik, an socket_timeout/socket_connect_timeout oder an der Handshake-Wiederholung (F4-14) in backend/valkey/connection.py
  - eine Fehlersuche meldet minutenlange Befehle gegen einen Valkey-Host, der Pakete verwirft
tags: [valkey, retry, verbindung, pf-16, e-075, f4-14, f3-33, spike]
entscheidungen: [E-075]
---

# Spike PF-16 (b): Verbindungs-Retry vom Befehls-Retry trennen

Stand: Worktree `fix/fehlersuche-runde-4-p2` (HEAD b022439, 0.12.4 mit U4: Gesamtfrist beim
Verbinden, Handshake-Wiederholung F4-14, kein Health-Check-PING im Handshake), valkey-py 6.1.1.
Kein Produktionscode geändert.

## Frage

valkey-py kopiert die Retry-Politik des Clients (`Retry(ExponentialBackoff(cap=2, base=0.05),
retries=5)`) in jede Verbindung. `Connection.connect()` wiederholt damit den TCP-Aufbau, und
`execute_command` wiederholt mit **demselben Objekt** (`conn.retry`) den Befehl. Multiplizieren sich
die beiden Wiederholungen mit dem Stand nach U4 noch, und hilft eine eigene Retry-Politik für den
TCP-Aufbau mit 0–1 Wiederholungen (Option b aus PF-16), ohne den Handshake-Fall (F4-14) oder die
Toleranz gegen kurze Neustarts zu verlieren?

## Erfolgskriterium

Die im Repo auffindbare Fix-Recherche enthält keinen Teil „A5“; das Kriterium ist aus PF-16
(Register, Spalte Empfehlung), „Vor dem Fix“ Punkt 11 und E-075 abgeleitet:

1. Verbindungs- und Befehls-Wiederholung multiplizieren sich nicht: gegen einen Host, der Pakete
   verwirft, höchstens so viele TCP-Versuche je Befehl wie Befehlsversuche (6), nicht 30.
2. Der Handshake-Fall von F4-14 bleibt gedeckt; die E-075-Tests bleiben grün.
3. Kein Rückschritt gegen einen abweisenden Port: Start 6 Versuche/3,1 s, Neustart von 2 s zur
   Laufzeit wird überbrückt.
4. F3-33 (Wiederholung eines schon gelaufenen `lock_release.lua`) wird nicht schlechter.

## Aufbau

Ohne Testdienste, alles auf `127.0.0.1` im Probe-Prozess:

- **Fake-RESP-Server** (asyncio): `serve` beantwortet jeden Befehl, `silent` nimmt Daten an und
  antwortet nie. `silent` entspricht dem Toxiproxy-Toxic `timeout` mit `timeout=0` (Verbindung
  angenommen, Daten verschluckt, kein Schließen); ein Toxiproxy-Container war dafür nicht nötig.
- **SYN-Blackhole**: Listen-Socket mit `listen(0)` und einer Füllverbindung; weitere SYNs verwirft
  der Kernel, `connect` läuft in `socket_connect_timeout` (wie Firewall-`DROP`, falsche VPC-Adresse).
- Zählung: `valkey.asyncio.connection.Connection._connect` (TCP-Versuche) und
  `valkey.asyncio.retry.sleep` (Backoffs) im Probe-Prozess umwickelt.
- Szenarien: `connect()` gegen Blackhole (Frist 30 s und ohne wirksame Frist, 600 s), `connect()`
  gegen abweisenden Port; zur Laufzeit ein `GET` ohne `op_timeout` nach Partition (alte Verbindung
  stumm, neue SYNs verworfen), gegen „stumm“ (Toxiproxy `timeout`) und gegen einen Neustart
  ohne Proxy (FIN, Port 2/8/25 s abgewiesen, dann wieder da).
- Prototypen der Trennung (nur im Probe-Prozess, Unterklasse von `Connection`, deren `connect()`
  für den TCP-Aufbau eine eigene Retry nutzt, `conn.retry` bleibt Befehls-Retry):
  - `n0` / `n1`: Option (b) wörtlich, `Retry(NoBackoff(), 0 bzw. 1)`.
  - `timeout0`: Backoff und Versuche wie heute, aber ein **Zeitablauf** des TCP-Aufbaus wird nicht
    wiederholt (abgewiesene Verbindungen weiter fünfmal).
- Zeitwerte: „real“ mit den Produktionswerten (`socket_timeout` 10 s, `socket_connect_timeout` 5 s),
  „skaliert“ mit 0,4 s/0,2 s zum Zählen.
- Probe-Code (flüchtig): Scratchpad `fix4/spikes/pf16_probe.py`, `pf16_proto.py` (auch als
  pytest-Plugin `-p pf16_proto` mit `SWARMQ_PF16_MODE`).

## Messwerte

Heute (0.12.4 + U4), Produktionswerte:

| Szenario | Ausgang | Wanduhr | TCP-Versuche | Backoffs (Summe) |
|---|---|---|---|---|
| `connect()` gegen Blackhole, Frist 30 s | `BackendConnectionError` (Frist) | 30,0 s | 6 | 5 (3,1 s) |
| `connect()` gegen Blackhole, Frist 600 s | `BackendConnectionError` | 33,1 s | 6 | 5 (3,1 s) |
| `connect()` gegen abweisenden Port | `BackendConnectionError` | 3,1 s | 6 | 5 (3,1 s) |
| Laufzeit, Partition (Blackhole) | `BackendConnectionError` | **178,8 s** | **30** | 30 (18,6 s) |
| Laufzeit, stumm (Toxiproxy `timeout`) | `BackendConnectionError` | 63,2 s | 5 | 5 (3,1 s) |
| Laufzeit, Neustart 2 s | Antwort | 3,1 s | 6 | 5 |
| Laufzeit, Neustart 8 s / 25 s | `BackendConnectionError` | 3,1 s | 6 | 5 |

Befund heute: Beim **Start** gibt es keine Vervielfachung mehr (6 Versuche statt der 31 aus PF-16;
U4 hat den Health-Check-PING im Handshake unterdrückt, und `get_connection` liegt außerhalb der
Befehls-Retry). Zur **Laufzeit** multiplizieren sich beide weiter, sobald eine Verbindung mitten im
Befehl stirbt: 5 Befehlswiederholungen × 6 TCP-Versuche = 30 Versuche, rund 3 min für ein `GET`.

Prototypen im Vergleich (Produktionswerte; `n0`/`n1` skaliert gemessen, Versuchszahlen gelten
unverändert):

| Szenario | heute | `n0` | `n1` | `timeout0` |
|---|---|---|---|---|
| Laufzeit, Partition: TCP-Versuche / Wanduhr | 30 / 178,8 s | 5 / ≈ 38 s | 10 / ≈ 63 s | **5 / 38,1 s** |
| `connect()` Blackhole: Versuche / Wanduhr | 6 / 30,0 s (Frist) | 1 / 5 s | 2 / 10 s | 1 / 5,0 s |
| `connect()` abweisender Port: Versuche / Wanduhr | 6 / 3,1 s | **1 / 0 s** | **2 / 0 s** | 6 / 3,1 s |
| Laufzeit, Neustart 2 s | Antwort | **Fehler nach 0 s** | **Fehler nach 0 s** | Antwort |
| Laufzeit, stumm (Toxiproxy `timeout`) | 63,2 s | 63 s | 63 s | 63,2 s |
| Unit-Tests `test_valkey_handshake_retry.py` + `test_valkey_connect_timeout.py` (25) | grün | **2 rot** | **2 rot** | grün |

Rot unter `n0`/`n1` sind genau zwei Tests mit Marker `entscheidung("E-075")`:
`test_refused_tcp_is_retried_five_times_without_outer_retry` und
`test_handshake_resets_then_refused_tcp_stay_below_twice_the_window`.

Weitere Beobachtungen:

- Ein Neustart ohne Proxy (FIN, Port abgewiesen) scheitert heute schon in `get_connection`
  (`can_read_destructive` → Neuaufbau), also außerhalb der Befehls-Retry. Überbrückt werden nur die
  3,1 s der inneren TCP-Wiederholung; die Trennung mit 0–1 Wiederholungen nähme genau diese weg.
- „Stumm“ (Toxiproxy `timeout`) wird von keiner Variante berührt: 6 × `socket_timeout` (10 s) +
  3,1 s Backoff. Das ist die Laufzeitgrenze, die E-075 bewusst lässt.
- F3-33: Keine Variante ändert die Befehls-Retry; `lock_release.lua` wird nach verlorener Antwort
  weiter wiederholt, weder besser noch schlechter.

## Ergebnis

Option (b) wörtlich (eigene Retry mit 0–1 Wiederholungen für den Verbindungsaufbau) erfüllt
Kriterium 1, verletzt aber 2 und 3: Gegen einen abweisenden Port scheitert `connect()` sofort, und
ein 2-s-Neustart von Valkey zur Laufzeit wird nicht mehr überbrückt. Damit widerspricht sie den mit
E-075 festgeschriebenen Tests. Die Variante `timeout0` (nur Zeitabläufe des TCP-Aufbaus nicht
wiederholen) erfüllt alle vier Kriterien: Laufzeit-Partition 178,8 s → 38,1 s, 30 → 5 TCP-Versuche,
alle 25 Retry-/Frist-Tests grün, Neustart-Toleranz unverändert.

## Empfehlung für Georg

- **Option (b) wörtlich: nicht umsetzen.** **Entscheidung überdenken?** Nein für E-075, aber der
  Prüfauftrag in E-075 („eigene `Retry` mit 0–1 Wiederholungen“) ist so nicht haltbar. Argument:
  Die Trennung mit 0–1 Wiederholungen nimmt die einzige Wiederholung weg, die einen abgewiesenen
  Port beim Start und zur Laufzeit (Neustart ohne Proxy, Fehler in `get_connection`) überbrückt;
  zwei E-075-Tests würden rot. Nach der Stop condition ist hier nicht weitergebaut.
- **Variante `timeout0`: umsetzen empfohlen, nach Georgs Ok** (eigene kleine Änderung, Nachtrag
  E-075). Sie trennt nicht die Anzahl, sondern die Fehlerart: Ein Zeitablauf beim TCP-Aufbau wird
  innen nicht wiederholt, die Befehls-Retry wiederholt ihn höchstens fünfmal. Bewusste Folge, die
  Georg mitentscheiden sollte: `connect()` gegen einen Host, der Pakete verwirft, endet nach einem
  `socket_connect_timeout` (5 s) statt nach der Gesamtfrist (30 s); ein Worker startet dann früher
  neu (E-075 „Bewusste Folgen“ bleibt sinngemäß). Wer beim Start länger warten will, bräuchte eine
  Schleife bis zur Frist in `connect()`.
- Festschreibender Test dafür: Toxiproxy-Chaos-Test mit Partition (Blackhole) und `timeout`-Toxic,
  Messgröße TCP-Versuche je Befehl (≤ 6) und Wanduhr (≤ `socket_timeout` + 5 ×
  `socket_connect_timeout` + 3,1 s).
- Die Laufzeitgrenze „stumm“ (rund 63 s ohne `op_timeout`) löst keine Variante; das bleibt E-075.

## Umsetzung 0.14

Plan `docs/plans/2026-10-19-1500-feat-swarmq-0-14-plan.md` (R3, KTD3, U3), Nachtrag 0.14 zu E-075.
Umgesetzt ist die Variante `timeout0` als Option (c), mit einer Schleife bis zur Frist in `connect()`:

- `_mark_tcp_connect_timeouts` (in `make_marked_connection` von `_retry_handshake`) wickelt
  `connection._connect` jeder Pool-Verbindung: Ein `TimeoutError` wird `_TcpConnectTimeout`, weder
  `OSError` noch `ValkeyError`. valkey-py wiederholt ihn in `Connection.connect` nicht und wickelt ihn in
  `ConnectionError("Timeout connecting to server")`; die Befehls-Retry wiederholt diesen wie bisher.
  Statt einer Unterklasse von `Connection` (Prototyp) ein Instanz-Patch, weil rund 30 Tests
  `Valkey.from_url` ersetzen.
- `ValkeyConnection._ping_until_deadline` wiederholt den PING nur, wenn `_TcpConnectTimeout` in der
  Ursachenkette steht, mit `ExponentialBackoff(cap=2, base=0.05)` bis zur Gesamtfrist; Ende mit
  `Valkey connect timed out after {Frist}s`. Jeder andere Fehler endet wie bisher sofort.
- Die drei E-075-Tests mit Marker blieben unverändert grün.

Gemessen im `timing`-Durchgang (`tests/unit/backend/test_valkey_handshake_retry.py`), SYN-Blackhole wie
oben, skaliert (Connect-Timeout 0,2 s, `socket_timeout` 0,4 s, Backoff ein Zehntel, Summe 0,31 s), je
drei Läufe:

| Szenario | vorher (HEAD 46a8241) | 0.14 |
|---|---|---|
| `connect()`, Frist 2,5 s | 1,51 s, `Valkey connect failed: Timeout connecting to server` | 2,50 s, `timed out after 2.5s` |
| Laufzeit, Partition (`GET`, alte Verbindung stumm) | 8,29 s, 30 TCP-Versuche | 1,72 s, 5 TCP-Versuche |
| Befehl mit neuer Pool-Verbindung | 6 TCP-Versuche (≈ 1,51 s) | 0,20 s, 1 TCP-Versuch |

Auf Produktionswerte übertragen: Partition 178,8 s → rund 38 s, neue Pool-Verbindung rund 33 s → rund
5 s, Start gegen einen Host, der Pakete verwirft, bis zur Frist (vorher höchstens rund 33 s).

Der Fall „stumm“ (Toxiproxy-`timeout`-Toxic) bleibt unberührt; sein Chaos-Test als Regressionswächter
folgt nach dem Chaos-Stack 4.3.6 (U5). Toxiproxy kann den SYN-Verwurf nicht erzeugen, deshalb ist der
festschreibende Test der `timing`-Test mit echtem Blackhole (Entscheid zu Frage 3).
