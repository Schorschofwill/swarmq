# CPU-Worker-Modus: Entwurf und Machbarkeitsnachweis

Arbeitsstand vom 2026-08-31. Dieses Dokument haelt fest, **was entschieden
ist, was gemessen ist und was noch offen ist** — damit die Arbeit ohne den
Gespraechsverlauf fortsetzbar bleibt.

Der Anforderungsplan ist noch **nicht** geschrieben; er ist der naechste
Schritt.

## Die Ausgangsfrage

SwarmQ ist reines asyncio und bietet kein Offloading rechenintensiver
Task-Koerper. Ein synchroner Pillow- oder numpy-Aufruf blockiert den
Event-Loop und damit alle nebenlaeufigen Tasks des Workers. Drei Probleme
folgen daraus:

1. **Rechenleistung** — CPU-Tasks laufen nicht wirklich nebenlaeufig.
2. **Abbrechbarkeit** — ein Hard-Timeout greift nicht, weil `asyncio`-Cancel
   eine blockierte Loop nie erreicht.
3. **Ergonomie** — jeder Betreiber mit CPU-Last dupliziert das Offloading im
   Task-Koerper.

In der Lasttest-Suite ist das Backlog-Eintrag **B4** (`swarmq-loadsuite`,
`docs/debug-feature-backlog.md`), dort aus Konsumentensicht beschrieben.

## Die Entscheidung: Ansatz B — CPU-Worker als eigener Betriebsmodus

Ein Loop-Prozess bedient ausschliesslich Broker, Health, Metriken und
Aufsicht und **rechnet nie**. N kurzlebige Rechenkinder, aus einem
vorgewaermten `forkserver` geforkt, fuehren die Tasks vollstaendig aus.

```
Supervisor ── Loop-Prozess (Broker, Health, Aufsicht — rechnet nie)
                  ├── Rechenkind 1 ─┐
                  ├── Rechenkind 2  ├─ je 1 Task, killbar, wegwerfbar
                  ├── Rechenkind 3  │
                  └── Rechenkind 4 ─┘
```

Das ist **nicht** dasselbe wie `--processes 4 --concurrency 1`: heute sind
das vier unabhaengige Broker-Consumer mit je eigener Verbindung, die niemand
beobachtet. Hier ist es **ein** Consumer mit vier Rechenslots — Celerys
Prefork-Topologie, aber mit einem Beobachter, der strukturell immer frei
ist.

Die Leitfrage, die den Loesungsraum sortiert hat: Von den drei Problemen ist
nur eines ein **Parallelitaetsproblem**. Abbrechbarkeit und
Leak-Recycling sind **Isolationsprobleme**, und Isolation gibt es in CPython
ausschliesslich an der Prozessgrenze.

### Verworfene Alternativen

| Weg | Warum nicht |
|---|---|
| **A — Offload-Primitive im Task** | Opt-in pro Task; wer es vergisst, hat weiter das alte Problem. Bleibt als **spaetere Ausbaustufe** offen fuer Tasks, die IO und Rechnen in einem `run()` mischen. |
| **C — Aufsicht statt Auslagerung** | Der Kill trifft den ganzen Worker, der Task landet per Redelivery in derselben Falle. Schrumpft im gewaehlten Entwurf auf die Recycling-Mechanik zusammen. |
| **Free-Threading (3.14)** | Overhead ist inzwischen ertraeglich (5–10 % statt 40 %), aber **ein Thread ist nicht killbar** und ein Leak im Thread ist ein Leak im Worker. Loest genau das Problem, das `--processes` schon loest, und keines der beiden schmerzhaften. |
| **ThreadPool wie taskiq** | Genau das laeuft heute in Produktion (Bildpipeline, DTF-Verifier) und funktioniert, solange die Bibliothek die GIL freigibt oder als Subprozess laeuft. Scheitert an denselben zwei Stellen wie Free-Threading: nicht killbar, nicht isoliert. Ist damit die **Messlatte**, nicht die Alternative — der CPU-Modus muss mindestens gleichziehen. |
| **Subinterpreter (PEP 734)** | **NumPy laedt nicht in Subinterpretern** (`numpy.core._multiarray_umath` verweigert). Damit faellt der gesamte wissenschaftliche Stack aus. Zusaetzlich dasselbe Killbarkeits-Problem, weil ein Subinterpreter in einem Thread laeuft. |
| **PEP 810 lazy imports (3.15)** | Real und nuetzlich, aber kein Architektur-Hebel: die Importkosten fallen im Forkserver **einmal** an und amortisieren ueber alle Kinder. Gehoert zur CLI-Startzeit, nicht hierher. |

### Der Fund, der den Entwurf getragen hat: `forkserver` statt `spawn`

| Variante | Kindstart (Median) |
|---|---|
| `spawn`, ohne Import | 34 ms |
| `spawn`, mit swarmq-Import | 114 ms |
| **`forkserver` + Preload** | **6 ms** |

Bei 6 ms wird das Recycling-Problem **gegenstandslos statt geloest**: man
nimmt fuer jeden Task ein frisches Kind, und kein Leak ueberlebt je einen
Task. Kein RSS-Limit, kein Taskzaehler, keine Schwellwerte.

Daraus die Regel:

- **Task ohne DI → frisches Kind pro Task.** Maximale Isolation, keine
  Konfiguration noetig.
- **Task mit DI → Kind wird wiederverwendet**, mit Recycling nach RSS
  (primaer) und Taskanzahl (zweite Linie), um den Verbindungsaufbau zu
  amortisieren. Gemessen wird **nach** dem Taskende, nie waehrenddessen —
  waehrend der Rechnung ist hoher Speicherbedarf legitim.

SwarmQ kann das selbst entscheiden: `_dishka_injected_keys` steht beim
Worker-Start fest.

### Was fuer den Task-Autor unveraendert bleibt

Die anfaengliche Sorge um eine Zweiklassen-Semantik war falsch. Der
Dishka-Container ueberquert die Prozessgrenze heute schon nicht — er wird
pro Prozess neu gebaut (`src/swarmq/dishka.py:88-90`). Ein Rechenkind ist
derselbe Fall wie ein Worker-Subprozess.

| Faehigkeit | Im CPU-Modus |
|---|---|
| `FromDishka[...]` im `run()` | unveraendert, loest im Rechenkind auf |
| `update_progress()` | ueber den Rueckkanal; der Loop-Prozess schreibt nach Valkey |
| Middleware-Hooks, Signale | laufen im Loop-Prozess um den Offload herum |
| Cancel | Loop-Prozess pollt Valkey und killt das Kind — **funktioniert erstmals wirklich** |
| Retry, DLQ, Rate-Limit, Locks | Loop-Prozess, unveraendert |

Der Preis liegt bei den Ressourcen, nicht bei den Faehigkeiten:
Verbindungen vervielfachen sich (N Kinder = N DB-Pools), Startkosten fallen
je Recycling erneut an, und **APP-scoped Singletons werden kurzlebiger** —
eine echte Verhaltensaenderung, die niemand aus der Task-Signatur ablesen
kann und die dokumentiert gehoert.

## Die Messungen

Sieben Serien, **52 Punkte, alle bestaetigt**, an Python 3.14.4 auf 24 Kernen. Die
Prototypen liegen in `prototypes/` und sind untracked; sie sind kein
Paketbestandteil (`only-include = ["src"]`).

Die Serien A bis F belegen die **Bausteine**, G bis I den **Lebenszyklus**
und die **Wiederverwendung** — jene Haelfte der Entwurfsregel, die bis zum
2026-08-31 unbelegt war.

G, H und I sind in zwei unabhaengigen Vollaeufen gemessen worden; die
tragenden Zahlen sind reproduzierbar (G4 +0,031 gegen +0,030 MB/min, G2
1,4 gegen 1,3 ms, H5 5,1 gegen 5,3 ms je Recycling).

### A-Serie — Machbarkeit (11/11)

`prototypes/cpu_worker_feasibility.py` + `cpu_feasibility_tasks.py`
Aufruf: `uv run --extra dishka python prototypes/cpu_worker_feasibility.py`

| | Befund |
|---|---|
| A1 | forkserver 9,0 ms vs. spawn 162,1 ms (Faktor 18); Hochlauf einmalig 144 ms |
| A2 | 803 ms CPU-Arbeit im Kind → Heartbeat des Loop-Prozesses schlug 78× |
| A3 | Kind ohne `await`-Punkt in 2,2 ms beendet, exitcode −9 |
| A4 | `FromDishka` im Rechenkind aufgeloest: `db(pid=1425382,no=1)` |
| A5 | `TaskContext` traegt ueber die Prozessgrenze in den REQUEST-Scope |
| A6 | 2 Fortschrittsmeldungen ueber den Rueckkanal waehrend der Rechnung (4 erwartet, 2 durch die 500-ms-Drosselung — die Task-Instanz-Logik laeuft im Kind unveraendert) |
| A7 | Exceptions kommen typerhaltend beim Loop-Prozess an |
| A8 | RSS im selben Kind [32, 72, 112, 152] MB (+120); frisches Kind startet bei 32 MB |
| A9 | Das Kind erbt keinen Event-Loop-Zustand (`RuntimeError`) |
| A10 | Jedes Kind baut sein eigenes APP-Singleton |
| A11 | **0 `Database`-Instanzen im frisch geforkten Kind** — der Forkserver registriert den Container, loest ihn aber nie auf. Keine geerbte Verbindung, die zwei Kinder sich teilen koennten. |

### B/C/D-Serie — Integration (11/11)

`prototypes/cpu_worker_integration.py` + `cpu_integration_tasks.py`
Braucht RabbitMQ auf 5699 und Valkey auf 6399 (Startanleitung im Docstring).

| | Befund |
|---|---|
| B1 | Forkserver hatte **1 Thread** beim Fork, obwohl numpy und Pillow vorgeladen sind |
| B2 | BLAS im Kind: 600×600 → Spur 3600,0 (korrekt) |
| B3 | Pillow im Kind: 1400 px → 700 px, Mittelpixel (10, 120, 240) |
| B4 | NumPy-Leak im selben Kind [40, 81, 121, 161] MB; frisches Kind bei 40 MB |
| B5 | Kind **mitten im BLAS-Aufruf** in 5,3 ms beendet, exitcode −9 |
| C1 | Fortschritt aus dem Kind ueber den Rueckkanal in echtes Valkey geschrieben |
| C2 | Cancel ueber das Valkey-Flag beendet ein rechnendes Kind nach 7 ms |
| C3 | Ende-zu-Ende ueber echtes RabbitMQ: publish → consume → Rechenkind |
| D1 | 200/200 Tasks in 3,6 s, 200 verschiedene Kinder; Eltern-RSS 43 → 43 MB |
| D2 | Forkserver getoetet — `multiprocessing` baut ihn transparent neu auf |
| D3 | Kind mitten im Task getoetet (join 3,5 ms, kein Ergebnis in der Queue); Folgetask ok |

**B1 ist die Auflage, die in den Plan muss.** NumPy startet seine
OpenBLAS-Threads erst beim **ersten BLAS-Aufruf**, nicht beim Import. Der
Forkserver importiert nur und rechnet nie, also bleibt er threadfrei und ist
eine sichere Fork-Quelle. *Der Preload darf importieren, aber niemals
rechnen.* Eine Aufwaerm-Berechnung im Preload-Modul kippt genau diese
Eigenschaft — das gehoert als gepruefte Bedingung in die Implementierung,
nicht als Kommentar.

**B5 ist der haerteste Fall.** Ein Kind in `np.linalg.svd` fuehrt keinerlei
Python-Bytecode aus: kein Signal-Handler, kein Interpreter-Check, keine
`asyncio`-Cancel-Chance. Trotzdem 5,3 ms bis zum Ende. Das ist die
Faehigkeit, die taskiq und arq strukturell nicht haben und Celery nur grob.

### E-Serie — Parallelbetrieb (7/7)

`prototypes/cpu_worker_parallel.py` + `cpu_parallel_tasks.py`, 4 Slots.
Schliesst die Luecke „es lief immer genau ein Rechenkind".

| | Befund |
|---|---|
| E1 | 4 Tasks: sequenziell 4,44 s, parallel 1,34 s → **Speedup 3,3–3,8×** von 4× moeglich |
| E2 | Loop-Prozess kam waehrend der Vollast **128× dran**, Jitter max 0,9 ms / median 0,5 ms |
| E3 | 4/4 korrekt in 4 verschiedenen Prozessen, je rund 1200 ms BLAS bei 1 Thread |
| E4 | 4 Kinder **gleichzeitig** mit je 443 MB resident (1,6 GB Druck); Loop-RSS unveraendert 27 MB, Jitter max 1,8 ms |
| E5 | Ein Slot getoetet — Ergebnis fehlt korrekt, **3/3 Geschwister rechneten unbeeindruckt zu Ende** |
| E6 | 40 Tasks ungleicher Laenge: Slot-Auslastung 88–89 %, kein Slot liegt brach |
| E7 | **Ohne Thread-Begrenzung macht jedes Kind 24 Threads auf** = 96 auf 24 Kernen, und der Lauf dauert 2,21–2,98 s statt 1,39–1,44 s |

**E7 ist ein neuer Befund und eine harte Auflage.** OpenBLAS liest die
Threadzahl beim **Laden der Bibliothek**, also im Forkserver — lange bevor
ein Rechenkind existiert. Der CPU-Modus muss `OMP_NUM_THREADS` und
Verwandte setzen, **bevor** der Forkserver startet. Ohne das ueberbucht der
Pool die Kerne N-fach und ist ueber doppelt so langsam. Das ist keine
Optimierung, sondern Voraussetzung.

### F-Serie — Dauerlauf (7/7)

10 Minuten, 4 Slots, rund 60.000 Rechenkinder — ein frisches Kind je Task.

| | Befund |
|---|---|
| F1 | **59.482/59.482 Tasks korrekt**, 298 absichtlich mitten im Task getoetet — kein Verlust, kein falsches Ergebnis |
| F2 | Loop-RSS 43,8 → **44,6 MB und ab Sekunde 50 unveraendert** ueber 550 s; Steigung +0,08 MB/min |
| F3 | Dateideskriptoren 25 → 25, **keine Drift** |
| F4 | Durchsatz erstes Fuenftel 103/s, letztes 98/s (**95 %**) — keine Degradation |
| F5 | **0 Zombies** nach 60.000 Kindprozessen |
| F6 | 298× Kindtod im laufenden Betrieb, jedes Mal verkraftet |
| F7 | Nach 600 s Leerlauf sind Valkey und RabbitMQ unveraendert benutzbar |

Ein Vorlauf mass zusaetzlich **61.344 verschiedene PIDs bei 61.344 Tasks** —
das „frisches Kind pro Task" ist damit nicht nur behauptet, sondern
abgezaehlt. Im Endlauf ist die PID-Menge auf 5.000 gedeckelt, weil sie sonst
selbst zum Leck im Loop-Prozess wuerde (siehe unten).

Zur **F3-Konstruktion**: Der Pool benutzt bewusst *geteilte* Queues statt
eines Queue-Paars je Task. Ein Paar je Task waere ein Dateideskriptor-Paar
je Task gewesen — genau die Drift, die F3 messen soll. Der Entwurf braucht
die geteilte Variante ohnehin, weil der Loop-Prozess einen einzigen
Rueckkanal bedient.

### Methodischer Nachtrag zu F2 — was der Dauerlauf zuerst gefunden hat

Der erste 10-Minuten-Lauf meldete eine **monotone Steigung von
+1,10 MB/min**, also rund 1,6 GB pro Tag. Die Quelle war nicht der Entwurf,
sondern das Messwerkzeug: der Pool hielt fuer jedes Kind einen Eintrag in
seinem Startzeit-Dictionary und in der PID-Menge und loeschte beide nie —
bei 61.000 Kindern zusammen rund 11 MB, exakt die gemessene Steigung. Nach
dem Aufraeumen ist der Verlauf flach, und der Test gibt die Groesse seiner
eigenen Buchfuehrung mit aus (`0 offen, 0 Ergebnisse`), damit ein
Restanstieg kuenftig belegt statt erklaert werden muss.

Zwei Lehren, die in den Plan gehoeren — eine dritte kommt beim
G4-Nachtrag weiter unten hinzu:

1. **Die Steigung zaehlt, nicht der Endwert.** Ein Leck von 5 MB/min bleibt
   in fuenf Minuten unter jeder absoluten Schranke und fuellt in einem Tag
   7 GB. Die Pruefung misst deshalb MB/min und rechnet auf einen Tag hoch.
2. **Der Loop-Prozess darf nichts pro Task behalten.** Genau das ist die
   Stelle, an der eine Implementierung mit einem Kind je Task still leckt —
   nicht im Rechenkind, das ohnehin stirbt, sondern in der Buchfuehrung des
   Beobachters.

### G-Serie — Speicherdruck ueber Zeit (5/5)

`prototypes/cpu_worker_lifecycle.py` + `cpu_lifecycle_tasks.py`
Aufruf wie im Docstring; `PROTO_SERIES=g|h|i|all`, `PROTO_MINUTES` setzt die Dauer.

Schliesst die Luecke, die E4 offen liess: dort standen vier Kinder mit
1,6 GB **1,5 Sekunden** lang unter Druck. Hier fuenf Minuten am Stueck,
knapp 4.000 Rechenkinder, durchgehend 4 x 400 MB resident.

| | Befund |
|---|---|
| G1 | 3.927 Tasks mit je 400 MB in 300 s, **0 falsch, 0 Fehler** |
| G2 | Fork-Latenz haelt unter Dauerdruck: erste 1000 Kinder median 1,3 ms, letzte 1000 median **1,4 ms** (p95 2,2 ms, max 7,6 ms) |
| G3 | Durchsatz erstes Fuenftel 12,9/s, letztes 13,2/s (**102 %**) |
| G4 | Loop-RSS 28,2 → 28,6 MB, Regression **+0,031 MB/min = 44 MB/Tag**; FDs 22 → 22; Jitter max 5,0 ms / median 0,54 ms ueber 28.297 Proben |
| G5 | Nach dem Lauf **0 Rechenkinder, 0 Zombies**, nur Forkserver (46 MB) und resource\_tracker (15 MB); SwapFree unveraendert |

**G2 ist der wichtigste Punkt der Serie.** Die niedrige Forkzeit ist die
Grundlage des ganzen Entwurfs — wenn sie unter Speicherdruck wegbricht,
wird „frisches Kind je Task" teuer. Sie bricht nicht: bei 1,6 GB
Dauerdruck bleibt der Median bei 1,4 ms, und er ist am Ende des Laufs
nicht schlechter als am Anfang.

*Nicht mit A1 verwechseln.* A1 misst `start()` **plus** `join()`, also den
vollen Zyklus bis ein No-Op-Kind beendet ist — das sind die 6 bis 9 ms,
die den Entwurf tragen. G2 misst nur `start()`, also den
Forkserver-Roundtrip, bis der Prozess existiert und der Loop-Prozess
weiterarbeiten kann. Beide Groessen sind richtig und beantworten
verschiedene Fragen: A1 „was kostet ein Kind insgesamt", G2 „wie lange
blockiert die Vergabe den Beobachter".

Zu G5: gemessen wird, was uns gehoert — ueberlebende Nachkommen, Zombies,
Swap. `MemAvailable` ist auf einem Host mit fremden Containern zu
verrauscht fuer eine Schranke und wird nur berichtet.

### H-Serie — Wiederverwendung, Recycling, Verbindungen (6/6)

Die Serie schliesst eine Luecke, die das Dokument selbst nicht als offen
gefuehrt hat: **alle 36 vorherigen Messpunkte laufen mit einem frischen
Kind je Task.** Kein einziger misst ein wiederverwendetes Kind — obwohl
die Regel „Task mit DI → Kind wird wiederverwendet, Recycling nach RSS"
genau darauf beruht. Der Prototyp bringt dafuer einen APP-Provider mit
echter Valkey-Verbindung mit, damit Amortisation und
Verbindungsvervielfachung zaehlbar statt schaetzbar sind.

| | Befund |
|---|---|
| H1 | 8 Tasks in **einem** Kind: 1 APP-Identitaet (wiederverwendet), **8/8 verschiedene REQUEST-Identitaeten** — die Scopes verschmieren nicht |
| H2 | 40 DI-Tasks: frisches Kind 0,68 s, wiederverwendet 0,11 s → **Faktor 6,0** |
| H3 | RSS nach Taskende steigt 103 → 163 → 223 → 283 MB und faellt bei Grenze 250 MB auf 103 MB zurueck — dreimal in Folge, **3 Recyclings, 3 Kinder** |
| H4 | **4 Kindverbindungen bei 4 Slots** am echten Valkey gezaehlt; nach dem Ende **0** — keine Leiche |
| H5 | Rollierendes Recycling kostet **5,1 ms je Recycling**; Slot-Luecke median 1,2 ms / max 2,1 ms |
| H6 | Kind mitten im Task getoetet — der Slot und seine Job-Queue bleiben brauchbar, die Folgetasks laufen korrekt |

**H2 ist eine Untergrenze, keine Kennzahl.** Der DI-Aufbau war hier ein
lokaler Valkey-Ping mit 4 ms — der billigste denkbare Fall. Der Faktor 6
entsteht deshalb groesstenteils aus Forkzeit plus Container-Aufbau, nicht
aus der Verbindung. Bei einem echten Datenbankpool (Zehntel- bis
Sekundenbereich) faellt er deutlich hoeher aus. Die Aussage lautet also:
*Wiederverwendung amortisiert messbar, und je teurer die APP-Ressource,
desto mehr.*

**H5 belegt den Vorsprung vor Celery konkret.** `max_tasks_per_child`
laesst den Slot waehrend des Neustarts brachliegen. Hier startet das
Ersatzkind, bevor das alte tot ist: ein Recycling kostet rund eine
Forkzeit, nicht eine Kindlebensdauer. Der Prozentaufschlag waere an dieser
Stelle irrefuehrend — die Testtasks sind kuerzer als ein Fork —, deshalb
misst der Punkt Millisekunden je Recycling.

**H6 ist eine Eigenschaft des Entwurfs, nicht des Tests.** Im
Wiederverwendungs-Modus hat jeder Slot seine eigene Job-Queue; eine
geteilte waere falsch, weil der Loop-Prozess ein *bestimmtes* Kind zum
Recycling ansprechen muss. Stirbt ein Kind mitten im Task, muss der Slot
trotzdem wieder brauchbar werden — sonst waere jeder Kindtod ein dauerhaft
verlorener Slot.

### I-Serie — Lebenszyklus am echten Broker (5/5)

Die riskanten Teile des Zusammenbaus, vorab gemessen statt spaeter
entdeckt. RabbitMQ 4.3.5, Quorum-Queues wie im Produktivpfad.

| | Befund |
|---|---|
| I1 | Shutdown-Signal bei **4 rechnenden Kindern**: alle 4 angenommenen Tasks in 1,78 s Drain zu Ende gerechnet, 4 nicht angenommene zurueck an den Broker |
| I2 | Kind **vor dem Ack** getoetet → 2 Zustellungen (`redelivered`-Flag gesetzt), zweiter Versuch korrekt — **kein Verlust** |
| I3 | 20 s reine Rechenzeit gehen sauber durch; Loop-Jitter waehrenddessen max 0,5 ms |
| I4 | Hoechstens **4 Nachrichten gleichzeitig unbestaetigt bei 4 Slots** — der Loop-Prozess hortet nicht |
| I5 | Exception im Rechenkind: 3 Versuche bei `max_retries=2`, **typerhaltend** uebertragen, danach im DLQ |

**I2 ist die Zusage, die at-least-once ueber die Prozessgrenze traegt.**
Der Loop-Prozess darf erst ackken, wenn das Kind ein Ergebnis geliefert
hat — nie schon beim Zustellen. Wird das verwechselt, verschwindet jeder
Task, dessen Kind stirbt, spurlos.

**I3 ist ein Teilbefund, kein Freibrief.** Der geltende
`consumer_timeout` ist 30 Minuten (`{ok,1800000}` am Broker gelesen); er
begrenzt damit die maximale Laufzeit **einer einzelnen Taskausfuehrung**,
unabhaengig davon, wie frei der Loop-Prozess ist. Der Versuch, die Grenze
durch Absenken auf 8 s zu provozieren, hat an RabbitMQ 4.3.5 **nicht
gegriffen** — weder ueber `application:set_env` noch ueber `conf.d`,
weder mit Classic- noch mit Quorum-Queue; ein 20-s-Task wurde in allen
Varianten anstandslos geackt. Die Grenze ist damit **dokumentiert, aber
nicht nachgemessen**. Sie bleibt ein benanntes Restrisiko fuer die
Lasttest-Suite, die einen echten Langlaeufer fahren kann.

### Methodischer Nachtrag zu G4 — die F2-Lehre, zum zweiten Mal

Der erste Fuenf-Minuten-Lauf der G-Serie meldete **+0,27 MB/min** bei
sauber monotonem Verlauf (28,0 → 29,4 MB) — 382 MB pro Tag. Wieder war
die Quelle nicht der Entwurf, sondern das Messwerkzeug: der
Heartbeat sammelte bei 10-ms-Takt jeden einzelnen Jitter-Wert in einer
Liste, ueber 300 Sekunden rund 30.000 Eintraege. Nach dem Umbau auf
laufende Statistik mit Reservoir-Stichprobe steht die Steigung bei
**+0,031 MB/min**, und der Verlauf ist ab Sekunde 30 flach.

Damit hat sich dieselbe Falle in zwei unabhaengigen Serien wiederholt.
Das hebt sie von einer Anekdote zu einer Regel — die dritte Lehre in der
Zaehlung des F2-Nachtrags:

3. **Auch das Messwerkzeug im Loop-Prozess darf nichts pro Task oder pro
   Tick behalten.** Jede Pruefung weist die Groesse ihrer eigenen
   Buchfuehrung mit aus (`0 offen, 0 Ergebnisse, 2000 Spawn-Zeiten,
   61 Messproben`), damit ein Restanstieg belegt statt erklaert wird.

Und eine Praezisierung zu Lehre 1: die Steigung wird per **linearer
Regression ueber alle Proben** bestimmt, nicht als Differenz der beiden
Endpunkte. Zwei Endpunkte sind dafuer das schlechteste Schaetzverfahren —
ein einzelner Ausreisser am Rand kippt das Ergebnis.

Ein zweiter, feinerer Fund derselben Art: ein einfacher Deckel auf die
gesammelten Fork-Zeiten waere falsch gewesen, weil er die **spaeten**
Werte abschneidet — genau die, die G2 fuer den Vergleich Anfang gegen
Ende braucht. Der Test haelt deshalb zwei feste Fenster: die ersten 1000
Werte und, ringfoermig, die letzten 1000.

### Ein Nebenbefund zur DI-Signatur

Beim Bau der H-Serie ist aufgefallen: **`FromDishka[X] | None = None`
wird von dishka nicht als Abhaengigkeit erkannt.** Der Parameter bleibt
still `None`, und `validate_registered_tasks` schlaegt nicht an, weil
`_dishka_injected_keys` leer bleibt — die Injektion faellt lautlos aus.
`prototypes/cpu_integration_tasks.py:62` hat genau diese Signatur; dort
wird nie ein `db` injiziert. Die A-Serie ist davon **nicht** betroffen
(`cpu_feasibility_tasks.py:95` deklariert korrekt ohne `| None`), A4
wurde am 2026-08-31 nachgemessen und steht.

Das ist keine Eigenschaft des CPU-Modus, sondern eine allgemeine Falle
fuer Task-Autoren — und ein Kandidat fuer eine Startpruefung: eine
`FromDishka`-Erwaehnung in einer Annotation, die keinen Key ergibt, ist
mit hoher Wahrscheinlichkeit ein Versehen.

## Auflagen fuer die Implementierung

Aus den Messungen abgeleitet, nicht aus der Theorie:

1. **Der Forkserver-Preload darf importieren, aber niemals rechnen** (B1).
   Eine Aufwaerm-Berechnung im Preload-Modul macht den Forkserver
   threadbehaftet und die Fork-Quelle unsicher. Gehoert als geprueft
   erzwungene Bedingung in den Code.
2. **Threadzahl je Rechenkind setzen, bevor der Forkserver startet** (E7).
   `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`,
   `NUMEXPR_NUM_THREADS` — und fuer den ersten echten Anwendungsfall
   zusaetzlich **`MAGICK_THREAD_LIMIT`**, weil ImageMagick ueber OpenMP
   genauso ueberbucht. In Phoenix ist das bereits als offene Massnahme
   S3 erfasst (zusammen mit `MAGICK_MEMORY_LIMIT` und
   `MAGICK_AREA_LIMIT`); im CPU-Modus wird daraus eine Startbedingung
   statt einer Deployment-Einstellung.

   Dazu ein Dimensionierungsbefund aus derselben Quelle: **Pillow
   skaliert nur bis zur Kernzahl** (4 Kerne: 1/2/4/8 Threads → 18,0 /
   11,5 / **7,8** / 8,2 s). Die Slotzahl bemisst sich an den Kernen, nie
   an der Queue-Tiefe.
3. **Kein APP-Provider, der beim Import eine Verbindung oeffnet.** A11 zeigt,
   dass der Forkserver den Container kalt haelt; taete er es nicht, teilten
   sich zwei Kinder eine Verbindung — ein stiller Datenkorruptions-Bug.
4. **Recycling protokollieren**: jedes Recycling mit Grund und Messwert als
   Log und Metrik. Ohne das debuggt man einen Durchsatzeinbruch blind.
5. **Kein harter Kill mitten in der Rechnung** in der ersten Stufe. Ein Task,
   der bei Minute 14 von 15 gekillt wird, geht per Redelivery in dieselbe
   Falle. Den Host vor OOM zu schuetzen ist Aufgabe des cgroup-Limits.
6. **`docs/worker-architecture.md:99` korrigieren** (Abschnitt 2.3, „Warum
   `spawn` und nicht `forkserver`"). Fuer Worker gilt die alte Begruendung
   weiter — sie starten selten. Fuer recycelte Rechenkinder traegt sie
   nicht mehr; die Messung 6 ms vs. 114 ms gehoert als Begruendung dorthin.
   *Entscheidung des Nutzers vom 2026-08-31: im selben Zug korrigieren.*
   **Erledigt am 2026-08-31**: Abschnitt 2.3 traegt die Messtabelle und
   trennt jetzt Worker-Subprozesse (`spawn`) von Rechenkindern
   (`forkserver`).
7. **Der Ack gehoert hinter das Ergebnis des Kindes, nie vor den
   Offload** (I2). Wird das verwechselt, verschwindet jeder Task, dessen
   Kind stirbt, spurlos — die Redelivery ist das einzige, was
   at-least-once ueber die Prozessgrenze traegt.
8. **Prefetch = Zahl der Rechenslots** (I4). Haelt der Loop-Prozess mehr
   Nachrichten unbestaetigt als er Slots hat, liegen sie bei ihm fest,
   waehrend ein anderer Worker frei ist.
9. **Im Wiederverwendungs-Modus gehoert jedem Slot seine eigene
   Job-Queue** (H6). Eine geteilte Queue kann das Ende-Signal nicht an ein
   *bestimmtes* Kind richten, und ein Kindtod duerfte den Slot nicht
   dauerhaft verbrennen. Die Zahl der Queues ist mit den Slots gedeckelt
   und waechst nicht mit den Tasks.
10. **Die maximale Tasklaufzeit ist eine Brokereinstellung**, nicht eine
    Entwurfsentscheidung (I3). RabbitMQs `consumer_timeout` steht auf
    30 Minuten; er zaehlt von der Zustellung bis zum Ack und interessiert
    sich nicht dafuer, wie frei der Loop-Prozess ist. Der CPU-Modus muss
    den Wert kennen, dokumentieren und im Zweifel anheben.
11. **Eine wirkungslose `FromDishka`-Annotation muss beim Start auffallen.**
    `FromDishka[X] | None = None` ergibt keinen `DependencyKey`, die
    Injektion faellt lautlos aus, und `validate_registered_tasks` schweigt,
    weil `_dishka_injected_keys` leer bleibt. Eine `FromDishka`-Erwaehnung
    in einer Annotation, die keinen Key ergibt, ist mit hoher
    Wahrscheinlichkeit ein Versehen und gehoert als `ConfigurationError`
    gemeldet. *Entscheidung des Nutzers vom 2026-08-31: als Auflage in den
    Plan, nicht als eigener Fix vorab.* Betrifft ganz SwarmQ, kommt aber
    mit dem CPU-Modus, weil der die DI-Regel traegt.
12. **`MALLOC_TRIM_THRESHOLD_` gehoert in die Umgebung der Rechenkinder**,
    zusammen mit `MALLOC_ARENA_MAX`. Fuer wiederverwendete Kinder ist es
    die billigere Haelfte der Loesung gegen das glibc-Wachstum (siehe den
    Abschnitt weiter unten), fuer frische Kinder eine Ruecken-Deckung. Der
    Wert kostet rund 14 % Laufzeit je Bild und gehoert deshalb
    dokumentiert, nicht stillschweigend gesetzt. **Einzige Auflage, die
    nicht aus den eigenen Messungen stammt**, sondern aus dem
    Produktionsbefund des ersten Anwendungsfalls.

## Was der Entwurf nebenbei loest: das glibc-Wachstum

Der erste echte Anwendungsfall bringt ein Problem mit, das im Entwurf
nicht vorkam und das er trotzdem erledigt. In Phoenix waechst der RSS
der Bildpipeline um rund **240 MB je 180 Bilder**; `gc.collect()` gibt
nichts frei, `malloc_trim(0)` gibt **alles** frei. Es ist kein Leck:
glibc haelt den Speicher fest, weil es seinen Trim-Schwellwert dynamisch
nach oben schiebt. Ein explizit gesetztes `MALLOC_TRIM_THRESHOLD_`
drueckt das Wachstum von +288/+305/+246 MB auf +5/+5/+6 MB, zum Preis
von rund 14 % Laufzeit je Bild. Quelle: Vollanalyse in Phoenix,
`docs/solutions/infrastructure/2026-08-28-worker-cpu-oom-vollanalyse.md`.

Fuer SwarmQ galt bisher dieselbe Anfaelligkeit — kein `MALLOC_*`, kein
Prozess-Recycling, keine Speicherueberwachung im Prozess-Manager. Der
CPU-Modus aendert das an beiden Enden:

- **Task ohne DI → frisches Kind je Task.** Der Prozess stirbt, bevor
  glibc ueberhaupt etwas festhalten kann; der Kernel nimmt alle Seiten
  zurueck. Das Problem entsteht gar nicht erst — dieselbe Bauart, mit
  der schon das Leak-Recycling gegenstandslos wurde. Gemessen in G5:
  nach 3.900 Kindern kein einziger ueberlebender Prozess.
- **Task mit DI → wiederverwendetes Kind.** Hier gilt der Effekt weiter,
  und genau darauf misst die Recycling-Regel: H3 zeigt den RSS nach
  Taskende steigen und bei der Grenze zurueckfallen. `MALLOC_TRIM_THRESHOLD_`
  bleibt trotzdem sinnvoll, weil es billiger ist als ein Recycling.

Daraus folgt Auflage 12 oben — die einzige, die nicht aus den eigenen
Messungen stammt, sondern aus dem Produktionsbefund des ersten
Anwendungsfalls.

## Wo SwarmQ damit vor Celery und taskiq liegt

Bei Tasks von Sekunden bis Minuten ist Dispatch-Overhead irrelevant — der
No-Op-Benchmark im Repo misst eine Groesse, die hier nicht zaehlt. Der
Vorsprung entsteht woanders:

- **Ein Deployment fuer beide Welten** — Celery braucht zwei Worker mit zwei
  Pool-Typen.

**Korrektur vom 2026-08-31.** Hier stand zuvor „taskiq kann CPU-Tasks gar
nicht". Das ist zu scharf und widerspricht der eigenen Evaluation:
taskiq fuehrt synchrone Tasks via `loop.run_in_executor()` aus
(`docs/evaluation-task-queues.md:818`), also in einem ThreadPool. Fuer
Bibliotheken, die die GIL freigeben (Pillow, numpy) oder als Subprozess
laufen (ImageMagick), traegt das — und traegt es heute produktiv: die
Bildpipeline und der DTF-Verifier laufen genau so.

Der Vorsprung ist damit nicht „geht ueberhaupt", sondern **„geht
killbar und isoliert"**: ein Thread laesst sich nicht toeten, und ein
Leak im Thread ist ein Leak im Worker. Das ist dieselbe Grenze, an der
oben schon Free-Threading gescheitert ist.
- **Hard-Timeout und Cancel, die greifen** — taskiq und arq koennen es
  prinzipbedingt nicht, Celery nur grob.
- **Fortschritt bei Langlaeufern** ueber den Rueckkanal — Celery hat dafuer
  nichts Eingebautes.
- **DI unveraendert im Rechenkind** — Celery kennt kein DI.
- **Rollierendes Recycling** — Celerys `max_tasks_per_child` laesst den Slot
  waehrend des Neustarts brachliegen; bei 6 ms startet das Ersatzkind, bevor
  das alte stirbt.
- **Externe Introspektion** — `python -m asyncio ps <pid>` (3.14) zeigt den
  Task-Stack eines haengenden Workers von aussen.

## Entscheidungen des Nutzers (2026-08-31)

1. **`docs/worker-architecture.md` wird im selben Zug korrigiert**, statt zwei
   widersprechende Aussagen im Repo stehen zu lassen.
2. **Der Schutz gegen versehentlich blockierende Tasks kommt auch in den
   normalen IO-Worker** — Lebenszeichen-Kanal zum Supervisor plus
   Selbst-Recycling nach N Tasks oder X MB RSS. Der CPU-Modus bleibt also
   nicht die einzige Antwort darauf. Das ist der Rest von Ansatz C, der
   damit eigenstaendigen Umfang bekommt.

## Was weiterhin offen ist

- **Die swarmq-Worker-Integration selbst.** Die riskanten Bausteine des
  Zusammenbaus sind inzwischen gemessen (I-Serie: Graceful Shutdown,
  Ack-Reihenfolge und Redelivery, Prefetch, Retry bis DLQ). Offen bleibt
  die **Middleware- und Signal-Kette um den Offload herum** und die
  Verdrahtung in den echten `worker/pipeline.py` — dort entscheidet sich,
  ob Rate-Limit, Locks und Expiry-Gates unveraendert vor dem Offload
  greifen.
- **Kein 24-Stunden-Lauf.** Fuenf bis zehn Minuten loesen eine Drift von
  rund 0,03 MB/min auf; langsamere Effekte brauchen die Lasttest-Suite
  (`swarmq-loadsuite`), fuer die bereits ein Plan existiert.
- **`consumer_timeout` ist dokumentiert, aber nicht nachgemessen** (I3).
  Der Provokationsversuch griff an RabbitMQ 4.3.5 in keiner Variante. Ob
  ein Task jenseits von 30 Minuten die Zustellung wirklich verliert,
  gehoert in die Lasttest-Suite.
- **Verbindungsvervielfachung ist gezaehlt, nicht geloest** (H4). Vier
  Slots ergeben vier Verbindungen, und Recycling hinterlaesst keine
  Leiche — die Bibliothek redet dem Nutzer aber weiterhin nicht in seine
  Pool-Groessen hinein. Das bleibt eine Dokumentationsaufgabe.
- **Die Amortisation ist eine Untergrenze** (H2). Gemessen mit einem
  4-ms-Valkey-Ping; mit einem echten Datenbankpool faellt sie hoeher aus,
  aber wie viel hoeher, weiss nur der Betreiber.

## Naechster Schritt

Den **Anforderungsplan** schreiben (`ce-brainstorm` → requirements-only
unified plan unter `docs/plans/`), mit:

- dem Scope oben,
- den **zehn** Auflagen als Verifikationsbedingungen,
- den zwei Nutzerentscheidungen (Auflage 6 ist inzwischen erledigt),
- und den offenen Punkten als benannte Restrisiken, nicht als erledigt.

Der Zuschnitt steht seit dem 2026-08-31: Nutzerentscheidung 2 —
Lebenszeichen-Kanal und Selbst-Recycling im normalen IO-Worker — wird
eine **eigene, unabhaengig lieferbare Phase im selben Plan**. Beide
Phasen teilen die Recycling-Mechanik (RSS-Messung nach Taskende,
rollierender Ersatz), die die H-Serie gemessen hat; ein Dokument haelt
die Begrifflichkeit zusammen, die Umsetzung bleibt trennbar.

## Nachmessung gegen die Implementierung (2026-09-05, swarmq 0.5.0)

Die I-Serie und die Startlatenz wurden nach dem Merge von MR !47 gegen die
echte Implementierung nachgefahren — `SwarmQ(pool="cpu")` am lokalen
RabbitMQ 4 mit Quorum-Queue, Valkey 8, Skript
`prototypes/cpu_worker_measure.py` (manuell, nicht Teil der Testsuite).

| | Befund |
|---|---|
| M0 | Startlatenz Dispatch → `run()` im frischen Kind, n=40: **median 5,3 ms**, p95 5,9 ms, max 6,3 ms; davon `process.start()` 1,1 ms, Spawn bis Ready 4,2 ms; 40 verschiedene Kind-PIDs |
| I1 | SIGTERM bei 4 rechnenden Kindern (8 geplant, 4 Slots): Drain 2,01 s, 4/8 im ersten Lauf fertig, 8/8 nach dem zweiten Lauf, keine Retries, keine Fehler |
| I2 | Kind vor dem Ack per SIGKILL getoetet: 2 Versuche, 1 Retry-Signal, Ergebnis des zweiten Versuchs — **kein Verlust** |
| I3 | 20 s reine Rechenzeit im Kind: Loop-Jitter waehrenddessen median 0,11 ms, p99 0,85 ms, max 2,2 ms |
| I4 | 24 Tasks auf 4 Slots: hoechstens **4 gleichzeitig angenommen**, 24/24 Ergebnisse |
| I5 | `ValueError` im Kind mit `max_retries=2`: 3 Fehlversuche, 2 Retries, 1 DLQ-Signal, Fehlertext **typerhaltend** |

Alle fuenf I-Punkte bestaetigen die Machbarkeitsmessung. **M0 tat es zunaechst
nicht:** die erste Messung lag bei 57–67 ms je Kind statt im einstelligen
Millisekundenbereich. Die Zerlegung (Spawn bis Ready 55 ms, `process.start()`
1 ms, Ready bis `run()` 1 ms) zeigte, dass die Zeit im Kind zwischen Fork und
Ready-Meldung verging — in Importen. Zwei Ursachen, beide im Preload des
Forkservers:

1. `set_forkserver_preload()` **ersetzt** die multiprocessing-Standardliste
   `["__main__"]`. Ohne `__main__` importiert jedes frische Kind das
   Hauptmodul des Loop-Prozesses samt aio_pika und valkey neu (~45 ms). Bei
   `python -m paket.modul` laedt multiprocessing das Hauptmodul zudem ueber
   seinen *Namen* nach, weshalb der Preload den echten Modulnamen tragen muss.
2. Die Kind-Laufzeit selbst (`swarmq.worker.compute.child`, `envelope`,
   `swarmq.logging`) war nicht im Preload und wurde je Kind importiert (~25 ms).

Mit beidem im Preload (`wiring.preload_modules`) liegt die Startlatenz bei
5 ms. Der Befund ist ein Beispiel fuer die Lehre der F-Serie: Der Prototyp
hatte ein winziges Hauptmodul und konnte den Effekt nicht zeigen; erst die
Messung gegen den echten Loop-Prozess machte ihn sichtbar.
