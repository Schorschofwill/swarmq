# Vergleich mit Celery, Dramatiq, arq und taskiq

SwarmQ wurde auf einer Referenzmaschine gegen Celery, Dramatiq, arq und
taskiq gemessen, unter Störungen und unter Last. Diese Seite zeigt die
Ergebnisse und ihre Grenzen. Wie gemessen wird und die Rohdaten stehen im
Repository unter `benchmarks/comparison/` (`METHODIK.md`, `wertung/`).

**Aufbau.** celsius: Xeon E5-2650 v3, 20 Threads, 60 GiB ECC-RAM, SATA-SSD;
RabbitMQ 4.3.6 und Valkey 8.1.10 in Containern. Jedes System läuft in zwei
Profilen:

- `ab_werk`: die Voreinstellungen des Systems.
- `stark`: seine stärksten vergleichbaren Einstellungen, etwa späte
  Bestätigung, Publisher-Confirms und Quorum-Queues.

SwarmQ hat feste Garantien, beide Profile sind bei SwarmQ gleich. Dramatiq
kann keine Quorum-Queues anlegen und nutzt in `stark` klassische dauerhafte
Queues.

| System | Version |
|---|---|
| SwarmQ | `main` an `a9237312` (Garantien: `b18499a9`) |
| Celery | 5.6.3 |
| Dramatiq | 2.2.1 |
| arq | 0.28.0 |
| taskiq | 0.13.0 mit taskiq-aio-pika 0.6.1 |

## Garantien unter Störungen

Jede Kombination reiht eine Weile Tasks ein, löst eine Störung aus, wartet,
bis sich das System erholt hat, und gleicht ein Hauptbuch der bestätigten
Publishes mit den ausgeführten Tasks ab. Drei Durchgänge am 2026-10-25; die
Zahlen sind Summen über die drei Durchgänge.

Störungen: Worker mit `SIGKILL` beendet, Broker-Neustart, Netztrennung,
Valkey-Neustart mit Persistenz, Valkey-Neustart ohne Persistenz.

| System | Profil | Verlorene Tasks | Doppelte Ausführungen | Abgelehnte Publishes |
|---|---|---:|---:|---:|
| SwarmQ | beide | **0** | **0** | **0** |
| Celery | `ab_werk` | 60 (`SIGKILL`) | 0 | 29 |
| Celery | `stark` | 0 | 120 (Broker-Neustart, Netztrennung) | 31 |
| Dramatiq | `ab_werk` | 0 | 189 (Broker-Neustart, Netztrennung) | 171 |
| Dramatiq | `stark` | 0 | 107 (Broker-Neustart, Netztrennung) | 172 |
| arq | beide | 65 / 67 (Valkey-Neustart ohne Persistenz) | 7 / 3 | 115 / 113 |
| taskiq | `ab_werk` | 0 | 120 (Broker-Neustart, Netztrennung) | 321 |
| taskiq | `stark` | 0 | 120 (Broker-Neustart, Netztrennung) | 291 |

- **Verlorene Tasks.** Celery bestätigt ab Werk vor der Ausführung und
  verliert die laufenden Tasks, wenn ein Worker beendet wird. arq nutzt
  Valkey als Broker und verliert die wartenden Jobs, wenn Valkey ohne
  Persistenz neu startet.
- **Doppelte Ausführungen.** Nach einem Broker-Neustart oder einer
  Netztrennung führen Celery (`stark`), Dramatiq und taskiq Tasks ein
  zweites Mal aus, deren Bestätigung mit der Verbindung verloren ging.
  SwarmQ hält einen Ausführungsschutz in Valkey und überspringt solche
  Neuzustellungen ([Zustellungssemantik](delivery-semantics.md)).
- **Abgelehnte Publishes.** Der Erzeuger von taskiq nimmt nach einem
  Broker-Neustart oder einer Netztrennung für den Rest des Laufs keinen Task
  mehr an. SwarmQ wiederholt Publishes, die nachweislich den Client nie
  verlassen haben, und lehnte keinen ab.

## Leistung

Median aus drei Durchgängen am 2026-10-26, bei jedem System zehn
gleichzeitige Tasks je Worker, Profil `stark`. Die Werte für `ab_werk`
stehen in den Rohdaten.

**`noop`**: ein leerer Task nach dem anderen, vom Einreihen bis zum
Ergebnis.

| System | Tasks/s | P50 | P99 |
|---|---:|---:|---:|
| Dramatiq | 288 | 3,3 ms | 13,1 ms |
| Celery | 119 | 8,1 ms | 15,1 ms |
| taskiq | 117 | 8,6 ms | 11,7 ms |
| SwarmQ | 111 | 9,0 ms | 12,9 ms |
| arq | 2 | 501 ms | 504 ms |

**`latency`**: 30 s lang 100 Tasks pro Sekunde, jeder schläft 10 ms.

| System | P50 | P95 | P99 |
|---|---:|---:|---:|
| Dramatiq | 14,9 ms | 16,7 ms | 17,7 ms |
| SwarmQ | 19,5 ms | 23,1 ms | 25,0 ms |
| Celery | 19,5 ms | 22,9 ms | 25,6 ms |
| taskiq | 19,6 ms | 22,9 ms | 24,5 ms |
| arq | 369 ms | 550 ms | 588 ms |

**`io_bound`**: 1000 Tasks, die je 10 ms schlafen, nebenläufig eingereiht
und eingesammelt.

| System | Tasks/s | P50 |
|---|---:|---:|
| taskiq | 447 | 102 ms |
| Dramatiq | 435 | 50 ms |
| SwarmQ | 386 | 552 ms |
| Celery | 299 | 48 ms |
| arq | 181 | 1106 ms |

Was die Zahlen zeigen:

- **Quorum-Queues kosten Zeit je Task.** Eine Quorum-Queue bestätigt einen
  Publish erst nach dem Schreiben auf die Platte. In `noop` liegen SwarmQ,
  Celery (`stark`) und taskiq, alle auf Quorum-Queues, gleichauf; Dramatiq
  auf klassischen Queues ist schneller. Lokal dauert der Publish in SwarmQ
  mit Quorum-Queue 2,1 ms, mit klassischer Queue 0,45 ms.
- **In `io_bound` begrenzt der Worker von SwarmQ.** Ein Task belegt einen der
  zehn Worker-Plätze 15,1 ms (lokal), davon 10,5 ms für den Task selbst; der
  Rest sind sechs Backend-Aufrufe nacheinander und Aufwand im Worker. Das
  hohe P50 sind Tasks, die auf einen Platz warten. Hier muss SwarmQ
  schneller werden.
- **arq fragt ab Werk nur alle 0,5 s nach Jobs**; das bestimmt `noop` und
  `latency`.

## Grenzen

- Eine Maschine; sie ist für jedes System 2,3- bis 3,3-mal langsamer als
  ein aktueller Laptop. Zahlen nur innerhalb dieser Seite vergleichen.
- In `noop` wartet SwarmQ per Pub/Sub auf das Ergebnis, die anderen werden
  alle 1 ms abgefragt und verlieren dabei bis zu 1 ms je Task. In `latency`
  werden alle gleich abgefragt.
- Ob bei Celery, Dramatiq und taskiq in `io_bound` Worker oder Erzeuger
  begrenzt, ist nicht untersucht.
- 61 bis 90 Tasks je Störungskombination; seltene Fälle zeigen sich damit
  nicht sicher.
