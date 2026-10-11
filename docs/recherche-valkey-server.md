# Recherche: Valkey Server-Konfiguration, Datentypen und Best Practices fuer SwarmQ

**Erstellt:** 2026-04-15
**Zweck:** Referenzdokument fuer die serverseitige Valkey-Konfiguration des SwarmQ Result-Backends und Distributed-Lock-Systems. Ergaenzt die bestehende valkey-py Client-Recherche.

---

## Inhaltsverzeichnis

1. [Valkey Datentypen — Deep Dive fuer SwarmQ](#1-valkey-datentypen--deep-dive-fuer-swarmq)
2. [Memory Management](#2-memory-management--kritisch-fuer-result-backend)
3. [Persistenz](#3-persistenz--was-swarmq-empfehlen-sollte)
4. [Sicherheit](#4-sicherheit)
5. [Performance Tuning](#5-performance-tuning)
6. [Replikation und Hochverfuegbarkeit](#6-replikation-und-hochverfuegbarkeit)
7. [Monitoring und Observability](#7-monitoring-und-observability)
8. [Docker/Container Best Practices](#8-dockercontainer-best-practices)
9. [Valkey vs Redis Unterschiede](#9-valkey-vs-redis-unterschiede)
10. [Konkrete Konfigurationen fuer SwarmQ](#10-konkrete-konfigurationen-fuer-swarmq)

---

# 1. Valkey Datentypen — Deep Dive fuer SwarmQ

## 1.1 Strings (SET/GET) — Primaerer Datentyp fuer Result Storage und Lock

**Kernbefehle fuer SwarmQ:**

```
SET sq:r:{task_id} <msgpack_blob> PX 3600000    # Result mit 1h TTL
SET sq:l:scheduler <worker_id> NX PX 30000       # Lock-Erwerb
GET sq:r:{task_id}                                # Result abrufen
GETDEL sq:r:{task_id}                             # Atomar abrufen + loeschen
GETEX sq:r:{task_id} PX 3600000                   # Abrufen + TTL verlaengern
```

**Encoding-Typen (intern):**

| Encoding | Wann verwendet | Speicherverbrauch |
|---|---|---|
| `int` | Wert ist Integer <= 2^63 | Minimal (shared objects fuer 0-9999) |
| `embstr` | Wert <= 44 Bytes | Effizient (ein Speicherblock) |
| `raw` | Wert > 44 Bytes | Standard (separater SDS-String) |

Fuer SwarmQ: msgpack-Results sind fast immer > 44 Bytes → `raw`-Encoding. Die Kompaktheit kommt durch msgpack, nicht durch Valkey-Encodings.

**Maximale Value-Groesse:** 512 MB pro Key.

**Memory-Overhead pro String-Key:**
- Dictionary-Entry: 24 Bytes (Valkey 8.x mit Swiss Tables)
- Key-Objekt (robj): 16 Bytes
- Key-String (SDS): len + 1 + SDS-Header (3-9 Bytes)
- Value-Objekt (robj): 16 Bytes
- Value-String (SDS): len + 1 + SDS-Header
- **Gesamt: ca. 80-120 Bytes Overhead** zusaetzlich zur Payload

**GETDEL:** Atomar abrufen und loeschen. Perfekt fuer fire-and-forget Results. Optional als `get_result(delete=True)` anbieten.

**GETEX:** Atomar abrufen und TTL aendern. Ideal fuer Sliding-Expiration. Optional als `get_result(extend_ttl=True)`.

**Empfehlung:** Strings sind der **optimale Datentyp** fuer SwarmQ's Result Storage.

## 1.2 Hashes (HSET/HGET) — Alternative fuer strukturierte Daten

```
HSET sq:r:{task_id} status completed result <blob> error "" timing 1234
HGET sq:r:{task_id} result
HGETALL sq:r:{task_id}
```

**Encoding-Schwellwerte:**

| Parameter | Default | Bedeutung |
|---|---|---|
| `hash-max-listpack-entries` | 128 | Max Felder fuer Listpack |
| `hash-max-listpack-value` | 64 | Max Bytes pro Feld fuer Listpack |

SwarmQ Results haben 4-5 Felder, ABER das `result`-Feld kann > 64 Bytes sein → Hashtable-Encoding → **deutlich mehr Speicher** als ein einzelner String.

**HEXPIRE (Per-Field TTL):** Verfuegbar ab Valkey 9.0. Fuer Phase 1 (Valkey 8.x) nicht relevant.

**Empfehlung: Strings statt Hashes.** Ein msgpack-Blob als String ist einfacher, schneller (GETDEL/GETEX), und speichereffizienter.

## 1.3 Sorted Sets (ZADD/ZRANGEBYSCORE) — Ideal fuer Scheduler

```
ZADD sq:s:next_runs 1713168000.0 "cleanup_expired_sessions"
ZADD sq:s:next_runs 1713168600.0 "send_daily_report"
ZRANGEBYSCORE sq:s:next_runs 0 <now_timestamp> LIMIT 0 100
ZREM sq:s:next_runs "cleanup_expired_sessions"
```

**Komplexitaet:** ZADD O(log N), ZRANGEBYSCORE O(log N + M), ZREM O(log N)

Typischerweise wenige Dutzend Schedules → bleibt in Listpack-Encoding. **Perfekt geeignet** fuer Cron-Schedule-Verwaltung.

## 1.4 Listen (LPUSH/BRPOP) — Fuer zukuenftigen Valkey-Broker

Nicht Phase 1. Wenn Valkey-Broker implementiert wird: **Streams statt Listen** verwenden, weil BRPOP eine dedizierte Connection blockiert.

## 1.5 Streams (XADD/XREADGROUP) — Zukuenftiger Valkey-Broker

```
XADD sq:stream:default * task example_task payload <msgpack>
XGROUP CREATE sq:stream:default workers 0 MKSTREAM
XREADGROUP GROUP workers worker-1 COUNT 10 BLOCK 5000 STREAMS sq:stream:default >
XACK sq:stream:default workers <message_id>
XAUTOCLAIM sq:stream:default workers worker-2 3600000 0-0 COUNT 100
XTRIM sq:stream:default MAXLEN ~ 100000
```

**Streams vs RabbitMQ:**

| Aspekt | Valkey Streams | RabbitMQ |
|---|---|---|
| Throughput | >100K msg/s | 40-90K msg/s |
| Dead Letter Queue | Manuell | Eingebaut (DLX/DLQ) |
| Message TTL | Nur via XTRIM | Per-Message und per-Queue |
| Retry mit Backoff | Manuell | Eingebaut (DLX + TTL) |
| Persistence | RDB/AOF (Datenverlust moeglich) | Durable Queues + Confirms |
| Routing | Kein Routing | Exchange-basiert |
| Backpressure | Kein Mechanismus | Credit-based Flow Control |
| Komplexitaet | Gering (gleicher Server) | Mittel (separater Service) |

**Empfehlung:** RabbitMQ bleibt besserer Broker fuer Phase 1. Streams als optionaler leichtgewichtiger Broker fuer spaetere Phasen.

## 1.6 Sets — Worker-Registrierung und Task-Tracking

```
SADD sq:workers:active worker-1 worker-2
SREM sq:workers:active worker-1
SCARD sq:workers:active       # O(1) — Anzahl Worker
```

---

# 2. Memory Management — Kritisch fuer Result Backend

## 2.1 Eviction Policies

> **Korrigiert (Plan 0.12.0, MR 14, BZ-04):** Die fruehere Empfehlung
> `volatile-lru` war falsch. `volatile-*` verdraengt genau die Keys mit TTL,
> und das sind Locks, Leader-Lock, Ausfuehrungsschutz sowie Unique- und
> Fired-Marker; die Folge ist Doppelausfuehrung. Einzige sichere Policy ist
> `noeviction` (ein volles Valkey verweigert dann Schreibzugriffe, swarmq
> behandelt das als Ausfall), dazu AOF. Seit 0.12 prueft der Worker die
> Policy beim Start (`SWARMQ_VALKEY_EVICTION_CHECK`), siehe
> `website/reference/configuration.md#valkey-requirements`.

| Policy | Fuer SwarmQ? | Grund |
|---|---|---|
| **`noeviction`** | **EINZIG SICHER** | Nichts wird verdraengt; bei vollem Speicher OOM statt stillem Verlust |
| `allkeys-*` | NEIN | Verdraengt Locks, Marker und Schedule-State |
| `volatile-lru` / `volatile-lfu` / `volatile-ttl` / `volatile-random` | NEIN | Verdraengt genau die Keys mit TTL: Locks, Leader-Lock, Ausfuehrungsschutz, Unique- und Fired-Marker |

~~**Warum volatile-lru:**~~ (ueberholt, siehe oben)

## 2.2 Memory-Optimierung: Kurze Key-Praefixe

```
# Lang (45 Bytes):
swarmq:result:01926f3a-87c4-7def-8abc-123456789012

# Kurz (33 Bytes):
sq:r:01926f3a87c47def8abc123456789012
```

Bei 1 Million Results spart das **~12 MB RAM**. UUID7 ohne Bindestriche spart weitere 4 Bytes/Key.

**Empfehlung:** Intern kurze Praefixe: `sq:r:` (Result), `sq:l:` (Lock), `sq:s:` (Schedule), `sq:n:` (Notify). Konfigurierbar fuer Benutzer.

## 2.3 Memory-Overhead-Analyse

**Szenario: 1 Million aktive Results (200 Bytes avg. Payload):**
- Pro Key: ~350 Bytes (inkl. Overhead, TTL-Eintrag, jemalloc-Alignment)
- **Gesamt: ~350 MB RAM**
- Pub/Sub Buffer worst-case bei 100 Subscribern: bis zu 3.2 GB → Subscriber-Anzahl begrenzen!

## 2.4 Profiling-Befehle

```
MEMORY USAGE sq:r:01926f3a87c47def8abc123456789012    # Bytes pro Key
OBJECT ENCODING sq:r:01926f3a87c47def8abc123456789012  # Encoding-Typ
MEMORY DOCTOR                                           # Analyse-Bericht
```

## 2.5 Keyspace Management

**NIEMALS `KEYS *` in Produktion!** Blockiert den Server. Immer `SCAN` verwenden:

```
SCAN 0 MATCH sq:r:* COUNT 100
```

**active-expire-effort:** Default 1 belassen. Nur erhoehen wenn Monitoring zeigt, dass viele abgelaufene Keys im Speicher verbleiben.

---

# 3. Persistenz — Was SwarmQ empfehlen sollte

## 3.1 Was bei Neustart verloren geht

| Daten | Verlust akzeptabel? | Grund |
|---|---|---|
| Results | Ja | Transient, TTL-basiert |
| Locks | Ja | Automatische Recovery nach Ablauf |
| Schedule-State (last_run) | Problematisch | Cron koennte wiederholt/uebersprungen werden |
| Worker-Registrierung | Ja | Workers re-registrieren sich |

## 3.2 RDB (Snapshots)

```
save 3600 1      # Snapshot nach 1h wenn >= 1 Key geaendert
save 300 100     # Snapshot nach 5min wenn >= 100 Keys geaendert
save 60 10000    # Snapshot nach 1min wenn >= 10000 Keys geaendert

stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
```

**fork()-Overhead:** Bei Write-Heavy-Workloads kann sich Speicher durch COW verdoppeln.

## 3.3 AOF (Append-Only File)

```
appendonly yes
appendfsync everysec     # EMPFOHLEN: Max 1s Datenverlust, gute Performance
aof-use-rdb-preamble yes # Hybrid: schnelles Laden + minimaler Verlust
```

## 3.4 Empfehlung fuer SwarmQ

| Umgebung | Konfiguration | Begruendung |
|---|---|---|
| **Entwicklung** | `save ""`, `appendonly no` | Schnellster Start |
| **Produktion Standard** | `save 300 10`, `appendonly no` | RDB reicht fuer Schedule-State |
| **Produktion HA** | `appendonly yes`, `aof-use-rdb-preamble yes`, `appendfsync everysec` | Minimaler Datenverlust |

---

# 4. Sicherheit

## 4.1 ACL-Konfiguration fuer SwarmQ

**/etc/valkey/users.acl:**

```
# Default-User deaktivieren
user default off

# Worker: Lese-/Schreibzugriff auf sq:* Keys und Kanaele
user swarmq-worker on >WorkerPassw0rd! ~sq:* &sq:* +@string +@hash +@sortedset +@set +@generic +@pubsub +@scripting +@connection +ping +info -@admin -@dangerous

# Admin: Voller Zugriff auf sq:* + Monitoring
user swarmq-admin on >AdminPassw0rd! ~sq:* &sq:* +@all -@dangerous

# Monitoring: Nur-Lese + Server-Info
user swarmq-monitor on >MonitorPassw0rd! ~sq:* +get +mget +hget +hgetall +scard +zrangebyscore +zcard +scan +dbsize +info +ping +client|list +slowlog|get +latency|latest +memory|usage +memory|doctor
```

## 4.2 Netzwerk-Sicherheit

```
# valkey.conf
bind 127.0.0.1 -::1            # NIEMALS 0.0.0.0 in Produktion!
protected-mode yes

# TLS (empfohlen fuer Produktion)
port 0
tls-port 6379
tls-cert-file /etc/valkey/tls/valkey.crt
tls-key-file /etc/valkey/tls/valkey.key
tls-ca-cert-file /etc/valkey/tls/ca.crt
tls-auth-clients yes
```

## 4.3 Gefaehrliche Befehle einschraenken

```
rename-command FLUSHALL ""
rename-command FLUSHDB ""
rename-command DEBUG ""
rename-command CONFIG "SWARMQ_CONFIG_a8f3b2c1"
```

| Befehl | Gefahr | Loesung |
|---|---|---|
| `KEYS *` | Blockiert Server O(N) | ACL: `-keys`, SCAN verwenden |
| `FLUSHALL` | Loescht alle Daten | ACL + rename |
| `DEBUG` | Crash-Befehle | ACL + rename |
| `CONFIG` | Konfiguration aendern | ACL + rename |
| `SHUTDOWN` | Server herunterfahren | ACL + rename |

---

# 5. Performance Tuning

## 5.1 I/O Multithreading (Valkey 8.x)

```
io-threads 4              # 1 Main + 3 I/O Threads
io-threads-do-reads yes   # Default seit 8.0
```

| Cores | io-threads | Begruendung |
|---|---|---|
| 1-2 | 1 (kein MT) | Overhead ueberwiegt |
| 4 | 2-3 | Main + 1-2 I/O |
| 8 | 4-5 | Main + 3-4 I/O |
| 16+ | 8-9 | Mehr bringt selten Vorteile |

**Befehlsausfuehrung bleibt single-threaded!** I/O Threads behandeln nur Netzwerk-I/O. CPU-intensive Lua-Scripts blockieren weiterhin.

**Performance-Gewinn:** Bis zu 230% Throughput, 70% Latenz-Verbesserung. Valkey 8.x: >1 Million RPS.

## 5.2 Connection Tuning

```
maxclients 10000
tcp-backlog 1024
tcp-keepalive 300
timeout 300              # 5 Min Idle-Timeout (schuetzt vor Connection-Leaks)
```

## 5.3 Command Performance

| Komplexitaet | Befehle | SwarmQ-Relevanz |
|---|---|---|
| O(1) | GET, SET, HGET, HSET, SADD, SCARD | Alle Result-Operationen |
| O(log N) | ZADD, ZRANGEBYSCORE, ZREM | Scheduler next_run |
| O(N) | KEYS, SMEMBERS, HGETALL | KEYS vermeiden! |

## 5.4 Slow Log

```
slowlog-log-slower-than 5000    # 5ms in Produktion
slowlog-max-len 256
```

## 5.5 Lua Script vs Pipeline

- **Lua:** Atomar, kein Roundtrip zwischen Befehlen, Server-seitige Logik. Ideal fuer SET+PUBLISH.
- **Pipeline:** Gebuendelt, nicht atomar ohne MULTI/EXEC. Ideal fuer Batch-Operationen.
- **Lua vermeiden bei:** Grossen Datenmengen (blockiert Single-Threaded Server), >10 Befehle.

## 5.6 Pub/Sub Buffer Limits

```
client-output-buffer-limit pubsub 32mb 8mb 60
```

- Channel Subscribe schneller als Pattern Subscribe (kein Pattern-Matching)
- **SwarmQ:** Channel Subscribe pro Task-ID, Pattern nur fuer Monitoring

## 5.7 Lazy Freeing

```
lazyfree-lazy-eviction yes
lazyfree-lazy-expire yes
lazyfree-lazy-server-del yes
lazyfree-lazy-user-del yes
```

Nicht-blockierendes Loeschen grosser Objekte im Hintergrund-Thread.

---

# 6. Replikation und Hochverfuegbarkeit

## 6.1 Primary-Replica

```
replicaof <primary_host> <primary_port>
masterauth <password>
replica-read-only yes
min-replicas-to-write 1
min-replicas-max-lag 10
```

Asynchrone Replikation ist fuer SwarmQ akzeptabel (Results transient, Lock-Verlust wird automatisch behandelt).

## 6.2 Sentinel (Empfohlen fuer Produktion)

```
# sentinel.conf (auf 3 Instanzen)
sentinel monitor swarmq-primary 10.0.0.1 6379 2
sentinel auth-pass swarmq-primary <password>
sentinel down-after-milliseconds swarmq-primary 5000
sentinel failover-timeout swarmq-primary 60000
sentinel parallel-syncs swarmq-primary 1
```

```python
# valkey-py Integration
from valkey.asyncio.sentinel import Sentinel
sentinel = Sentinel([("s1", 26379), ("s2", 26379), ("s3", 26379)])
master = sentinel.master_for("swarmq-primary", password="pass")
replica = sentinel.slave_for("swarmq-primary", password="pass")
```

## 6.3 Cluster

- Hash Tags fuer Co-Lokation: `{task_id}:result` und `{task_id}:meta` im gleichen Slot
- Cross-Slot Limitation: Lua Scripts KEYS[] muessen gleichen Slot haben
- **Fuer die meisten SwarmQ-Deployments: Sentinel reicht**

---

# 7. Monitoring und Observability

## 7.1 Kritische Metriken

| Metrik | Schwellwert | Aktion |
|---|---|---|
| `used_memory / maxmemory` | > 80% | TTL reduzieren oder maxmemory erhoehen |
| `evicted_keys` (pro Minute) | > 0 | Results werden vorzeitig entfernt |
| `mem_fragmentation_ratio` | > 1.5 | `MEMORY PURGE` oder Neustart |
| `connected_clients` | > 80% maxclients | Pool ueberpruefen |
| `keyspace_misses / (hits+misses)` | > 20% | Results verfallen zu schnell |
| `pubsub_channels` | = aktive Tasks | Channel-Leaks pruefen |

## 7.2 Prometheus Exporter

```yaml
valkey-exporter:
  image: oliver006/redis_exporter:latest
  environment:
    REDIS_ADDR: "valkey:6379"
    REDIS_USER: "swarmq-monitor"
    REDIS_PASSWORD: "${VALKEY_MONITOR_PASSWORD}"
  ports:
    - "127.0.0.1:9121:9121"
```

## 7.3 Latenz-Monitoring

```
CONFIG SET latency-monitor-threshold 5    # ms
LATENCY LATEST
LATENCY HISTORY command
```

---

# 8. Docker/Container Best Practices

## 8.1 docker-compose.yml — Entwicklung

```yaml
services:
  valkey:
    image: valkey/valkey:8.1-alpine
    container_name: swarmq-valkey
    ports:
      - "6379:6379"
    volumes:
      - valkey-data:/data
    command: >
      valkey-server
      --save ""
      --appendonly no
      --maxmemory 256mb
      --maxmemory-policy noeviction  # korrigiert (BZ-04), frueher volatile-lru
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3

volumes:
  valkey-data:
```

## 8.2 Sysctl (Host-Seite)

```bash
# /etc/sysctl.d/99-valkey.conf
vm.overcommit_memory = 1          # KRITISCH fuer RDB fork()
net.core.somaxconn = 1024
```

**Container-Memory vs maxmemory:** Container-Limit = maxmemory × 1.5 (Platz fuer fork()-COW, Buffer, Lua).

---

# 9. Valkey vs Redis Unterschiede

## 9.1 Lizenz

| Aspekt | Valkey | Redis |
|---|---|---|
| Lizenz | BSD 3-Clause | AGPLv3 (ab 8.0) |
| Stewardship | Linux Foundation | Redis Ltd. |

## 9.2 Performance Valkey 8.x

| Feature | Verbesserung |
|---|---|
| I/O Multithreading | Bis zu 230% Throughput |
| Swiss Tables Hash | 20-28% weniger Memory |
| Dictionary-per-Slot | 16 Bytes weniger pro Key (Cluster) |
| Sorted Set Optimierung | 27% weniger Memory |

## 9.3 Kompatibilitaet

- Wire-Protokoll identisch — Drop-in-Replacement
- `redis.call()` in Lua funktioniert identisch
- URL-Schemas `redis://` und `valkey://` beide unterstuetzt

---

# 10. Konkrete Konfigurationen fuer SwarmQ

## 10.1 valkey.conf — Entwicklung

```
# SwarmQ Valkey — ENTWICKLUNG

bind 127.0.0.1 -::1
port 6379
protected-mode yes
tcp-backlog 511
tcp-keepalive 300
timeout 0

maxmemory 256mb
maxmemory-policy noeviction  # korrigiert (BZ-04), frueher volatile-lru
maxmemory-samples 5

save ""
appendonly no

loglevel notice
logfile ""

slowlog-log-slower-than 10000
slowlog-max-len 128
latency-monitor-threshold 5

databases 1
always-show-logo no
```

## 10.2 valkey.conf — Produktion

```
# SwarmQ Valkey — PRODUKTION

bind 10.0.0.1
port 6379
protected-mode yes
tcp-backlog 1024
tcp-keepalive 300
timeout 300

aclfile /etc/valkey/users.acl

maxmemory 1536mb
maxmemory-policy noeviction  # korrigiert (BZ-04), frueher volatile-lru
maxmemory-samples 10

save 300 10
save 60 1000
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir /data

io-threads 4
io-threads-do-reads yes

maxclients 10000

slowlog-log-slower-than 5000
slowlog-max-len 256
latency-monitor-threshold 5

lazyfree-lazy-eviction yes
lazyfree-lazy-expire yes
lazyfree-lazy-server-del yes
lazyfree-lazy-user-del yes

client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60

active-expire-effort 1

databases 1
always-show-logo no

rename-command FLUSHALL ""
rename-command FLUSHDB ""
rename-command DEBUG ""
```

## 10.3 users.acl — Produktion

```
user default off
user swarmq-worker on >WorkerPassw0rd! ~sq:* &sq:* +@string +@hash +@sortedset +@set +@generic +@pubsub +@scripting +@connection +ping +info -@admin -@dangerous
user swarmq-admin on >AdminPassw0rd! ~sq:* &sq:* +@all -@dangerous
user swarmq-monitor on >MonitorPassw0rd! ~sq:* +get +mget +hget +hgetall +scard +zrangebyscore +zcard +scan +dbsize +info +ping +client|list +slowlog|get +latency|latest +memory|usage +memory|doctor
```

---

# Zusammenfassung der Kernentscheidungen

| Entscheidung | Empfehlung | Begruendung |
|---|---|---|
| **Result-Datentyp** | String (SET/GET) | Einfach, atomar (GETDEL/GETEX), effizient mit msgpack |
| **Scheduler next_run** | Sorted Set | Natuerliches Score-basiertes Sortieren nach Timestamp |
| **Lock** | SET NX PX + Lua (eingebaute Lock-Klasse) | Bewiesenes Pattern, atomare Ownership-Pruefung |
| **Eviction Policy** | noeviction (korrigiert, BZ-04) | volatile-lru verdraengt Locks und Marker mit TTL |
| **Persistenz (Prod)** | AOF (appendonly yes, everysec), korrigiert BZ-04 | RDB allein verliert Termine, CLI-Crons und Abschaltungen seit dem Snapshot |
| **Key-Praefix** | sq: (kurz) | Spart ~12 MB bei 1M Keys |
| **Threading** | io-threads 4 | Guter Kompromiss fuer 4-8 Core Server |
| **HA** | Sentinel (3 Instanzen) | Automatisches Failover ohne Cluster-Komplexitaet |
| **Auth** | ACL mit 3 Rollen | Worker, Admin, Monitor — Least Privilege |
| **Pub/Sub** | Channel Subscribe pro Task | Schneller als Pattern, Hybrid mit Polling |
