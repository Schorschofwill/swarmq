---
date: 2026-06-02
type: feat
status: active
origin: docs/brainstorms/inject-existing-pools-requirements.md
---

# feat: Bestehende RabbitMQ-/Valkey-Pools injizieren (Producer)

## Problem Frame

Eine App, die swarmq nur zum Senden von Tasks nutzt (`schedule`, `run`, `get_result`, `apply`, `cancel`), hält oft bereits eigene Verbindungen zu RabbitMQ und/oder Valkey. Heute ist das nicht teilbar: `RabbitMQBroker(url)` und `ValkeyBackend(url)` nehmen nur eine URL und bauen in `connect()` zwingend eine eigene robuste Connection plus Channels bzw. einen eigenen Valkey-Client auf. Das erzeugt doppelte, parallele Verbindungen zum selben Server für denselben Prozess — mehr Sockets, mehr Handshakes, zwei getrennte Lebenszyklen.

Dieses Feature lässt `RabbitMQBroker` und `ValkeyBackend` optional ein bereits bestehendes Verbindungsobjekt am Konstruktor entgegennehmen — wahlweise einen High-Level-Client/Connection oder ein Low-Level-Pool-Primitiv — statt einer URL. swarmq nutzt dann die übergebene Ressource und schließt sie beim Teardown nicht.

---

## Scope

Producer-/Client-Pfad. Der Worker-Pfad bleibt bewusst URL-basiert: Worker-Subprozesse werden via `spawn` gestartet und rekonstruieren Broker/Backend aus den Env-Variablen `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL` (`process_manager.py:1013-1058`, `process_manager.py:831`). Ein injiziertes Verbindungsobjekt lässt sich nicht über die `spawn`-Boundary picklen — die Producer-only-Grenze ist damit eine technische Notwendigkeit, kein bloßer Vertrag (see origin: `docs/brainstorms/inject-existing-pools-requirements.md`; Learning `docs/solutions/architecture-patterns/dishka-di-integration-async-task-queue.md` #10).

---

## Requirements Traceability

- **R1/R2** Injektion am Konstruktor (Connection/Client ODER Pool) → U1 (Valkey), U2 (RabbitMQ)
- **R3** Pro Backend unabhängig wählbar, Mischen erlaubt → U1, U2, U3
- **R4** Bei Injektion keine eigene Verbindung; geborgte Connection → eigener Publish-Channel → U1, U2
- **R5** Injizierte Ressourcen nicht schließen → U1, U2
- **R6** Selbst erzeugte Channels schließen → U2
- **R7** `start_worker()` wirft `ConfigurationError` bei injiziertem Pool → U4
- **R8** URL-Pflicht entfällt pro Backend bei Injektion → U3
- **AE1–AE4** → Test-Szenarien in U1, U2, U3, U4

---

## Key Technical Decisions

- **Injektion am Konstruktor, nicht über Config/dishka.** Der Konstruktor von `RabbitMQBroker`/`ValkeyBackend` ist der vorhandene Einstiegspunkt; Pools sind in der Ist-Architektur bewusst keine DI-Kandidaten. (see origin)
- **Ownership-Flag (`_owns_client` für Valkey, `_owns_connection` für RabbitMQ) als einzige Wahrheitsquelle für jedes Close.** Jede Close-Stelle (inkl. Cancel-/Exception-Pfade) konsultiert das jeweilige Flag. Spiegelt die bereits ratifizierte Team-Konvention aus `docs/solutions/patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md`: „Teardown nur für selbst erzeugte Ressourcen; geliehene Connections gehören dem Caller." Das Flag deckt zugleich den dishka-Doppel-Close-Fall ab (ein dishka-APP-scoped-Finalizer und swarmq dürfen nicht beide schließen).
- **Mehrdeutigkeit URL + Objekt → `ConfigurationError`.** Werden für dasselbe Backend gleichzeitig URL und Verbindungsobjekt übergeben, ist das ein harter Fehler statt stillem Vorrang. (Bestätigt im Scoping.)
- **`connect()` wird bei Injektion zum gezielten No-op bzw. Channel-Open.** Valkey: Client/Pool existiert bereits → `connect()` öffnet keinen neuen Client. RabbitMQ: die geborgte Connection existiert, aber swarmq braucht seinen **eigenen** Publish-Channel darauf — der bestehende Early-Return-Guard (`if self._connection is not None: return`, `rabbitmq.py:60`) muss „Connection da, aber Channels noch nicht geöffnet" vom Voll-verbunden-Zustand unterscheiden.
- **Robustheits-Vertrag liegt beim Caller.** Ein injizierter `valkey.asyncio.Valkey`/`ConnectionPool` erbt **nicht** swarmqs `Retry`/`retry_on_error`/`health_check_interval` (`valkey.py:44-50`); eine injizierte aio-pika-Connection ist evtl. keine `RobustConnection`. swarmqs Channel-Recreate-Logik (`_ensure_publish_channel`/`_publish_with_retry`) setzt robustes Verhalten voraus. Das wird dokumentiert, nicht erzwungen (Learning `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md`).
- **URL-optional bleibt producer-seitig.** Nur `SwarmQ.__init__` + `SwarmQConfig`-Validierung werden gelockert. CLI (`cli/_shared.py`, `cli/worker_cmd.py`) und `process_manager.py` bleiben URL-pflichtig.

---

## High-Level Technical Design

Konstruktor-Eingaberesolution pro Backend (illustrativ, **directional guidance für Review, keine Implementierungsspezifikation** — der implementierende Agent behandelt dies als Kontext, nicht als zu reproduzierenden Code):

```
Eingabe an Konstruktor
  ├── url (str)            → wie heute: Verbindung selbst bauen; _owns = True
  ├── high-level Objekt    → valkey.asyncio.Valkey  /  aio_pika (Robust)Connection
  │                          _owns = False; connect() = no-op bzw. nur eigener Publish-Channel
  ├── low-level Pool       → valkey ConnectionPool  /  aio_pika.pool.Pool
  │                          _owns = False; Client/Channel wird aus Pool bezogen
  └── url UND Objekt       → ConfigurationError (Mehrdeutigkeit)

Teardown (close / __aexit__ / cancel-Pfad)
  _owns == True   → Verbindung/Client schließen (wie heute)
  _owns == False  → geborgte Ressource NICHT schließen;
                    RabbitMQ: selbst geöffnete Channels schließen (finally, nie ch.close() swallowen)
```

Ownership-/Close-Wahrheitstabelle (RabbitMQ):

| Eingabe | `connect()` | `close()` schließt Connection? | `close()` schließt eigene Channels? |
|---|---|---|---|
| URL | Connection + Publish-Channel bauen | ja | n/a (gehören zur eigenen Connection) |
| High-Level Connection | nur eigenen Publish-Channel öffnen | **nein** | **ja** |
| Low-Level Pool | Channel/Connection aus Pool beziehen | **nein** | ja (selbst bezogene/geöffnete) |

---

## Implementation Units

### U1. ValkeyBackend: Verbindungs-Injektion + Ownership

**Goal:** `ValkeyBackend` akzeptiert neben der URL einen bestehenden `valkey.asyncio.Valkey`-Client oder einen `ConnectionPool`; bei Injektion baut `connect()` keinen neuen Client und `close()` schließt den geborgten Client/Pool nicht.

**Requirements:** R1, R2, R3, R4, R5, R8 (teilw.)

**Dependencies:** keine

**Files:**
- `src/swarmq/backend/valkey.py` (Konstruktor, `connect`, `close`, `_owns`-Flag, Eingaberesolution)
- `tests/` — bestehende Backend-Testdatei (z. B. `tests/test_valkey_backend.py` oder Äquivalent; im Repo lokalisieren) erweitern

**Approach:**
- Konstruktorsignatur erweitern, sodass genau eine Quelle gilt: URL **oder** `client=` **oder** `pool=`. Mehr als eine gesetzt → `ConfigurationError`.
- Bei injiziertem Client: `self._client` direkt setzen, `self._url = None`, `self._owns_client = False`.
- Bei injiziertem Pool: Client aus Pool beziehen (`valkey.asyncio.Valkey(connection_pool=pool)`), `_owns_client = False` (Pool gehört dem Caller).
- `connect()`: bei bereits gesetztem injiziertem Client → optionales `ping()` zur Frühvalidierung, aber **kein** `from_url`. Bestehender Idempotenz-Guard (`if self._client is not None: return`) bleibt korrekt für den injizierten Fall.
- `close()` (`valkey.py:54-57`): nur `aclose()` aufrufen, wenn `self._owns_client`. Sonst Referenz fallenlassen ohne Schließen.
- Lazy-Lua-Script-Registrierung (`_ensure_unique_release_script`) funktioniert unverändert auf dem injizierten Client.

**Patterns to follow:** Ownership-Konvention aus `docs/solutions/patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md`; bestehender idempotenter `connect()`-Guard.

**Test scenarios:**
- Covers AE1. Injizierter Client wird für Backend-Operationen genutzt; nach `__aexit__`/`close()` ist `aclose()` **nicht** aufgerufen worden (Client bleibt offen, weiter nutzbar).
- URL-Konstruktion (Status quo) schließt den Client beim `close()` weiterhin (`_owns_client = True`).
- Injizierter `ConnectionPool` → Client wird daraus gebaut; `close()` schließt den Pool nicht.
- URL **und** `client=` gleichzeitig → `ConfigurationError`.
- `connect()` mit injiziertem Client ist idempotent und baut keinen zweiten Client.
- Lua-Script-abhängige Operation (`release_unique` / `get_result`-Pfad) funktioniert auf injiziertem Client.

**Verification:** Backend-Tests grün; ein geborgter Client überlebt den swarmq-Teardown beobachtbar (Mock/Spy auf `aclose`).

---

### U2. RabbitMQBroker: Verbindungs-Injektion + Channel-Ownership

**Goal:** `RabbitMQBroker` akzeptiert neben der URL eine bestehende aio-pika-(Robust-)Connection oder einen `aio_pika.pool.Pool`; bei injizierter Connection öffnet swarmq seinen eigenen Publish-Channel darauf, schließt beim Teardown diesen Channel, aber **nicht** die geborgte Connection.

**Requirements:** R1, R2, R3, R4, R5, R6

**Dependencies:** keine (parallel zu U1)

**Files:**
- `src/swarmq/broker/rabbitmq.py` (Konstruktor `rabbitmq.py:46-57`, `connect` `:59-73`, `close` `:355-358`, `_ensure_publish_channel`/`_ensure_channel`, `sanitize_url`-Logsites `:197,204`)
- `tests/` — bestehende Broker-Testdatei erweitern

**Approach:**
- Konstruktor erweitern: URL **oder** `connection=` **oder** `connection_pool=`. Mehrfach gesetzt → `ConfigurationError`. `queue_type` bleibt erhalten.
- Injizierte Connection: `self._connection` setzen, `self._url = None`, `self._owns_connection = False`. `connect()` muss erkennen: Connection vorhanden, aber `self._publish_channel is None` → eigenen Publish-Channel öffnen. **Consume-Channel nicht öffnen** (Producer-Pfad braucht ihn nicht; der Consume-Pfad ist über U4 ohnehin gesperrt).
- Den Early-Return-Guard (`if self._connection is not None: return`, `:60`) so umbauen, dass er „voll verbunden" (Publish-Channel existiert) prüft statt nur „Connection existiert".
- `close()` (`:355-358`): geborgte Connection bei `_owns_connection = False` **nicht** schließen; den selbst geöffneten Publish-Channel jedoch garantiert schließen (`finally`-Disziplin, `ch.close()` **nicht** in try/except-swallow wickeln — korrumpiert Folge-`channel()`-Calls; Learning `docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`).
- `sanitize_url(self._url)` an `:197,204`: Fallback wenn `self._url is None` — ein **fester, credential-freier Literal** (z. B. `"<injected-connection>"`), niemals aus dem Connection-Objekt abgeleitet (`str(self._connection)`/`repr` können die AMQP-URL mit Credentials enthalten und in Logs lecken). Verhindert zugleich den Crash (`urlparse(None)` → `AttributeError`).
- Credential-Redaction über die zwei Log-Sites hinaus: auch die Exception-Meldung im `_publish_with_retry`-Pfad (`rabbitmq.py:161-164`, baut die Meldung aus `{last_exc}`) durch `sanitize_url` führen. aio-pika-Exceptions können die AMQP-URL mit eingebetteten Credentials in `str(exc)` tragen; auf dem Injektionspfad ist die geborgte (evtl. privilegierte, geteilte) Connection der Fehlerkontext, was die Exposure erhöht. Gilt auch für den bestehenden URL-Pfad.
- Low-Level-Pool: Publish bezieht Channel/Connection aus dem Pool (acquire), gibt ihn zurück. Genaue Integration ins bestehende Publish-mit-Retry-Modell → siehe Deferred Implementation Notes.
- `is_closed` **nicht** als Liveness-Check auf der injizierten Connection verwenden (unzuverlässig; selbes Learning).

**Patterns to follow:** garantierter Channel-Close wie `_bench_channel` aus dem aiormq-Learning; bestehende `_ensure_publish_channel`-Recreate-Logik.

**Technical design:** siehe Wahrheitstabelle im High-Level-Abschnitt — **directional guidance, keine Spezifikation.**

**Test scenarios:**
- Covers AE2. Injizierte Connection: Publish öffnet eigenen Channel; nach `close()` ist die Connection **nicht** geschlossen, der selbst geöffnete Channel **schon**.
- URL-Konstruktion (Status quo): `close()` schließt Connection weiterhin (`_owns_connection = True`).
- `connect()` mit injizierter Connection öffnet Publish-Channel, aber keinen Consume-Channel.
- URL **und** `connection=` gleichzeitig → `ConfigurationError`.
- Reopen-Logging (`_ensure_channel`) crasht nicht, wenn `_url is None` (Fallback greift).
- Injizierter `Pool` → Publish bezieht Channel aus dem Pool; `close()` schließt den Pool nicht.
- Edge: Publish auf injizierter Connection, deren Channel zwischenzeitlich geschlossen wurde → Recreate-Pfad öffnet neuen eigenen Channel.

**Verification:** Broker-Tests grün; geborgte Connection überlebt Teardown (Spy auf `connection.close`), eigener Channel ist geschlossen.

---

### U3. URL-optional unter Injektion (SwarmQ + SwarmQConfig)

**Goal:** Wird für ein Backend ein Verbindungsobjekt injiziert, verlangt die Config-Validierung für dieses Backend keine URL mehr. Mischen (URL für eins, Objekt für das andere) funktioniert.

**Requirements:** R3, R8

**Dependencies:** U1, U2 (das `_owns`/`_url`-Verhalten muss feststehen)

**Files:**
- `src/swarmq/app.py` (`SwarmQ.__init__` `:50-54` — liest `broker._url`/`backend._url`)
- `src/swarmq/config.py` (`SwarmQConfig.__post_init__` URL-Validierung `:171-177`)

**Approach:**
- `SwarmQ.__init__` darf `broker._url`/`backend._url` nicht mehr als garantiert nicht-`None` annehmen. Bei injiziertem Backend ist `_url = None`.
- Die Config muss wissen, dass eine fehlende URL bei Injektion legitim ist. Mechanismus → Deferred Implementation Notes (Optionen: Flag von Broker/Backend an `SwarmQ.__init__` weiterreichen und Validierung dort/parametrisiert führen). Leitlinie: `broker_url`/`backend_url` dürfen leer sein, **wenn** das jeweilige Backend injiziert wurde; sonst bleibt die `ConfigurationError`-Pflicht bestehen.
- `config.broker_url`/`config.backend_url` bleiben für den Producer-Pfad unkritisch (der Broker hat seine Connection bereits); Worker-seitige Leser sind über U4 gesperrt.
- Bei Injektion führen die Config-URL-Felder einen festen, credential-freien Platzhalter (z. B. `"<injected>"`) statt `None`, falls sie irgendwo log-formatiert ausgegeben werden — schließt das Fenster, in dem ein roher `None` (oder schlimmer: ein aus dem Verbindungsobjekt abgeleiteter Wert) in einen Startup-Log oder Config-Dump gelangt.

**Patterns to follow:** bestehende `__post_init__`-Validierungsstruktur.

**Test scenarios:**
- Covers AE4. `broker_url` gesetzt + injizierter Valkey-Client → Config validiert ohne Fehler trotz fehlender `backend_url`.
- Beide injiziert → keine URL nötig, Config OK.
- Beide per URL (Status quo) → leere URL wirft weiterhin `ConfigurationError`.
- Backend injiziert, aber Broker weder URL noch Objekt → `ConfigurationError` (Broker-Seite weiterhin pflichtig).

**Verification:** Config- und App-Konstruktionstests grün; Producer-Konstruktion mit gemischten Quellen erfolgreich.

---

### U4. start_worker(): Producer-only-Guard

**Goal:** `start_worker()` bricht mit klarer `ConfigurationError` ab, wenn Broker oder Backend mit injiziertem Pool/Objekt konstruiert wurde — bevor irgendein Subprozess gespawnt wird.

**Requirements:** R7

**Dependencies:** U1, U2 (`_owns`-Marker müssen existieren)

**Files:**
- `src/swarmq/app.py` (`start_worker` `:196-214`)

**Approach:**
- Vor der Worker-Konstruktion prüfen, ob `broker` oder `backend` injiziert wurden (über den in U1/U2 eingeführten Marker, z. B. `not broker._owns_connection`).
- Bei Injektion `ConfigurationError` mit Begründung über die `spawn`-Boundary: injizierte Verbindungsobjekte können nicht in Worker-Subprozesse gepickelt werden; der Worker erwartet `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL`.
- Guard feuert in-process vor jedem Spawn.

**Patterns to follow:** bestehende `ConfigurationError`-Nutzung; Spawn-Boundary aus `docs/solutions/architecture-patterns/dishka-di-integration-async-task-queue.md`.

**Test scenarios:**
- Covers AE3. Broker mit injizierter Connection → `start_worker()` wirft `ConfigurationError` vor Worker-/Spawn-Konstruktion.
- Backend mit injiziertem Client → `start_worker()` wirft `ConfigurationError`.
- Beide per URL → `start_worker()` läuft normal (kein Guard-Fehlalarm).
- Fehlermeldung nennt die Spawn-Boundary-/Env-URL-Ursache (Assertion auf Meldungsinhalt).

**Verification:** Test, dass injizierter-Pool-Worker sofort und mit klarer Meldung scheitert; URL-Worker unverändert startbar.

---

### U5. Dokumentation: API-Oberfläche + Ownership-/Robustheits-Vertrag

**Goal:** Öffentliche API, Ownership-Regel und die Robustheits-Caveats sind dokumentiert, sodass Nutzer Injektion korrekt und ohne Überraschungen verwenden.

**Requirements:** advanciert die Success Criteria (Handoff-Qualität) und den R5/R6-Ownership-Vertrag (kein eigenes R-ID)

**Dependencies:** U1, U2, U3, U4

**Files:**
- Docstrings in `src/swarmq/broker/rabbitmq.py`, `src/swarmq/backend/valkey.py`
- README / Nutzungsdoku (im Repo lokalisieren; vermutlich `README.md` und ggf. `docs/`)

**Approach:**
- Producer-Beispiel: bestehende App reicht `valkey.asyncio.Valkey`-Client und aio-pika-Connection rein.
- Ownership-Regel explizit: swarmq schließt Geborgtes nicht; eigene Channels schon.
- Caveat dokumentieren: injizierte Valkey-Clients/Pools erben **nicht** swarmqs Retry-/Health-Check-Policy; injizierte aio-pika-Connections sollten robust (`connect_robust`) sein, sonst greift swarmqs Reconnect-Annahme nicht.
- Producer-only-Grenze + `start_worker()`-Fehler dokumentieren (mit Spawn-Begründung).
- Trust-Boundary-Annahme dokumentieren: das injizierte Objekt muss denselben Server und logischen Namespace (vhost/DB) adressieren wie die konfigurierte Queue/vhost — swarmq validiert das nicht; ein Mismatch führt still zu Operationen in einem fremden Namespace.

**Patterns to follow:** bestehender README-/Docstring-Stil.

**Test scenarios:** Test expectation: none — reine Dokumentation, kein Verhaltensänderung. Doctests nur falls das Repo sie bereits ausführt.

**Verification:** Doku-Beispiel ist kohärent mit der finalen API; Review bestätigt vollständige Caveat-Abdeckung.

---

## System-Wide Impact

- **Producer-Pfad** (`SwarmQ` + `Client`): primärer Nutznießer; gemischte URL/Objekt-Quellen möglich.
- **Worker-Pfad** (`process_manager.py`, CLI): unverändert URL-pflichtig; durch U4-Guard aktiv gegen Fehlbedienung geschützt.
- **Logging** (`sanitize_url`-Sites in `broker/rabbitmq.py`): müssen `None`-URL unter Injektion vertragen.
- **dishka-DI**: ein dishka-APP-scoped Pool mit Finalizer wird durch das `_owns`-Flag nicht doppelt geschlossen.

---

## Deferred Implementation Notes

- **Typ-Erkennung (R1/R2):** isinstance gegen die konkreten Klassen (`valkey.asyncio.Valkey`, `valkey.asyncio.ConnectionPool`, `aio_pika.abc.AbstractConnection`/`AbstractRobustConnection`, `aio_pika.pool.Pool`) vs. Keyword-getrennte Parameter (`client=`/`pool=`/`connection=`/`connection_pool=`). Empfehlung: explizite Keyword-Parameter — eindeutiger als Typ-Sniffing und vermeidet Fehlklassifikation. Final beim Implementieren festlegen.
- **Config-Awareness-Mechanismus (R8):** Wie genau `SwarmQConfig` erfährt, dass eine fehlende URL legitim ist (Flag-Parameter vs. Validierung in `SwarmQ.__init__` vs. neues optionales Config-Feld). Erst gegen den realen Code entscheiden.
- **Low-Level-Pool-Integration RabbitMQ (R4):** Wie ein `aio_pika.pool.Pool` ins bestehende 1-Connection+2-Channel-Publish-mit-Retry-Modell eingebunden wird (acquire pro Publish vs. einmal Connection beziehen und eigenen Channel halten). Hängt vom Pool-Typ (Connection- vs. Channel-Pool) ab.
- **`_owns`-Marker-Benennung/-Sichtbarkeit:** ein Flag pro Broker/Backend, das alle Close-Sites und der U4-Guard konsultieren. Genauer Name/Privatheit beim Implementieren.

---

## Risks & Mitigations

- **Doppel-Close geborgter Ressourcen** (Regressionsrisiko, vgl. `test_worker_cancellation`): Mitigation — `_owns`-Flag als einzige Wahrheitsquelle, Regressionstests für „inject → close swarmq" auf beiden Backends.
- **`None`-URL crasht Logging/Config-Leser:** Mitigation — Audit aller `_url`/`broker_url`-Leser (in U2/U3 abgedeckt: `sanitize_url`-Sites, `SwarmQ.__init__`); Worker-Leser über U4 gesperrt.
- **Injizierte nicht-robuste Connection bricht Reconnect-Annahme:** Mitigation — dokumentierter Caveat (U5); `is_closed` nicht als Liveness verwenden.
- **API-Oberfläche wächst (drei Eingabearten pro Backend):** Mitigation — explizite Keyword-Parameter + harte Mehrdeutigkeits-Fehler statt stiller Heuristik.
- **Namespace-Mismatch des injizierten Objekts** (Trust-Boundary): zeigt ein injizierter Client auf einen anderen vhost/eine andere DB als die übrige Config erwartet, publiziert/liest swarmq still in einen fremden Namespace (`sq:result:*`, `sq:cancel:*`, `sq:unique:*`). Mitigation — dokumentierte Annahme in U5: das injizierte Objekt adressiert denselben Server und logischen Namespace wie die konfigurierte Queue/vhost; eine Validierung gegen die Config ist bewusst nicht im Scope.

---

## Verification Strategy

- Unit-Tests pro Backend für die Ownership-Wahrheitstabelle (selbst-erzeugt vs. geborgt).
- App-/Config-Tests für gemischte Quellen und URL-optional.
- Guard-Test für `start_worker()` mit injizierten Objekten.
- Bestehende Broker/Backend/Worker-Tests müssen grün bleiben (kein Verhaltensbruch im URL-Pfad).
