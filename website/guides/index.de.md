# How-to-Guides

Zielorientierte Rezepte: Jeder Guide beantwortet eine Frage der Form
*„Wie mache ich X?“* mit den Schritten, einem Code-Snippet und einem Weg,
das Ergebnis zu prüfen. Nimm den Guide, den du brauchst, und überspringe
den Rest.

Neu bei SwarmQ? Beginne mit den [Tutorials](../tutorials/index.md). Jeden
Parameter und jeden Default listet die [Referenz](../reference/index.md).

## Tasks und Scheduling

<div class="grid cards" markdown>

-   **[Tasks definieren](defining-tasks.md)**

    ---

    Eine `Task`-Subklasse mit passender `TaskInfo`, passenden
    Argument-Typen und Lifecycle-Hooks schreiben und die Anti-Pattern
    meiden, die in Produktion beißen.

-   **[Scheduling und Retries](scheduling-and-retries.md)**

    ---

    Einen Task sofort, später (`delay`, `eta`) oder per Cron starten und
    Retries, Backoff, Timeouts, Ablauf und Abbruch steuern.

-   **[Workflows](workflows.md)**

    ---

    Tasks zu `chain`, `group` und `chord` zusammensetzen, mit einem Aufruf
    starten und den ganzen Lauf abwarten, inspizieren oder abbrechen.

-   **[Tasks testen](testing-tasks.md)**

    ---

    `run()` ohne Broker unit-testen und danach das Zusammenspiel mit dem
    vollen Stack abdecken.

</div>

## Querschnittsthemen

<div class="grid cards" markdown>

-   **[Middleware](middleware.md)**

    ---

    Logging, Tracing oder Metriken einheitlich über sechs Hooks um
    Enqueue, Ausführung, Fehler und Retries legen.

-   **[Signals](signals.md)**

    ---

    Mit async-Handlern auf einzelne Lifecycle-Events wie `TASK_FAILED`
    oder einen Leader-Wechsel reagieren.

-   **[Rate-Limiting](rate-limiting.md)**

    ---

    Ein globales Kontingent wie `"100/m"` über alle Worker durchsetzen,
    die sich das Valkey teilen.

-   **[Locking und Deduplizierung](locking-and-dedup.md)**

    ---

    Verhindern, dass dieselbe Arbeit parallel läuft (Lock) oder doppelt
    eingereiht wird (Unique Task).

</div>

## Betrieb

<div class="grid cards" markdown>

-   **[Ausrollen und Upgraden](deployment.md)**

    ---

    In Produktion ausrollen und auf 0.14 upgraden, ohne Tasks zu verlieren
    oder doppelt auszuführen: Prüfungen, Reihenfolge, Grace-Periods,
    Rechte und Rollback.

-   **[Dead-Letter-Queues](dead-letter-queues.md)**

    ---

    Herausfinden, warum Nachrichten in `{queue}.XQ` gelandet sind, sie mit
    `swarmq dlq` auflisten, retryen oder purgen und begrenzen, wie lange
    sie bleiben.

-   **[CPU-Modus](cpu-mode.md)**

    ---

    Blockierende, rechenlastige Task-Körper mit `--pool cpu` in killbaren
    Rechenkindern ausführen und Slots und Speicher dafür bemessen.

-   **[Hot Reload (nur Dev)](hot-reload.md)**

    ---

    Worker in der lokalen Entwicklung mit `--reload` bei jeder
    gespeicherten Änderung automatisch neu starten.

</div>
