"""Eng begrenzte Ausnahmen von ``filterwarnings = error`` (Plan 2026-10-13-0902, KTD2).

Zurzeit gibt es keine. TS-01 bis TS-03 (offene ``StreamWriter`` von aiormq
nach abgebrochenem Aufbau, nach einem Schließen durch Server oder Proxy und
nach einem Wiederverbinden während des Schließens) sind mit aiormq 7.2.4 und
aio-pika 10.2.1 entfallen und wurden mit ihren Markern
(``aiormq_streamwriter_leak``, ``aiormq_restart_leak``) und dem Abräumen
verwaister Writer am Modulende (``close_orphaned_streams``) entfernt (Plan
2026-10-29-1945, U8; Befund in ``docs/befunde/teststrenge-2026-10.md``).

Eine neue Ausnahme kommt hier als Marker dazu, mit Befund-ID in
``docs/befunde/teststrenge-2026-10.md``, und steht nur an den dort genannten
Tests. Die Filter greifen an der Warnung selbst (etwa ``ResourceWarning``),
nicht an pytests ``PytestUnraisableExceptionWarning``: Wird die Warnung in
``__del__`` zur Exception, hält deren Traceback das Objekt fest, und
darunterliegende Objekte warnen erst in einem späteren, unbeteiligten Test.
Wird aiormq oder aio-pika angehoben, die Ausnahmen probeweise entfernen.
"""
