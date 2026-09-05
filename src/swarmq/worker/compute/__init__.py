"""CPU-Betriebsmodus des Workers (Plan ``docs/plans/2026-09-03-001``).

Ein Loop-Prozess, der nie rechnet, verteilt Task-Koerper auf Rechenkinder
aus einem vorgewaermten ``forkserver``. Dieses Paket haelt die dafuer
noetigen Bausteine; die Pipeline bleibt unveraendert und waehlt nur den
Executor nach Pool-Modus (KTD1).
"""
