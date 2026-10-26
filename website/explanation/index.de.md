# Erläuterungen

Hintergrundlektüre: das *Warum* hinter SwarmQ. Diese Seiten sind
Prosa, keine Anleitung. Lies sie, um ein mentales Modell aufzubauen,
auf das du zurückgreifen kannst, wenn etwas Ungewöhnliches passiert,
oder bevor du entscheidest, ob ein neuer Anwendungsfall zu SwarmQ passt.

- [Architektur](architecture.md): Client, Broker, Worker, Supervisor
  und Backend; das Prozessmodell, die Pipeline einer Nachricht, der
  Scheduler, Workflows, die Queue-Topologie und wo jeder Zustand lebt.
- [Zustellungssemantik](delivery-semantics.md): at-least-once, wann der
  Worker bestätigt, woher Duplikate kommen, der Ausführungsschutz,
  Delay-Stufen und die Idempotenz, die dein Task-Code liefern muss.
- [Designentscheidungen](design-decisions.md): die Abwägungen hinter dem
  Design, die geprüften Alternativen und was bewusst weggelassen wurde.

Die [Referenz](../reference/index.md) beantwortet das *Was*; diese
Seiten beantworten das *Warum*.
