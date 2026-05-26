# Erläuterungen

Hintergrundlektüre — das *Warum* hinter SwarmQs Designentscheidungen.
Diese Seiten sind Prosa, nicht Anleitung. Lies sie, um ein mentales
Modell aufzubauen, auf das du zurückgreifen kannst, wenn etwas
Ungewöhnliches passiert.

- [Architektur](architecture.md) — wie Broker, Backend, Workers und
  Clients zusammenpassen und wo State lebt.
- [Zustellungssemantik](delivery-semantics.md) — at-least-once,
  Ack-Timing, Redelivery-Szenarien und der Idempotenzvertrag, den
  dein Task-Code einhalten muss.
- [Designentscheidungen](design-decisions.md) — getroffene Trade-offs,
  geprüfte Alternativen und die Dinge, die bewusst weggelassen
  wurden.

Nutze diese Seiten als Kontext, wenn die Referenz „was" beantwortet,
du aber „warum" brauchst. Sie sind auch der schnellste Weg, zu
verstehen, ob ein neuer Anwendungsfall zu SwarmQs Shape passt,
bevor du Code schreibst.
