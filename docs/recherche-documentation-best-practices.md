# Recherche: Best Practices fuer End-User-Dokumentation von Software-Bibliotheken

**Erstellt:** 2026-04-16
**Zweck:** Grundlagendokument fuer die Entwickler-Dokumentation von SwarmQ (Python, asyncio, RabbitMQ, Valkey). Synthese etablierter Style-Guides, Frameworks und Benchmark-Sites. Dient als Referenz fuer Inhalts- und Designentscheidungen im vorhandenen `docs/design-prototype/` und fuer die spaetere Migration in einen Generator.
**Zielgruppe dieser Recherche:** Entwickler, die SwarmQ-Doku schreiben oder reviewen. Nicht die Endnutzer der Doku selbst.

---

## Inhaltsverzeichnis

1. [TL;DR — 15 Kernprinzipien](#1-tl-dr--15-kernprinzipien)
2. [Teil A: Schreibstil, Struktur, Inhalt](#2-teil-a-schreibstil-struktur-inhalt)
   - 2.1 [Diataxis: die vier Doku-Typen](#21-diataxis-die-vier-doku-typen)
   - 2.2 [Ton, Stimme, Person](#22-ton-stimme-person)
   - 2.3 [Sprachlevel, Jargon, Definitionen](#23-sprachlevel-jargon-definitionen)
   - 2.4 [Satz- und Absatzbau, Scannability](#24-satz--und-absatzbau-scannability)
   - 2.5 [Ueberschriften-Hierarchie](#25-ueberschriften-hierarchie)
   - 2.6 [Progressive Disclosure](#26-progressive-disclosure)
   - 2.7 [API-Referenz-Pattern](#27-api-referenz-pattern)
   - 2.8 [Code-Beispiele](#28-code-beispiele)
   - 2.9 [Troubleshooting, FAQ, Fehlermeldungen](#29-troubleshooting-faq-fehlermeldungen)
   - 2.10 [Versionierung, Changelog, Deprecation](#210-versionierung-changelog-deprecation)
   - 2.11 [Quellen-Kanon](#211-quellen-kanon)
3. [Teil B: Visuelle Praesentation und UX](#3-teil-b-visuelle-praesentation-und-ux)
   - 3.1 [Typografie](#31-typografie)
   - 3.2 [Farbsystem und Dark Mode](#32-farbsystem-und-dark-mode)
   - 3.3 [Accessibility und Kontrast](#33-accessibility-und-kontrast)
   - 3.4 [Code-Block-Design](#34-code-block-design)
   - 3.5 [Callouts und Admonitions](#35-callouts-und-admonitions)
   - 3.6 [Navigation und Layout](#36-navigation-und-layout)
   - 3.7 [Diagramme und Screenshots](#37-diagramme-und-screenshots)
   - 3.8 [Benchmark-Sites im Vergleich](#38-benchmark-sites-im-vergleich)
4. [Teil C: Tooling-Landschaft](#4-teil-c-tooling-landschaft)
5. [Anwendung auf SwarmQ: Checklisten pro Seitentyp](#5-anwendung-auf-swarmq-checklisten-pro-seitentyp)
6. [Annotierte Quellenliste](#6-annotierte-quellenliste)
7. [Delta zum vorhandenen Design-Prototyp](#7-delta-zum-vorhandenen-design-prototyp)

---

## 1. TL;DR — 15 Kernprinzipien

Jede SwarmQ-Doc-Seite sollte gegen diese Liste pruefbar sein.

1. **Ein Seitentyp pro Seite.** Eine Seite ist entweder Tutorial, How-To, Reference oder Explanation — nie Mischformen. Prozida: "One of the most common mistakes in documentation is to try to blend these four kinds of material into something that tries to do them all at once" ([Diataxis](https://diataxis.fr/)).
2. **Outcomes, nicht Funktionen.** Stripe dokumentiert "How to accept your first payment", nicht "How to authenticate" ([Mintlify Teardown](https://www.mintlify.com/blog/stripe-docs)). Jede Page-Ueberschrift beantwortet "Was kann der Leser nach dieser Seite tun?"
3. **Direkt adressieren ("you"), aktiv, Praesens, Imperativ bei Anleitungen.** Google und Microsoft sind sich einig. "We" vermeiden (Kubernetes: "the reader might not know whether they're part of the 'we'" — [Kubernetes Style Guide](https://kubernetes.io/docs/contribute/style/style-guide/)).
4. **Jedes Konzept beginnt mit einem lauffaehigen Minimalbeispiel.** Danach kommen Varianten. Das ist bereits Vorgabe aus `aufgabe.md` §17.1.
5. **Copy-paste-Runnability ist Pflicht.** Code-Beispiele laufen ohne Umschreiben, inklusive Imports. Fehlende Imports sind der haeufigste Doc-Bug.
6. **Progressive Disclosure.** Getting Started zeigt den kleinsten gluecklichen Pfad. Alles Optionale (Tuning, Edge Cases, seltene Parameter) wird verlinkt, nicht inlined ([Tom Johnson](https://idratherbewriting.com/ucd-progressive-disclosure/)).
7. **Referenz erklaert nicht.** API-Reference listet Signaturen, Parameter, Rueckgaben, Exceptions. Warum man sie benutzt, steht im How-To oder in Explanation.
8. **Zeilenlaenge 45–75 Zeichen, Ziel 66.** Harte UX-Regel aus der Typografie-Forschung ([Baymard](https://baymard.com/blog/line-length-readability), [Bringhurst via webtypography.net](http://webtypography.net/2.1.2)). Content-Spalte auf `max-width: 66ch` begrenzen.
9. **WCAG 2.1 AA als Untergrenze:** 4.5:1 fuer Body-Text, 3:1 fuer grosse Schrift und UI-Komponenten. Fokus-Ringe sichtbar, `prefers-reduced-motion` respektieren ([W3C WCAG 2.1](https://www.w3.org/TR/WCAG21/)).
10. **Ein Akzentton, der etwas bedeutet.** Linear- und Stripe-Stil: Farbe ist funktional, nicht dekorativ ([LogRocket zu Linear-Stil](https://blog.logrocket.com/ux-design/linear-design/)). SwarmQs Amber markiert interaktive Elemente und semantische "warn"-Callouts — nie Fliesstext.
11. **Callouts sind teuer. Benutze sie sparsam.** "If every paragraph is important, then nothing is" ([BufferBuffer](https://bufferbuffer.com/when-to-use-notes-warnings-and-callouts-in-tech-writing-and-when-not-to/)). Bevor ein Callout gesetzt wird, pruefen: Kann der Inhalt in den Fliesstext?
12. **Code-Block-UX ist pflicht: Sprach-Label, Copy-Button, Syntax-Highlight.** Tabs fuer Alternativen (uv/pip/poetry), nicht fuer echte Sprachvarianten ohne Aequivalenz.
13. **Fehlermeldungen sind eine Doku-Surface.** Jede eigene Exception wird dokumentiert mit: was sie bedeutet, wann sie fliegt, wie man sie behebt. Auch Grundlage fuer die LLM-Doku aus §17.2.
14. **Changelog fuer Menschen.** `keepachangelog.com`-Format: `Added / Changed / Deprecated / Removed / Fixed / Security`. Deprecations mindestens eine Minor-Version ankuendigen, mit migrations-Snippet.
15. **Sichtbar halten, was das Projekt ist.** Landing sagt in zwei Zeilen, was SwarmQ ist und fuer wen. Darunter: Five-Minute-Beispiel. Kein generischer Marketing-Hero ohne Code.

---

## 2. Teil A: Schreibstil, Struktur, Inhalt

### 2.1 Diataxis: die vier Doku-Typen

Das Diataxis-Framework von Daniele Procida ist der de-facto-Standard fuer die Struktur technischer Dokumentation. Es unterscheidet entlang von zwei Achsen — *practical vs. theoretical* und *acquisition vs. application* — und ergibt vier Quadranten. Jeder Quadrant adressiert ein anderes Nutzer-Beduerfnis, und das Vermischen ist der haeufigste Fehler in Open-Source-Doku ([Diataxis Start](https://diataxis.fr/start-here/)).

| Typ | Nutzer-Beduerfnis | Nutzer-Zustand | Achse |
|---|---|---|---|
| **Tutorial** | Lernen | Anfaenger, unsicher, braucht Fuehrung | practical + acquisition |
| **How-To Guide** | Ein Problem loesen | Erfahren, hat konkretes Ziel | practical + application |
| **Reference** | Nachschlagen | Erfahren, braucht Fakten | theoretical + application |
| **Explanation** | Verstehen | Neugierig, will Zusammenhaenge | theoretical + acquisition |

**Tutorial.** Eine gefuehrte Lernsession. Der Leser ist ein Studierender, der Autor ist ein Lehrer. Das Ziel ist nicht, eine Aufgabe zu erledigen, sondern ein Lernerfolg. Tutorials sind **konkret, handlungsleitend, linear, ergebnisoffen-gerichtet** — der Lernende weiss am Ende noch nicht, *welche* Aufgabe er damit loesen kann, aber er hat ein Gefuehl fuer das Werkzeug. Tutorials muessen zuverlaessig funktionieren: jeder Schritt fuehrt zu einem sichtbaren Ergebnis. Typische Fallen: zu viele Optionen, zu viel Erklaerung, Abzweigungen in How-Tos.

**How-To Guide.** Ein Rezept fuer ein reales Problem. Der Leser hat Erfahrung und weiss, was er will — er braucht die Schritte, nicht die Grundlagen. How-Tos sind **orientiert an Zielen, nicht an Funktionen**. Gute Titel lauten "Wie man Tasks auf mehrere Queues routet", nicht "Die `queues`-Option". Typische Falle: Verwandlung in Tutorial (zu viel Erklaerung) oder Reference (nur API-Listen ohne roten Faden).

**Reference.** Technische Beschreibung. Vollstaendig, akkurat, frei von Interpretation. Procida: Reference ist "what the machinery looks like", waehrend Explanation "what the machinery is for" beantwortet. Referenz erklaert nicht, warum man etwas macht — sie beschreibt, was es tut. Typische Falle: Beispiele und Meinungen mischen sich in die Signatur. Beispiele duerfen vorkommen, aber als Illustration, nicht als Anleitung.

**Explanation.** Hintergrund, Designentscheidungen, Architektur. Der Leser sitzt nicht an der Tastatur, er liest nach. Themen: "Warum at-least-once statt exactly-once?", "Wie funktioniert das Rate-Limiting intern?", "Trade-offs bei der Message-Serialisierung". Typische Falle: verschwimmt in Reference (zu viele Details) oder fehlt ganz.

**Anti-Muster beim Vermischen:**
- **Reference in Tutorials.** "Schritt 3: `max_retries` nimmt einen int, `timeout` einen float, `backoff` kann linear oder exponential sein." Der Lernende ertrinkt in Optionen. Loesung: im Tutorial nur die *eine* verwendete Variante zeigen, auf die Reference verlinken.
- **Tutorials als Reference.** API-Seiten mit langen Beispielen-Kaskaden. Wer nachschlaegt, scannt und will nicht lesen.
- **Explanation in How-Tos.** "Bevor wir weitermachen, ein bisschen Theorie zu AMQP …". Der How-To-Leser will Schritt 1.
- **Vierfach-Vermengung.** Die Seite hat Abschnitte "Installation", "Konzepte", "Referenz", "Tipps" — klassisch gescheiterte README. Diataxis-Loesung: vier Seitenbaeume, zwischen denen verlinkt wird.

Quellen: [Diataxis Hauptseite](https://diataxis.fr/), [Procida PyCon AU 2017 "What Nobody Tells You About Documentation"](https://pyvideo.org/pycon-au-2017/what-nobody-tells-you-about-documentation.html), [Nick Groenen: "The 4 types of technical documentation"](https://nick.groenen.me/posts/the-4-types-of-technical-documentation/).

### 2.2 Ton, Stimme, Person

**Person:** Zweite Person Singular ("you"). Erste Person Plural ("we") vermeiden — es ist unklar, ob der Autor oder der Leser gemeint ist, und erzeugt falsche Kameradschaftlichkeit. Kubernetes: "Using 'we' in a sentence can be confusing because the reader might not know whether they're part of the 'we'" ([Kubernetes Style Guide](https://kubernetes.io/docs/contribute/style/style-guide/)). Erste Person Singular ("I") nie — Doku hat keinen Autor-Standpunkt.

**Tempus:** Praesens. "The worker consumes messages from the queue" — nicht "will consume", nicht "is consuming". Zukunft und Verlaufsform nur, wenn sie *sachlich* noetig sind (tatsaechlich zukuenftige Aktion: "The task will be retried three times").

**Genus Verbi:** Aktiv bevorzugen. Kubernetes formuliert es weich: "Use active voice and present tense when possible. If you are using passive voice because of an awkward sentence, rewrite the sentence." ([Kubernetes Style Guide](https://kubernetes.io/docs/contribute/style/style-guide/)). Passiv ist legitim, wenn der Handelnde irrelevant oder unbekannt ist ("Messages are persisted to disk").

**Modus bei Anleitungen:** Imperativ. "Run `swarmq worker`", nicht "You can run `swarmq worker`" und nicht "The user should run `swarmq worker`". Der Imperativ ist die kuerzeste, klarste Form — und international verstaendlich, weil er keine Personalformen braucht.

**Microsoft-Voice in drei Woertern:** *warm and relaxed, crisp and clear, ready to lend a hand* ([Microsoft Top 10 tips](https://learn.microsoft.com/en-us/style-guide/top-10-tips-style-voice)). Fuer SwarmQ heisst das: kein Corporate-Ton, aber auch nicht salopp. "SwarmQ does X" statt "We're super excited that SwarmQ can finally X!".

**Kontraktionen** (it's, you'll, don't) sind bei Microsoft explizit gewuenscht — sie machen Text lesbarer. Bei Google eher neutral, bei Kubernetes kein Problem. In der deutschen Fassung entfaellt die Frage; im englischen Content bei SwarmQ empfehle ich Kontraktionen in Tutorials und Explanations, **keine** Kontraktionen in Reference (dort zaehlt Exaktheit).

**Vermeiden:**
- Hoeflichkeitsphrasen ("please", "kindly") — Kubernetes, Google und Microsoft verbieten sie.
- Fuellphrasen ("basically", "actually", "simply", "just"). "Just" ist besonders tueckisch: impliziert, dass der Schritt trivial ist — fuer den Anfaenger ist er es nicht.
- "Easy", "intuitive", "simple" als Adjektive fuer die eigene Software. Stripe tut das nicht. Linear tut das nicht. Der Leser entscheidet, was einfach ist.
- Emojis in technischer Prosa. In GitHub-Commits oder Changelogs okay, im Doc-Fliesstext nein.

### 2.3 Sprachlevel, Jargon, Definitionen

**Jargon-Regel:** Ein Fachbegriff wird beim ersten Auftauchen auf der *aktuellen* Seite definiert, danach benutzt. Der Leser ueberspringt oft Abschnitte, deshalb pro Seite neu, nicht pro Dokumentation. Ausnahme: sehr elementare Begriffe (task, queue, broker), die in einer Glossar-Seite definiert sind und in der Sidebar verlinkbar bleiben.

**Abkuerzungen:** Erste Verwendung ausgeschrieben, Abkuerzung in Klammern. "Dead-Letter Queue (DLQ)". Danach nur die Abkuerzung. Ausnahmen fuer universelle Begriffe (JSON, HTTP, URL) — nicht ausschreiben.

**Laienbegriffe, wo moeglich.** Google: "Write for a global audience. Avoid jargon and idioms" ([Google Style, Summary via Medium](https://medium.com/ux-pulse/microsofts-writing-style-guide-simplified-for-tech-writers-2b6f947fb93c) — Prinzip gilt bei Google identisch). Wenn der Fachbegriff praeziser ist, ihn benutzen und erklaeren; wenn nicht, den einfachen Begriff waehlen.

**Beispiel:** "Der Worker *acknowledged* die Message" ist praezise AMQP-Sprache und sollte so bleiben, aber *einmal* definiert werden: "Der Worker bestaetigt dem Broker, dass die Message verarbeitet ist (`ack`)".

**Negativbeispiele, die SwarmQ vermeiden sollte:**
- "Simply instantiate the SwarmQ object" — "simply" + Jargon in einem Satz.
- "You can easily achieve high throughput" — hohler Superlativ.
- "In order to be able to" — "to" reicht.

### 2.4 Satz- und Absatzbau, Scannability

Leser *scannen* Dokumentation, bevor sie sie lesen. Studien aus der Nielsen-Norman-Tradition zeigen F-Pattern- und Layer-Cake-Scanning. Text muss dafuer *gebaut* sein:

- **Kurze Saetze.** Ziel 15–25 Woerter. Lange Saetze nur dort, wo ein Gedanke nicht sinnvoll trennbar ist.
- **Ein Gedanke pro Absatz.** Absatz beginnt mit der Kernaussage (journalistisches *topic sentence*), der Rest begruendet oder detailliert.
- **Kein doppelter Absatz-Einstieg.** Kein "Also", "Additionally", "Furthermore" am Absatzanfang; wenn zwei Absaetze so zusammenhaengen, gehoeren sie vermutlich zusammen oder brauchen eine Zwischenueberschrift.
- **Listen statt Prosa, wenn es *Aufzaehlungen* sind.** Drei oder mehr gleichrangige Items: Bullet- oder nummerierte Liste. Keine semantische Nuance verschluckt, keine Satzzaehlerei.
- **Tabellen, wenn es um *vergleichen* geht.** Optionen, Werte, Trade-offs gehoeren in Tabellen.
- **Scannbare Ueberschriften.** Jede H2/H3 muss ohne Kontext verstaendlich sein. Kein "Then do this" — konkret sagen, was.

Microsoft: "Text should be scannable. Front-load the most important content. Use headings, short paragraphs, bulleted lists, and tables." ([Microsoft Top 10 tips](https://learn.microsoft.com/en-us/style-guide/top-10-tips-style-voice)).

### 2.5 Ueberschriften-Hierarchie

**Zweck vor Form.** Ueberschriften benennen **Aufgaben** (Task-oriented) oder **Konzepte**, nicht Features. "Rate-Limiting konfigurieren" schlaegt "Die rate_limit-Option"; "Wie SwarmQ Retries anwendet" schlaegt "Retry-Klasse".

**H1 pro Seite genau einmal.** Das ist Semantik und SEO. Im Prototyp ist das schon korrekt umgesetzt.

**Konsistenz der Grammatik innerhalb einer Hierarchiestufe.** Wenn H2 "Install" ist, dann "Run a worker" und "Enqueue from your app", nicht "Running a worker" — alle im gleichen Modus. Der Prototyp mischt das leicht ("Start the broker" vs. "Define your first task") — funktioniert, weil alle Imperativ, aber beide grammatikalisch unterschiedlich ("start" = zur Handlung, "define your first" = mit Objekt). Ist vertretbar; strengere Variante: einheitlich Imperativ+Objekt.

**Tiefe.** Nicht mehr als drei Ebenen (H1 → H2 → H3). H4 tauchen in API-Reference-Seiten auf (Parameters / Returns / Raises / Example als H4 innerhalb eines Method-Blocks) — das ist akzeptabel, aber nur dort.

**Ankernamen stabil halten.** H2/H3 bekommen stabile `id="…"`-Slugs — Links von aussen (Stack Overflow, Blogs) brechen sonst bei jeder Umformulierung.

### 2.6 Progressive Disclosure

Tom Johnson formuliert das Prinzip: "You avoid giving the reader all the information up front. Instead, you present a little bit of information to the reader and then let the reader choose to view more if he or she wants." ([idratherbewriting.com](https://idratherbewriting.com/ucd-progressive-disclosure/)).

**Was in Getting Started gehoert:**
- Installation
- Broker/Backend-Start (Docker Compose)
- Minimale Task-Definition (*eine* Klasse, *ein* Parameter, keine Extras)
- Worker starten (*eine* Queue, Defaults)
- Einmal schedulen, Ergebnis abholen
- Ein Satz pro Next-Step mit Link

**Was nicht in Getting Started gehoert:** Middleware, Signals, Rate-Limiting, Locks, Dead-Letter-Routing, Dependency Injection, Testing-Setup, Worker-Tuning, Serialisierungsdetails, Observability-Integrationen. All das erhaelt eigene How-To- oder Guide-Seiten und wird im "Next steps"-Abschnitt verlinkt.

**Faustregel:** Getting Started darf keinen Absatz enthalten, der mit "Alternatively" oder "You can also" beginnt.

### 2.7 API-Referenz-Pattern

Reference-Seiten folgen einem festen Template. Abweichung kostet den Leser Zeit, weil er seinen Scan-Modus neu einstellen muss.

**Kanonische Reihenfolge pro Method/Function:**

1. **Name** (als H3, in Monospace).
2. **Badges:** `async`, `classmethod`, `deprecated`, `since vX.Y`, `context manager`. Sichtbar neben dem Namen.
3. **Signatur** in Monospace, syntaktisch farbig, in eigenem Block. Der Leser muss die Signatur in Millisekunden erfassen koennen.
4. **Ein-Satz-Zusammenfassung** direkt unter der Signatur. Was tut diese Methode? Kein "allows you to" — "Schedules a task for asynchronous execution."
5. **Parameter-Tabelle.** Spalten: Name, Type, Default, Description. Required-Params markieren (Fettdruck oder `*`-Spalte).
6. **Returns.** Typ und Bedeutung in einem Satz oder kurzer Prosa.
7. **Raises.** Liste mit Exception-Klasse und Grund pro Eintrag.
8. **Example.** *Ein* minimales, lauffaehiges Beispiel. Keine Kaskade von Varianten.
9. **See also.** Links zu verwandten Methoden oder zum How-To, das die Methode benutzt.

Diese Reihenfolge ist derart stabil, dass Sphinx' `autodoc`, `pydoc-markdown`, `mkdocstrings` sie alle produzieren. Der SwarmQ-Prototyp implementiert sie in `api-reference.html` korrekt (Signatur → Prosa → Parameters → Returns → Raises → Example).

**Parameter-Tabellen-Regeln:**
- Typen in Monospace, kein `| None` ausschreiben wenn Default `None` ist — dann `T | None = None` direkt in die Signatur.
- Beschreibung beginnt mit Grossbuchstaben, endet mit Punkt.
- Beschreibung erklaert **Bedeutung**, nicht Typ. "Delay in seconds before delivery" ist gut. "A float" ist schlecht.
- `*args`, `**kwargs` bekommen je eine Zeile, keine zwei.

**Klassen-Reference-Pattern:**
1. Konstruktor-Signatur
2. Kurzbeschreibung + Lifecycle (context manager ja/nein, thread-safety, reuse)
3. **Attributes** (falls public)
4. **Methods** — jede nach obigem Template
5. **Class Methods / Static Methods** separat
6. **Example** auf Klassen-Ebene (vollstaendiges Szenario)

### 2.8 Code-Beispiele

Code-Beispiele sind in der Bibliotheks-Doku oft *das wichtigste* Element. Entwickler lesen den Fliesstext nur, wenn der Code sie nicht selbst klaert.

**Harte Regeln:**

1. **Lauffaehig.** Ein Leser, der das Snippet kopiert und es einsetzt, bekommt dasselbe Ergebnis wie dokumentiert. Import-Statements sind drin. Falls Kontext noetig ist (z. B. `async def main()` Wrapper), steht er im Snippet.
2. **Minimal.** Keine Zeile, die nichts mit der erklaerten Sache zu tun hat. Kein `import logging; logging.basicConfig(...)` im Rate-Limiting-Beispiel, wenn Logging nicht das Thema ist.
3. **Dateiname als Hinweis.** `codeblock-lang` zeigt den Dateinamen, wenn es um eine konkrete Datei geht (`tasks.py`, `enqueue.py`), oder die Sprache, wenn es ein isoliertes Snippet ist (`python`, `shell`).
4. **Konsistenter Stil.** PEP 8 fuer Python. Keine `from swarmq import *`. Typ-Annotationen, wo die Bibliothek sie ohnehin erwartet.
5. **Realistische Daten.** `to="alice@example.com"` statt `to="foo"`. Platzhalter, die aussehen wie echte Daten, reduzieren kognitive Last.
6. **Kommentare im Code vs. Prosa daneben.** Regel: *was* das Snippet tut, steht in der Prosa davor; *warum eine bestimmte Zeile* ausgewaehlt ist, kann als Inline-Kommentar (`# max_retries=3 because SendGrid throttles after 3 retries`). Inline-Kommentare nie fuer Offensichtliches ("# create the app").

**Laengen-Faustregel:** 5–20 Zeilen. Ein Ausnahme-Snippet ist ok, aber mehr als 30 Zeilen am Stueck sind ein Smell — entweder kann man das Beispiel aufbrechen (mehrere Snippets mit Prosa dazwischen) oder es gehoert in ein Repository-Example-Directory mit Link.

**Tabs fuer Alternativen** (Prototyp hat das fuer `uv`/`pip`/`poetry`): sinnvoll, wenn der Code-Inhalt semantisch dasselbe ist. Nicht missbrauchen fuer "Python"/"TypeScript"/"Ruby" wenn die Funktion sprachspezifisch ist — dann getrennte Seiten. Stripe zeigt im Prototyp ueberzeugend, wie Sprach-Tabs zur API-Praesentation gehoeren, aber das ist eine Multi-Language-Client-Situation ([Mintlify zu Stripe](https://www.mintlify.com/blog/stripe-docs)).

**Terminal-Output von Code trennen.** Shell-Beispiele mit Ausgabe: Eingabe- und Output-Zeilen visuell unterscheiden (z. B. Eingabe mit `$ `-Prefix, Output ohne, oder zwei Code-Bloecke). Oder explizit zwei Bloecke. Der Leser muss wissen, was er eintippt und was er sehen wird.

### 2.9 Troubleshooting, FAQ, Fehlermeldungen

**Troubleshooting-Seite** = "Ich habe ein Problem, was jetzt?". Struktur:

- **Symptom** (als H3): "Worker starts but consumes no messages"
- **Ursache(n):** Kurzliste der wahrscheinlichsten Gruende in absteigender Haeufigkeit.
- **Diagnose-Befehle:** Ein bis drei Kommandos, die den konkreten Fall verifizieren (`rabbitmqctl list_queues`, `swarmq inspect --module …`).
- **Loesung:** Konkrete Aktion.
- **Prevention:** optional, falls es ein Konfigurations-Fehler ist.

Jeder Eintrag sollte durch Copy-Paste der Fehlermeldung *findbar* sein — das heisst, die exakte Fehlermeldung steht irgendwo auf der Seite im Text oder Code-Block, nicht paraphrasiert.

**FAQ** = "Fragen, die immer wieder kommen". Nicht mit Troubleshooting vermengen. FAQ darf auch konzeptuelle Fragen enthalten ("Warum async-only?", "Koennen Tasks Results von anderen Tasks konsumieren?"). Format: Frage als H3 in Interrogativ-Form. Antwort 1–3 Absaetze, dann Link ins How-To oder Explanation.

**Fehlermeldungen als Doku-Surface.** Die Python-Exception-Messages der Bibliothek selbst sollten:
- den **Kontext** nennen (welche Operation),
- das **Problem** nennen (was ist schief),
- den **naechsten Schritt** nennen (was der Nutzer pruefen soll) — oder einen Doc-URL, der das tut.

Beispiel: `BrokerUnavailable("publish failed after 3 reconnect attempts; check broker URL and firewall. See docs/troubleshooting#broker-unavailable")`. Das macht Rust vorbildlich in Compiler-Messages, und der Rust-Book-Stil hat das in die Doku-Welt getragen.

Fuer SwarmQ: **Jede Custom-Exception** hat einen Eintrag in einer "Error reference"-Seite. Das erfuellt zugleich eine Forderung aus `aufgabe.md §17.2` (Fehlerkatalog fuer KI-Agenten).

### 2.10 Versionierung, Changelog, Deprecation

**Semver strikt anwenden.** Major = breaking, Minor = additive, Patch = fix. Dokumentiert, welche APIs unter die Stabilitaets-Garantie fallen und welche *internal* sind (bei SwarmQ: alles unter `swarmq._internal` ist per Konvention kein Teil des Public API; das muss die Doku klar stellen).

**Changelog-Format:** `keepachangelog.com`. Sektionen pro Release: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`. Neueste Version oben, `[Unreleased]` als Arbeitsbereich. Jeder Eintrag:
- Ein Satz, der **den Nutzereffekt** beschreibt. "Workers now drain gracefully on SIGTERM" ist gut. "Refactored worker loop" ist intern und gehoert nicht hier.
- Link zum PR oder Issue, falls relevant.
- Bei breaking Changes: **Migrations-Snippet** inline oder als Link.

**Deprecation-Kommunikation:**
1. **Ankuendigung** in der Release, die das Feature als deprecated markiert. `DeprecationWarning` im Code, Hinweis in der API-Reference (`deprecated`-Badge), Changelog-Eintrag.
2. **Migrations-Anleitung** im How-To- oder Migration-Guide-Abschnitt. Vorher-Nachher-Code.
3. **Zeitplan** nennen: "Wird in v1.0 entfernt."
4. **Entfernung** in der angekuendigten Release — nie frueher.

Beispielhaft macht das Django konsequent: Deprecations sind im Release-Note-Abschnitt ausformuliert, und jede Version listet, was aus der vorletzten Major entfernt wurde.

**Versionen der Doku-Seiten selbst.** Wenn SwarmQ ueber v1.0 hinauswaechst und signifikante Breaking Changes bringt, braucht die Doku-Site pro Major-Version einen Branch (Versions-Umschalter in der Header-Nav). Docusaurus, Mintlify und Sphinx-basierte Sites machen das nativ. Vor v1.0 ist eine einzige, aktuelle Version ausreichend.

### 2.11 Quellen-Kanon

Die fuer SwarmQ wichtigsten Style-Guides und Referenzen, in absteigender Autoritaet:

**Primaer:**
- [Diataxis Framework](https://diataxis.fr/) — die Struktur-Bibel. Wenn Zweifel bestehen, gewinnt Diataxis.
- [Google Developer Documentation Style Guide](https://developers.google.com/style) — Default fuer Formulierungsfragen. Klar, aktuell, global-tauglich.
- [Microsoft Writing Style Guide](https://learn.microsoft.com/en-us/style-guide/) — etwas waermer im Ton, starke Inklusions-Guidelines.
- [Write the Docs — Documentation Guide](https://www.writethedocs.org/guide/) — Community-konsens, gut fuer Meta-Fragen (wie teste ich Doku, wie organisiere ich ein Doku-Team).

**Sekundaer / Projekt-spezifisch:**
- [Kubernetes Documentation Style Guide](https://kubernetes.io/docs/contribute/style/style-guide/) — praktisch, viele Beispiele, fokussiert auf API-Projekte.
- [Django Documentation Style](https://docs.djangoproject.com/en/stable/internals/contributing/writing-docs/) — fuer Python-Spezifika besonders relevant.
- [Red Hat Style Guide (Modular Documentation)](https://redhat-documentation.github.io/modular-docs/) — fuer Concept/Procedure/Reference-Modularitaet.
- [18F Content Guide](https://content-guide.18f.gov/) — kurze, sehr konkrete Listen.
- [GitLab Documentation Style Guide](https://docs.gitlab.com/ee/development/documentation/styleguide/) — grosse Installation, viele Edge-Cases dokumentiert.

**Praktiker-Literatur:**
- Tom Johnson, [*I'd Rather Be Writing*](https://idratherbewriting.com/) — v. a. sein [API-Doc-Kurs](https://idratherbewriting.com/learnapidoc/) und Artikel zu Progressive Disclosure.
- Daniele Procida — [Diataxis](https://diataxis.fr/) und der PyCon-AU-2017-Vortrag [What Nobody Tells You About Documentation](https://pyvideo.org/pycon-au-2017/what-nobody-tells-you-about-documentation.html).
- Jacob Kaplan-Moss — [*Writing Great Documentation*](https://jacobian.org/series/great-documentation/). Kurze Essays, aeltere Schule, viel Gueltiges zum Thema Editierprozess und Minimal Viable Docs.
- Shawn Wang (swyx) — diverse Essays zu Developer Experience und "Learn in Public", v. a. [*swyx.io*](https://www.swyx.io/).
- Bob Watson — [*Expert Python Programming*](https://pythonbooks.org/) fuer Projekt-Kontext; relevantes Kapitel: Dokumentieren von Python-APIs.

**Exemplarische Projekt-Docs zum Abgucken:**
- [Stripe Docs](https://docs.stripe.com/) — Gold-Standard API-Referenz.
- [Django Docs](https://docs.djangoproject.com/) — beispielhafte Reference + Topic Guides + Tutorial in klarer Trennung.
- [The Rust Book](https://doc.rust-lang.org/book/) — vorbildliches Tutorial-Layering.
- [Kubernetes Docs](https://kubernetes.io/docs/) — gute Tasks/Concepts/Reference-Trennung nach Diataxis.
- [Prisma Docs](https://www.prisma.io/docs) — saubere Getting-Started-to-Depth-Progression.
- [Tailwind CSS Docs](https://tailwindcss.com/docs) — vorbildliche UX trotz massiver Reference-Menge.
- [Supabase Docs](https://supabase.com/docs) — Nextra-basiert, gute Multi-Language-Code-Tabs.

---

## 3. Teil B: Visuelle Praesentation und UX

### 3.1 Typografie

**Font-Wahl.** Der Prototyp hat sich korrekt fuer **Inter** (UI/Body) und **JetBrains Mono** (Code) entschieden. Inter ist praktisch Industriestandard (Linear, Vercel, Stripe, GitHub teilweise) — hohe x-Height, zahlreiche OpenType-Features, exzellente Bildschirm-Rendering. JetBrains Mono ist einer der wenigen Mono-Fonts mit guten Ligaturen und klarer Unterscheidung `0O`, `l1I`, `|:;` — kritisch in Code-Blocks.

Alternative System-Stacks, falls Web-Fonts nicht gewuenscht: `system-ui, -apple-system, "Segoe UI", Roboto, sans-serif` fuer Body und `ui-monospace, "SF Mono", Menlo, Consolas, monospace` fuer Code. Der Prototyp hat beides als Fallback korrekt gesetzt.

**Font-Subsetting.** Inter und JetBrains Mono von Google Fonts laden die ganze Unicode-Range — spart Latenz, wenn man sich auf Latin + Latin Ext + Symbols beschraenkt. Bei statischem HTML mit Google Fonts CDN ist das weitgehend automatisch. Bei Self-Hosting: `unicode-range`-Descriptors setzen.

**Schriftgroessen-Skala.** Der Prototyp nutzt eine harmonische Skala (12, 13, 15, 16, 18, 22, 28, 36, 48, 60 px). Das entspricht ungefaehr einem *Perfect Fourth*-Verhaeltnis (1.333) in den Headline-Groessen. Body ist 15 px — am unteren Ende der "modern large body"-Schule. Stripe laeuft auf 16 px, Linear ebenfalls. Empfehlung: **16 px Body**, weil pixel-scharfer gerendert und auf hoeheren DPRs konsistent. Viele aktuelle Doc-Sites liegen bei 16/17 px.

**Zeilenhoehe.** 1.6–1.7 fuer Body-Text ist der Zielkorridor. Der Prototyp hat 1.65 — passt. In Code-Bloecken 1.55–1.7 (abhaengig von Zeichen-Deszendern).

**Zeilenlaenge.** Harte Regel: **45–75 Zeichen pro Zeile, Ziel 66**. Typografische Tradition bei Robert Bringhurst, reproduziert in empirischen Studien ([Baymard](https://baymard.com/blog/line-length-readability)). Ueber 75 Zeichen steigt die Fehlerrate beim Zeilenumbruch, unter 45 wird das Lesen hakelig. In CSS: `max-width: 66ch` auf die Content-Spalte. Der Prototyp setzt `--content-w: 760px` — bei 15 px Body entspricht das rund 85 Zeichen. **Zu breit.** Empfehlung: `--content-w` auf ca. 680–700 px reduzieren oder direkt auf `max-width: 70ch` umstellen.

**Gewichte.** Inter bietet 400/500/600/700 im Prototyp. Body 400, Betonung 500 oder 600 (nicht 700 — zu fett fuer Fliesstext), Headlines 600. 700 nur fuer Hero-H1 auf der Landing. Der Prototyp macht das fast so; Headlines sollten nicht unter 600 fallen, sonst wird die Hierarchie flach.

**Letterspacing.** Negativ bei grossen Headlines (`-0.02em` bis `-0.03em`), neutral im Body. Inter ist dafuer ausgelegt, Tight-Tracking in Headlines zu vertragen. Der Prototyp hat `-0.02em` auf H-Tags und `-0.035em` auf der Hero-H1 — konsistent mit Linear/Vercel/Stripe-Stil.

### 3.2 Farbsystem und Dark Mode

**Palette-Struktur.** Der Prototyp folgt einer sauberen Token-Architektur: neutrale Graues (bg, bg-muted, surface, surface-2, surface-hover), Textschattierungen (text, muted, subtle), Borders (border, border-strong), Akzent (accent, hover, soft, text, glow) und semantische Farben (info, warn, danger, success). Das ist die Tailwind/Radix-Schule und deckt 95 % der Faelle.

**Akzentfarbe.** Amber (`#D97706` im Light, `#FBBF24` im Dark) ist ein guter Kompromiss zwischen "warm" und "kann auf Dark stehen". Linear selbst nutzt eine Purple-Akzent, Stripe Blurple, Vercel Schwarz/Weiss, Tailwind ein kaltes Teal, Supabase Neon-Green. Amber ist wiedererkennbar (nicht-generisch), gleichzeitig nicht aggressiv. Kritisch bei Akzentfarben: **keine Fliess-Textfarbe**. Akzent markiert Interaktive Elemente (Links, Buttons, aktive Nav), **nicht** statische Begriffe im Body.

**Dark Mode.** Erstklassig. Im Jahr 2026 ist das Pflicht. Implementation via `[data-theme="dark"]` Attribut am `<html>` + CSS-Variablen-Ueberschreibung ist der bewaehrte Weg. Respektieren von `prefers-color-scheme` beim initialen Laden, aber User-Override per Toggle moeglich (via `localStorage`). Der Prototyp hat den Toggle, aber der JS-Code muss `prefers-color-scheme` beim ersten Besuch einmal auslesen und respektieren — pruefen, ob `app.js` das tut.

**Semantische Callout-Farben** im Prototyp:
- Info: Blue
- Tip: Green
- Warn: Amber
- Danger: Red

Konvention passt zu GitHub, Material for MkDocs und Docusaurus. Wichtig: im Dark Mode **nicht** die saturation der Soft-Backgrounds auf Light-Werte uebertragen — der Prototyp macht das korrekt mit `rgba(..., 0.16)`-Werten.

**Neutrale im Dark Mode.** Der Prototyp hat `--c-bg: #0B1220` — kein reines Schwarz. Das ist gut: reines Schwarz wird bei OLED-Displays als "ausgeschaltet" wahrgenommen, reduziert Kontrast auf Schriften mit leichter Abschwaechung, und wirkt oft klinisch. `#0B1220` ist dunkelblau-grau, waerm und bewusst leicht gefaerbt — klassischer Linear/Stripe-Dark-Stil.

### 3.3 Accessibility und Kontrast

**WCAG 2.1 AA** ist die Zielvorgabe. Konkret:

- **Body Text** auf Hintergrund: mind. 4.5:1.
- **Large Text** (≥ 18 px regular oder ≥ 14 px bold): mind. 3:1.
- **Non-Text Contrast** (Icons, UI-Komponenten-Grenzen, Fokus-Indikatoren): mind. 3:1 gegen adjazente Flaechen.

Quelle: [W3C WCAG 2.1](https://www.w3.org/TR/WCAG21/), konkret Success Criteria [1.4.3 Contrast (Minimum)](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html) und [1.4.11 Non-text Contrast](https://www.w3.org/WAI/WCAG21/Understanding/non-text-contrast.html).

**Pruefen.** Die im Prototyp verwendeten Token sollten mit [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/) verifiziert werden. Kritische Paare:
- `--c-text-subtle` (#71717A light / #94A3B8 dark) gegen den jeweiligen Background — pruefen, ob fuer Sekundaer-Text (nicht deaktivierte Controls, sondern lesbarer Text) 4.5:1 erreicht wird. Bei `#71717A` auf `#FAFAF9` ergibt das 4.6:1 — grenzwertig bestanden. Bei kleineren Schriftgroessen (< 16 px) schwierig — deshalb: Subtle nur fuer Meta-Labels (Breadcrumbs, TOC-Numerierung), nicht fuer Fliesstext.
- Link-Akzent auf Body-BG. Amber auf White ist knapp — pruefen.
- Code-Block Syntax-Tokens. `.tok-str` `#15803D` auf Codeblock-Dark-BG `#0A0F1E` ergibt ca. 5.3:1 — okay. Aber jeden Token einzeln pruefen.

**Fokus-Ringe.** Der Prototyp hat `:focus-visible { outline: 2px solid var(--c-accent); outline-offset: 2px; }` — genau richtig. Nie `outline: none` ohne Ersatz.

**Tastaturnavigation.** Alle interaktiven Elemente (Nav-Links, Copy-Buttons, Theme-Toggle, Search) per Tab erreichbar. Buttons brauchen `aria-label`, wenn sie nur Icon zeigen. Der Prototyp hat das fuer Theme-Toggle (`aria-label="Toggle theme"`) — fuer Copy-Buttons fehlt es noch (zeigen zwar "Copy" im Label, aber ein expliziter `aria-label="Copy code to clipboard"` waere sauberer).

**`prefers-reduced-motion`.** Noch nicht im Prototyp. Einzuege:

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    transition-duration: 0.01ms !important;
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    scroll-behavior: auto !important;
  }
}
```

**Semantik.** Sidebar ist `<aside>` + `<nav>` — korrekt. TOC ebenso. Breadcrumbs sollten in `<nav aria-label="Breadcrumb">` liegen. Code-Bloecke sind `<pre><code>` — richtig.

**Skip-Link.** Fehlt im Prototyp. Gute Praxis fuer Keyboard/Screenreader: erster fokussierbarer Link im `<body>` springt zu `<main>` (`<a class="skip" href="#main">Skip to content</a>`, per Default visuell versteckt, bei Fokus sichtbar).

### 3.4 Code-Block-Design

**Pflicht-Features** jedes Code-Blocks:
1. **Syntax-Highlighting** mit Token-Klassen (der Prototyp macht das manuell via `<span class="tok-kw">` usw. — bei Migration zu einem Generator uebernimmt Shiki, Prism oder Highlight.js das automatisch).
2. **Sprach- oder Dateinamen-Label** links oben.
3. **Copy-Button** rechts oben. Visuelles Feedback beim Klick (Haken-Icon oder Text "Copied").
4. **Horizontal-Scroll** statt Zeilenumbruch fuer lange Zeilen. `white-space: pre; overflow-x: auto`. Der Prototyp hat das.
5. **Konsistente Padding** — 1–1.25 rem. Der Prototyp: 1.1 rem / 1.25 rem — passt.

**Optional, je nach Bedarf:**
- **Zeilennummern.** Nur in Reference/Explanation, wenn der Fliesstext sich auf bestimmte Zeilen beruft ("Line 7 initializes …"). In Tutorials und How-Tos eher weglassen — sie sind visuelles Rauschen.
- **Zeilen-Highlights.** Sehr hilfreich bei Diffs oder wenn die Aufmerksamkeit auf eine neu hinzugefuegte Zeile gerichtet werden soll. Shiki und Docusaurus unterstuetzen das mit `// highlight-next-line`-Kommentaren.
- **Diff-Darstellung.** Bei Migration-Guides wichtig. `diff`-Sprach-Label, rote/gruene Linke-Spalten-Markierung.
- **Tabs fuer verwandte Snippets.** uv/pip/poetry, sync/async, client/worker. Nicht fuer unabhaengige Sprachen.

**Terminal vs. Code-Unterscheidung.** Stripe und Linear unterscheiden visuell zwischen Editor-Code und Shell-Kommandos: Shell bekommt oft einen Top-Bar mit "zsh" oder "bash" und `$ `-Prefix. Der Prototyp markiert das mit `codeblock-lang` ("shell") — gut genug. Fuer SwarmQ-CLI-Reference: Terminal-Stil mit `$ ` Prefix und separater Output-Darstellung (oder eigenem Output-Block).

**Copy-Button-UX.** Sichtbar (nicht nur on-hover), weil Touch-Geraete kein Hover haben. Klick → Feedback in < 200 ms. Der Prototyp hat `.copy-btn.copied`-State — implementiert via `app.js` den State-Switch sichtbar.

**Lange Code-Bloecke.** Ab ca. 40 Zeilen nachdenken: entweder aufteilen mit Zwischen-Prosa, oder max-height und Scroll-within-Block (letzteres nur mit Warnung, weil der Copy-Button dann trotzdem den ganzen Inhalt kopieren muss — `<pre>`-Inhalt kopieren, nicht sichtbaren Viewport).

### 3.5 Callouts und Admonitions

**Typen und Bedeutung.** Konvention (GitHub, Material for MkDocs, Docusaurus, AsciiDoc, Microsoft):

| Typ | Zweck | Farbe |
|---|---|---|
| **Note** | Nuetzliche, nicht-kritische Information | Blue / neutral |
| **Tip** | Optimierung, Shortcut, best practice | Green |
| **Warning / Heads-up** | Etwas kann unerwartet passieren, wenn man nicht aufpasst | Amber |
| **Danger / Caution** | Wer das ignoriert, verliert Daten oder provoziert Security-Issues | Red |

Das deckt den Prototyp ab (`note`, `tip`, `warn`, `danger`).

**Regel: sparsam.** Splunk, Google und die WtD-Community sind sich einig: Callouts verlieren Wirkung, wenn sie ueberall sind. Konkret:
- **Google** ([notices](https://developers.google.com/style/notices)): *"Limit notices, avoid grouping them, and prioritize integrating information into the main text."*
- **Splunk**: *"Don't place an admonition immediately after a topic title. Don't use an admonition in place of prerequisites. Avoid writing long or detailed text in an admonition."*
- **BufferBuffer**: *"Just saying 'Note' doesn't tell readers why it matters."*

**Faustregeln:**
- Maximal 1 Callout pro Bildschirm-Hoehe. Mehr = Inhalt umstrukturieren.
- Nie zwei Callouts direkt untereinander.
- Nicht direkt unter eine Ueberschrift. Erst eine Zeile Fliesstext, dann ggf. Callout.
- Callout-Titel fuer "Note" ist oft redundant — lieber den spezifischen Punkt im ersten Satz ("Heads up: the connection pool stays warm across …").
- **Warning** bedeutet: wenn ignoriert, Datenverlust oder Production-Incident. *Nicht* fuer "man kann es auch anders machen".

**Icon-Regeln.** Icon ja, aber semantisch passend: Ausrufezeichen fuer Warning, Info-Kreis fuer Note, Gluehbirne fuer Tip, Stop-Schild fuer Danger. Der Prototyp nutzt saubere SVG-Icons — ok. Sekundaer-Empfehlung: im Dark Mode die Icon-Farbe heben, nicht die Hintergrundsaturation erhoehen.

### 3.6 Navigation und Layout

**Drei-Spalten-Layout** (Sidebar — Content — TOC) ist bei Doc-Sites seit Stripe 2014 die Referenz. Der Prototyp implementiert das korrekt. Auf < 1200 px faellt die TOC weg, auf < 900 px auch die Sidebar (Hamburger).

**Sticky-Header** mit Backdrop-Blur (`backdrop-filter: saturate(180%) blur(10px)`) ist moderner Standard — Linear, Vercel, Tailwind. Der Prototyp macht das korrekt. Wichtig: `z-index` hoch genug (40) und `position: sticky` nicht `fixed`, damit Scroll-Anker funktionieren.

**Sidebar-Struktur.**
- **Sektionen** mit Uppercase-Label (`nav-title`): *Introduction, Guides, Reference, Ecosystem*. Der Prototyp hat dieses Pattern.
- **Aktiver Eintrag** farblich hervorgehoben (Akzent-Hintergrund + Akzent-Text) — macht der Prototyp.
- **Tiefe**: Zwei Ebenen maximal. Bei tieferen Reference-Baeumen (z. B. API-Modules) expand/collapse verwenden.
- **Keine Page-Navigation in der Sidebar**. Die TOC ist fuer innerhalb-der-Seite.

**On-this-Page TOC.** Rechts, sticky, fasst H2 (und ggf. H3) der aktuellen Seite zusammen. Aktive Sektion visuell markiert (Scroll-Spy per IntersectionObserver — pruefen, ob `app.js` das tut). Bricht auf schmalen Viewports weg.

**Breadcrumbs.** Sinnvoll bei tiefen Hierarchien. `Docs › Guides › Middleware`. Der Prototyp hat das. Semantisch in `<nav aria-label="Breadcrumb">` einpacken.

**Prev/Next-Pager.** Am Ende jeder Seite. Reduziert Abbrueche, weil der Leser nicht zur Sidebar zurueck muss, um weiterzulesen. Besonders wichtig in Tutorial-Baeumen, wo Seiten linear sind. Weniger wichtig in Reference (dort springen Leser). Der Prototyp implementiert Prev/Next — behalten.

**Search.** Muss. Developer-Doku ohne Suche ist nicht benutzbar. Optionen:
- **Algolia DocSearch** — de-facto-Standard, kostenlos fuer Open Source. Muss beantragt werden, dauert einige Tage. Ergebnisse sind relevant und schnell.
- **Pagefind** — statisch generierter Index, keine Third-Party-Dependency, funktioniert offline. Gut fuer kleine bis mittlere Sites.
- **Orama** — moderner Open-Source-Index mit Typo-Tolerance, Client-seitig. Schnell ramping up.
- **Mintlify Search** — wenn sowieso Mintlify.

Fuer SwarmQ sinnvoll: erst Pagefind (kein Blocker fuer Launch), spaeter Algolia DocSearch, wenn das Projekt oeffentlich genug ist.

**Header-Suchfeld mit `⌘K`-Shortcut.** Ist im Prototyp vorhanden, aber noch nicht an einen Index angebunden. Muster aus Linear, Vercel, Stripe — Keyboard-First. Pflicht bei Launch.

### 3.7 Diagramme und Screenshots

**ASCII-Art.** Wenn das Diagramm primitiv ist (Queue-Sequenz, Event-Timeline) und im Monospace-Font perfekt gerendert wird. Vorteil: copy-paste-freundlich, editierbar, funktioniert offline.

**SVG.** Fuer komplexere Architektur-Diagramme. Produziert mit Figma/Excalidraw/draw.io, exportiert als SVG. Pflicht: Dark/Light-Varianten (Colors ueber `currentColor` und CSS-Variablen im SVG), oder zwei Dateien mit `<picture>`-Tag und `prefers-color-scheme`.

**Mermaid.** Text-als-Diagramm. Grossartig fuer Flowcharts und Sequenzen, die sich oft aendern. Nachteil: Layout kontrolliert der Renderer, manchmal unuebersichtlich. Fuer SwarmQ empfehle ich Mermaid fuer Lifecycle-Diagramme (Task-States, Retry-Flow) und echten SVG fuer Architektur-Uebersichten (Broker/Worker/Backend/Client-Topology).

**D2.** Textuelle Alternative zu Mermaid mit besserem Layout-Output. Weniger verbreitet, aber gutes Tooling. Optional.

**Screenshots.** Minimal einsetzen — Screenshots veralten. Wenn doch:
- 2x DPR (Retina). PNG oder AVIF.
- Dark- und Light-Variante.
- Annotation (Pfeile, Markierungen): besser per SVG-Overlay als in die Datei gebrannt, damit spaeter editierbar.
- Immer Alt-Text.

**Terminal-Recordings.** `asciinema` fuer CLI-Demos. Hosted-Player-Einbetten oder `svg-term-cli` fuer statisches SVG. Bei Landing-Pages, wo "so sieht es aus" wichtig ist.

### 3.8 Benchmark-Sites im Vergleich

| Site | Was sie besonders gut macht | Lehre fuer SwarmQ |
|---|---|---|
| **[Stripe](https://docs.stripe.com/)** | Drei-Spalten-Layout mit Live-Code pro Endpoint, Sprach-Tabs synchronisiert ueber die ganze Doku, "Try in Shell" direkt in Doku. Code-Beispiele sind Outcome-orientiert ("How to accept your first payment"). | Outcome-Titel uebernehmen. Sprach-Tabs einheitlich halten. Bei v1.0 ueberlegen, ob ein interaktiver Shell sinnvoll ist. |
| **[Linear](https://linear.app/docs)** | Minimalismus. Ein Akzent, eine Typografie-Skala, ruhige Flaechen. Dark-Mode nativ. Keine Feature-Walls, die Inhalte zerreissen. | Akzent zurueckhalten, keine mehreren Akzentfarben. Der Prototyp folgt Linear-Stil ohnehin. |
| **[Vercel](https://vercel.com/docs)** | Kombination aus Doku und Blog, starke Search, gute Cross-Links zwischen How-To und Concept. Dark-Mode first. | Cross-Links systematisch: jede How-To-Seite verlinkt zum passenden Concept. |
| **[Tailwind CSS](https://tailwindcss.com/docs)** | Beispiel jedes CSS-Utilities inline mit Live-Render. Suche extrem schnell. Sidebar mit vielen Eintraegen uebersichtlich dank klarem Hierarchie-Stil. | Reference-Seiten muessen mit *vielen* Eintraegen klarkommen. Sidebar gruppieren, nicht flach listen. |
| **[Supabase](https://supabase.com/docs)** | Multi-Language-Code-Tabs (JS, Python, Dart, Swift) ueber die ganze Doku. Reference auto-generiert, aber manuell kuratiert. | Wenn SwarmQ mal Client-Libs in anderen Sprachen bekommt, Tabs ready halten. Bis dahin nicht unnoetig vorbereiten. |
| **[The Rust Book](https://doc.rust-lang.org/book/)** | Perfektes Tutorial-Layering. Jedes Kapitel baut auf dem vorigen. Beispiele vollstaendig, inklusive `Cargo.toml`. Kein Vermischen mit Reference (die steht separat im Std-Lib-Doc). | Das Tutorial als *Lehrbuch* behandeln, nicht als Feature-Demo. |
| **[Django Docs](https://docs.djangoproject.com/)** | Saubere Trennung: Tutorial, Topics, Reference, How-To. Release-Notes vorbildlich detailliert. | Release-Notes mit Migrations-Snippets uebernehmen. |
| **[Prisma](https://www.prisma.io/docs)** | Getting-Started-Flow, der den Leser durch Installation, Schema, Query in 10 Minuten bringt. Viele "See also"-Boxen. | "See also"-Pattern an How-To-Enden uebernehmen. |
| **[Astro Docs](https://docs.astro.build/)** | Built on Starlight, exemplarisch fuer i18n, Versioning, Performance. | Wenn Migration, Starlight ernsthaft pruefen. |
| **[Tauri](https://tauri.app/)** | Multi-Stack-Doku mit klarer Platform-Tab-Struktur. | Nur relevant wenn SwarmQ mal Platform-Unterschiede dokumentiert. |
| **[Clerk](https://clerk.com/docs)** | Mintlify-basiert, sehr gute Copy-Paste-UX. Every code block copyable, every link contextual. | Copy-Paste-First-Kultur konsequent durchziehen. |
| **[Python.org (offizielle Docs)](https://docs.python.org/3/)** | Sphinx + klassisches Layout. Extrem dicht, fuer Reference-Konsum optimiert. Nicht fuer Tutorials. | Nicht der Style, den SwarmQ braucht, aber der **Index** und die **Cross-References** sind state of the art. |

**Gemeinsame Muster aller "guten" Sites:**
1. Drei-Spalten-Layout, Sidebar links, TOC rechts, Content 600–760 px.
2. Inter oder vergleichbare Grotesk fuer UI, Monospace-Font fuer Code.
3. Dark-Mode first oder gleichwertig.
4. Sticky-Header mit Search.
5. Code-Bloecke mit Copy-Button und Sprach-Label.
6. Callouts sparsam, klar typisiert.
7. Prev/Next-Pager.
8. Semantic Color Tokens (Info/Tip/Warn/Danger).

Der Prototyp erfuellt alle acht Punkte.

---

## 4. Teil C: Tooling-Landschaft

Kurzer Ueberblick ueber gaengige Generatoren, damit bei Bedarf eine Migration diskutiert werden kann. Quelle: [pkgpulse — Best Documentation Frameworks 2026](https://www.pkgpulse.com/blog/best-documentation-frameworks-2026), [apidog — Top 10 Doc Generators 2026](https://apidog.com/blog/documentation-site-generator/).

| Tool | Basis | Staerken | Schwaechen | Passt fuer SwarmQ? |
|---|---|---|---|---|
| **Material for MkDocs** | Python, Markdown | Einfach, schnell, Python-Kultur-nahe, viele Themes, starke Admonitions/Code-Features | Weniger modern als JS-basierte Alternativen, Suche rudimentaer ohne Plugin | Ja, natuerlich. Python-Projekt + Markdown + `mkdocstrings` fuer Autodoc aus Docstrings. |
| **Sphinx + Furo** | Python, reStructuredText oder Markdown (MyST) | Defacto-Standard fuer Python-Projekte, `autodoc` extrem maechtig, Version-Switcher, perfekte Reference-Generierung | rST-Lernkurve, weniger hip, statisches Styling | Ja. Wenn Reference-Qualitaet Prio 1 ist, besser als MkDocs. |
| **Docusaurus** | React, MDX | Interaktive Komponenten, Versioning, i18n, grosses Ecosystem | React-Boilerplate, Build langsam, JS-schwer | Uebertrieben fuer SwarmQs aktuellen Umfang. |
| **Starlight (Astro)** | Astro, Markdown/MDX | Sehr schnell (Islands-Architektur), i18n, minimal JS-ship, accessible-by-default | Juenger, kleineres Plugin-Ecosystem | Sehr gut geeignet. Moderne Alternative zu MkDocs, wenn mehr Interaktion gewuenscht. |
| **VitePress** | Vue, Markdown | Extrem schnell, Vue-ecosystem, markdown-first | Vue-spezifisch, kleinere Doku-Features vs. Docusaurus | Technisch gut, Vue aber fuer Python-Zielgruppe weniger natuerlich. |
| **Nextra** | Next.js, MDX | Next-Ecosystem, MDX, gute Theme-Qualitaet | Overhead wenn kein Next sowieso im Einsatz | Nur wenn Vercel-Stack ohnehin. |
| **Mintlify** | Hosted (proprietary backend) + Markdown | Erstklassige Out-of-the-Box-UX, AI-Features, schnelle Iteration | Hosted, vendor lock-in, Kosten bei groesserem Volumen | Fuer kommerzielle Projekte stark. Fuer ein Open-Source-SwarmQ weniger natuerlich. |
| **Fern** | Hosted + OpenAPI-Input | SDK-Generation + Docs aus einer Spec, Stripe-like Look | Hosted, Fokus auf REST-APIs | Nicht passend: SwarmQ ist kein REST-Service. |
| **Read the Docs (Hosting)** | Hosting fuer Sphinx/MkDocs | Version-Switcher, Search, free hosting fuer OSS | Keine eigenen Doku-Features, nur Hosting/Build | Sinnvolle Hosting-Schicht ueber MkDocs oder Sphinx. |

**Empfehlung fuer SwarmQ (sofern migriert werden soll):**

1. **Kurzfristig:** statisches HTML (der Prototyp) beibehalten, bis die Content-Mengen einen Generator rechtfertigen. Alle Inhaltsregeln dieses Dokuments sind Generator-unabhaengig.
2. **Mittelfristig:** **Material for MkDocs** oder **Sphinx + Furo + MyST**. Beides ist Python-nativ, beides kann auf Read the Docs oder als statischer GitHub-Pages-Build deployed werden.
   - **MkDocs** wenn Dev-Velocity und einfacher Start zaehlen (Markdown, schneller Build, viele Themes).
   - **Sphinx** wenn API-Reference aus Docstrings auto-generiert werden soll und Versioning wichtig ist. `mkdocstrings` fuer MkDocs schliesst die Luecke teilweise.
3. **Als Alternative auf JS-Seite:** **Starlight**. Am naehesten am Look-and-Feel des aktuellen Prototyps, beste Performance aller JS-basierten Generatoren.

Nicht empfohlen fuer SwarmQ: Docusaurus (Overkill), Mintlify (Vendor-Lock), Nextra (Next-only).

---

## 5. Anwendung auf SwarmQ: Checklisten pro Seitentyp

### 5.1 Landing / Index

**Zweck:** In 10 Sekunden soll der Besucher wissen: Was ist SwarmQ, fuer wen ist es, warum soll ich es benutzen? In 30 Sekunden: Ich sehe den ersten Code und weiss, wie es sich anfuehlt.

**Must:**
- [ ] Hero-H1 mit Outcome ("A task queue that gets out of your way" — Prototyp hat das bereits gut).
- [ ] Ein-Absatz-Lead mit drei Fakten: Async-first, RabbitMQ/Valkey, *one-command worker*.
- [ ] Live-Code neben dem Hero (Prototyp: `tasks.py`-Beispiel) — minimal, 10–12 Zeilen.
- [ ] Primaer-CTA "Get started" → `getting-started`. Sekundaer-CTA "API reference".
- [ ] Drei bis sechs Feature-Tiles mit *konkreten* Behauptungen (nicht "Easy to use", sondern "Sliding-window rate limits via Lua on Valkey").
- [ ] Ein "Ready in five minutes"-Abschnitt mit One-Liner-Install und Link ins Getting Started.
- [ ] Footer: GitHub, PyPI, Changelog, License. Aktuelle Version im Header-Badge.

**Should:**
- [ ] Social-Proof (Stars, Downloads, CI-Status) **nur wenn echt**. Fake Stats schaden mehr als sie nuetzen.
- [ ] Dark/Light-Toggle (Prototyp: ja).
- [ ] Keyboard-Shortcut `⌘K` Search im Header (Prototyp: UI da, Backend fehlt).

**Must not:**
- [ ] Buzzword-Bingo ("cloud-native", "AI-ready", "blazing fast") ohne Substanz.
- [ ] Endlose Feature-Liste, die nicht gruppiert ist.
- [ ] Hero ohne Code.

### 5.2 Getting Started

**Zweck:** Ein Anfaenger hat eine leere Maschine und 5 Minuten Zeit. Am Ende laeuft ein Worker, der eine Task ausgefuehrt hat.

**Must (strikte Reihenfolge):**
- [ ] Prerequisites (Python-Version, Docker oder Broker-Pair, Package-Manager).
- [ ] Install (Tabs `uv`/`pip`/`poetry`; `uv` default).
- [ ] Broker starten (`docker compose up -d`, im Repo vorhandene `docker-compose.yml` referenzieren).
- [ ] Minimale Task-Definition (`Greet`-Klasse, genau ein Parameter, kein `rate_limit`, kein `max_retries`).
- [ ] Worker starten (eine Queue, `--module`, `--queues default`, optional `--concurrency`).
- [ ] Enqueue (Client-Code mit `async with app:`, einmal `schedule()`, `await handle.wait()`, `print(result)`).
- [ ] Next steps: drei bis fuenf Links zu den wichtigsten Guides, je mit einem Satz "Wann brauchst du das".

**Should:**
- [ ] Je ein `note`/`tip`-Callout an kritischen Stellen (async-context-manager, Graceful-Shutdown-Hinweis).
- [ ] Pager am Ende (Prev: Overview / Next: erste Guide-Seite).

**Must not:**
- [ ] Erwaehnung von Middleware, Signals, DLQ, Rate-Limits, DI. Jede dieser Faehigkeiten bekommt eine eigene Seite.
- [ ] Alternative Install-Pfade inline beschrieben ("Oder wenn du pyenv nutzt …"). Nur Tabs.
- [ ] Konfiguration via YAML/ENV-File erzwingen, wenn die CLI reicht.

### 5.3 Tutorial (mehrstufig, z. B. "Build an email worker")

**Zweck:** Lernziel, nicht Aufgabe. Leser lernt *wie es sich anfuehlt*, mit SwarmQ zu arbeiten. Linear, kumulativ.

**Must:**
- [ ] Kapitel 0: Lernziel expliziet — "You'll build an email task that retries on failure, rate-limits itself, and emits a signal on completion."
- [ ] Linearer Aufbau, jedes Kapitel endet mit einem lauffaehigen Zustand.
- [ ] Keine Abzweigungen, keine "Alternatively …".
- [ ] Vollstaendiger Code an jedem Kapitelende, damit verlorene Leser neu starten koennen.
- [ ] Abschliessender "Wrap-up"-Abschnitt mit Recap und Links zu tieferer Reference.

**Must not:**
- [ ] Parameter-Exhaustivitaet ("`schedule` accepts these 11 kwargs …"). Nur die im Tutorial verwendeten.
- [ ] Theorie-Exkurse laenger als ein Absatz. Wenn laenger: in Explanation verlinken.

### 5.4 How-To Guides

**Zweck:** Der Leser hat ein konkretes Problem. Die Seite liefert die Schritte.

**Titel-Regel:** Verben und Outcomes. "Route tasks to multiple queues", "Retry only on specific exceptions", "Integrate with Sentry". Nicht "The `queues` option", nicht "Retry configuration".

**Must:**
- [ ] Ein-Satz-Problem am Anfang: "You want to X."
- [ ] Voraussetzungen, falls noetig ("This guide assumes you have a running worker").
- [ ] Schritte nummeriert, jeder Schritt 1–3 Saetze + Code-Snippet.
- [ ] Kurzer "Verify"-Abschnitt: wie pruefe ich, dass es funktioniert (Log-Output, RabbitMQ-UI, Valkey-Command).
- [ ] Related Links am Ende (andere How-Tos, relevante Reference).

**Must not:**
- [ ] Konzept-Erklaerungen, die fuer andere How-Tos wiederholt werden. Stattdessen zu Explanation linken.
- [ ] Vollstaendige Parameter-Listen. Gehoert in Reference.

### 5.5 API Reference

**Zweck:** Nachschlagen. Vollstaendig, maschinenlesbar, stabil.

**Struktur pro Modul-Seite:**
- [ ] Modul-Titel + Kurzbeschreibung.
- [ ] Klassen, alphabetisch oder nach logischer Gruppierung.
- [ ] Pro Klasse: Konstruktor-Signatur, Prosa, Attributes, Methods.
- [ ] Pro Method: Signatur, Prosa (1 Satz), Parameters, Returns, Raises, Example.

**Must:**
- [ ] Signaturen syntactically hervorgehoben, in Monospace.
- [ ] Typ-Annotationen vollstaendig (kein "T | None" weggelassen).
- [ ] Parameters-Tabelle fuer jede Methode.
- [ ] Raises-Sektion listet jede Exception mit Grund.
- [ ] Mindestens ein Example pro Method.
- [ ] Deprecated-Badge bei deprecatedem Method, mit Since-Version und Verweis auf Ersatz.

**Should:**
- [ ] "See also"-Sektion am Method-Ende.
- [ ] Anker-Links pro Method fuer Deep-Linking.
- [ ] Auto-Generation aus Docstrings (via Sphinx autodoc oder mkdocstrings) — das schuetzt vor Drift zwischen Code und Doku.

**Must not:**
- [ ] Tutorials-artige Prosa.
- [ ] Mehrfach-Beispiele pro Method (stattdessen auf How-To verlinken).
- [ ] Meinungen ("Diese Methode ist besser als X").

### 5.6 CLI Reference

**Zweck:** Kommando-Syntax, Flags, Exit-Codes, Beispielausgaben.

**Struktur pro Kommando:**
- [ ] Kommando-Name als H2, Synopsis-Zeile in Monospace.
- [ ] Ein-Satz-Beschreibung.
- [ ] Options-Tabelle (Flag, Type, Default, Description).
- [ ] Exit-Codes-Tabelle (0 = success, 1 = generic error, 2 = usage error, spezifische Codes dokumentieren).
- [ ] Ein bis drei Beispiele mit vollstaendiger Kommando-Zeile und sichtbarer Output.
- [ ] Env-Var-Aequivalenzen: Jede CLI-Option, die auch per ENV setzbar ist, hat den ENV-Namen (`SWARMQ_BROKER_URL`) aufgelistet.

**Must:**
- [ ] Konsistente Darstellung des Terminal-Inputs (`$ ` Prefix oder klarer Editor-Tabs "shell").
- [ ] Output in separatem Code-Block oder klar abgegrenzt.

**Must not:**
- [ ] Ausfuehrliche Prosa — CLI-Reference ist Nachschlagewerk.

### 5.7 Troubleshooting

**Struktur pro Symptom:**
- [ ] Symptom als H3 (konkret, copy-pasteable): "Worker starts but consumes no messages".
- [ ] Waehrscheinlichste Ursachen in absteigender Haeufigkeit.
- [ ] Diagnose-Kommandos.
- [ ] Fix.
- [ ] Optional: Vermeidung.

**Muss insgesamt enthalten sein:**
- [ ] Jede Custom-Exception von SwarmQ ist dokumentiert.
- [ ] Cross-Links aus Code-Exception-Messages in dieses Dokument moeglich (stabile Anker).
- [ ] Haeufige Broker/Backend-Issues (AMQP-Connection-Refused, Valkey-Auth-Fehler, TLS-Probleme).

### 5.8 Changelog

**Format:** keepachangelog.com.

**Struktur pro Release:**
- [ ] `## [Version] — YYYY-MM-DD`.
- [ ] Sektionen in fester Reihenfolge: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`. Leere Sektionen weglassen.
- [ ] Jeder Eintrag: Nutzereffekt + PR-Link.
- [ ] Breaking Changes mit `[BREAKING]`-Prefix oder eigener Unterueberschrift **+ Migrations-Snippet**.

**Top der Datei:**
- [ ] `[Unreleased]` als aktuelle Arbeitsversion.
- [ ] Link zu keepachangelog im Header ("This project follows Keep a Changelog").
- [ ] Hinweis auf Semver ("This project adheres to Semantic Versioning").

---

## 6. Annotierte Quellenliste

### Style-Guides (primaer)

- [Diataxis Framework](https://diataxis.fr/) — Daniele Procidas Doku-Framework. Pflichtlektuere. Insbesondere [Start here](https://diataxis.fr/start-here/) und die vier Typen-Seiten.
- [Google Developer Documentation Style Guide](https://developers.google.com/style) — Moderner Industrial-Standard fuer API-Doc-Sprache. Gut als Default-Tiebreaker.
- [Microsoft Writing Style Guide](https://learn.microsoft.com/en-us/style-guide/) — Besonders [Top 10 tips for Microsoft style and voice](https://learn.microsoft.com/en-us/style-guide/top-10-tips-style-voice).
- [Write the Docs Guide](https://www.writethedocs.org/guide/) — Community-Konsens, inkl. [Style Guides-Uebersicht](https://www.writethedocs.org/guide/writing/style-guides/).
- [Kubernetes Documentation Style Guide](https://kubernetes.io/docs/contribute/style/style-guide/) — praxisnahe Liste, explizit fuer Dev-Docs.
- [Django Style Guide](https://docs.djangoproject.com/en/stable/internals/contributing/writing-docs/) — v. a. fuer Python-Konventionen.
- [18F Content Guide](https://content-guide.18f.gov/) — kurze, knackige Public-Sector-Richtlinie.
- [GitLab Documentation Style Guide](https://docs.gitlab.com/ee/development/documentation/styleguide/) — groesstes bekanntes Enterprise-Style-Guide mit vielen Edge-Cases.
- [Red Hat Modular Documentation](https://redhat-documentation.github.io/modular-docs/) — Concept/Procedure/Reference-Schema; parallele Traditionslinie zu Diataxis.

### Praktiker und Essays

- Tom Johnson, [*I'd Rather Be Writing*](https://idratherbewriting.com/). Besonders [Progressive Disclosure](https://idratherbewriting.com/ucd-progressive-disclosure/) und der [API-Doc-Kurs](https://idratherbewriting.com/learnapidoc/).
- Daniele Procida, [*What Nobody Tells You About Documentation*](https://pyvideo.org/pycon-au-2017/what-nobody-tells-you-about-documentation.html) (PyCon AU 2017, Video, 30 min).
- Jacob Kaplan-Moss, [*Writing Great Documentation*](https://jacobian.org/series/great-documentation/) (Blog-Serie).
- Shawn Wang / swyx, [swyx.io](https://www.swyx.io/) — Essays zu Developer Experience und Learn-in-Public.
- Mintlify Blog, [*How Stripe creates the best documentation*](https://www.mintlify.com/blog/stripe-docs) — Analyse der Stripe-Doku.
- [Nick Groenen, "The 4 types of technical documentation"](https://nick.groenen.me/posts/the-4-types-of-technical-documentation/) — knappe Diataxis-Zusammenfassung.

### Typografie und UX

- Robert Bringhurst, *The Elements of Typographic Style*, angewandt aufs Web unter [webtypography.net](http://webtypography.net/2.1.2) — Ursprung der 66-Zeichen-Faustregel.
- [Baymard: Readability: The Optimal Line Length](https://baymard.com/blog/line-length-readability) — empirische Zusammenfassung.
- [W3C WCAG 2.1](https://www.w3.org/TR/WCAG21/), insbesondere [1.4.3 Contrast (Minimum)](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html), [1.4.11 Non-text Contrast](https://www.w3.org/WAI/WCAG21/Understanding/non-text-contrast.html).
- [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/).
- [MDN: Color contrast](https://developer.mozilla.org/en-US/docs/Web/Accessibility/Guides/Understanding_WCAG/Perceivable/Color_contrast).

### Benchmark-Sites

- [Stripe Docs](https://docs.stripe.com/), [Linear Docs](https://linear.app/docs), [Vercel Docs](https://vercel.com/docs), [Tailwind CSS Docs](https://tailwindcss.com/docs), [Supabase Docs](https://supabase.com/docs), [Clerk Docs](https://clerk.com/docs), [The Rust Book](https://doc.rust-lang.org/book/), [Django Docs](https://docs.djangoproject.com/), [Kubernetes Docs](https://kubernetes.io/docs/), [Astro Docs](https://docs.astro.build/), [Prisma Docs](https://www.prisma.io/docs), [Python.org Docs](https://docs.python.org/3/), [Redwood Docs](https://redwoodjs.com/docs), [Tauri Docs](https://tauri.app/).

### Tooling

- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/), [mkdocstrings](https://mkdocstrings.github.io/).
- [Sphinx](https://www.sphinx-doc.org/), [Furo Theme](https://pradyunsg.me/furo/), [MyST Parser](https://myst-parser.readthedocs.io/).
- [Docusaurus](https://docusaurus.io/), [Starlight](https://starlight.astro.build/), [VitePress](https://vitepress.dev/), [Nextra](https://nextra.site/), [Mintlify](https://www.mintlify.com/), [Fern](https://buildwithfern.com/).
- [Read the Docs](https://about.readthedocs.com/), [Algolia DocSearch](https://docsearch.algolia.com/), [Pagefind](https://pagefind.app/), [Orama](https://orama.com/).

### Changelog und Versionierung

- [Keep a Changelog](https://keepachangelog.com/).
- [Semantic Versioning](https://semver.org/).
- [Django Release Notes](https://docs.djangoproject.com/en/stable/releases/) — als Vorbild fuer Release-Schreibstil.

---

## 7. Delta zum vorhandenen Design-Prototyp

Der Prototyp unter `docs/design-prototype/` erfuellt den Grossteil der visuellen und strukturellen Anforderungen. Diese Delta-Liste listet auf, was **bereits richtig** ist und was **noch fehlt oder nachgebessert** werden sollte.

### Was der Prototyp bereits korrekt macht

- Drei-Spalten-Layout (Sidebar / Content / TOC) mit korrekten Breakpoints bei 1200 px und 900 px.
- Sticky-Header mit Backdrop-Blur, Dark/Light-Toggle, Search-UI mit `⌘K`-Hinweis.
- Typografie-Wahl (Inter + JetBrains Mono) mit vollstaendigen System-Fallbacks.
- Design-Token-Architektur: neutrale Graues, semantische Colors, Dark-Mode-Overrides — konsistent benannt.
- Akzentfarbe (Amber) nur auf interaktive und semantische Elemente angewendet, nicht auf Fliesstext.
- Code-Bloecke mit Sprach-Label, Copy-Button, horizontal-Scroll, Tabs-Support (uv/pip/poetry).
- Callout-Typen (note/tip/warn/danger) mit passenden Farben, Icons, Soft-Backgrounds; im Dark-Mode Saturation korrekt reduziert.
- Breadcrumbs, Pager, TOC-Sticky — alle Standard-Navigation-Elemente.
- API-Reference mit Signatur-Block (Linker-Akzent-Border), Parameter-Tabelle, async/classmethod/deprecated-Badges, Method-Separator-Border.
- Focus-Visible-Outline mit Akzentfarbe und Offset — WCAG-konform.
- Hero mit Live-Code-Snippet neben dem Text, konkrete Stat-Badges, klare CTAs.
- Feature-Grid mit substantiellen, konkreten Aussagen (nicht "Easy to use", sondern "Sliding-window counters via Lua").
- Footer mit GitHub / PyPI / Changelog / License.

### Was noch fehlt oder nachgebessert werden sollte

**Struktur / Inhalt:**
- Die Sidebar-Gruppen stehen (Introduction, Guides, Reference), aber die **Diataxis-Vier-Achsen-Struktur ist noch nicht explizit**. Vorschlag: Sidebar-Sektionen als `Tutorials` (= Getting Started + Multi-Chapter-Tutorials), `How-To Guides` (feature-orientiert), `Reference` (API + CLI + Config), `Explanation` (Architektur, Lifecycle, Trade-offs). Der bisherige Begriff "Guides" mischt How-To und Explanation und sollte aufgebrochen werden.
- **Explanation-Seiten fehlen komplett** im Prototyp (keine Seite fuer "Why async-first?", "Delivery guarantees", "Rate-limit algorithm"). Das ist aktuell der groesste inhaltliche Gap.
- **Troubleshooting-Seite fehlt** im Prototyp — Pflicht-Seitentyp fuer eine Library.
- **Changelog-Seite fehlt** (nur als Footer-Link erwaehnt). Pflicht-Seite, sobald erste Releases raus sind.
- **Error Reference fehlt** — wichtig fuer `aufgabe.md §17.2` (Fehlerkatalog fuer KI-Agenten).
- **Migration-Guide-Muster fehlt** — noch nicht akut, aber Template jetzt anlegen.
- **FAQ-Seite fehlt**.

**Typografie:**
- **Zeilenlaenge zu breit.** `--content-w: 760px` bei 15 px Body ergibt ca. 85 Zeichen/Zeile. Ziel 66ch. Empfehlung: `--content-w: 640px` oder `max-width: 70ch` direkt auf `.content`.
- **Body-Font-Groesse ueberdenken.** 15 px ist am unteren Ende. Stripe, Linear und moderne Doc-Sites liegen bei 16–17 px. Pruefen, ob 16 px ohne Layout-Brueche moeglich ist.

**Accessibility:**
- **`prefers-reduced-motion`-Query fehlt.** Hinzufuegen (Snippet in Abschnitt 3.3).
- **Skip-Link fehlt.** Vor allem fuer Screenreader wichtig. Erster fokussierbarer Link, springt zu `<main>`.
- **ARIA-Labels pruefen:** Copy-Button hat visuelles Label, aber kein explizites `aria-label`. Breadcrumbs sollten in `<nav aria-label="Breadcrumb">`. Theme-Toggle hat korrektes Label.
- **Kontrast-Pruefung.** Alle Token-Paare mit WebAIM-Checker gegen WCAG AA verifizieren — besonders `--c-text-subtle` auf Body-Background und Akzent-Links.

**Funktional:**
- **Search-Backend nicht angebunden.** UI da, aber `⌘K` oeffnet (vermutlich) nichts. Als MVP: Pagefind einbinden; spaeter Algolia DocSearch beantragen.
- **Scroll-Spy fuer TOC** pruefen (IntersectionObserver) — sichtbar nur, wenn ein Besuch mit JS-ein-/ausgeschaltet getestet wird.
- **Theme-Toggle + `prefers-color-scheme`:** pruefen, ob das Initial-Theme vom OS respektiert wird und User-Override in `localStorage` liegt.

**Inhaltliche Seiten, die noch fehlen oder stubs sind:**
- Alle Links in der Sidebar, die aktuell auf `#` zeigen (Why SwarmQ, Core Concepts, Architecture, Defining Tasks, Scheduling & Retries, Signals, Rate Limiting, Locking & Dedup, Dead Letter Queues, Testing Tasks, Configuration, Changelog).
- API-Reference ist aktuell nur `swarmq.SwarmQ` — weitere Module (`swarmq.task`, `swarmq.client`, `swarmq.middleware`, `swarmq.signals`, `swarmq.limits`, `swarmq.broker`, `swarmq.backend`) fehlen.

**Mittelfristige Entscheidung:**
- Statisches HTML ausbauen vs. Migration auf **Material for MkDocs** oder **Sphinx + Furo**. Sobald der Inhalt mehr als 15–20 Seiten umfasst und Reference auto-generiert werden soll, wird Generator-Nutzung wirtschaftlich. Die Design-Tokens und das Layout aus dem Prototyp lassen sich in beide uebertragen — Material for MkDocs ueber `extra.css` und ein eigenes Theme-Overlay; Sphinx ueber Furo-Theme-Anpassung.

---

**Ende.**
