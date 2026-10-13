---
title: Liegt die DLQ-Kopie sicher in der XQ und scheitert nur das Ack des Originals, gilt das als terminal wie ein Erfolg
applies_when:
  - Änderungen an dead_letter_copy, an Gates mit DLQ-Pfad (Expiry, Lock, Delivery-Limit) oder an SettleFailedAfterPublish
  - eine Fehlersuche meldet Unique-Freigabe oder TASK_DLQ trotz gescheitertem Ack nach dem DLQ-Publish
  - ein neues Gate leitet Nachrichten in die DLQ
tags: [dlq, ack, execution-guard, unique, broker]
id: E-043
datum: 2026-10-09
status: gilt
quelle:
  - Commit 417df44 (Dead-Letter mit gescheitertem Ack nach dem Publish gilt als terminal)
  - Commit 9cf72c7 (Ausführungsschutz U8, SettleFailedAfterPublish)
  - src/swarmq/worker/gates/__init__.py:304-308 (dead_letter_copy)
  - website/explanation/delivery-semantics.md:316-320
tests:
  - tests/unit/worker/test_retry.py::test_dead_letter_with_failed_ack_after_publish_is_terminal
  - tests/unit/worker/gates/test_lock_dlq_ack_failed.py::test_render_error_with_failed_ack_after_publish_is_terminal
  - tests/unit/worker/gates/test_lock_dlq_ack_failed.py::test_wait_limit_with_failed_ack_after_publish_is_terminal
---

**Entscheidung.** Hat der Broker die DLQ-Kopie bestätigt und scheitert danach nur das Ack des Originals (`SettleFailedAfterPublish`, etwa bis RabbitMQ 4.2 nach einem Consumer-Timeout), behandelt der Worker das wie einen erfolgreichen Dead-Letter: Warnung `dlq_ack_failed_after_publish`, Unique-Freigabe und `TASK_DLQ`. Die Neuzustellung des Originals überspringt der Ausführungsschutz als `done`.

**Begründung.** Die Kopie liegt in der DLQ; eine Behandlung als gescheiterter Publish hätte das Unique-Flag bis zur TTL stehen lassen, weil die übersprungene Neuzustellung die Freigabe nie mehr erreicht.

**Bewusste Folgen.** Ein `TASK_DLQ` und eine Unique-Freigabe trotz gescheitertem Ack sind kein Fehler. Mit `SWARMQ_EXECUTION_GUARD=false` kann die Neuzustellung erneut verarbeitet werden.

**Überdenken nur mit neuem Argument.** Offen: FS-17. Das Expiry-Gate (`expiry.py:118-132`) hält sich nicht an diese Regel und wertet den Fall als Fehlschlag; das Unique-Flag hängt dann bis `unique_for`. Das ist ein Fehler gegen diese Entscheidung.
