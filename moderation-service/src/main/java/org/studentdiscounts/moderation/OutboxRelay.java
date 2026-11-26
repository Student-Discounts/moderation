package org.studentdiscounts.moderation;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
class OutboxRelay {
  private final ReviewStore store;
  private final KafkaTemplate<String, String> kafka;
  private final ModerationProperties properties;
  private final TransactionTemplate transaction;
  private final MeterRegistry metrics;

  OutboxRelay(
      ReviewStore store,
      KafkaTemplate<String, String> kafka,
      ModerationProperties properties,
      PlatformTransactionManager manager,
      MeterRegistry metrics) {
    this.store = store;
    this.kafka = kafka;
    this.properties = properties;
    this.transaction = new TransactionTemplate(manager);
    this.metrics = metrics;
  }

  @Scheduled(fixedDelay = 500)
  void publish() {
    if (!properties.outboxEnabled()) return;
    for (int i = 0; i < 20; i++) {
      try {
        if (!Boolean.TRUE.equals(transaction.execute(status -> publishOne()))) return;
      } catch (RuntimeException unavailable) {
        metrics.counter("moderation.outbox.failures").increment();
        return;
      }
    }
  }

  private boolean publishOne() {
    var rows =
        store.records(
            "select o.* from outbox o where published_at is null and next_attempt_at<=now() and not"
                + " exists(select 1 from outbox previous where previous.aggregate_id=o.aggregate_id"
                + " and previous.aggregate_sequence<o.aggregate_sequence and previous.published_at"
                + " is null) order by o.created_at,o.id limit 1 for update of o skip locked");
    if (rows.isEmpty()) return false;
    var row = rows.getFirst();
    try {
      kafka
          .send(
              row.get("topic").toString(),
              row.get("aggregateId").toString(),
              row.get("payload").toString())
          .get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      defer(row.get("id"));
      return false;
    } catch (Exception failure) {
      defer(row.get("id"));
      return true;
    }
    store.jdbc().update("update outbox set published_at=now() where id=?", row.get("id"));
    metrics.counter("moderation.outbox.published").increment();
    return true;
  }

  private void defer(Object id) {
    store
        .jdbc()
        .update(
            "update outbox set"
                + " failure_count=failure_count+1,last_error='BROKER_ACK_FAILED',next_attempt_at=now()+interval"
                + " '1 second'*least(60,power(2,least(failure_count+1,6))) where id=?",
            id);
    metrics.counter("moderation.outbox.failures").increment();
  }
}
