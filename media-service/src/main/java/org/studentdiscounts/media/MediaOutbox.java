package org.studentdiscounts.media;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class MediaOutbox {
  private static final Logger LOG = LoggerFactory.getLogger(MediaOutbox.class);
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, String> kafka;
  private final MediaProperties properties;

  public MediaOutbox(
      JdbcTemplate jdbc,
      TransactionTemplate tx,
      KafkaTemplate<String, String> kafka,
      MediaProperties properties) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.kafka = kafka;
    this.properties = properties;
  }

  record Event(UUID id, UUID asset, String payload) {}

  @Scheduled(fixedDelayString = "${SD_MEDIA_OUTBOX_DELAY_MS:500}")
  public void relay() {
    if (!properties.outboxEnabled()) return;
    try {
      for (int i = 0; i < 20; i++) if (!publishOne()) break;
    } catch (RuntimeException e) {
      LOG.warn("Media outbox relay unavailable; committed event retained");
    }
  }

  boolean publishOne() {
    return Boolean.TRUE.equals(
        tx.execute(transaction -> {
          var events =
              jdbc.query(
                  """
                  select o.event_id,o.asset_id,o.payload::text from media_outbox o
                  where o.published_at is null and not exists
                  (select 1 from media_outbox earlier where earlier.asset_id=o.asset_id
                   and earlier.published_at is null and earlier.revision<o.revision)
                  order by o.created_at,o.event_id limit 1 for update of o skip locked
                  """,
                  (rs, n) ->
                      new Event(
                          rs.getObject(1, UUID.class),
                          rs.getObject(2, UUID.class),
                          rs.getString(3)));
          if (events.isEmpty()) return false;
          Event event = events.getFirst();
          try {
            kafka
                .send(properties.workflowTopic(), event.asset().toString(), event.payload())
                .get(12, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka acknowledgement interrupted", e);
          } catch (java.util.concurrent.ExecutionException
              | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Kafka acknowledgement unavailable", e);
          }
          // A crash after broker ACK but before commit resends the SAME eventId. Consumers
          // deduplicate.
          jdbc.update("update media_outbox set published_at=now() where event_id=?", event.id());
          return true;
        }));
  }

  @Configuration
  @Profile("local")
  static class LocalTopic {
    @Bean
    NewTopic mediaWorkflowTopic(MediaProperties properties) {
      return TopicBuilder.name(properties.workflowTopic()).partitions(1).replicas(1).build();
    }
  }
}
