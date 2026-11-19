package org.studentdiscounts.media;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class MediaObservability {
  @Bean(destroyMethod = "close")
  AdminClient mediaKafkaAdmin(KafkaAdmin kafkaAdmin) {
    var config = new HashMap<String, Object>(kafkaAdmin.getConfigurationProperties());
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
    return AdminClient.create(config);
  }

  @Bean("kafkaDelivery")
  HealthIndicator kafkaDelivery(AdminClient mediaKafkaAdmin, MediaProperties properties) {
    return () -> {
      try {
        var topic =
            mediaKafkaAdmin
                .describeTopics(java.util.List.of(properties.workflowTopic()))
                .allTopicNames()
                .get(3, TimeUnit.SECONDS)
                .get(properties.workflowTopic());
        boolean available =
            topic != null
                && !topic.partitions().isEmpty()
                && topic.partitions().stream()
                    .allMatch(
                        p -> p.leader() != null && p.leader().id() >= 0 && !p.isr().isEmpty());
        return available ? Health.up().build() : Health.down().build();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Health.down().build();
      } catch (Exception e) {
        return Health.down().build();
      }
    };
  }

  @Bean
  Object mediaQueueMetrics(MeterRegistry registry, JdbcTemplate jdbc) {
    Object marker = new Object();
    Gauge.builder(
            "media.jobs.pending",
            marker,
            x ->
                number(
                    jdbc,
                    "select count(*) from media_asset where status in"
                        + " ('QUARANTINED','PROCESSING')"))
        .description("Durable quarantine/processing jobs")
        .register(registry);
    Gauge.builder(
            "media.outbox.pending",
            marker,
            x -> number(jdbc, "select count(*) from media_outbox where published_at is null"))
        .description("Committed events awaiting broker acknowledgement")
        .register(registry);
    Gauge.builder(
            "media.outbox.oldest.age.seconds",
            marker,
            x ->
                number(
                    jdbc,
                    "select coalesce(extract(epoch from now()-min(created_at)),0) from media_outbox"
                        + " where published_at is null"))
        .description("Oldest committed unsent event age")
        .register(registry);
    return marker;
  }

  private static double number(JdbcTemplate jdbc, String query) {
    try {
      Number result = jdbc.queryForObject(query, Number.class);
      return result == null ? 0 : result.doubleValue();
    } catch (RuntimeException e) {
      return Double.NaN;
    }
  }
}
