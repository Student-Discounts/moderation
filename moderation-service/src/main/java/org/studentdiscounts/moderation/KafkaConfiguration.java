package org.studentdiscounts.moderation;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.*;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@EnableKafka
class KafkaConfiguration {
  @Bean
  ConcurrentKafkaListenerContainerFactory<String, String> moderationKafkaFactory(
      ConsumerFactory<String, String> consumers,
      KafkaTemplate<String, String> kafka,
      ReviewStore store,
      MeterRegistry metrics,
      ModerationProperties properties) {
    var publisher =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, error) ->
                new TopicPartition("moderation.consumer.v1.DLT", record.partition()));
    publisher.setFailIfSendResultIsError(true);
    publisher.setWaitForSendResultTimeout(Duration.ofMillis(properties.sendTimeoutMs()));
    var handler =
        new DefaultErrorHandler(
            (record, error) -> {
              publisher.accept(record, error);
              UUID eventId = null, aggregate = null;
              try {
                var event = store.json().readTree(record.value().toString());
                eventId = UUID.fromString(event.path("eventId").asString());
                aggregate = UUID.fromString(event.path("data").path("submissionId").asString());
              } catch (RuntimeException malformed) {
                /* Transport location still identifies malformed input. */
              }
              store
                  .jdbc()
                  .update(
                      "insert into"
                          + " consumer_failures(source_topic,source_partition,source_offset,event_id,aggregate_id,error_code)"
                          + " values(?,?,?,?,?,?) on conflict do nothing",
                      record.topic(),
                      record.partition(),
                      record.offset(),
                      eventId,
                      aggregate,
                      "CONSUMER_DLT");
              metrics.counter("moderation.consumer.dlt.published").increment();
            },
            new FixedBackOff(1000L, 2L));
    handler.addNotRetryableExceptions(WorkflowService.PoisonEvent.class);
    handler.setAckAfterHandle(true);
    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(consumers);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
    factory.setCommonErrorHandler(handler);
    return factory;
  }

  @Bean
  @ConditionalOnProperty(name = "moderation.local-topics-enabled", havingValue = "true")
  KafkaAdmin.NewTopics topics() {
    return new KafkaAdmin.NewTopics(
        topic("submission.events.v1"),
        topic("submission.workflow.v1"),
        topic("submission.workflow.v1.DLT"),
        topic("moderation.events.v1"),
        topic("moderation.consumer.v1.DLT"));
  }

  private NewTopic topic(String name) {
    return TopicBuilder.name(name).partitions(1).replicas(1).build();
  }

  @Bean(destroyMethod = "close")
  AdminClient moderationKafkaAdmin(KafkaAdmin admin) {
    var config = new HashMap<String, Object>(admin.getConfigurationProperties());
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
    return AdminClient.create(config);
  }

  @Bean
  HealthIndicator kafkaHealthIndicator(AdminClient moderationKafkaAdmin) {
    return () -> {
      try {
        return moderationKafkaAdmin.describeCluster().nodes().get(3, TimeUnit.SECONDS).isEmpty()
            ? Health.down().build()
            : Health.up().build();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Health.down().build();
      } catch (Exception unavailable) {
        return Health.down().build();
      }
    };
  }

  @Bean
  Listener listener(WorkflowService workflow) {
    return new Listener(workflow);
  }

  static class Listener {
    private final WorkflowService workflow;

    Listener(WorkflowService workflow) {
      this.workflow = workflow;
    }

    @KafkaListener(
        topics = "submission.events.v1",
        containerFactory = "moderationKafkaFactory",
        autoStartup = "${moderation.listener-enabled:true}")
    void committed(ConsumerRecord<String, String> event) {
      workflow.accept(event.value(), event.key());
    }

    @KafkaListener(
        topics = "submission.workflow.v1.DLT",
        groupId = "${spring.kafka.consumer.group-id}-command-outcomes",
        containerFactory = "moderationKafkaFactory",
        autoStartup = "${moderation.listener-enabled:true}")
    void rejected(ConsumerRecord<String, String> event) {
      workflow.rejected(event.value(), event.topic());
    }
  }
}
