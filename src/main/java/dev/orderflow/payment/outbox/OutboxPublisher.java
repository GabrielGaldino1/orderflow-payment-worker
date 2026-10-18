package dev.orderflow.payment.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OutboxPublisher {

    private static final Logger LOGGER = Logger.getLogger(OutboxPublisher.class);

    private final String claimant = UUID.randomUUID().toString();
    private final AtomicLong backlogSize = new AtomicLong();
    private final AtomicLong oldestAgeSeconds = new AtomicLong();

    @Inject
    OutboxRepository repository;

    @Inject
    Producer<String, String> producer;

    @Inject
    MeterRegistry registry;

    @ConfigProperty(name = "orderflow.outbox.enabled")
    boolean enabled;

    @ConfigProperty(name = "orderflow.outbox.batch-size")
    int batchSize;

    @ConfigProperty(name = "orderflow.outbox.claim-timeout")
    Duration claimTimeout;

    @ConfigProperty(name = "orderflow.outbox.send-timeout")
    Duration sendTimeout;

    @PostConstruct
    void registerGauges() {
        if (claimTimeout.compareTo(sendTimeout) <= 0) {
            throw new IllegalStateException(
                    "orderflow.outbox.claim-timeout must be greater than orderflow.outbox.send-timeout");
        }
        registry.gauge("orderflow.outbox.backlog.size", backlogSize);
        registry.gauge("orderflow.outbox.oldest.age.seconds", oldestAgeSeconds);
    }

    @Scheduled(every = "${orderflow.outbox.poll-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void poll() {
        if (!enabled) {
            return;
        }
        for (OutboxMessage message : repository.claimBatch(claimant, batchSize, claimTimeout)) {
            repository.renewClaims(claimant);
            publish(message);
        }
        refreshStats();
    }

    private void publish(OutboxMessage message) {
        counter("orderflow.outbox.publish.attempts", message).increment();
        try {
            producer.send(new ProducerRecord<>(message.destination(), message.messageKey(), message.payload()))
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            release(message, exception);
            return;
        } catch (Exception exception) {
            release(message, exception);
            return;
        }

        try {
            if (repository.markPublished(message.messageId(), claimant)) {
                Timer.builder("orderflow.outbox.publish.latency")
                        .tag("destination", message.destination())
                        .tag("message.type", message.eventType())
                        .register(registry)
                        .record(Duration.between(message.createdAt().toInstant(), Instant.now()));
                LOGGER.infof(
                        "Outbox message published messageId=%s aggregateId=%s messageType=%s destination=%s attempt=%d",
                        message.messageId(), message.aggregateId(), message.eventType(),
                        message.destination(), message.attemptCount());
            }
        } catch (RuntimeException exception) {
            LOGGER.errorf(
                    "Outbox message acknowledged but mark failed messageId=%s messageType=%s destination=%s failureType=%s",
                    message.messageId(), message.eventType(), message.destination(),
                    exception.getClass().getSimpleName());
        }
    }

    private void release(OutboxMessage message, Exception exception) {
        String failureType = exception.getClass().getSimpleName();
        repository.releaseAfterFailure(message.messageId(), claimant, failureType);
        counter("orderflow.outbox.publish.failures", message).increment();
        LOGGER.warnf(
                "Outbox publication failed messageId=%s messageType=%s destination=%s attempt=%d failureType=%s",
                message.messageId(), message.eventType(), message.destination(), message.attemptCount(), failureType);
    }

    private Counter counter(String name, OutboxMessage message) {
        return Counter.builder(name)
                .tag("destination", message.destination())
                .tag("message.type", message.eventType())
                .register(registry);
    }

    private void refreshStats() {
        OutboxRepository.Stats stats = repository.stats();
        backlogSize.set(stats.backlogSize());
        oldestAgeSeconds.set(stats.oldestAgeSeconds());
    }
}
