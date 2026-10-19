package dev.orderflow.payment.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(value = OutboxTestResource.class, restrictToAnnotatedClass = true)
@TestProfile(OutboxPostgresProfile.class)
class OutboxPublisherTest {

    @Inject
    DataSource dataSource;

    @Inject
    OutboxPublisher publisher;

    @BeforeEach
    void cleanOutbox() throws Exception {
        try (Connection connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "delete from outbox_messages")) {
            statement.executeUpdate();
        }
    }

    @Test
    void publishesFromPostgresToKafkaAndMarksTheMessage() throws Exception {
        String topic = "orderflow-payment-outbox-test-" + UUID.randomUUID();
        UUID messageId = insertMessage(topic, "payment-key");

        try (KafkaConsumer<String, String> consumer = consumer(topic)) {
            publisher.poll();

            var records = awaitRecords(consumer, 1);
            assertEquals(1, records.size());
            assertEquals("payment-key", records.get(0).key());
            assertTrue(records.get(0).value().contains(messageId.toString()));
        }

        try (Connection connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                select published_at is not null, attempt_count
                from outbox_messages where message_id = ?
                """)) {
            statement.setObject(1, messageId);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertTrue(result.getBoolean(1));
                assertEquals(1, result.getInt(2));
            }
        }
    }

    private UUID insertMessage(String topic, String key) throws Exception {
        UUID messageId = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();
        String payload = "{\"eventId\":\"" + messageId + "\",\"orderId\":\"" + aggregateId + "\"}";
        try (Connection connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                insert into outbox_messages
                    (message_id, aggregate_id, event_type, event_version, destination,
                     message_key, payload, created_at)
                values (?, ?, 'PaymentApproved', 1, ?, ?, ?, current_timestamp)
                """)) {
            statement.setObject(1, messageId);
            statement.setObject(2, aggregateId);
            statement.setString(3, topic);
            statement.setString(4, key);
            statement.setString(5, payload);
            statement.executeUpdate();
        }
        return messageId;
    }

    private KafkaConsumer<String, String> consumer(String topic) {
        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, OutboxTestResource.kafkaBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "payment-outbox-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(topic));
        consumer.poll(Duration.ofMillis(500));
        return consumer;
    }

    private List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> awaitRecords(
            KafkaConsumer<String, String> consumer,
            int expected) {
        var records = new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (records.size() < expected && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(500)).forEach(records::add);
        }
        return records;
    }
}
