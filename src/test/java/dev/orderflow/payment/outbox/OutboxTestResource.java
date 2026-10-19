package dev.orderflow.payment.outbox;

import java.util.Map;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class OutboxTestResource implements QuarkusTestResourceLifecycleManager {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:17.6-alpine"));
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.9.2"));

    @Override
    public Map<String, String> start() {
        POSTGRES.start();
        KAFKA.start();
        return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.username", POSTGRES.getUsername(),
                "quarkus.datasource.password", POSTGRES.getPassword(),
                "quarkus.datasource.jdbc.url", POSTGRES.getJdbcUrl(),
                "quarkus.flyway.migrate-at-start", "true",
                "kafka.bootstrap.servers", KAFKA.getBootstrapServers(),
                "orderflow.outbox.enabled", "true",
                "orderflow.outbox.poll-interval", "1h");
    }

    @Override
    public void stop() {
        KAFKA.stop();
        POSTGRES.stop();
    }

    static String kafkaBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
