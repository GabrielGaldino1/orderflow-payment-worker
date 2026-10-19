package dev.orderflow.payment.outbox;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class OutboxPostgresProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("quarkus.datasource.db-kind", "postgresql");
    }
}
