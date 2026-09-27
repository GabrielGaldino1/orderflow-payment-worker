# OrderFlow Payment Worker

Quarkus application that will process payments, integrate with the provider, and receive signed webhooks.

## F02 foundation

- Java 17 and Maven Wrapper.
- Quarkus Health, Hibernate ORM with Panache, PostgreSQL, Flyway, Kafka client, and REST Client.
- Owned PostgreSQL database configured only through environment variables.
- Liveness at `/q/health/live` and readiness at `/q/health/ready`.
- Foundation health tests backed by an in-memory PostgreSQL-compatible H2 profile.

No Kafka consumer, provider call, webhook endpoint, or payment domain table is implemented in F02.

## Build and test

```powershell
./mvnw.cmd clean verify
```

Local runtime variables and orchestration commands are documented in `orderflow-platform`.
