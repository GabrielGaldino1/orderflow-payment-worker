package dev.orderflow.payment.outbox;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class OutboxRepository {

    @Inject
    DataSource dataSource;

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public List<OutboxMessage> claimBatch(String claimant, int batchSize, Duration claimTimeout) {
        String sql = """
                with candidates as (
                    select message_id
                    from outbox_messages
                    where published_at is null
                      and (claimed_at is null or claimed_at < current_timestamp - (? * interval '1 millisecond'))
                    order by created_at
                    for update skip locked
                    limit ?
                )
                update outbox_messages message
                set claimed_by = ?, claimed_at = current_timestamp,
                    last_attempt_at = current_timestamp,
                    attempt_count = message.attempt_count + 1, last_error = null
                from candidates
                where message.message_id = candidates.message_id
                returning message.message_id, message.aggregate_id, message.event_type,
                          message.event_version, message.destination, message.message_key,
                          message.payload, message.created_at, message.attempt_count
                """;
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setLong(1, claimTimeout.toMillis());
            statement.setInt(2, batchSize);
            statement.setString(3, claimant);
            try (var result = statement.executeQuery()) {
                List<OutboxMessage> messages = new ArrayList<>();
                while (result.next()) {
                    messages.add(new OutboxMessage(
                            result.getObject("message_id", UUID.class),
                            result.getObject("aggregate_id", UUID.class),
                            result.getString("event_type"),
                            result.getInt("event_version"),
                            result.getString("destination"),
                            result.getString("message_key"),
                            result.getString("payload"),
                            result.getObject("created_at", OffsetDateTime.class),
                            result.getInt("attempt_count")));
                }
                return List.copyOf(messages);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not claim outbox messages", exception);
        }
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public boolean markPublished(UUID messageId, String claimant) {
        return update("""
                update outbox_messages
                set published_at = current_timestamp, claimed_by = null, claimed_at = null, last_error = null
                where message_id = ? and claimed_by = ? and published_at is null
                """, null, messageId, claimant) == 1;
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public int renewClaims(String claimant) {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                update outbox_messages
                set claimed_at = current_timestamp
                where claimed_by = ? and published_at is null
                """)) {
            statement.setString(1, claimant);
            return statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not renew outbox claims", exception);
        }
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void releaseAfterFailure(UUID messageId, String claimant, String failureType) {
        update("""
                update outbox_messages
                set claimed_by = null, claimed_at = null, last_error = ?
                where message_id = ? and claimed_by = ? and published_at is null
                """, failureType, messageId, claimant);
    }

    public Stats stats() {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        select count(*) as backlog_size,
                               coalesce(extract(epoch from (current_timestamp - min(created_at))), 0)
                                   as oldest_age_seconds
                        from outbox_messages where published_at is null
                        """);
                var result = statement.executeQuery()) {
            result.next();
            return new Stats(result.getLong("backlog_size"), result.getLong("oldest_age_seconds"));
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not read outbox statistics", exception);
        }
    }

    private int update(String sql, String value, UUID messageId, String claimant) {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (value != null) {
                statement.setString(index++, value);
            }
            statement.setObject(index++, messageId);
            statement.setString(index, claimant);
            return statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not update outbox message", exception);
        }
    }

    public record Stats(long backlogSize, long oldestAgeSeconds) {
    }
}
