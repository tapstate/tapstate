package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RetrySettingsStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.Document;

final class MongoRetrySettingsStore implements RetrySettingsStore {
    private static final String ID = "cluster";
    private static final int FORMAT = 1;
    private static final Set<String> FIELDS = Set.of("_id", "clusterId", "formatVersion", "revision", "defaults", "operations");
    private static final Set<String> POLICY_FIELDS = Set.of("enabled", "intervalMillis", "maxDurationMillis");

    private final MongoCollection<Document> rows;
    private final String clusterId;
    private final Runnable requireOpen;

    MongoRetrySettingsStore(MongoDatabase database, String clusterId, Runnable requireOpen) {
        rows = SystemCollections.CONFIG_RETRY_SETTINGS.on(database).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.clusterId = clusterId;
        this.requireOpen = requireOpen;
    }

    @Override
    public Settings read() {
        requireOpen.run();
        var result = StoreIo.call(() -> rows.find(new Document("_id", ID)).first());
        requireOpen.run();
        return result == null ? Settings.unconfigured() : decode(result);
    }

    @Override
    public Optional<Settings> replace(long expectedRevision, RetryPolicy defaults, Map<String, OperationOverride> operations) {
        var replacement = new Settings(Math.addExact(expectedRevision, 1), defaults, operations);
        var current = read();
        if (current.revision() != expectedRevision) return Optional.empty();
        var expect = new Document("_id", ID).append("clusterId", clusterId)
                .append("formatVersion", FORMAT).append("revision", expectedRevision);
        var next = encode(replacement);
        boolean replaced = StoreIo.call(() -> {
            requireOpen.run();
            try {
                var result = rows.replaceOne(expect, next, new ReplaceOptions().upsert(expectedRevision == 0));
                return result.getMatchedCount() == 1 || result.getUpsertedId() != null;
            } catch (MongoWriteException conflict) {
                if (expectedRevision == 0 && conflict.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) return false;
                throw conflict;
            }
        });
        requireOpen.run();
        return replaced ? Optional.of(replacement) : Optional.empty();
    }

    private Document encode(Settings settings) {
        var defaults = settings.defaults();
        var overrides = new Document();
        settings.operations().forEach((operation, value) -> overrides.append(operation, policy(
                value.enabled(), value.interval(), value.maxDuration())));
        return new Document("_id", ID).append("clusterId", clusterId).append("formatVersion", FORMAT)
                .append("revision", settings.revision())
                .append("defaults", policy(defaults.enabled(), defaults.interval(), defaults.maxDuration()))
                .append("operations", overrides);
    }

    private Settings decode(Document row) {
        try {
            if (!row.keySet().equals(FIELDS) || !ID.equals(row.getString("_id")) || !clusterId.equals(row.getString("clusterId"))
                    || !Integer.valueOf(FORMAT).equals(row.get("formatVersion")) || row.getLong("revision") <= 0) throw unreadable();
            var defaults = row.get("defaults", Document.class);
            if (!defaults.keySet().equals(POLICY_FIELDS)) throw unreadable();
            var policy = new RetryPolicy(defaults.getBoolean("enabled"), duration(defaults, "intervalMillis"),
                    duration(defaults, "maxDurationMillis"));
            var overrides = new LinkedHashMap<String, OperationOverride>();
            row.get("operations", Document.class).forEach((operation, raw) -> {
                var fields = (Document) raw;
                if (!POLICY_FIELDS.containsAll(fields.keySet())) throw unreadable();
                overrides.put(operation, new OperationOverride(fields.getBoolean("enabled"),
                        duration(fields, "intervalMillis"), duration(fields, "maxDurationMillis")));
            });
            return new Settings(row.getLong("revision"), policy, overrides);
        } catch (ClassCastException | NullPointerException | IllegalArgumentException | ArithmeticException invalid) {
            throw unreadable();
        }
    }

    private static Document policy(Boolean enabled, Duration interval, Duration maxDuration) {
        var fields = new Document();
        if (enabled != null) fields.append("enabled", enabled);
        if (interval != null) fields.append("intervalMillis", interval.toMillis());
        if (maxDuration != null) fields.append("maxDurationMillis", maxDuration.toMillis());
        return fields;
    }

    private static Duration duration(Document value, String field) {
        return value.containsKey(field) ? Duration.ofMillis(value.getLong(field)) : null;
    }

    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "retry-settings", "field", "policy"), null);
    }
}
