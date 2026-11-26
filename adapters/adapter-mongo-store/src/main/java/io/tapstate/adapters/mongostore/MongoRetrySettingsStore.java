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
    private static final String ID_FIELD = "_id";
    private static final String CLUSTER_ID = "clusterId";
    private static final String FORMAT_VERSION = "formatVersion";
    private static final String REVISION = "revision";
    private static final String DEFAULTS = "defaults";
    private static final String OPERATIONS = "operations";
    private static final String ENABLED = "enabled";
    private static final String INTERVAL = "intervalMillis";
    private static final String MAX_DURATION = "maxDurationMillis";
    private static final int FORMAT = 1;
    private static final Set<String> FIELDS = Set.of(ID_FIELD, CLUSTER_ID, FORMAT_VERSION, REVISION, DEFAULTS, OPERATIONS);
    private static final Set<String> POLICY_FIELDS = Set.of(ENABLED, INTERVAL, MAX_DURATION);

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
        var result = StoreIo.call(() -> rows.find(new Document(ID_FIELD, ID)).first());
        requireOpen.run();
        return result == null ? Settings.unconfigured() : decode(result);
    }

    @Override
    public Optional<Settings> replace(long expectedRevision, RetryPolicy defaults, Map<String, OperationOverride> operations) {
        var replacement = new Settings(Math.addExact(expectedRevision, 1), defaults, operations);
        var current = read();
        if (current.revision() != expectedRevision) return Optional.empty();
        var expect = new Document(ID_FIELD, ID).append(CLUSTER_ID, clusterId)
                .append(FORMAT_VERSION, FORMAT).append(REVISION, expectedRevision);
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
        return new Document(ID_FIELD, ID).append(CLUSTER_ID, clusterId).append(FORMAT_VERSION, FORMAT)
                .append(REVISION, settings.revision())
                .append(DEFAULTS, policy(defaults.enabled(), defaults.interval(), defaults.maxDuration()))
                .append(OPERATIONS, overrides);
    }

    private Settings decode(Document row) {
        try {
            if (!row.keySet().equals(FIELDS) || !ID.equals(row.getString(ID_FIELD)) || !clusterId.equals(row.getString(CLUSTER_ID))
                    || !Integer.valueOf(FORMAT).equals(row.get(FORMAT_VERSION)) || row.getLong(REVISION) <= 0) throw unreadable();
            var defaults = row.get(DEFAULTS, Document.class);
            if (!defaults.keySet().equals(POLICY_FIELDS)) throw unreadable();
            var policy = new RetryPolicy(defaults.getBoolean(ENABLED), duration(defaults, INTERVAL),
                    duration(defaults, MAX_DURATION));
            var overrides = new LinkedHashMap<String, OperationOverride>();
            row.get(OPERATIONS, Document.class).forEach((operation, raw) -> {
                var fields = (Document) raw;
                if (!POLICY_FIELDS.containsAll(fields.keySet())) throw unreadable();
                overrides.put(operation, new OperationOverride(fields.getBoolean(ENABLED),
                        duration(fields, INTERVAL), duration(fields, MAX_DURATION)));
            });
            return new Settings(row.getLong(REVISION), policy, overrides);
        } catch (ClassCastException | NullPointerException | IllegalArgumentException | ArithmeticException invalid) {
            throw unreadable(invalid);
        }
    }

    private static Document policy(Boolean enabled, Duration interval, Duration maxDuration) {
        var fields = new Document();
        if (enabled != null) fields.append(ENABLED, enabled);
        if (interval != null) fields.append(INTERVAL, wholeMillis(interval));
        if (maxDuration != null) fields.append(MAX_DURATION, wholeMillis(maxDuration));
        return fields;
    }

    private static long wholeMillis(Duration duration) {
        if (duration.getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException("Stored retry durations must use whole milliseconds");
        }
        return duration.toMillis();
    }

    private static Duration duration(Document value, String field) {
        return value.containsKey(field) ? Duration.ofMillis(value.getLong(field)) : null;
    }

    private static TapstateException unreadable() {
        return unreadable(null);
    }

    private static TapstateException unreadable(RuntimeException cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "retry-settings", "field", "policy"), cause);
    }
}
