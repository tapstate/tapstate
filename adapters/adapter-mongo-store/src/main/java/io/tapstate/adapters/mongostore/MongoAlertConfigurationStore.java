package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmTypeCatalog;
import io.tapstate.core.alerting.AlertConfiguration;
import io.tapstate.core.alerting.AlertConfiguration.Definition;
import io.tapstate.core.alerting.AlertConfiguration.Kind;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.bson.Document;

/**
 * Transaction-bound configuration fragments. The CONFIG coordinator owns current actor authorization,
 * receiver-reference validation and the configuration-version write in the same session. This fragment
 * has no standalone production management or delivery-permission entry point.
 */
final class MongoAlertConfigurationStore {
    static final List<SystemCollections> COLLECTIONS = List.of(SystemCollections.ALERT_RULES,
            SystemCollections.ALERT_RECEIVERS, SystemCollections.ALERT_SUBSCRIPTIONS, SystemCollections.ALERT_SILENCES);

    enum Stage { STAGED, UNCHANGED, CONFLICT }
    private final MongoDatabase configuration;
    private final AlarmKey.Scope scope;
    private final AlarmTypeCatalog catalog;
    private final Runnable requireActive;

    MongoAlertConfigurationStore(MongoDatabase configuration, AlarmKey.Scope scope,
            AlarmTypeCatalog catalog, Runnable requireActive) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
    }

    static void prepare(ClientSession session, MongoDatabase database) {
        MongoAlertStateStore.requireTransaction(session);
        for (SystemCollections collection : COLLECTIONS) {
            collection.prepareCollectionOn(database, session);
            for (SystemCollections.IndexSpec index : collection.indexes()) {
                IndexEnsure.createInTransaction(session, collection.on(database), index);
            }
        }
    }

    Optional<MongoAlertConfigurationCodec.Stored> load(ClientSession session, Kind kind, String id) {
        MongoAlertStateStore.requireTransaction(session); requireActive.run();
        Document row = collection(kind).find(session, new Document("_id", MongoAlertConfigurationCodec.key(scope, id))).first();
        if (row == null) { requireActive.run(); return Optional.empty(); }
        var result = MongoAlertConfigurationCodec.read(kind, scope, id, row);
        requireActive.run(); return Optional.of(result);
    }

    /** expected null is a create with an identity already fixed outside transaction retries. */
    Stage stage(ClientSession session, Definition expected, Definition candidate) {
        MongoAlertStateStore.requireTransaction(session); requireActive.run();
        Objects.requireNonNull(candidate, "candidate");
        candidate = normalized(candidate);
        if (expected != null) { expected = normalized(expected); }
        var header = candidate.header(); var identity = header.identity();
        requireScope(identity.scope());
        if (expected == null) {
            if (header.revision() != 1 || !header.active()) { throw new IllegalArgumentException("invalid initial definition"); }
        } else {
            requireScope(expected.header().identity().scope());
            if (!header.succeeds(expected.header())) { throw new IllegalArgumentException("definition does not succeed its original revision"); }
            if (!header.active() && !MongoAlertConfigurationCodec.write(expected, 0).get("body")
                    .equals(MongoAlertConfigurationCodec.write(candidate, 0).get("body"))) {
                throw new IllegalArgumentException("retirement cannot replace the definition body");
            }
        }
        var actual = load(session, identity.kind(), identity.id());
        if (actual.isPresent() && actual.orElseThrow().definition().equals(candidate)) { return Stage.UNCHANGED; }
        if (expected == null ? actual.isPresent() : actual.isEmpty()
                || !actual.orElseThrow().definition().equals(expected)) { return Stage.CONFLICT; }
        long fence = actual.isEmpty() ? 0 : increment(actual.orElseThrow().writeFence());
        Document row = MongoAlertConfigurationCodec.write(candidate, fence);
        MongoAlertConfigurationCodec.read(identity.kind(), scope, identity.id(), row);
        if (expected == null) {
            collection(identity.kind()).insertOne(session, row);
        } else {
            Document filter = new Document("_id", MongoAlertConfigurationCodec.key(scope, identity.id()))
                    .append("incarnation", identity.incarnation()).append("recordRevision", expected.header().revision())
                    .append("writeFence", actual.orElseThrow().writeFence());
            if (collection(identity.kind()).replaceOne(session, filter, row).getMatchedCount() != 1) { return Stage.CONFLICT; }
        }
        requireActive.run(); return Stage.STAGED;
    }

    /** A real protected-row write orders a current binding check against edit or retirement. */
    boolean fence(ClientSession session, Definition expected) {
        Objects.requireNonNull(expected, "expected");
        expected = normalized(expected);
        var identity = expected.header().identity(); requireScope(identity.scope());
        if (!expected.header().active()) { return false; }
        var actual = load(session, identity.kind(), identity.id());
        if (actual.isEmpty() || !actual.orElseThrow().definition().equals(expected)) { return false; }
        long original = actual.orElseThrow().writeFence(); long next = increment(original);
        Document filter = new Document("_id", MongoAlertConfigurationCodec.key(scope, identity.id()))
                .append("incarnation", identity.incarnation()).append("recordRevision", expected.header().revision())
                .append("writeFence", original);
        boolean changed = collection(identity.kind()).updateOne(session, filter,
                new Document("$set", new Document("writeFence", next))).getMatchedCount() == 1;
        requireActive.run(); return changed;
    }

    private MongoCollection<Document> collection(Kind kind) {
        return switch (kind) {
            case RULE -> SystemCollections.ALERT_RULES.on(configuration);
            case SUBSCRIPTION -> SystemCollections.ALERT_SUBSCRIPTIONS.on(configuration);
            case SILENCE -> SystemCollections.ALERT_SILENCES.on(configuration);
            case RECEIVER -> throw new IllegalArgumentException("receiver persistence is not installed");
        };
    }
    private Definition normalized(Definition value) {
        return value instanceof AlertConfiguration.Rule rule ? rule.validated(catalog) : value;
    }
    private void requireScope(AlarmKey.Scope supplied) {
        if (!scope.equals(supplied)) { throw unreadable("scope"); }
    }
    private static long increment(long value) {
        if (value == Long.MAX_VALUE) { throw unreadable("fence-exhausted"); }
        return value + 1;
    }
    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-configuration", "field", field), null);
    }
}
