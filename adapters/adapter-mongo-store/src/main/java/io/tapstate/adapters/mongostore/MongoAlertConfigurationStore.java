package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmTypeCatalog;
import io.tapstate.core.alerting.AlertConfiguration;
import io.tapstate.core.alerting.AlertConfiguration.Definition;
import io.tapstate.core.alerting.AlertConfiguration.Kind;
import io.tapstate.core.alerting.AlertReceiver;
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
    private final SourceConfigCipherProvider keys;

    MongoAlertConfigurationStore(MongoDatabase configuration, AlarmKey.Scope scope,
            AlarmTypeCatalog catalog, Runnable requireActive) {
        this(configuration, scope, catalog, requireActive, null);
    }
    MongoAlertConfigurationStore(MongoDatabase configuration, AlarmKey.Scope scope,
            AlarmTypeCatalog catalog, Runnable requireActive, SourceConfigCipherProvider keys) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.keys = keys;
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
        if (candidate instanceof AlertReceiver) { throw new IllegalArgumentException("receiver changes require prepared private material"); }
        candidate = normalized(candidate);
        if (expected != null) { expected = normalized(expected); }
        var header = candidate.header(); var identity = header.identity();
        requireScope(identity.scope());
        if (expected == null) {
            if (header.revision() != 1 || !header.active()) { throw new IllegalArgumentException("invalid initial definition"); }
            if (candidate instanceof AlertConfiguration.Subscription route && route.bindingRevision() != 1) {
                throw new IllegalArgumentException("a new subscription requires its first semantic binding");
            }
        } else {
            requireScope(expected.header().identity().scope());
            if (!header.succeeds(expected.header())) { throw new IllegalArgumentException("definition does not succeed its original revision"); }
            if (candidate instanceof AlertConfiguration.Subscription route
                    && (!(expected instanceof AlertConfiguration.Subscription original) || !route.succeeds(original))) {
                throw new IllegalArgumentException("subscription does not succeed its original semantic binding");
            }
            if (!header.active() && !retirementBody(expected).equals(retirementBody(candidate))) {
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
        if (expected instanceof AlertReceiver) { throw new IllegalArgumentException("receiver use requires its captured private material"); }
        if (expected instanceof AlertConfiguration.Subscription route && route.bindingRevision() == 0) { return false; }
        return fenceRecord(session, expected);
    }

    /** A name or message-only edit keeps eligibility; changing away and back cannot revive an old binding. */
    boolean fenceSubscriptionBinding(ClientSession session, AlertConfiguration.Identity identity, long bindingRevision) {
        if (identity.kind() != Kind.SUBSCRIPTION) { throw new IllegalArgumentException("subscription identity required"); }
        var current = load(session, Kind.SUBSCRIPTION, identity.id());
        if (current.isEmpty()) { return false; }
        var route = (AlertConfiguration.Subscription) current.orElseThrow().definition();
        return route.hasBinding(identity, bindingRevision) && fenceRecord(session, route);
    }
    private boolean fenceRecord(ClientSession session, Definition expected) {
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
                new Document("$set", new Document("writeFence", next))).getModifiedCount() == 1;
        requireActive.run(); return changed;
    }

    private MongoCollection<Document> collection(Kind kind) {
        return switch (kind) {
            case RULE -> SystemCollections.ALERT_RULES.on(configuration);
            case SUBSCRIPTION -> SystemCollections.ALERT_SUBSCRIPTIONS.on(configuration);
            case SILENCE -> SystemCollections.ALERT_SILENCES.on(configuration);
            case RECEIVER -> SystemCollections.ALERT_RECEIVERS.on(configuration);
        };
    }

    record ReceiverCapture(AlertReceiver receiver, Document privateSlot, boolean forUse) {
        ReceiverCapture { privateSlot = privateSlot == null ? null : new Document(privateSlot); }
        @Override public String toString() { return "ReceiverCapture[redacted]"; }
    }

    /** Invoke before entering a retried transaction; the generated ciphertext is reused on retries. */
    MongoAlertReceiverSecrets.Prepared prepareReceiver(AlertReceiver receiver, io.tapstate.spi.alerting.ReceiverSecretMaterial material) {
        requireScope(receiver.header().identity().scope()); requireActive.run(); requireKeys();
        var cipher = keys.refresh(new MongoReadBudget(java.time.Duration.ofSeconds(5)));
        return MongoAlertReceiverSecrets.prepare(receiver, material, cipher);
    }

    ReceiverCapture captureReceiver(ClientSession session, AlertConfiguration.Identity identity) {
        return captureReceiver(session, identity, true);
    }
    ReceiverCapture captureReceiverForManagement(ClientSession session, AlertConfiguration.Identity identity) {
        return captureReceiver(session, identity, false);
    }
    private ReceiverCapture captureReceiver(ClientSession session, AlertConfiguration.Identity identity, boolean forUse) {
        MongoAlertStateStore.requireTransaction(session); requireScope(identity.scope()); requireActive.run();
        if (identity.kind() != Kind.RECEIVER) { throw new IllegalArgumentException("receiver identity required"); }
        Document row = receiverRow(session, identity.id());
        if (row == null) { throw unreadable("missing-receiver"); }
        AlertReceiver actual = (AlertReceiver) MongoAlertConfigurationCodec.read(Kind.RECEIVER, scope, identity.id(), row).definition();
        if (!actual.header().identity().equals(identity) || forUse && (!actual.header().active() || !actual.enabled())) { throw unreadable("stale-receiver"); }
        requireActive.run(); return new ReceiverCapture(actual, row.get("privateSlot", Document.class), forUse);
    }

    /** Pure material decoding is outside the permission transaction; it does not authorize IO. */
    io.tapstate.spi.alerting.ReceiverSecretMaterial resolveReceiver(ReceiverCapture captured) {
        if (!captured.forUse()) { throw new IllegalArgumentException("management captures do not authorize material resolution"); }
        requireScope(captured.receiver().header().identity().scope()); requireActive.run(); requireKeys();
        var cipher = keys.refresh(new MongoReadBudget(java.time.Duration.ofSeconds(5)));
        return MongoAlertReceiverSecrets.resolve(captured.receiver(), captured.privateSlot(), cipher);
    }

    MongoAlertReceiverSecrets.Prepared retainReceiverMaterial(AlertReceiver candidate, ReceiverCapture original) {
        requireScope(candidate.header().identity().scope());
        if (!candidate.header().identity().equals(original.receiver().header().identity())
                || !Objects.equals(candidate.credentials(), original.receiver().credentials())
                || !candidate.transport().equals(original.receiver().transport())) { throw unreadable("material-binding-changed"); }
        requireKeys();
        String keyId = original.privateSlot() == null ? keys.current().activeKeyId() : original.privateSlot().getString("keyId");
        return new MongoAlertReceiverSecrets.Prepared(candidate, original.privateSlot() == null ? null : original.privateSlot().getString("ciphertext"), keyId);
    }

    Stage stageReceiver(ClientSession session, AlertReceiver expected, MongoAlertReceiverSecrets.Prepared prepared) {
        MongoAlertStateStore.requireTransaction(session); requireActive.run(); requireKeys();
        AlertReceiver candidate = prepared.receiver(); var identity = candidate.header().identity(); requireScope(identity.scope());
        if (expected == null ? candidate.header().revision() != 1 : !candidate.succeeds(expected)) {
            throw new IllegalArgumentException("receiver does not succeed its original revision");
        }
        Document original = receiverRow(session, identity.id());
        var actual = original == null ? null : MongoAlertConfigurationCodec.read(Kind.RECEIVER, scope, identity.id(), original);
        Document slot = MongoAlertReceiverSecrets.slot(prepared);
        MongoAlertReceiverSecrets.validateSlot(candidate, slot);
        if (!keys.fenceWrite(new SourceConfigWriteScope(session), prepared.keyId())) { throw unreadable("configuration-key"); }
        if (actual != null && actual.definition().equals(candidate) && Objects.equals(original.get("privateSlot"), slot)) { return Stage.UNCHANGED; }
        if (expected == null ? actual != null : actual == null || !actual.definition().equals(expected)) { return Stage.CONFLICT; }
        if (!candidate.header().active() && !Objects.equals(original.get("privateSlot"), slot)) { throw unreadable("retirement-material-changed"); }
        long nextFence = actual == null ? 0 : increment(actual.writeFence());
        Document row = MongoAlertConfigurationCodec.write(candidate, nextFence).append("privateSlot", slot);
        MongoAlertConfigurationCodec.read(Kind.RECEIVER, scope, identity.id(), row);
        if (expected == null) { collection(Kind.RECEIVER).insertOne(session, row); }
        else {
            Document filter = new Document("_id", MongoAlertConfigurationCodec.key(scope, identity.id()))
                    .append("incarnation", identity.incarnation()).append("recordRevision", expected.header().revision()).append("writeFence", actual.writeFence());
            if (collection(Kind.RECEIVER).replaceOne(session, filter, row).getMatchedCount() != 1) { return Stage.CONFLICT; }
        }
        requireActive.run(); return Stage.STAGED;
    }

    /** Rechecks the exact material resolved before the transaction, then writes both original fences. */
    boolean fenceReceiverUse(ClientSession session, ReceiverCapture captured) {
        MongoAlertStateStore.requireTransaction(session); requireKeys();
        if (!captured.forUse()) { return false; }
        AlertReceiver before = captured.receiver(); var identity = before.header().identity(); requireScope(identity.scope());
        Document row = receiverRow(session, identity.id());
        if (row == null) { return false; }
        var actual = (AlertReceiver) MongoAlertConfigurationCodec.read(Kind.RECEIVER, scope, identity.id(), row).definition();
        if (!actual.header().active() || !actual.enabled() || !actual.header().identity().equals(identity)
                || actual.bindingRevision() != before.bindingRevision() || !Objects.equals(actual.credentials(), before.credentials())
                || !actual.transport().equals(before.transport())
                || !Objects.equals(row.get("privateSlot"), captured.privateSlot())) { return false; }
        String keyId = captured.privateSlot() == null ? keys.current().activeKeyId() : captured.privateSlot().getString("keyId");
        if (!keys.fenceWrite(new SourceConfigWriteScope(session), keyId)) { throw unreadable("configuration-key"); }
        return fenceRecord(session, actual);
    }

    private Document receiverRow(ClientSession session, String id) {
        requireActive.run(); return collection(Kind.RECEIVER).find(session, new Document("_id", MongoAlertConfigurationCodec.key(scope, id))).first();
    }
    private void requireKeys() {
        if (keys == null || !keys.requiresWriteFence()) {
            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
        }
    }
    private Definition normalized(Definition value) {
        return value instanceof AlertConfiguration.Rule rule ? rule.validated(catalog) : value;
    }
    private static Document retirementBody(Definition value) {
        var body = new Document((Document) MongoAlertConfigurationCodec.write(value, 0).get("body"));
        if (value instanceof AlertConfiguration.Subscription) { body.remove("bindingRevision"); }
        return body;
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
