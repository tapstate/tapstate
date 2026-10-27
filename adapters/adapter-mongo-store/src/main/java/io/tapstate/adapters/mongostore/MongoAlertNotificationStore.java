package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertTargetLimit;
import io.tapstate.core.alerting.AlertTestQuota;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.bson.Document;

/** Same-session notification fragments; the CONFIG coordinator owns authorization and the complete atomic mutation. */
final class MongoAlertNotificationStore {
    static final List<SystemCollections> COLLECTIONS = List.of(SystemCollections.ALERT_NOTIFICATION_SCHEDULES,
            SystemCollections.ALERT_DELIVERY_INTENTS, SystemCollections.ALERT_TARGET_LIMITS, SystemCollections.ALERT_TEST_QUOTAS);

    enum Stage { STAGED, UNCHANGED, CONFLICT }
    private final MongoDatabase configuration;
    private final AlarmKey.Scope scope;
    private final AlertChannelPolicy.Deployment deployment;
    private final Runnable requireActive;

    MongoAlertNotificationStore(MongoDatabase configuration, AlarmKey.Scope scope,
            AlertChannelPolicy.Deployment deployment, Runnable requireActive) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.scope = Objects.requireNonNull(scope, "scope"); this.deployment = Objects.requireNonNull(deployment, "deployment");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (deployment == AlertChannelPolicy.Deployment.UNRESOLVED) { throw new IllegalArgumentException("resolved deployment required"); }
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

    Optional<MongoAlertNotificationCodec.Quota> quota(ClientSession session, Instant minute) {
        context(session, scope);
        Document row = SystemCollections.ALERT_TEST_QUOTAS.on(configuration)
                .find(session, new Document("_id", MongoAlertNotificationCodec.quotaKey(scope, minute))).first();
        var result = row == null ? Optional.<MongoAlertNotificationCodec.Quota>empty()
                : Optional.of(MongoAlertNotificationCodec.readQuota(scope, minute, row));
        requireActive.run(); return result;
    }

    /** A quota replay is established by the original intent key, never by equal bucket counts. */
    Stage stageQuota(ClientSession session, MongoAlertNotificationCodec.Quota expected, AlertTestQuota.Bucket candidate) {
        context(session, candidate.scope()); requireOnPrem();
        if (expected == null ? candidate.scopeAccepted() != 1 : !candidate.succeeds(expected.value())) {
            throw new IllegalArgumentException("quota must consume exactly one newly admitted test");
        }
        var actual = quota(session, candidate.minute());
        if (expected == null ? actual.isPresent() : actual.isEmpty() || !actual.orElseThrow().equals(expected)) { return Stage.CONFLICT; }
        long revision = expected == null ? 1 : next(expected.recordRevision());
        Document row = MongoAlertNotificationCodec.quota(candidate, revision);
        MongoAlertNotificationCodec.readQuota(scope, candidate.minute(), row);
        var collection = SystemCollections.ALERT_TEST_QUOTAS.on(configuration);
        if (expected == null) { collection.insertOne(session, row); }
        else if (collection.replaceOne(session, new Document("_id", row.get("_id"))
                .append("recordRevision", expected.recordRevision()), row).getModifiedCount() != 1) { return Stage.CONFLICT; }
        requireActive.run(); return Stage.STAGED;
    }

    Optional<MongoAlertNotificationCodec.Limit> limit(ClientSession session, AlertTargetLimit.Domain domain) {
        context(session, domain.scope());
        Document row = SystemCollections.ALERT_TARGET_LIMITS.on(configuration)
                .find(session, new Document("_id", MongoAlertNotificationCodec.limitKey(domain))).first();
        var result = row == null ? Optional.<MongoAlertNotificationCodec.Limit>empty()
                : Optional.of(MongoAlertNotificationCodec.readLimit(domain, row));
        requireActive.run(); return result;
    }

    MongoAlertNotificationCodec.Limit requiredLimit(ClientSession session, AlertTargetLimit.Domain domain, long minimumRevision) {
        if (minimumRevision < 1) { throw new IllegalArgumentException("an original restriction reference is required"); }
        var current = limit(session, domain);
        if (current.isEmpty() || current.orElseThrow().recordRevision() < minimumRevision) { throw unreadable("missing-target-limit"); }
        return current.orElseThrow();
    }

    Stage stageLimit(ClientSession session, MongoAlertNotificationCodec.Limit expected, AlertTargetLimit candidate) {
        context(session, candidate.domain().scope()); requireOnPrem();
        if (expected != null && (!expected.value().domain().equals(candidate.domain())
                || candidate.notBefore().isBefore(expected.value().notBefore()))) {
            throw new IllegalArgumentException("target restriction cannot shrink or change domain");
        }
        var actual = limit(session, candidate.domain());
        if (expected == null ? actual.isPresent() : actual.isEmpty() || !actual.orElseThrow().equals(expected)) { return Stage.CONFLICT; }
        if (expected != null && expected.value().equals(candidate)) { return Stage.UNCHANGED; }
        long revision = expected == null ? 1 : next(expected.recordRevision());
        Document row = MongoAlertNotificationCodec.limit(candidate, revision);
        MongoAlertNotificationCodec.readLimit(candidate.domain(), row);
        var collection = SystemCollections.ALERT_TARGET_LIMITS.on(configuration);
        if (expected == null) { collection.insertOne(session, row); }
        else if (collection.replaceOne(session, new Document("_id", row.get("_id"))
                .append("recordRevision", expected.recordRevision()), row).getModifiedCount() != 1) { return Stage.CONFLICT; }
        requireActive.run(); return Stage.STAGED;
    }

    private void context(ClientSession session, AlarmKey.Scope supplied) {
        MongoAlertStateStore.requireTransaction(session); requireActive.run();
        if (!scope.equals(supplied)) { throw unreadable("scope"); }
    }
    private void requireOnPrem() {
        if (deployment != AlertChannelPolicy.Deployment.ON_PREM) {
            throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                    Map.of("field", "deployment", "reason", "external-control-not-permitted"), null);
        }
    }
    private static long next(long revision) {
        if (revision == Long.MAX_VALUE) { throw unreadable("revision-exhausted"); }
        return revision + 1;
    }
    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-notification", "field", field), null);
    }
}
