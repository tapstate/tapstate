package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertTargetLimit;
import io.tapstate.core.alerting.AlertTestQuota;
import io.tapstate.core.alerting.AlertIntent;
import io.tapstate.core.alerting.AlertTestCommand;
import io.tapstate.core.alerting.DeliveryEvidence;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Instant;
import java.time.Clock;
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
    private final Clock clock;

    MongoAlertNotificationStore(MongoDatabase configuration, AlarmKey.Scope scope,
            AlertChannelPolicy.Deployment deployment, Runnable requireActive) {
        this(configuration, scope, deployment, requireActive, Clock.systemUTC());
    }
    MongoAlertNotificationStore(MongoDatabase configuration, AlarmKey.Scope scope,
            AlertChannelPolicy.Deployment deployment, Runnable requireActive, Clock clock) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.scope = Objects.requireNonNull(scope, "scope"); this.deployment = Objects.requireNonNull(deployment, "deployment");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.clock = Objects.requireNonNull(clock, "clock");
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

    /** Controlled feature preparation only; no data mutation or grant is implied by an index build. */
    static void prepareQueryIndex(MongoDatabase database, SystemCollections collection, SystemCollections.IndexSpec index) {
        if (!COLLECTIONS.contains(collection) || !index.indexName().startsWith("alert_query_")) { throw new IllegalArgumentException("undeclared notification query index"); }
        Document keys = new Document(); index.keys().forEach(key -> keys.append(key, 1));
        collection.on(database).createIndex(keys, new com.mongodb.client.model.IndexOptions().name(index.indexName()).unique(false));
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

    Optional<MongoAlertDeliveryCodec.Stored> intent(ClientSession session, AlertIntent.Identity identity) {
        context(session, identity.scope());
        Document row = SystemCollections.ALERT_DELIVERY_INTENTS.on(configuration)
                .find(session, new Document("_id", MongoAlertDeliveryCodec.key(identity))).first();
        var found = row == null ? Optional.<MongoAlertDeliveryCodec.Stored>empty() : Optional.of(MongoAlertDeliveryCodec.read(identity, row));
        requireActive.run(); return found;
    }

    Optional<MongoAlertDeliveryCodec.Stored> testIntent(ClientSession session, AlertTestCommand.Key key) {
        context(session, key.scope());
        Document id = new Document("authorityId", scope.authorityId()).append("clusterId", scope.clusterId())
                .append("purpose", "channel-test").append("deliveryId", key.testId());
        Document row = SystemCollections.ALERT_DELIVERY_INTENTS.on(configuration).find(session, new Document("_id", id)).first();
        var found = row == null ? Optional.<MongoAlertDeliveryCodec.Stored>empty() : Optional.of(MongoAlertDeliveryCodec.readTest(key, row));
        requireActive.run(); return found;
    }

    MongoAlertDeliveryCodec.Stored requiredIntent(ClientSession session, AlertIntent.Identity identity, long minimumRevision) {
        if (minimumRevision < 1) { throw new IllegalArgumentException("original intent reference required"); }
        var found = intent(session, identity);
        if (found.isEmpty() || found.orElseThrow().recordRevision() < minimumRevision) { throw unreadable("missing-intent"); }
        return found.orElseThrow();
    }

    /** This stages facts only. It cannot by itself authorize a network call or establish a test-command replay. */
    Stage stageIntent(ClientSession session, MongoAlertDeliveryCodec.Stored expected, AlertIntent.Record candidate) {
        context(session, candidate.frozen().identity().scope());
        if (candidate.frozen().retention().deployment() != deployment) { throw unreadable("deployment"); }
        var actual = intent(session, candidate.frozen().identity());
        if (actual.isPresent() && actual.orElseThrow().value().equals(candidate)) { return Stage.UNCHANGED; }
        if (expected == null ? actual.isPresent() : actual.isEmpty() || !actual.orElseThrow().equals(expected)) { return Stage.CONFLICT; }
        Instant now = clock.instant();
        timely(candidate.state(), now);
        if (expected == null) {
            if (candidate.state().owner().generation() != 0 || candidate.state().manualCycles() != 0 || candidate.state().attemptsConsumed() != 0
                    || candidate.state().payloadPrunedAt() != null || candidate.state().candidatePrunedAt() != null || candidate.state().detailsPrunedAt() != null) {
                throw new IllegalArgumentException("a new intent cannot carry prior claims or consumed attempts");
            }
            if (candidate.frozen().createdAt().isAfter(now) || !candidate.state().permitsStopped() && !now.isBefore(candidate.frozen().deadline())) {
                throw stale("intent-admission");
            }
        } else { successor(expected.value(), candidate, now); }
        long revision = expected == null ? 1 : next(expected.recordRevision());
        Document row = MongoAlertDeliveryCodec.write(candidate, revision);
        MongoAlertDeliveryCodec.read(candidate.frozen().identity(), row);
        var collection = SystemCollections.ALERT_DELIVERY_INTENTS.on(configuration);
        if (expected == null) { collection.insertOne(session, row); }
        else if (collection.replaceOne(session, new Document("_id", row.get("_id"))
                .append("recordRevision", expected.recordRevision()), row).getModifiedCount() != 1) { return Stage.CONFLICT; }
        requireActive.run(); return Stage.STAGED;
    }

    private static void successor(AlertIntent.Record before, AlertIntent.Record after, Instant now) {
        if (!before.frozen().equals(after.frozen())) { throw new IllegalArgumentException("intent identity and snapshot are immutable"); }
        var a = before.state(); var b = after.state(); var x = a.retained(); var y = b.retained();
        Instant restriction = responseRestriction(after);
        if (b.nextAttemptAt() != null && restriction != null && b.nextAttemptAt().isBefore(restriction)) {
            throw new IllegalArgumentException("response restriction applies across every automatic and manual cycle");
        }
        if (b.manualCycles() == a.manualCycles()) {
            if (!b.cycleStarts().equals(a.cycleStarts())) { throw new IllegalArgumentException("original cycle starts are immutable"); }
            if (a.nextAttemptAt() != null && b.nextAttemptAt() == null && b.attemptsConsumed() == a.attemptsConsumed() && !b.permitsStopped()) {
                throw new IllegalArgumentException("an unconsumed retry lower bound cannot be discarded");
            }
            if (a.nextAttemptAt() != null && b.nextAttemptAt() != null && b.nextAttemptAt().isBefore(a.nextAttemptAt())) {
                throw new IllegalArgumentException("persisted retry eligibility cannot move earlier");
            }
        } else {
            var expectedStarts = new java.util.ArrayList<>(a.cycleStarts()); expectedStarts.add(a.attemptsConsumed());
            if (b.manualCycles() != a.manualCycles() + 1 || b.attemptsConsumed() != a.attemptsConsumed()
                    || !b.cycleStarts().equals(expectedStarts) || a.permitsStopped() || a.nextAttemptAt() != null
                    || a.currentCycleConsumed() == 0 || a.attempts().stream().anyMatch(attempt -> attempt.endedAt() == null)
                    || !manualReady(before) || b.nextAttemptAt() == null || !now.isBefore(before.frozen().deadline())) {
                throw new IllegalArgumentException("manual cycle must open explicitly from the original settled progress");
            }
        }
        if (b.nextAttemptAt() != null && b.currentCycleConsumed() > 0) {
            var last = b.attempts().getLast();
            if (last.protocol() != null && last.protocol().action() == AlertIntent.Action.STOP) {
                throw new IllegalArgumentException("permanent automatic rejection requires a new authorized manual cycle");
            }
            var minimum = before.frozen().retry().nextAttemptAt(b.currentCycleConsumed(), last.endedAt(), 0, before.frozen().deadline(),
                    last.protocol() == null ? null : last.protocol().retryAfter(), null);
            if (minimum.isEmpty() || b.nextAttemptAt().isBefore(minimum.orElseThrow())) {
                throw new IllegalArgumentException("retry schedule cannot precede the frozen policy and response restriction");
            }
        }
        if (x.acceptedAt() == null && y.acceptedAt() != null && (y.acceptedAt().isAfter(now) || !now.isBefore(x.resultUntil()))) { throw stale("delivery-progress"); }
        if (b.stopReason() == AlertIntent.StopReason.EXPIRED
                && (!b.permitsStoppedAt().equals(before.frozen().deadline()) || now.isBefore(before.frozen().deadline()))) { throw stale("delivery-expiry"); }
        if (b.attemptsConsumed() < a.attemptsConsumed() || b.attemptsConsumed() > a.attemptsConsumed() + 1
                || b.manualCycles() < a.manualCycles() || b.manualCycles() > a.manualCycles() + 1
                || a.permitsStopped() && (!b.permitsStopped() || a.stopReason() != b.stopReason())
                || a.permitsStoppedAt() != null && !a.permitsStoppedAt().equals(b.permitsStoppedAt())
                || a.lastAttemptEnd() != null && (b.lastAttemptEnd() == null || b.lastAttemptEnd().isBefore(a.lastAttemptEnd()))
                || x.acceptedAt() != null && !x.acceptedAt().equals(y.acceptedAt())
                || x.stoppedAt() != null && !x.stoppedAt().equals(y.stoppedAt())
                || y.payloadUntil().isAfter(x.payloadUntil()) || y.resultUntil().isAfter(x.resultUntil())
                || a.payloadPrunedAt() != null && (!a.payloadPrunedAt().equals(b.payloadPrunedAt()) || b.payload() != null)
                || a.candidatePrunedAt() != null && (!a.candidatePrunedAt().equals(b.candidatePrunedAt()) || b.candidate() != null)
                || a.detailsPrunedAt() != null && (!a.detailsPrunedAt().equals(b.detailsPrunedAt()) || !b.attempts().isEmpty())
                || a.summary() == DeliveryEvidence.Outcome.ACCEPTED && b.summary() != DeliveryEvidence.Outcome.ACCEPTED) {
            throw new IllegalArgumentException("intent progress cannot reset consumed budgets, stops, evidence or retention");
        }
        if (a.acceptedReceipt() != null && !a.acceptedReceipt().equals(b.acceptedReceipt())) {
            throw new IllegalArgumentException("first accepted receipt summary cannot be dropped or replaced");
        }
        var retention = x.value(before.frozen());
        var floor = java.util.stream.Stream.concat(a.attempts().stream(), b.attempts().stream()).map(attempt -> attempt.permit().deadline())
                .max(Instant::compareTo).orElse(before.frozen().createdAt());
        boolean accepted = x.acceptedAt() == null && y.acceptedAt() != null;
        boolean stopped = x.stoppedAt() == null && y.stoppedAt() != null;
        if (accepted && stopped && y.stoppedAt().isBefore(y.acceptedAt())) {
            retention = retention.stop(y.stoppedAt(), floor);
            retention = retention.accept(y.acceptedAt(), floor, y.acceptedAt()).orElseThrow();
        } else {
            if (accepted) { retention = retention.accept(y.acceptedAt(), floor, y.acceptedAt()).orElseThrow(); }
            if (stopped) { retention = retention.stop(y.stoppedAt(), floor); }
        }
        if (!AlertIntent.Retained.capture(retention).equals(y)) {
            throw new IllegalArgumentException("retention can change only through original accepted or stopped facts");
        }
        var oldOwner = a.owner(); var owner = b.owner();
        if (owner.generation() < oldOwner.generation()
                || owner.generation() != oldOwner.generation() && (oldOwner.generation() == Long.MAX_VALUE || owner.generation() != oldOwner.generation() + 1)
                || owner.generation() == oldOwner.generation() && (!Objects.equals(owner.ownerId(), oldOwner.ownerId())
                        || oldOwner.leaseUntil() != null && owner.leaseUntil().isBefore(oldOwner.leaseUntil()))) {
            throw new IllegalArgumentException("intent owner cannot move backwards or change within a generation");
        }
        boolean claimed = owner.generation() != oldOwner.generation();
        boolean renewed = !claimed && owner.leaseUntil() != null && !Objects.equals(owner.leaseUntil(), oldOwner.leaseUntil());
        if (claimed && (oldOwner.activeAt(now) || !owner.activeAt(now))
                || renewed && (!oldOwner.activeAt(now) || !owner.activeAt(now))) { throw stale("delivery-owner"); }
        if (b.detailsPrunedAt() != null) {
            if (b.attemptsConsumed() != a.attemptsConsumed() || b.manualCycles() != a.manualCycles() || b.summary() != a.summary()) {
                throw new IllegalArgumentException("detail pruning must preserve the original control summary");
            }
            if (!Objects.equals(a.acceptedReceipt(), b.acceptedReceipt())) { throw new IllegalArgumentException("detail pruning must preserve original receipt evidence"); }
            if (!a.permitsStopped() || x.stoppedAt() == null || a.attempts().stream().anyMatch(attempt -> attempt.endedAt() == null)
                    || !Objects.equals(a.lastAttemptEnd(), b.lastAttemptEnd())) {
                throw new IllegalArgumentException("detail pruning requires original settled control facts");
            }
            return;
        }
        for (int index = 0; index < a.attempts().size(); index++) {
            var original = a.attempts().get(index); var updated = b.attempts().get(index);
            if (!original.permit().equals(updated.permit())
                    || original.endedAt() != null && (!original.endedAt().equals(updated.endedAt())
                            || DeliveryEvidence.aggregate(List.of(original.outcome(), updated.outcome())) != updated.outcome())
                    || original.acceptedRecordedAt() != null && !original.acceptedRecordedAt().equals(updated.acceptedRecordedAt())
                    || original.receipt() != null && !original.receipt().equals(updated.receipt())
                    || original.protocol() != null && original.protocol().action() == AlertIntent.Action.STOP
                            && (updated.protocol() == null || updated.protocol().action() != AlertIntent.Action.STOP)
                    || original.protocol() != null && original.protocol().retryAfter() != null
                            && (updated.protocol() == null || updated.protocol().retryAfter() == null
                            || updated.protocol().retryAfter().isBefore(original.protocol().retryAfter()))) {
                throw new IllegalArgumentException("original permit and durable evidence cannot be replaced");
            }
        }
        if (b.attemptsConsumed() > a.attemptsConsumed()) {
            var permit = b.attempts().getLast().permit();
            if (a.nextAttemptAt() == null || permit.grantedAt().isBefore(a.nextAttemptAt())) { throw stale("delivery-retry-time"); }
            Instant originalRestriction = responseRestriction(before);
            if (originalRestriction != null && permit.grantedAt().isBefore(originalRestriction)) { throw stale("delivery-response-restriction"); }
            if (a.permitsStopped() || b.owner().generation() != permit.ownerGeneration() || !b.owner().ownerId().equals(permit.ownerId())
                    || !b.owner().activeAt(permit.grantedAt()) || b.summary() == DeliveryEvidence.Outcome.ACCEPTED
                    || b.permitsStopped() && (b.stopReason() != AlertIntent.StopReason.EXHAUSTED
                            || !b.permitsStoppedAt().equals(permit.grantedAt()))) {
                throw new IllegalArgumentException("new permit requires the current owner and open budget");
            }
            if (permit.grantedAt().isAfter(now) || !b.owner().activeAt(now) || !now.isBefore(permit.deadline())
                    || !now.isBefore(before.frozen().deadline())) { throw stale("delivery-permit"); }
        }
    }
    private static Instant responseRestriction(AlertIntent.Record value) {
        return value.state().attempts().stream().map(AlertIntent.Attempt::protocol).filter(Objects::nonNull)
                .map(AlertIntent.Protocol::retryAfter).filter(Objects::nonNull).max(Instant::compareTo).orElse(null);
    }

    private static boolean manualReady(AlertIntent.Record before) {
        var state = before.state(); var policy = before.frozen().retry();
        if (state.currentCycleConsumed() == policy.attemptsPerCycle()) { return true; }
        var last = state.attempts().getLast();
        if (last.protocol() != null && last.protocol().action() != AlertIntent.Action.RETRY) { return true; }
        return policy.nextAttemptAt(state.currentCycleConsumed(), last.endedAt(), 0, before.frozen().deadline(),
                last.protocol() == null ? null : last.protocol().retryAfter(), null).isEmpty();
    }

    private static void timely(AlertIntent.State state, Instant now) {
        if (state.permitsStoppedAt() != null && state.permitsStoppedAt().isAfter(now)
                || state.lastAttemptEnd() != null && state.lastAttemptEnd().isAfter(now)
                || state.payloadPrunedAt() != null && state.payloadPrunedAt().isAfter(now)
                || state.candidatePrunedAt() != null && state.candidatePrunedAt().isAfter(now)
                || state.detailsPrunedAt() != null && state.detailsPrunedAt().isAfter(now)) { throw stale("delivery-progress"); }
    }

    private static TapstateException stale(String action) { return new TapstateException(AlertError.STALE_ACTION, Map.of("action", action), null); }

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
