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
import io.tapstate.core.alerting.AlertSchedule;
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

    Optional<MongoAlertScheduleCodec.Stored> schedule(ClientSession session, AlertSchedule.Key key) {
        context(session, key.scope());
        Document row = SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(configuration)
                .find(session, new Document("_id", MongoAlertScheduleCodec.key(key))).first();
        var found = row == null ? Optional.<MongoAlertScheduleCodec.Stored>empty() : Optional.of(MongoAlertScheduleCodec.read(key, row));
        requireActive.run(); return found;
    }
    MongoAlertScheduleCodec.Stored requiredSchedule(ClientSession session, AlertSchedule.Key key, long minimumRevision) {
        if (minimumRevision < 1) { throw new IllegalArgumentException("original schedule reference required"); }
        var found = schedule(session, key);
        if (found.isEmpty() || found.orElseThrow().recordRevision() < minimumRevision) { throw unreadable("missing-schedule"); }
        return found.orElseThrow();
    }
    Stage stageSchedule(ClientSession session, MongoAlertScheduleCodec.Stored expected, AlertSchedule.Record candidate) {
        context(session, candidate.key().scope());
        if (candidate.retention().deployment() != deployment) { throw unreadable("deployment"); }
        var actual = schedule(session, candidate.key());
        if (actual.isPresent() && actual.orElseThrow().value().equals(candidate)) { return Stage.UNCHANGED; }
        if (expected == null ? actual.isPresent() : actual.isEmpty() || !actual.orElseThrow().equals(expected)) { return Stage.CONFLICT; }
        if (candidate.createdAt().isAfter(clock.instant()) || candidate.retiredAt() != null && candidate.retiredAt().isAfter(clock.instant())) { throw stale("schedule-progress"); }
        if (expected != null) {
            scheduleSuccessor(expected.value(), candidate, clock.instant());
            validateSummaryHandoff(session, expected.value(), candidate);
            if (candidate.clock().round() > expected.value().clock().round() && candidate.protectedIntents().stream().noneMatch(key ->
                    !expected.value().protectedIntents().contains(key) && key.notification().round() == candidate.clock().round())) {
                throw unreadable("planned-round-without-intent");
            }
        } else if (!candidate.summaries().isEmpty()) { throw new IllegalArgumentException("a new schedule cannot fabricate predecessor summaries"); }
        validateScheduleReferences(session, candidate);
        long revision = expected == null ? 1 : next(expected.recordRevision());
        Document row = MongoAlertScheduleCodec.write(candidate, revision); MongoAlertScheduleCodec.read(candidate.key(), row);
        var collection = SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(configuration);
        if (expected == null) { collection.insertOne(session, row); }
        else if (collection.replaceOne(session, new Document("_id", row.get("_id")).append("recordRevision", expected.recordRevision()), row).getModifiedCount() != 1) { return Stage.CONFLICT; }
        requireActive.run(); return Stage.STAGED;
    }
    private void validateScheduleReferences(ClientSession session, AlertSchedule.Record value) {
        for (var key : value.protectedIntents()) { requiredIntent(session, new AlertIntent.Business(key), 1); }
        if (value.batch() != null) {
            var original = requiredIntent(session, new AlertIntent.Business(value.batch().delivery()), 1).value();
            if (!original.frozen().members().equals(value.batch().members())) { throw unreadable("frozen-batch-members"); }
            if (value.disposition() != null) {
                var frozen = new io.tapstate.core.alerting.FrozenGroupPolicy.Batch(value.batch().delivery(), value.batch().revision(),
                        value.batch().members().stream().map(AlertIntent.Member::episodeMember).toList());
                var eligible = value.batch().members().stream().filter(value.members()::contains).map(AlertIntent.Member::episodeMember)
                        .collect(java.util.stream.Collectors.toSet());
                var inFlight = original.state().attempts().stream().filter(attempt -> attempt.endedAt() == null)
                        .map(attempt -> attempt.permit().deadline()).min(Instant::compareTo).orElse(null);
                var evidence = new DeliveryEvidence.Previous(original.state().summary(), original.frozen().content().renderingFailed(), inFlight);
                var decision = io.tapstate.core.alerting.FrozenGroupPolicy.reconcile(frozen, eligible, List.of(evidence), clock.instant());
                if (value.disposition().action() != decision.action() || value.disposition().previousOutcome() != original.state().summary()
                        || !Objects.equals(value.disposition().recheckAt(), decision.recheckAt())) { throw unreadable("disposition-evidence-or-action"); }
                if (value.disposition().action() != io.tapstate.core.alerting.FrozenGroupPolicy.Action.STOP_COVERED || value.disposition().successor() == null) {
                    var pending = value.batch().members().stream().filter(member -> decision.pendingMembers().contains(member.episodeMember())).toList();
                    if (!pending.equals(value.disposition().pending())) { throw unreadable("missing-original-survivor"); }
                }
            }
            if (value.disposition() != null && value.disposition().action() != io.tapstate.core.alerting.FrozenGroupPolicy.Action.KEEP_ORIGINAL) {
                if (!original.state().permitsStopped()) { throw unreadable("original-batch-still-permitted"); }
                if (value.disposition().action() == io.tapstate.core.alerting.FrozenGroupPolicy.Action.STOP_PENDING
                        && (original.state().summary() == DeliveryEvidence.Outcome.ACCEPTED
                        || original.state().attempts().stream().anyMatch(attempt -> attempt.endedAt() == null))) { throw stale("group-successor"); }
                if (value.disposition().action() == io.tapstate.core.alerting.FrozenGroupPolicy.Action.STOP_COVERED
                        && original.state().summary() != DeliveryEvidence.Outcome.ACCEPTED) { throw unreadable("missing-covered-evidence"); }
                if (value.disposition().action() == io.tapstate.core.alerting.FrozenGroupPolicy.Action.STOP_WAIT) {
                    var pending = original.state().attempts().stream().filter(attempt -> attempt.endedAt() == null)
                            .map(attempt -> attempt.permit().deadline()).min(Instant::compareTo);
                    if (pending.isEmpty() || !pending.orElseThrow().equals(value.disposition().recheckAt())) { throw unreadable("original-batch-wait"); }
                }
                if (value.disposition().successor() != null) {
                    var successor = requiredIntent(session, new AlertIntent.Business(value.disposition().successor()), 1).value();
                    if (!successor.frozen().members().equals(value.disposition().pending())) { throw unreadable("successor-members"); }
                    if (successor.frozen().deadline().isAfter(original.frozen().deadline()) || !successor.frozen().retry().equals(original.frozen().retry())
                            || !successor.frozen().retention().equals(original.frozen().retention())) {
                        throw unreadable("successor-expanded-window-or-policy");
                    }
                    if (value.disposition().action() == io.tapstate.core.alerting.FrozenGroupPolicy.Action.STOP_COVERED
                            && !successor.state().permitsStopped()) { throw unreadable("covered-successor-still-permitted"); }
                }
            }
        }
    }
    private void validateSummaryHandoff(ClientSession session, AlertSchedule.Record before, AlertSchedule.Record after) {
        var expected = new java.util.LinkedHashMap<io.tapstate.core.alerting.FrozenGroupPolicy.Member, AlertSchedule.Previous>();
        for (var summary : before.summaries()) { expected.put(new io.tapstate.core.alerting.FrozenGroupPolicy.Member(summary.alarm(), summary.episode()), summary); }
        for (var key : before.protectedIntents()) {
            if (after.protectedIntents().contains(key)) { continue; }
            var original = requiredIntent(session, new AlertIntent.Business(key), 1).value();
            if (original.state().retained().stoppedAt() == null || original.state().attempts().stream().anyMatch(attempt -> attempt.endedAt() == null)) {
                throw unreadable("unsettled-intent-reference");
            }
            if (key.notification().kind() != io.tapstate.core.alerting.DeliveryKey.Kind.FAULT
                    && key.notification().kind() != io.tapstate.core.alerting.DeliveryKey.Kind.REMINDER) { continue; }
            for (var member : original.frozen().members()) {
                var id = member.episodeMember(); var prior = expected.get(id);
                var outcome = prior == null ? original.state().summary() : DeliveryEvidence.aggregate(List.of(prior.outcome(), original.state().summary()));
                expected.put(id, new AlertSchedule.Previous(id.key(), id.episode(), outcome,
                        original.frozen().content().renderingFailed() || prior != null && prior.renderingFailed()));
            }
        }
        var actual = new java.util.LinkedHashMap<io.tapstate.core.alerting.FrozenGroupPolicy.Member, AlertSchedule.Previous>();
        for (var summary : after.summaries()) { actual.put(new io.tapstate.core.alerting.FrozenGroupPolicy.Member(summary.alarm(), summary.episode()), summary); }
        if (!expected.equals(actual)) { throw unreadable("summary-without-original-handoff"); }
    }
    private static void scheduleSuccessor(AlertSchedule.Record a, AlertSchedule.Record b, Instant now) {
        if (!a.key().equals(b.key()) || !a.retention().equals(b.retention()) || !a.createdAt().equals(b.createdAt())
                || b.clock().round() < a.clock().round() || b.clock().round() > a.clock().round() && (a.clock().round() == Long.MAX_VALUE || b.clock().round() != a.clock().round() + 1)
                || b.batchSequence() < a.batchSequence() || b.batchSequence() > a.batchSequence() && (a.batchSequence() == Long.MAX_VALUE || b.batchSequence() != a.batchSequence() + 1)
                || a.clock().firstCandidateAt() != null && !a.clock().firstCandidateAt().equals(b.clock().firstCandidateAt())
                || a.clock().lastPlannedAt() != null && (b.clock().lastPlannedAt() == null || b.clock().lastPlannedAt().isBefore(a.clock().lastPlannedAt()))
                || a.clock().reminderEligibleAt() != null && (b.clock().reminderEligibleAt() == null || b.clock().reminderEligibleAt().isBefore(a.clock().reminderEligibleAt()))
                || a.retiredAt() != null && !a.retiredAt().equals(b.retiredAt()) || a.summariesHandedOff() && !b.summariesHandedOff()
                || a.batch() != null && !a.batch().equals(b.batch())
                || a.batch() != null && !a.members().containsAll(b.members())
                || a.disposition() != null && a.disposition().successor() != null
                        && (b.disposition() == null || !a.disposition().successor().equals(b.disposition().successor()))) {
            throw new IllegalArgumentException("schedule identity, original batch, successor and progress cannot reset");
        }
        var priorOwner = a.owner(); var owner = b.owner();
        if (owner.generation() < priorOwner.generation()
                || owner.generation() > priorOwner.generation() && (priorOwner.generation() == Long.MAX_VALUE || owner.generation() != priorOwner.generation() + 1)
                || owner.generation() == priorOwner.generation() && (!Objects.equals(owner.ownerId(), priorOwner.ownerId())
                        || priorOwner.leaseUntil() != null && owner.leaseUntil().isBefore(priorOwner.leaseUntil()))) {
            throw new IllegalArgumentException("schedule owner cannot move backwards or change within one generation");
        }
        if (owner.generation() > priorOwner.generation() && (priorOwner.activeAt(now) || !owner.activeAt(now))
                || owner.generation() == priorOwner.generation() && !Objects.equals(owner.leaseUntil(), priorOwner.leaseUntil())
                        && (!priorOwner.activeAt(now) || !owner.activeAt(now))) { throw stale("schedule-owner"); }
        if (a.route() instanceof AlertIntent.ConfiguredRoute old && (!(b.route() instanceof AlertIntent.ConfiguredRoute updated)
                || !old.identity().equals(updated.identity()) || updated.bindingRevision() < old.bindingRevision())
                || a.route() instanceof AlertIntent.SystemRoute && !a.route().equals(b.route())) {
            throw new IllegalArgumentException("schedule route binding cannot change identity or move backwards");
        }
        if (a.clock().round() == b.clock().round() && !Objects.equals(a.clock().lastPlannedAt(), b.clock().lastPlannedAt())) {
            throw new IllegalArgumentException("an original group round cannot acquire a new planning time");
        }
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
