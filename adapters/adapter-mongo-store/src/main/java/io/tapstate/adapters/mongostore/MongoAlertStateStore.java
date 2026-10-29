package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertSnapshot;
import io.tapstate.core.alerting.AlertUpdate;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.bson.Document;

/**
 * Transaction-bound state fragment. The owning CONFIG coordinator must stage all required scheduling
 * and intent writes before committing; this class deliberately has no standalone acceptance API.
 * Driver failures retain their transaction labels inside these fragments. The coordinator translates
 * them with StoreIo only after the enclosing withTransaction operation has completed its retries.
 */
final class MongoAlertStateStore {
    static final List<SystemCollections> COLLECTIONS = List.of(SystemCollections.ALERT_INSTANCES,
            SystemCollections.ALERT_EPISODES, SystemCollections.ALERT_TRANSITIONS);
    enum Stage { STAGED, UNCHANGED, CONFLICT }

    private final MongoCollection<Document> heads;
    private final MongoCollection<Document> episodes;
    private final MongoCollection<Document> transitions;
    private final AlarmKey.Scope trustedScope;
    private final Runnable requireActive;

    MongoAlertStateStore(MongoDatabase configuration, AlarmKey.Scope trustedScope, Runnable requireActive) {
        Objects.requireNonNull(configuration, "configuration");
        this.trustedScope = Objects.requireNonNull(trustedScope, "trustedScope");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.heads = SystemCollections.ALERT_INSTANCES.on(configuration);
        this.episodes = SystemCollections.ALERT_EPISODES.on(configuration);
        this.transitions = SystemCollections.ALERT_TRANSITIONS.on(configuration);
    }

    static void prepare(ClientSession session, MongoDatabase database) {
        requireTransaction(session);
        for (SystemCollections collection : COLLECTIONS) {
            collection.prepareCollectionOn(database, session);
            for (SystemCollections.IndexSpec index : collection.indexes()) {
                IndexEnsure.createInTransaction(session, collection.on(database), index);
            }
        }
    }

    /** Controlled feature preparation only; existing populated collections receive indexes outside document transactions. */
    static void prepareQueryIndex(MongoDatabase database, SystemCollections collection, SystemCollections.IndexSpec index) {
        if (!COLLECTIONS.contains(collection) || !index.indexName().startsWith("alert_query_")) { throw new IllegalArgumentException("undeclared state query index"); }
        Document keys = new Document(); index.keys().forEach(key -> keys.append(key, 1));
        collection.on(database).createIndex(keys, new com.mongodb.client.model.IndexOptions().name(index.indexName()).unique(false));
    }

    Optional<MongoAlertStateCodec.State> load(ClientSession session, AlarmKey key) {
        requireContext(session, key);
        Document head = heads.find(session, new Document("_id", MongoAlertStateCodec.key(key))).first();
        if (head == null) {
            Document original = new Document("_id.alarm", MongoAlertStateCodec.key(key));
            if (episodes.find(session, original).limit(1).first() != null
                    || transitions.find(session, original).limit(1).first() != null) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", "alert-state", "field", "missing-head"), null);
            }
            requireActive.run(); return Optional.empty();
        }
        Object pointer = head.get("episodeNumber");
        Document episode = pointer == null ? null : episodes.find(session, new Document("_id",
                new Document("alarm", MongoAlertStateCodec.key(key)).append("number", pointer))).first();
        var decoded = MongoAlertStateCodec.read(key, head, episode);
        requireActive.run();
        return Optional.of(decoded);
    }

    /** expectedRevision zero means a genuinely new head, never a replacement for retained history. */
    Stage stage(ClientSession session, long expectedRevision, AlertSnapshot before, AlertUpdate candidate) {
        Objects.requireNonNull(before, "before"); Objects.requireNonNull(candidate, "candidate");
        AlarmKey key = before.rule().key();
        requireContext(session, key);
        if (expectedRevision < 0 || expectedRevision == Long.MAX_VALUE
                || !key.equals(candidate.snapshot().rule().key())) {
            throw new IllegalArgumentException("invalid state commit identity or revision");
        }
        if (expectedRevision == 0 && !before.equals(AlertSnapshot.initial(before.rule()))) {
            throw new IllegalArgumentException("a new head requires the original initial state");
        }
        if (candidate.ignored() && (!before.equals(candidate.snapshot()) || !candidate.transitions().isEmpty())) {
            throw new IllegalArgumentException("an ignored input cannot carry mutations");
        }
        Optional<MongoAlertStateCodec.State> actual = load(session, key);
        if (expectedRevision == 0 ? actual.isPresent() : actual.isEmpty()
                || actual.orElseThrow().recordRevision() != expectedRevision
                || !actual.orElseThrow().snapshot().equals(before)) { return Stage.CONFLICT; }
        if (candidate.ignored() || before.equals(candidate.snapshot()) && candidate.transitions().isEmpty()) {
            return Stage.UNCHANGED;
        }
        long nextRevision = Math.incrementExact(expectedRevision);
        Document head = MongoAlertStateCodec.head(candidate.snapshot(), nextRevision);
        Document episode = candidate.snapshot().episode() == null ? null
                : MongoAlertStateCodec.episode(key, candidate.snapshot().episode());
        // Validate the exact candidate before any write, including cross-row time/frontier invariants.
        MongoAlertStateCodec.read(key, head, episode);
        if (expectedRevision == 0) {
            // A concurrent first insert can abort this transaction; its caller reconciles the same
            // original key rather than treating a failed/unknown submission as a fresh identity.
            heads.insertOne(session, head);
        } else if (heads.replaceOne(session, new Document("_id", MongoAlertStateCodec.key(key))
                .append("recordRevision", expectedRevision), head).getMatchedCount() != 1) {
            return Stage.CONFLICT;
        }
        if (episode != null) {
            episodes.replaceOne(session, new Document("_id", episode.get("_id")), episode,
                    new ReplaceOptions().upsert(true));
        }
        for (int ordinal = 0; ordinal < candidate.transitions().size(); ordinal++) {
            Document transition = MongoAlertStateCodec.transition(key, nextRevision, ordinal,
                    candidate.transitions().get(ordinal));
            MongoAlertStateCodec.readTransition(key, nextRevision, ordinal, transition);
            transitions.insertOne(session, transition);
        }
        requireActive.run();
        return Stage.STAGED;
    }

    Optional<MongoAlertStateEnvelope.Stored> loadEnvelope(ClientSession session, AlarmKey key) {
        requireContext(session, key);
        Document head = heads.find(session, new Document("_id", MongoAlertStateCodec.key(key))).first();
        if (head == null) { load(session, key); return Optional.empty(); }
        Object pointer = head.get("episodeNumber");
        Document episode = pointer == null ? null : episodes.find(session, new Document("_id",
                new Document("alarm", MongoAlertStateCodec.key(key)).append("number", pointer))).first();
        var result = MongoAlertStateEnvelope.read(key, head, episode); requireActive.run(); return Optional.of(result);
    }

    Optional<io.tapstate.core.alerting.AlertStateRecord.Episode> episodeEnvelope(ClientSession session, AlarmKey key, long number) {
        requireContext(session, key);
        Document row = episodes.find(session, new Document("_id", new Document("alarm", MongoAlertStateCodec.key(key)).append("number", number))).first();
        return row == null ? Optional.empty() : Optional.of(MongoAlertStateEnvelope.readEpisode(key, number, row));
    }

    Stage stageEnvelope(ClientSession session, long revision, io.tapstate.core.alerting.AlertStateRecord.Head before,
            io.tapstate.core.alerting.AlertStateRecord.Head candidate,
            List<io.tapstate.core.alerting.AlertStateRecord.Transition> changes,
            io.tapstate.core.alerting.AlertSignal diagnosticSignal, java.time.Clock clock) {
        Objects.requireNonNull(candidate, "candidate"); Objects.requireNonNull(clock, "clock");
        AlarmKey key = candidate.snapshot().rule().key(); requireContext(session, key);
        if (revision < 0 || revision == Long.MAX_VALUE || (revision == 0) != (before == null)) { throw new IllegalArgumentException("invalid state revision"); }
        var actual = loadEnvelope(session, key);
        if (actual.isPresent() && actual.get().value().equals(candidate) && actual.get().revision() == revision + 1) { return Stage.UNCHANGED; }
        if (before == null ? actual.isPresent() : actual.isEmpty() || actual.get().revision() != revision || !actual.get().value().equals(before)) { return Stage.CONFLICT; }
        if (before != null && before.equals(candidate) && changes.isEmpty()) { return Stage.UNCHANGED; }
        if (!changes.isEmpty() && candidate.owner().generation() > 0 && !candidate.owner().activeAt(clock.instant())) { throw new IllegalArgumentException("expired owner cannot append transitions"); }
        if (changes.size() > 8) { throw new IllegalArgumentException("too many state transitions"); }
        java.time.Instant now = clock.instant();
        validateEnvelope(before, candidate, diagnosticSignal, now);
        Document head = MongoAlertStateEnvelope.head(candidate, revision + 1);
        Document episode = candidate.episode() == null ? null : MongoAlertStateEnvelope.episode(candidate.episode());
        if (episode != null && (before == null || before.episode() == null || before.episode().value().number() != candidate.episode().value().number())
                && episodes.find(session,new Document("_id",episode.get("_id"))).first() != null) { throw new IllegalArgumentException("original episode identity already exists"); }
        MongoAlertStateEnvelope.read(key, head, episode);
        var encodedChanges = new java.util.ArrayList<Document>();
        for (int i = 0; i < changes.size(); i++) {
            var change = changes.get(i);
            if (change.value().at().isAfter(now) || change.value().episode() > candidate.snapshot().episodeSequence()
                    || change.policy().deployment() != candidate.deployment()
                    || change.diagnosticReference() != null && (candidate.episode() == null
                    || !Objects.equals(change.diagnosticReference(), candidate.episode().diagnostics().latest()))) {
                throw new IllegalArgumentException("foreign state transition metadata");
            }
            if (change.endReason() != null && (candidate.episode() == null || change.endReason() != candidate.episode().value().endReason()
                    || !change.value().at().equals(candidate.episode().value().endedAt()))) { throw new IllegalArgumentException("transition lacks original episode end evidence"); }
            Document row = MongoAlertStateEnvelope.transition(key, revision + 1, i, change);
            MongoAlertStateEnvelope.readTransition(key, revision + 1, i, row); encodedChanges.add(row);
        }
        if (before == null) { heads.insertOne(session, head); }
        else if (heads.replaceOne(session, new Document("_id", head.get("_id")).append("recordRevision", revision), head).getMatchedCount() != 1) { return Stage.CONFLICT; }
        if (episode != null) { episodes.replaceOne(session, new Document("_id", episode.get("_id")), episode, new ReplaceOptions().upsert(true)); }
        for (Document row : encodedChanges) { transitions.insertOne(session, row); }
        requireActive.run(); return Stage.STAGED;
    }

    private static void validateEnvelope(io.tapstate.core.alerting.AlertStateRecord.Head before,
            io.tapstate.core.alerting.AlertStateRecord.Head candidate, io.tapstate.core.alerting.AlertSignal signal, java.time.Instant now) {
        var next = candidate.snapshot();
        if (next.lastObservedAt() != null && next.lastObservedAt().isAfter(now) || candidate.retiredAt() != null && candidate.retiredAt().isAfter(now)) {
            throw new IllegalArgumentException("state evidence is in the future");
        }
        var oldOwner = before == null ? io.tapstate.core.alerting.AlertOwnership.unclaimed() : before.owner(); var owner = candidate.owner();
        if (owner.generation() < oldOwner.generation() || owner.generation() > oldOwner.generation()
                && (oldOwner.generation() == Long.MAX_VALUE || owner.generation() != oldOwner.generation() + 1 || oldOwner.activeAt(now) || !owner.activeAt(now))
                || owner.generation() == oldOwner.generation() && (!Objects.equals(owner.ownerId(), oldOwner.ownerId())
                || !Objects.equals(owner.leaseUntil(), oldOwner.leaseUntil()) && (!oldOwner.activeAt(now) || !owner.activeAt(now) || owner.leaseUntil().isBefore(oldOwner.leaseUntil())))) {
            throw new IllegalArgumentException("state owner is stale or moves backwards");
        }
        if (before != null && !before.equals(candidate) && owner.generation() > 0 && !owner.activeAt(now)) {
            throw new IllegalArgumentException("expired state owner cannot mutate");
        }
        if (before != null) {
            var old = before.snapshot();
            if (next.rule().generation() == old.rule().generation()) {
                var a = before.binding(); var b = candidate.binding();
                if ((a == null) != (b == null) || a != null && (!a.ruleIdentity().equals(b.ruleIdentity()) || a.sourceBindingRevision() != b.sourceBindingRevision())) {
                    throw new IllegalArgumentException("evaluation generation cannot retarget its original configuration or resource binding");
                }
            }
            if (next.rule().generation() > old.rule().generation() && old.episode() != null && old.episode().endedAt() == null
                    && (next.episode() == null || next.episode().number() != old.episode().number() || next.episode().endedAt() == null)) {
                throw new IllegalArgumentException("replacement must end the original open episode");
            }
            if (!old.rule().key().equals(next.rule().key()) || before.deployment() != candidate.deployment()
                    || before.retiredAt() != null && !before.equals(candidate)
                    || old.episodeSequence() > next.episodeSequence() || next.episodeSequence() - old.episodeSequence() > 1
                    || next.rule().generation() < old.rule().generation()
                    || next.rule().generation() == old.rule().generation() && (!next.rule().equals(old.rule()) || next.lastSequence() < old.lastSequence()
                    || old.lastObservedAt() != null && (next.lastObservedAt() == null || next.lastObservedAt().isBefore(old.lastObservedAt()))
                    || next.lastSequence() == old.lastSequence() && !Objects.equals(next.lastObservedAt(),old.lastObservedAt()))
                    || before.summariesHandedOff() != candidate.summariesHandedOff()) { throw new IllegalArgumentException("state identity or frontier moved backwards"); }
            if (old.episode() != null && (next.episode() == null || next.episode().number() != old.episode().number()) && old.episode().endedAt() == null) {
                throw new IllegalArgumentException("open original episode cannot disappear");
            }
        } else if (next.episodeSequence() > 1 || next.episode() == null && next.episodeSequence() != 0 || candidate.summariesHandedOff()) { throw new IllegalArgumentException("new head lacks original state history"); }
        var ep = candidate.episode(); var oldEp = before == null ? null : before.episode();
        if (ep == null) { return; }
        if (ep.value().firedAt().isAfter(now) || ep.value().endedAt() != null && ep.value().endedAt().isAfter(now)) { throw new IllegalArgumentException("episode is in the future"); }
        boolean same = oldEp != null && oldEp.value().number() == ep.value().number();
        if (same && (!oldEp.originalBinding().equals(ep.originalBinding()) || !oldEp.policy().equals(ep.policy())
                || !oldEp.value().firedAt().equals(ep.value().firedAt()) || oldEp.value().endedAt() != null && !oldEp.value().equals(ep.value())
                || oldEp.summariesHandedOff() != ep.summariesHandedOff())) { throw new IllegalArgumentException("original episode facts changed"); }
        if (!same && (ep.value().number() != (before == null ? 1 : Math.incrementExact(before.snapshot().episodeSequence()))
                || !ep.originalBinding().equals(next.rule()) || ep.summariesHandedOff())) { throw new IllegalArgumentException("new episode has foreign binding"); }
        var d = ep.diagnostics(); var oldD = same ? oldEp.diagnostics() : null;
        if (Objects.equals(oldD, d)) { return; }
        if (oldD != null && oldD.status() == io.tapstate.core.alerting.AlertStateRecord.DiagnosticStatus.PRUNED) { throw new IllegalArgumentException("pruned diagnostics cannot return"); }
        if (d.status() == io.tapstate.core.alerting.AlertStateRecord.DiagnosticStatus.PRUNED) {
            if (oldD == null || oldD.history() == null || !oldD.prune(d.prunedAt()).equals(d) || d.prunedAt().isAfter(now)) { throw new IllegalArgumentException("invalid diagnostic pruning"); }
        } else if (d.history() != null) {
            if (signal == null || !io.tapstate.core.alerting.AlarmDiagnostics.attach(next, signal, d.history().latest().trace(), oldD == null ? null : oldD.history()).orElseThrow().equals(d.history())) {
                throw new IllegalArgumentException("capture lacks the just-accepted original occurrence");
            }
        } else if (oldD != null || d.first() != null) { throw new IllegalArgumentException("diagnostic status cannot erase evidence"); }
    }

    private void requireContext(ClientSession session, AlarmKey key) {
        requireTransaction(session); Objects.requireNonNull(key, "key"); requireActive.run();
        if (!trustedScope.equals(key.scope())) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-state", "field", "scope"), null);
        }
    }

    static void requireTransaction(ClientSession session) {
        Objects.requireNonNull(session, "session");
        if (!session.hasActiveTransaction()) {
            throw new IllegalStateException("alert state staging requires an active CONFIG transaction");
        }
    }
    boolean removeForMaintenance(ClientSession session, SystemCollections collection, Document original) {
        MongoAlertStateStore.requireTransaction(session); requireActive.run();
        if (!COLLECTIONS.contains(collection)) throw new IllegalArgumentException("foreign maintenance resource");
        return maintenanceCollection(collection).deleteOne(session, original).getDeletedCount() == 1;
    }
    boolean replaceForMaintenance(ClientSession session, SystemCollections collection, Document original, Document replacement) {
        MongoAlertStateStore.requireTransaction(session); requireActive.run();
        if (!COLLECTIONS.contains(collection) || !original.get("_id").equals(replacement.get("_id"))) throw new IllegalArgumentException("foreign maintenance identity");
        return maintenanceCollection(collection).replaceOne(session, MongoAlertMaintenance.versionFilter(original), replacement).getModifiedCount() == 1;
    }

    private MongoCollection<Document> maintenanceCollection(SystemCollections collection) {
        return switch(collection) { case ALERT_INSTANCES -> heads; case ALERT_EPISODES -> episodes; case ALERT_TRANSITIONS -> transitions; default -> throw new IllegalArgumentException("foreign state resource"); };
    }

}
