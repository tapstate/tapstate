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
}
