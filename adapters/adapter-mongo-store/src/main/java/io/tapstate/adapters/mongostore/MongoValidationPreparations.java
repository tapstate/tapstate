package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import static io.tapstate.adapters.mongostore.MongoValidationPreparationFacts.*;

/** CONFIG authority for validation-only first creation; production preparation and models are never consulted. */
final class MongoValidationPreparations {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final Runnable active;

    MongoValidationPreparations(MongoClient client, MongoDatabase database, String clusterId, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, Runnable active) {
        this.client = Objects.requireNonNull(client); this.database = Objects.requireNonNull(database); this.clusterId = Objects.requireNonNull(clusterId);
        this.bindings = Objects.requireNonNull(bindings); this.artifacts = Objects.requireNonNull(artifacts); this.active = Objects.requireNonNull(active);
        if (!bindings.belongsTo(client, database.getName()) || !artifacts.belongsTo(client, database.getName())) throw new IllegalArgumentException("validation preparation needs the original CONFIG client");
    }

    /** A descriptor is not authority. Only this native object can carry a one-time first-attempt right. */
    static final class Permission {
        private final MongoValidationPreparations owner;
        private final Subject subject;
        private final boolean first;
        private final AtomicBoolean attempted = new AtomicBoolean();
        private Permission(MongoValidationPreparations owner, Subject subject, boolean first) { this.owner = owner; this.subject = subject; this.first = first; }
        Subject subject() { return subject; }
        boolean consumeFirstAttempt() { return first && attempted.compareAndSet(false, true); }
        State requireCurrent() { return owner.require(this, false); }
        State requireComplete() { return owner.require(this, true); }
        StorageBinding binding() { return owner.binding(subject.scope()); }
        @Override public String toString() { return "ValidationPreparation[original]"; }
    }
    static final class Closing {
        private final MongoValidationPreparations owner;
        private final String pipeline;
        private final UUID generation;
        private final UUID operation;
        private Closing(MongoValidationPreparations owner, String pipeline, UUID generation, UUID operation) {
            this.owner = owner; this.pipeline = pipeline; this.generation = generation; this.operation = operation;
        }
        State requireCurrent() { return owner.requireClosing(this); }
        UUID operation() { return operation; }
        StorageBinding binding() {
            var state = requireCurrent(); if (state.subject() == null) throw fenced(); return owner.binding(state.subject().scope());
        }
        @Override public String toString() { return "ValidationClosing[original]"; }
    }

    State read(String pipeline) { return transaction(session -> state(mapping(session, pipeline), pipeline)); }

    Permission issue(String pipeline, CacheBusinessScope expected) {
        Objects.requireNonNull(expected); UUID preparation = UUID.randomUUID();
        return transaction(session -> {
            requireDefinition(session, pipeline); requireBinding(session, expected);
            var row = mapping(session, pipeline); var current = state(row, pipeline);
            if (current.closingOperationId() != null || current.phase() == Phase.CLOSED) throw fenced();
            if (current.phase() == Phase.ISSUED || current.phase() == Phase.COMPLETE) {
                if (!current.subject().scope().equals(expected)) throw fenced(); return new Permission(this, current.subject(), false);
            }
            var subject = new Subject(expected, pipeline, current.generation(), preparation);
            replace(session, row, current.change(Phase.ISSUED, subject, null, current.previous()));
            return new Permission(this, subject, true);
        });
    }

    Permission complete(Permission permission, MongoValidationCachePreparations.Proof proof) {
        requireOwned(permission); proof.requirePrepared(permission.subject());
        transaction(session -> {
            requireDefinition(session, permission.subject().pipelineId()); requireBinding(session, permission.subject().scope());
            var row = mapping(session, permission.subject().pipelineId()); var current = state(row, permission.subject().pipelineId());
            requireSubject(current, permission.subject()); if (current.closingOperationId() != null) throw fenced();
            if (current.phase() == Phase.COMPLETE) return null;
            if (current.phase() != Phase.ISSUED) throw fenced();
            replace(session, row, current.change(Phase.COMPLETE, current.subject(), null, current.previous())); return null;
        });
        proof.requirePrepared(permission.subject()); permission.requireComplete(); return new Permission(this, permission.subject(), false);
    }

    Closing beginClose(String pipeline, UUID expectedGeneration, UUID operation) {
        Objects.requireNonNull(expectedGeneration); Objects.requireNonNull(operation);
        return transaction(session -> {
            var row = mapping(session, pipeline); var current = state(row, pipeline);
            if (!current.generation().equals(expectedGeneration)) throw fenced();
            if (current.phase() == Phase.CLOSED) return new Closing(this, pipeline, expectedGeneration, current.closingOperationId());
            requireDefinition(session, pipeline);
            if (current.closingOperationId() != null && !current.closingOperationId().equals(operation)) throw fenced();
            if (current.closingOperationId() == null) replace(session, row, current.change(current.phase(), current.subject(), operation, current.previous()));
            return new Closing(this, pipeline, expectedGeneration, operation);
        });
    }

    State finishClose(Closing closing, MongoValidationCachePreparations.Proof proof) {
        if (closing == null || closing.owner != this) throw fenced();
        var before = closing.requireCurrent(); if (before.phase() == Phase.CLOSED) return before;
        Receipt receipt = before.subject() == null ? before.previous() : Objects.requireNonNull(proof, "original CACHE closure").requireClosed(before.subject());
        return transaction(session -> {
            var row = mapping(session, closing.pipeline); var current = state(row, closing.pipeline); requireClosing(current, closing);
            if (current.phase() == Phase.CLOSED) return current;
            // Parent closure is not final while any original report creation/publication permission remains open.
            var tasks = SystemCollections.VALIDATION_TASKS.on(database);
            var unfinished = new Document("_id.clusterId", clusterId).append("_id.pipelineId", closing.pipeline)
                    .append("$or", List.of(new Document("modes.full.permission.phase", new Document("$in", List.of("OPEN", "BORN"))),
                            new Document("modes.incremental.permission.phase", new Document("$in", List.of("OPEN", "BORN")))));
            if (tasks.find(session, unfinished).limit(1).first() != null) throw fenced();
            var next = current.change(Phase.CLOSED, current.subject(), closing.operation, receipt); replace(session, row, next); return next;
        });
    }

    /** Explicit new intent only: ordinary issue/read never reopens a closed generation. */
    State restartClosed(String pipeline, UUID expectedGeneration, long expectedRevision) {
        UUID generation = UUID.randomUUID();
        return transaction(session -> {
            requireDefinition(session, pipeline); var row = mapping(session, pipeline); var current = state(row, pipeline);
            if (current.phase() != Phase.CLOSED || !current.generation().equals(expectedGeneration) || current.revision() != expectedRevision) throw fenced();
            var next = new State(generation, 0, Phase.UNASSIGNED, null, null, current.previous());
            replace(session, row, next); return next;
        });
    }

    private State require(Permission permission, boolean complete) {
        requireOwned(permission);
        return transaction(session -> {
            requireDefinition(session, permission.subject().pipelineId()); requireBinding(session, permission.subject().scope());
            var current = state(mapping(session, permission.subject().pipelineId()), permission.subject().pipelineId()); requireSubject(current, permission.subject());
            if (current.closingOperationId() != null || current.phase() != Phase.COMPLETE && (complete || current.phase() != Phase.ISSUED)) throw fenced();
            return current;
        });
    }
    private State requireClosing(Closing closing) {
        if (closing == null || closing.owner != this) throw fenced();
        return transaction(session -> { var current = state(mapping(session, closing.pipeline), closing.pipeline); requireClosing(current, closing); return current; });
    }
    private static void requireClosing(State state, Closing closing) {
        if (!state.generation().equals(closing.generation) || !closing.operation.equals(state.closingOperationId())) throw fenced();
    }
    private void requireOwned(Permission permission) { if (permission == null || permission.owner != this) throw fenced(); }
    private static void requireSubject(State state, Subject expected) { if (!expected.equals(state.subject())) throw fenced(); }
    private void requireDefinition(ClientSession session, String pipeline) {
        if (!(artifacts.getInTransaction(session, pipeline).orElseThrow(MongoValidationPreparations::fenced) instanceof PipelineResource)) throw fenced();
        var row = mapping(session, pipeline);
        if (!(row.get("phase") instanceof String phase) || !Set.of("ACTIVE", "REBUILDING").contains(phase)) throw fenced();
    }
    private void requireBinding(ClientSession session, CacheBusinessScope expected) {
        if (!clusterId.equals(expected.clusterId()) || !bindings.cachePreparationForTransaction(session).map(value -> value.businessScope().equals(expected)).orElse(false)) throw fenced();
    }
    private StorageBinding binding(CacheBusinessScope expected) {
        return transaction(session -> { requireBinding(session, expected); return bindings.loadForTransaction(session).orElseThrow(MongoValidationPreparations::fenced); });
    }
    private Document mapping(ClientSession session, String pipeline) {
        if (pipeline == null || pipeline.isBlank() || pipeline.length() > 512) throw fenced();
        var row = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", pipeline)).first();
        if (row == null) throw unreadable(); return row;
    }
    private State state(Document row, String pipeline) {
        if (!(row.get("validationPreparation") instanceof Document value)) throw unreadable(); return decode(value, clusterId, pipeline);
    }
    private void replace(ClientSession session, Document row, State next) {
        if (SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,
                new Document("_id", row.get("_id")).append("validationPreparation", row.get("validationPreparation")),
                new Document("$set", new Document("validationPreparation", encode(next)))).getMatchedCount() != 1) throw fenced();
    }
    private <T> T transaction(Function<ClientSession, T> work) {
        return StoreIo.call(() -> { active.run(); try (var session = client.startSession()) {
            return session.withTransaction(() -> { active.run(); var result = work.apply(session); active.run(); return result; }, TRANSACTION);
        } });
    }
    static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
