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
        void requireReportsClosed() { owner.requireReportsClosed(this); }
        StorageBinding binding() {
            var state = requireCurrent(); if (state.subject() == null) throw fenced(); return owner.binding(state.subject().scope());
        }
        @Override public String toString() { return "ValidationClosing[original]"; }
    }

    /** Fixed native authority: closing can settle original work but can never admit another run. */
    static final class Authority {
        private final Permission permission;
        private final Closing closing;
        private final Subject subject;
        Authority(Permission permission) {
            this.permission = Objects.requireNonNull(permission); this.closing = null;
            permission.requireComplete(); this.subject = permission.subject();
        }
        Authority(Closing closing) {
            this.permission = null; this.closing = Objects.requireNonNull(closing);
            var state = closing.requireCurrent();
            this.subject = state.subject();
        }
        Subject subject() { if (subject == null) throw fenced(); return subject; }
        String pipelineId() { return permission == null ? closing.pipeline : subject.pipelineId(); }
        boolean closing() { return closing != null; }
        void require(boolean admission) {
            if (permission != null) permission.requireComplete();
            else {
                if (admission) throw fenced(); var state = closing.requireCurrent();
                if (!Objects.equals(subject, state.subject()) || state.phase() == Phase.CLOSED || subject == null) throw fenced();
            }
        }
        StorageBinding binding() { require(false); return permission == null ? closing.binding() : permission.binding(); }
        private MongoValidationPreparations owner() { return permission == null ? closing.owner : permission.owner; }
        void enter(MongoClient client, MongoDatabase database, ClientSession session, boolean admission) {
            var owner = owner(); owner.active.run();
            if (owner.client != client || !owner.database.getName().equals(database.getName())) throw fenced();
            var row = owner.mapping(session, pipelineId()); var state = owner.state(row, pipelineId());
            if (!Objects.equals(subject, state.subject())) throw fenced();
            if (subject != null) owner.requireBinding(session, subject.scope());
            if (permission != null) {
                owner.requireDefinition(session, pipelineId());
                if (state.phase() != Phase.COMPLETE || state.closingOperationId() != null) throw fenced();
            } else {
                if (admission) throw fenced(); requireClosing(state, closing);
            }
            if (admission) owner.replace(session, row, state.change(state.phase(), state.subject(), state.closingOperationId(), state.previous()));
        }
        void requireStart(io.tapstate.spi.store.ValidationReportAdmission.Start start, boolean closed) {
            requireStart(start, closed, false);
        }
        void requireBornStart(io.tapstate.spi.store.ValidationReportAdmission.Start start) { requireStart(start, false, true); }
        boolean savedDelay(io.tapstate.spi.store.ValidationControlStore.Delay change) {
            var start = change.run().admission();
            if (start == null || !pipelineId().equals(start.task().pipelineId()) || !owner().clusterId.equals(start.task().clusterId())) throw fenced();
            var owner = owner(); var store = new MongoValidationAdmissionStore(owner.client, owner.database, owner.clusterId, owner.active, this);
            return store.transaction(session -> store.savedDelayInTransaction(session, start, change.schedulingRevision(), change.commandId(), change.firstCheckDelayMillis()));
        }
        void requireRunSnapshot(io.tapstate.spi.store.ValidationReportAdmission.Start start, io.tapstate.core.validation.ValidationRunSpec snapshot) {
            if (start == null || !pipelineId().equals(start.task().pipelineId()) || !owner().clusterId.equals(start.task().clusterId())) throw fenced();
            var owner = owner(); var store = new MongoValidationAdmissionStore(owner.client, owner.database, owner.clusterId, owner.active, this);
            store.transaction(session -> { store.requireRunSnapshotInTransaction(session, start, snapshot); return null; });
        }
        private void requireStart(io.tapstate.spi.store.ValidationReportAdmission.Start start, boolean closed, boolean born) {
            if (start == null || !pipelineId().equals(start.task().pipelineId()) || !owner().clusterId.equals(start.task().clusterId())) throw fenced();
            var owner = owner();
            var store = new MongoValidationAdmissionStore(owner.client, owner.database, owner.clusterId, owner.active, this);
            store.transaction(session -> {
                if (born) store.requireBornInTransaction(session, start);
                else store.requireOriginalInTransaction(session, start, closed);
                return null;
            });
        }
    }

    State read(String pipeline) { return transaction(session -> state(mapping(session, pipeline), pipeline)); }

    MongoValidationPreparations guardedBy(Runnable local) {
        Objects.requireNonNull(local);
        return new MongoValidationPreparations(client, database, clusterId, bindings, artifacts, () -> { active.run(); local.run(); });
    }

    Closing forRetirement(io.tapstate.spi.store.PipelineRetirementPreparation original) {
        MongoPipelineRetirements.requireOriginalPending(original);
        return transaction(session -> {
            var request = original.request(); var row = mapping(session, request.scope().pipelineId());
            var fact = MongoPipelineRetirementFacts.read(row);
            if (!"RETIRING".equals(row.get("phase")) || !request.equals(fact.request())) throw fenced();
            var state = state(row, request.scope().pipelineId());
            if (state.closingOperationId() == null) throw fenced();
            return new Closing(this, request.scope().pipelineId(), state.generation(), state.closingOperationId());
        });
    }

    boolean closeReports(Closing closing, int maximum) {
        if (closing == null || closing.owner != this || maximum < 1 || maximum > 20) throw fenced();
        var authority = new Authority(closing);
        var store = new MongoValidationAdmissionStore(client, database, clusterId, active, authority);
        return store.transaction(session -> store.closePipelineInTransaction(session, closing.pipeline, maximum));
    }

    void requireReportsClosed(Closing closing) {
        if (closing == null || closing.owner != this) throw fenced();
        transaction(session -> {
            requireClosing(state(mapping(session, closing.pipeline), closing.pipeline), closing);
            var unfinished = unfinishedReports(clusterId, closing.pipeline);
            var activeTask = new Document("_id.clusterId", clusterId).append("_id.pipelineId", closing.pipeline).append("closed", new Document("$ne", true));
            if (SystemCollections.VALIDATION_TASKS.on(database).find(session, unfinished).limit(1).first() != null
                    || SystemCollections.VALIDATION_TASKS.on(database).find(session, activeTask).limit(1).first() != null) throw fenced();
            return null;
        });
    }

    static void beginRetirement(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster,
            io.tapstate.spi.store.PipelineRetirementRequest original, UUID closingOperation) {
        if (!validationEnabled(session, database, cluster)) return;
        var rows = SystemCollections.OPERATOR_GENERATIONS.on(database);
        var row = rows.find(session, new Document("_id", original.scope().pipelineId())).first();
        if (row == null || !(row.get("validationPreparation") instanceof Document value)) throw unreadable();
        var held = decode(value, cluster, original.scope().pipelineId());
        if (held.phase() == Phase.CLOSED) return;
        if (held.closingOperationId() != null || held.subject() != null && !held.subject().scope().equals(
                new CacheBusinessScope(original.scope().clusterId(), original.scope().bindingVersion(), original.scope().cacheIncarnation()))) throw fenced();
        var next = held.change(held.phase(), held.subject(), closingOperation, held.previous());
        if (rows.updateOne(session, new Document("_id", row.get("_id")).append("validationPreparation", value),
                new Document("$set", new Document("validationPreparation", encode(next)))).getMatchedCount() != 1) throw fenced();
    }

    static void requireRetired(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster, String pipeline) {
        if (!validationEnabled(session, database, cluster)) return;
        var row = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", pipeline)).first();
        if (row == null || !(row.get("validationPreparation") instanceof Document value) || decode(value, cluster, pipeline).phase() != Phase.CLOSED) throw fenced();
        if (SystemCollections.VALIDATION_TASKS.on(database).find(session, unfinishedReports(cluster, pipeline)).limit(1).first() != null) throw fenced();
    }

    static Document unfinishedReports(String cluster, String pipeline) {
        var filter = new Document("_id.clusterId", cluster);
        if (pipeline != null) filter.append("_id.pipelineId", pipeline);
        return filter
                .append("$or", List.of(new Document("closed", new Document("$ne", true)),
                        new Document("modes.full.permission.phase", new Document("$in", List.of("OPEN", "BORN"))),
                        new Document("modes.incremental.permission.phase", new Document("$in", List.of("OPEN", "BORN")))));
    }

    static Document openReportPermissions(String cluster, String pipeline) {
        return new Document("_id.clusterId", cluster).append("_id.pipelineId", pipeline).append("$or", List.of(
                new Document("modes.full.permission.phase", new Document("$in", List.of("OPEN", "BORN"))),
                new Document("modes.incremental.permission.phase", new Document("$in", List.of("OPEN", "BORN")))));
    }

    static boolean validationEnabled(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster) {
        var schema = SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first();
        if (schema != null && Integer.valueOf(1).equals(schema.get("installedVersion")) && !schema.containsKey("validation")) return false;
        var marker = MongoValidationSchema.requireVersionTwo(schema, "READY");
        if (!cluster.equals(MongoValidationSchema.scope(marker).clusterId())) throw unreadable(); return true;
    }

    static void requireTaskAuthoringParent(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster, String pipeline) {
        if (!validationEnabled(session, database, cluster)) throw fenced();
        var row = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", pipeline)).first();
        var mapping = MongoPipelineStateMappings.decode(pipeline, row);
        if (mapping.phase() == io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRING
                || mapping.phase() == io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRED) throw fenced();
        decode(row.get("validationPreparation", Document.class), cluster, pipeline);
    }

    static void beginCacheReplacement(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster,
            Document original, Document replacement, io.tapstate.spi.store.StorageRecoveryCommands.Plan running) {
        if (!validationEnabled(session, database, cluster)) return;
        String pipeline = original.getString("_id"); var state = decode(original.get("validationPreparation", Document.class), cluster, pipeline);
        var target = new CacheBusinessScope(cluster, running.originalScope().bindingVersion(), running.originalScope().cacheIncarnation());
        if (state.subject() != null && state.subject().scope().equals(target)
                || state.previous() != null && state.previous().subject().scope().equals(target)) throw fenced();
        replacement.put("validationPreparation", encode(state.change(state.phase(), state.subject(), UUID.fromString(running.operationId()), state.previous())));
    }

    /** The original whole-CACHE replacement is the authority; no missing-CACHE observation grants it. */
    static void requireCacheReplacement(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster, String pipeline,
            io.tapstate.spi.store.StorageRecoveryCommands.Plan running) {
        if (running.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING
                || running.manifest().selection().extent() != io.tapstate.spi.store.StorageRecoveryCommands.Extent.CLUSTER
                || !running.manifest().affectedPipelineIds().contains(pipeline) || !validationEnabled(session, database, cluster)) throw fenced();
        var row = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", pipeline)).first();
        var summary = MongoCacheRecoveryFacts.read(row).orElseThrow(MongoValidationPreparations::fenced);
        var state = decode(row.get("validationPreparation", Document.class), cluster, pipeline);
        if (!summary.recoveryId().equals(running.operationId()) || !summary.targetScope().cacheIncarnation().equals(running.originalScope().cacheIncarnation())
                || !Objects.equals(state.closingOperationId(), UUID.fromString(running.operationId()))) throw fenced();
    }

    static void completeCacheReplacement(com.mongodb.client.ClientSession session, MongoDatabase database, String cluster,
            io.tapstate.spi.store.StorageRecoveryCommands.Plan running, Map<String, UUID> generations) {
        if (!validationEnabled(session, database, cluster)) return;
        var marker = MongoValidationSchema.requireVersionTwo(SystemCollections.SCHEMA_MIGRATIONS.on(database)
                .find(session, new Document("_id", "config")).first(), "READY");
        var target = new CacheBusinessScope(cluster, running.originalScope().bindingVersion(), running.originalScope().cacheIncarnation());
        if (!target.equals(MongoValidationSchema.scope(marker)) || !running.operationId().equals(marker.get("operationId"))) throw fenced();
        for (String pipeline : running.manifest().affectedPipelineIds()) {
            requireCacheReplacement(session, database, cluster, pipeline, running);
            MongoValidationAdmissionStore.requireClosedPipelinePermissions(session, database, cluster, pipeline);
            var rows = SystemCollections.OPERATOR_GENERATIONS.on(database); var row = rows.find(session, new Document("_id", pipeline)).first();
            var next = new State(Objects.requireNonNull(generations.get(pipeline)), 0, Phase.UNASSIGNED, null, null, null);
            if (rows.updateOne(session, row, new Document("$set", new Document("validationPreparation", encode(next)))).getMatchedCount() != 1) throw fenced();
        }
    }

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
        var before = closing.requireCurrent(); if (before.phase() == Phase.CLOSED) { requireReportsClosed(closing); return before; }
        Receipt receipt = before.subject() == null ? before.previous() : Objects.requireNonNull(proof, "original CACHE closure").requireClosed(before.subject());
        if (before.phase() == Phase.COMPLETE && receipt.outcome() != Outcome.RETIRED) throw unreadable();
        return transaction(session -> {
            var row = mapping(session, closing.pipeline); var current = state(row, closing.pipeline); requireClosing(current, closing);
            if (current.phase() == Phase.CLOSED) return current;
            if (current.phase() == Phase.COMPLETE && receipt.outcome() != Outcome.RETIRED) throw unreadable();
            // Parent closure is not final while any original report creation/publication permission remains open.
            var tasks = SystemCollections.VALIDATION_TASKS.on(database);
            var unfinished = unfinishedReports(clusterId, closing.pipeline);
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
        return StoreIo.call(() -> { active.run(); MongoValidationSchema.requireReady(database, true); try (var session = client.startSession()) {
            var committed = session.withTransaction(() -> {
                active.run();
                var schema = MongoValidationSchema.requireVersionTwo(SystemCollections.SCHEMA_MIGRATIONS.on(database)
                        .find(session, new Document("_id", "config")).first(), "READY");
                if (!clusterId.equals(MongoValidationSchema.scope(schema).clusterId())) throw unreadable();
                var result = work.apply(session); active.run(); return result;
            }, TRANSACTION);
            MongoValidationSchema.requireReady(database, true); active.run(); return committed;
        } });
    }
    static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
