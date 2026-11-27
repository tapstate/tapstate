package io.tapstate.adapters.mongostore;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** An observation is isolated by the checkpoint and execution that measured it. */
final class MongoCachePipelineObservationStore implements ObservationStore {
    private static final String EPOCH = "epoch";
    private static final String SCOPE = "scope";
    private static final String CLAIM_GENERATION = "claimGeneration";
    private static final String EXECUTION_GENERATION = "executionGeneration";
    private static final String TOPOLOGY_REVISION = "topologyRevision";
    private static final String STATE = "state";

    private static final AtomicInteger WRITES = new AtomicInteger();
    private static final AtomicBoolean REAPING = new AtomicBoolean();
    private final MongoCachePipelineAccess access;

    MongoCachePipelineObservationStore(MongoCachePipelineAccess access) { this.access = access; }

    record Reading(MongoCachePipelineAccess access, Document state, Document claim, boolean live) {
        String pipeline() { return access.scope.pipelineId(); }
        long epoch() { return state.getLong(EPOCH); }
        PipelineState actual() { return StateJson.parse(state.getString("stateJson")); }
        Document key() {
            return keyOfClaim(access, epoch(), claim);
        }
    }

    static Document claimKey(MongoCachePipelineAccess access) {
        return MongoWorkloadClaimStore.id(new WorkloadClaimKey(access.scope.clusterId(),
                WorkloadClaimType.PIPELINE_ACTUATION, access.scope.pipelineId()));
    }

    static Map<Object, Document> claims(ClientSession session, MongoCachePipelineAccess access, List<Document> ids) {
        var collection = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(access.database);
        var results = new HashMap<Object, Document>();
        for (var row : collection.aggregate(session, List.of(
                new Document("$match", new Document("_id", new Document("$in", ids))),
                new Document("$project", new Document("claim", "$$ROOT")
                        .append("live", new Document("$gt", List.of("$leaseUntil", "$$NOW")))))))
            results.put(row.get("_id"), row);
        return results;
    }

    static Reading reading(ClientSession session, MongoCachePipelineAccess access, Document state, Document row) {
        access.validateState(state);
        if (row == null) throw MongoCachePipelinePreparations.rebuild(access.scope);
        var claim = row.get("claim", Document.class);
        access.requirePreparedClaim(session, claim);
        boolean live = Boolean.TRUE.equals(row.get("live"));
        if (MongoWorkloadClaimStore.nodeBacked(claim)) {
            var store = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(access.database)).inStrictSession(session);
            live = store.nodeLease(claim).isPresent();
        }
        return new Reading(access, state, claim, live);
    }

    @Override public Optional<Observation> read(String pipelineId) {
        access.requirePipeline(pipelineId);
        return access.observationTransaction(session -> {
            var state = SystemCollections.CACHE_PIPELINE_STATE.on(access.database)
                    .find(session, new Document("_id", pipelineId)).first();
            var id = claimKey(access);
            var reading = reading(session, access, state, claims(session, access, List.of(id)).get(id));
            var row = collection().find(session, new Document("_id", reading.key())).first();
            var selected = new HashMap<Object, Document>();
            if (row != null) selected.put(reading.key(), row);
            var terminal = terminalDetails(session, access, List.of(reading), selected);
            return select(reading, row, terminal.get(pipelineId));
        });
    }

    @Override public Optional<Observation> refreshTerminal(Observation expected, long checkpointEpoch, java.time.Instant at) {
        access.requirePipeline(expected.pipelineId());
        if (access.originalClaim() == null) throw MongoCachePipelineAccess.fenced();
        if (expected.state() != PipelineState.FAILED && expected.state() != PipelineState.STOPPED)
            throw new IllegalArgumentException("terminal refresh requires a failed or stopped projection");
        if (checkpointEpoch < 0 || expected.observedAt() == null || at == null)
            throw new IllegalArgumentException("invalid terminal refresh checkpoint or time");
        if (at.isBefore(expected.observedAt())) return Optional.empty();
        // A previous claim can still publish the coded failure for this epoch under its own key.
        // The ordinary reader must get a chance to recover that reason before any reuse.
        if (expected.state() == PipelineState.FAILED && expected.failure() == null) return Optional.empty();
        var id = keyOfFence(access, checkpointEpoch, access.originalClaim());
        var previous = document(expected, id);
        var refreshed = new Observation(expected.pipelineId(), expected.state(), expected.metrics(), expected.snapshot(),
                expected.positions(), expected.failure(), at, expected.facts(), expected.initialLoad());
        var captured = document(refreshed, id);
        return access.observationTransaction(session -> {
            var state = access.stateRow(session);
            if (!Long.valueOf(checkpointEpoch).equals(state.get(EPOCH))
                    || StateJson.parse(state.getString("stateJson")) != expected.state()) return Optional.empty();
            var claimId = claimKey(access);
            var reading = reading(session, access, state, claims(session, access, List.of(claimId)).get(claimId));
            // Never borrow terminal details from another execution for this compare-and-refresh.
            if (!id.equals(reading.key())) return Optional.empty();
            var row = collection().find(session, new Document("_id", id)).first();
            if (row == null || !previous.equals(row)) return Optional.empty();
            // Like ordinary observation saves, a concurrent transition leaves only an old-key
            // projection; it cannot modify a newer key. No lifecycle or claim authority is advanced.
            var result = collection().replaceOne(session, new Document("_id", id)
                    .append("observedAt", previous.get("observedAt")), captured);
            return result.getMatchedCount() == 1 ? Optional.of(refreshed) : Optional.empty();
        });
    }

    @Override public Optional<Observation> republishStopped(CheckpointDoc checkpoint) {
        access.requirePipeline(checkpoint.pipelineId());
        var original = access.originalClaim();
        if (original == null || StateJson.parse(checkpoint.stateJson()) != PipelineState.STOPPED) return Optional.empty();
        return access.observationTransaction(session -> {
            var state = access.stateRow(session);
            if (!checkpoint.equals(MongoStateStore.toCheckpoint(state))) return Optional.empty();
            var claimId = claimKey(access);
            var reading = reading(session, access, state, claims(session, access, List.of(claimId)).get(claimId));
            var id = keyOfFence(access, checkpoint.epoch(), original);
            if (!reading.live() || !id.equals(reading.key())) return Optional.empty();

            var collection = collection();
            var exact = collection.find(session, new Document("_id", id)).first();
            if (exact != null) return decode(access, exact, id).filter(value -> value.state() == PipelineState.STOPPED);
            var prefix = new Document(SCOPE, id.get(SCOPE)).append(EPOCH, checkpoint.epoch());
            var lower = new Document(prefix).append(CLAIM_GENERATION, new org.bson.types.MinKey());
            // The same claim can survive a topology advance. Its current topology is the exclusive upper bound.
            var upper = new Document(prefix).append(CLAIM_GENERATION, original.claimGeneration())
                    .append(EXECUTION_GENERATION, original.executionGeneration())
                    .append(TOPOLOGY_REVISION, original.topologyRevision());
            var filter = new Document("_id", new Document("$gte", lower).append("$lt", upper))
                    .append(STATE, PipelineState.STOPPED.name()).append("_id.executionGeneration", original.executionGeneration());
            var retained = collection.find(session, filter).sort(new Document("_id", -1)).first();
            if (retained == null) return Optional.empty();
            var stopped = decode(access, retained, retained.get("_id", Document.class)).orElseThrow();
            // Serialize the publication with execution changes and takeover of the claim.
            access.proveClaim(session, true);
            // Keep the original measurement and timestamp. A simultaneous fresh sample wins.
            var insert = document(stopped, id);
            insert.remove("_id");
            collection.updateOne(session, new Document("_id", id), new Document("$setOnInsert", insert),
                    new UpdateOptions().upsert(true));
            return Optional.of(stopped);
        });
    }

    static Optional<Observation> select(Reading reading, Document row, Document terminal) {
        var actual = reading.actual();
        if (actual == PipelineState.RUNNING && !reading.live()) return Optional.empty();
        var current = decode(reading.access(), row, reading.key()).filter(value -> value.state() == actual);
        if (terminal != null) return decode(reading.access(), terminal, terminal.get("_id", Document.class));
        if (current.isPresent()) return current;
        return switch (actual) {
            // STOP first commits the checkpoint, then drains the job and may rebuild CONFIG/CACHE.
            // Its matching observation witnesses that the stop actually finished.
            case FAILED, COMPLETED -> Optional.of(new Observation(reading.pipeline(), actual,
                    Map.of(), Map.of(), Map.of(), leaseRecoveryFailure(reading, actual), null, List.of()));
            default -> Optional.empty();
        };
    }

    private static io.tapstate.core.lifecycle.ObservationFailure leaseRecoveryFailure(Reading reading, PipelineState actual) {
        var recovery = actual == PipelineState.FAILED ? MongoStateStore.leaseRecovery(reading.state()) : null;
        return recovery == null || recovery.attempts() < 3 ? null
                : new io.tapstate.core.lifecycle.ObservationFailure(
                        io.tapstate.core.common.EngineErrorDefinitions.EXECUTION_NOT_AUTHORIZED.code(),
                        Map.of("pipeline", reading.pipeline()));
    }

    /** A previous RUNNING observation can witness progress, without serving its stale payload. */
    static java.util.Set<String> unavailableRunning(ClientSession session, MongoCachePipelineAccess access,
            List<Reading> readings, Map<Object, Document> current) {
        var unavailable = new java.util.HashSet<String>();
        var previousRuns = new java.util.ArrayList<Document>();
        for (var reading : readings) {
            if (reading.actual() != PipelineState.RUNNING) continue;
            var exact = current.get(reading.key());
            if (exact != null && PipelineState.RUNNING.name().equals(exact.get(STATE))) {
                if (!reading.live()) unavailable.add(reading.pipeline());
            } else {
                var prefix = new Document(SCOPE, MongoPipelinePreparationFacts.scope(reading.access().scope))
                        .append(EPOCH, reading.epoch());
                previousRuns.add(new Document("_id", new Document("$gte",
                        new Document(prefix).append(CLAIM_GENERATION, new org.bson.types.MinKey()))
                        .append("$lt", new Document(prefix).append(CLAIM_GENERATION, new org.bson.types.MaxKey()))));
            }
        }
        if (!previousRuns.isEmpty()) {
            for (var row : SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database).aggregate(session, List.of(
                    new Document("$match", new Document("$or", previousRuns).append(STATE, PipelineState.RUNNING.name())),
                    new Document("$group", new Document("_id", "$pipelineId"))))) {
                unavailable.add(row.getString("_id"));
            }
        }
        return unavailable;
    }

    static Map<String, Document> terminalDetails(ClientSession session, MongoCachePipelineAccess access,
            List<Reading> readings, Map<Object, Document> current) {
        var filters = new java.util.ArrayList<Document>();
        for (var reading : readings) {
            var state = reading.actual();
            if (state != PipelineState.FAILED && state != PipelineState.COMPLETED) continue;
            var exact = current.get(reading.key());
            if (exact != null && state.name().equals(exact.get(STATE)) && (state == PipelineState.FAILED
                    ? exact.containsKey("failure") : exact.containsKey("initialLoad")
                            || exact.get("snapshot") instanceof Document snapshot && !snapshot.isEmpty())) continue;
            var filter = scopeEpoch(reading.access(), reading.epoch()).append(STATE, state.name());
            if (state == PipelineState.FAILED) filter.append("failure", new Document("$type", "object"));
            else filter.append("$or", List.of(new Document("initialLoad", new Document("$type", "object")),
                    new Document("snapshot", new Document("$exists", true).append("$ne", new Document()))));
            filters.add(filter);
        }
        if (filters.isEmpty()) return Map.of();
        var result = new HashMap<String, Document>();
        for (var row : SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database).aggregate(session, List.of(
                new Document("$match", new Document("$or", filters)),
                new Document("$sort", new Document("_id.claimGeneration", -1).append("_id.executionGeneration", -1)),
                new Document("$group", new Document("_id", "$pipelineId").append("row", new Document("$first", "$$ROOT"))))))
            result.put(row.getString("_id"), row.get("row", Document.class));
        return result;
    }

    @Override public Optional<Observation> beforeCompletion(String pipeline, long checkpointEpoch) {
        access.requirePipeline(pipeline);
        if (checkpointEpoch < 1 || access.originalClaim() == null) return Optional.empty();
        var key = keyOfFence(access, checkpointEpoch - 1, access.originalClaim());
        return access.observationTransaction(session -> {
            var state = access.state(session, true);
            if (!Long.valueOf(checkpointEpoch).equals(state.get(EPOCH))
                    || StateJson.parse(state.getString("stateJson")) != PipelineState.COMPLETED) return Optional.empty();
            var row = collection().find(session, new Document("_id", key)).first();
            return decode(access, row, key).filter(value -> value.state() == PipelineState.RUNNING);
        });
    }

    private static Optional<Observation> decode(MongoCachePipelineAccess access, Document row, Document expected) {
        if (row == null) return Optional.empty();
        try {
            if (!expected.equals(row.get("_id"))
                    || !MongoPipelinePreparationFacts.scope(access.scope).equals(expected.get(SCOPE))
                    || !access.scope.pipelineId().equals(row.get("pipelineId"))) throw access.unreadable("observation");
            var decoded = MongoObservationStore.toObservation(new Document(row).append("_id", access.scope.pipelineId()));
            if (decoded.failure() != null && row.get("failure", Document.class).containsKey("diagnostic")) {
                var diagnostic = MongoRuntimeDiagnostic.read(row.get("failure", Document.class).get("diagnostic"));
                var original = decoded.failure();
                decoded = new Observation(decoded.pipelineId(), decoded.state(), decoded.metrics(), decoded.snapshot(),
                        decoded.positions(), new io.tapstate.core.lifecycle.ObservationFailure(original.code(), original.params(), diagnostic),
                        decoded.observedAt(), decoded.facts(), decoded.initialLoad());
            }
            var canonical = document(decoded, expected);
            var window = MongoResumeWindow.decode(row, access.scope.pipelineId());
            if (window != null) canonical.append(MongoResumeWindow.FIELD, MongoResumeWindow.encode(window));
            if (!canonical.equals(row)) throw access.unreadable("observation");
            return Optional.of(decoded);
        } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
            throw access.unreadable("observation");
        }
    }

    @Override public void save(Observation observation) {
        throw new IllegalArgumentException("scoped observations require the epoch measured by the publisher");
    }

    @Override public void save(Observation observation, long checkpointEpoch) {
        write(observation, checkpointEpoch, true, null);
    }

    @Override public boolean saveResumed(Observation observation, long checkpointEpoch, ResumeMetrics.Publication publication) {
        java.util.Objects.requireNonNull(publication);
        if (superseded(observation, publication)) return false;
        return write(observation, checkpointEpoch, !publication.expected().exists(), publication);
    }

    boolean updateExisting(Observation observation, long checkpointEpoch) {
        return write(observation, checkpointEpoch, false, null);
    }

    private boolean write(Observation observation, long checkpointEpoch, boolean create, ResumeMetrics.Publication publication) {
        var update = prepare(observation, checkpointEpoch, create, publication);
        var result = access.inspectObservationFailure( () -> StoreIo.call(() -> {
            requireWriteActive(create);
            try {
                return collection().updateOne(update.getFilter(), update.getUpdatePipeline(), update.getOptions());
            } catch (MongoWriteException conflict) {
                if (publication != null && conflict.getCode() == 11000) return null;
                throw conflict;
            }
        }));
        if (result == null || !create && result.getMatchedCount() != 1) return false;
        written(checkpointEpoch);
        return true;
    }

    record BatchWrite(MongoCachePipelineObservationStore store, Observation observation, long checkpointEpoch,
            ResumeMetrics.Publication publication) {}
    private record PreparedWrite(int index, BatchWrite write, UpdateOneModel<Document> update) {}

    static List<RuntimeException> writeBatch(List<BatchWrite> batch) {
        List<RuntimeException> failures = new ArrayList<>(Collections.nCopies(batch.size(), null));
        var prepared = new ArrayList<PreparedWrite>(batch.size());
        boolean verified = false;
        for (int i = 0; i < batch.size(); i++) {
            var write = batch.get(i);
            var store = write.store();
            try {
                if (superseded(write.observation(), write.publication())) throw observationChanged();
                var update = store.prepare(write.observation(), write.checkpointEpoch(), true, write.publication());
                boolean verifyStructure = !verified;
                StoreIo.run(() -> store.requireWriteActive(verifyStructure));
                verified = true;
                prepared.add(new PreparedWrite(i, write, update));
            } catch (RuntimeException failure) {
                failures.set(i, failure);
            }
        }
        if (prepared.isEmpty()) return Collections.unmodifiableList(failures);

        try {
            prepared.getFirst().write().store().access.inspectObservationFailure( () -> StoreIo.call(() -> {
                try {
                    prepared.getFirst().write().store().collection().bulkWrite(
                            prepared.stream().map(PreparedWrite::update).toList(),
                            new BulkWriteOptions().ordered(false));
                } catch (MongoBulkWriteException failure) {
                    if (failure.getWriteConcernError() != null) throw failure;
                    for (var error : failure.getWriteErrors()) {
                        int index = prepared.get(error.getIndex()).index();
                        failures.set(index, (error.getCode() == 11000 || error.getCode() == 40066)
                                && batch.get(index).publication() != null ? observationChanged()
                                : StoreIo.coded(new MongoWriteException(error, failure.getServerAddress())));
                    }
                }
                return null;
            }));
        } catch (RuntimeException failure) {
            for (var entry : prepared) {
                if (failures.get(entry.index()) == null) failures.set(entry.index(), failure);
            }
        }
        for (var entry : prepared) {
            if (failures.get(entry.index()) == null) entry.write().store().written(entry.write().checkpointEpoch());
        }
        return Collections.unmodifiableList(failures);
    }

    private void requireWriteActive(boolean verifyStructure) {
        access.requireObservationActive();

    }

    private UpdateOneModel<Document> prepare(Observation observation, long checkpointEpoch, boolean create, ResumeMetrics.Publication publication) {
        access.requirePipeline(observation.pipelineId());
        if (access.originalClaim() == null) throw MongoCachePipelineAccess.fenced();
        if (checkpointEpoch < 0 || observation.observedAt() == null) throw new IllegalArgumentException("invalid observation checkpoint");
        var id = keyOfFence(access, checkpointEpoch, access.originalClaim());
        var captured = document(observation, id);
        var filter = new Document("_id", id);
        if (publication != null) {
            if (!publication.key().equals(new ResumeMetrics.ObservationKey(access.scope, checkpointEpoch, access.originalClaim())))
                throw MongoCachePipelineAccess.claimFenced();
            var prior = publication.expected();
            filter.append("pipelineId", new Document("$exists", prior.exists()));
            filter.append("observedAt", prior.observedAt() == null ? new Document("$exists", false)
                    : java.util.Date.from(prior.observedAt()));
            filter.append(MongoResumeWindow.FIELD, prior.window() == null ? new Document("$exists", false)
                    : MongoResumeWindow.encode(prior.window()));
            if (publication.window() != null)
                captured.append(MongoResumeWindow.FIELD, MongoResumeWindow.bounded(publication.window(), observation.pipelineId()));
        }
        // A delayed sample may populate only its original key and cannot replace a later measurement.
        var keepExisting = new Document("$gte", List.of("$observedAt", captured.get("observedAt")));
        var replacement = new Document("$literal", captured);
        if (publication != null && create && publication.expected().exists()) {
            // A batch uses upsert to receive a per-entry duplicate-key error on a competing write.
            // A reaped predecessor must also fail its entry. An insert seed has no pipelineId.
            var exists = new Document("$eq", List.of("$pipelineId", new Document("$literal", observation.pipelineId())));
            replacement = new Document("$switch", new Document("branches",
                    List.of(new Document("case", exists).append("then", replacement))));
        }
        var changes = List.of(new Document("$replaceWith", new Document("$cond",
                List.of(keepExisting, "$$ROOT", replacement))));
        return new UpdateOneModel<>(filter, changes, new UpdateOptions().upsert(create));
    }

    private static boolean superseded(Observation value, ResumeMetrics.Publication publication) {
        return publication != null && publication.expected().observedAt() != null
                && !value.observedAt().isAfter(publication.expected().observedAt());
    }

    private static TapstateException observationChanged() {
        return new TapstateException(io.tapstate.spi.store.IoError.STORE_UNAVAILABLE,
                Map.of("detail", "the original observation changed before publication"), null);
    }

    private void written(long checkpointEpoch) {
        if ((WRITES.incrementAndGet() & 63) == 0) {
            var scope = access.scope;
            var prior = new Document("_id.scope.clusterId", scope.clusterId()).append("_id.scope.bindingVersion", scope.bindingVersion())
                    .append("_id.scope.cacheIncarnation", scope.cacheIncarnation()).append("_id.scope.pipelineId", scope.pipelineId());
            prior.append("$or", List.of(new Document("_id.scope.stateGeneration", new Document("$lt", scope.stateGeneration())),
                    scopeFilter(scope).append("_id.epoch", new Document("$lt", checkpointEpoch - 1))));
            reap(access.database, prior);
        }
    }

    private MongoCollection<Document> collection() {
        return SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database)
                .withWriteConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true));
    }

    private static Document document(Observation observation, Document id) {
        var document = MongoObservationStore.toDocument(observation).append("_id", id).append("pipelineId", observation.pipelineId());
        if (observation.failure() != null && !observation.failure().diagnostic().isEmpty()) {
            document.get("failure", Document.class).append("diagnostic", MongoRuntimeDiagnostic.document(observation.failure().diagnostic()));
        }
        return document;
    }

    static List<io.tapstate.core.lifecycle.MetricFact> pauseFacts(MongoCachePipelineAccess access,
            ClientSession session, long epoch) {
        var key = keyOfFence(access, epoch, access.originalClaim());
        var row = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database).find(session, new Document("_id", key))
                .projection(new Document("pipelineId", 1).append("state", 1).append("facts", 1)).first();
        if (row == null) return List.of();
        if (!key.equals(row.get("_id")) || !access.scope.pipelineId().equals(row.get("pipelineId")))
            throw access.unreadable("observation");
        if (!PipelineState.RUNNING.name().equals(row.get("state"))) return List.of();
        var facts = MongoObservationStore.readFacts(row, access.scope.pipelineId());
        if (row.containsKey("facts") && !facts.stream().map(MongoObservationStore::toDocument).toList().equals(row.get("facts")))
            throw access.unreadable("facts");
        return facts;
    }

    static Document keyOfFence(MongoCachePipelineAccess access, long epoch, WorkloadClaimFence fence) {
        return key(access, epoch, fence.claimGeneration(), fence.executionGeneration(), fence.topologyRevision(),
                fence.owner().nodeId(), fence.owner().bootId());
    }

    private static Document keyOfClaim(MongoCachePipelineAccess access, long epoch, Document claim) {
        return key(access, epoch, claim.getLong(CLAIM_GENERATION), claim.getLong(EXECUTION_GENERATION),
                claim.getLong(TOPOLOGY_REVISION), claim.getString("ownerNodeId"), claim.getString("ownerBootId"));
    }

    private static Document key(MongoCachePipelineAccess access, long epoch, long claim, long execution, long topology,
            String node, String boot) {
        return new Document(SCOPE, MongoPipelinePreparationFacts.scope(access.scope)).append(EPOCH, epoch)
                .append(CLAIM_GENERATION, claim).append(EXECUTION_GENERATION, execution).append(TOPOLOGY_REVISION, topology)
                .append("node", node).append("boot", boot);
    }

    static Document scopeFilter(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new Document("_id.scope", MongoPipelinePreparationFacts.scope(scope));
    }

    /** Best-effort housekeeping outside lifecycle transactions, with one bounded batch in flight. */
    static void reap(com.mongodb.client.MongoDatabase database, Document stale) {
        if (!REAPING.compareAndSet(false, true)) return;
        Thread.ofVirtual().name("tapstate-observation-reap").start(() -> {
            try {
                var rows = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database).withTimeout(1, TimeUnit.SECONDS);
                var ids = rows.find(stale).projection(new Document("_id", 1)).limit(32)
                        .map(row -> row.get("_id")).into(new java.util.ArrayList<>());
                if (!ids.isEmpty()) rows.deleteMany(new Document("_id", new Document("$in", ids)));
            } catch (RuntimeException unavailable) {
                System.getLogger(MongoCachePipelineObservationStore.class.getName()).log(System.Logger.Level.DEBUG,
                        "Old observation cleanup could not finish");
            } finally {
                REAPING.set(false);
            }
        });
    }

    private static Document scopeEpoch(MongoCachePipelineAccess access, long epoch) {
        return scopeFilter(access.scope).append("_id.epoch", epoch);
    }

    @Override public void delete(String pipelineId) {
        access.requirePipeline(pipelineId);
        throw MongoCachePipelineAccess.fenced();
    }
}
