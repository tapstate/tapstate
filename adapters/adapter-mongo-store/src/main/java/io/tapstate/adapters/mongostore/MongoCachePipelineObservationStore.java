package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** An observation is isolated by the checkpoint and execution that measured it. */
final class MongoCachePipelineObservationStore implements ObservationStore {
    private static final AtomicInteger WRITES = new AtomicInteger();
    private static final AtomicBoolean REAPING = new AtomicBoolean();
    private final MongoCachePipelineAccess access;

    MongoCachePipelineObservationStore(MongoCachePipelineAccess access) { this.access = access; }

    record Reading(MongoCachePipelineAccess access, Document state, Document claim, boolean live) {
        String pipeline() { return access.scope.pipelineId(); }
        long epoch() { return state.getLong("epoch"); }
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
        MongoCacheWorkloadClaimStore.requireCollection(access.database);
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
        var claim = row == null ? null : row.get("claim", Document.class);
        access.requirePreparedClaim(session, claim);
        return new Reading(access, state, claim, Boolean.TRUE.equals(row.get("live")));
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
                : new io.tapstate.core.lifecycle.ObservationFailure("engine.execution-not-authorized", Map.of("pipeline", reading.pipeline()));
    }

    /** A previous RUNNING observation can witness progress, without serving its stale payload. */
    static java.util.Set<String> unavailableRunning(ClientSession session, MongoCachePipelineAccess access,
            List<Reading> readings, Map<Object, Document> current) {
        var unavailable = new java.util.HashSet<String>();
        var previousRuns = new java.util.ArrayList<Document>();
        for (var reading : readings) {
            if (reading.actual() != PipelineState.RUNNING) continue;
            var exact = current.get(reading.key());
            if (exact != null && PipelineState.RUNNING.name().equals(exact.get("state"))) {
                if (!reading.live()) unavailable.add(reading.pipeline());
            } else {
                var prefix = new Document("scope", MongoPipelinePreparationFacts.scope(reading.access().scope))
                        .append("epoch", reading.epoch());
                previousRuns.add(new Document("_id", new Document("$gte",
                        new Document(prefix).append("claimGeneration", new org.bson.types.MinKey()))
                        .append("$lt", new Document(prefix).append("claimGeneration", new org.bson.types.MaxKey()))));
            }
        }
        if (!previousRuns.isEmpty()) {
            for (var row : SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database).aggregate(session, List.of(
                    new Document("$match", new Document("$or", previousRuns).append("state", PipelineState.RUNNING.name())),
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
            if (exact != null && state.name().equals(exact.get("state")) && (state == PipelineState.FAILED
                    ? exact.containsKey("failure") : exact.containsKey("initialLoad")
                            || exact.get("snapshot") instanceof Document snapshot && !snapshot.isEmpty())) continue;
            var filter = scopeEpoch(reading.access(), reading.epoch()).append("state", state.name());
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
            var state = access.state(session);
            if (!Long.valueOf(checkpointEpoch).equals(state.get("epoch"))
                    || StateJson.parse(state.getString("stateJson")) != PipelineState.COMPLETED) return Optional.empty();
            var row = collection().find(session, new Document("_id", key)).first();
            return decode(access, row, key).filter(value -> value.state() == PipelineState.RUNNING);
        });
    }

    private static Optional<Observation> decode(MongoCachePipelineAccess access, Document row, Document expected) {
        if (row == null) return Optional.empty();
        try {
            if (!expected.equals(row.get("_id"))
                    || !MongoPipelinePreparationFacts.scope(access.scope).equals(expected.get("scope"))
                    || !access.scope.pipelineId().equals(row.get("pipelineId"))) throw access.unreadable("observation");
            var decoded = MongoObservationStore.toObservation(new Document(row).append("_id", access.scope.pipelineId()));
            if (!document(decoded, expected).equals(row)) throw access.unreadable("observation");
            return Optional.of(decoded);
        } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
            throw access.unreadable("observation");
        }
    }

    @Override public void save(Observation observation) {
        throw new IllegalArgumentException("scoped observations require the epoch measured by the publisher");
    }

    @Override public void save(Observation observation, long checkpointEpoch) {
        access.requirePipeline(observation.pipelineId());
        if (access.originalClaim() == null) throw MongoCachePipelineAccess.fenced();
        if (checkpointEpoch < 0 || observation.observedAt() == null) throw new IllegalArgumentException("invalid observation checkpoint");
        var id = keyOfFence(access, checkpointEpoch, access.originalClaim());
        var captured = document(observation, id);
        // One document, one atomic update. A delayed sample may populate only its original key.
        var keepExisting = new Document("$gte", List.of("$observedAt", captured.get("observedAt")));
        StoreIo.run(() -> {
            access.requireObservationActive();
            access.verifyObservationStructure();
            collection().withWriteConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).updateOne(new Document("_id", id),
                    List.of(new Document("$replaceWith", new Document("$cond",
                            List.of(keepExisting, "$$ROOT", new Document("$literal", captured))))),
                    new UpdateOptions().upsert(true));
        });
        if ((WRITES.incrementAndGet() & 63) == 0) {
            var scope = access.scope;
            var prior = new Document("_id.scope.clusterId", scope.clusterId()).append("_id.scope.bindingVersion", scope.bindingVersion())
                    .append("_id.scope.cacheIncarnation", scope.cacheIncarnation()).append("_id.scope.pipelineId", scope.pipelineId());
            prior.append("$or", List.of(new Document("_id.scope.stateGeneration", new Document("$lt", scope.stateGeneration())),
                    scopeFilter(scope).append("_id.epoch", new Document("$lt", checkpointEpoch - 1))));
            reap(access.database, prior);
        }
    }

    private com.mongodb.client.MongoCollection<Document> collection() {
        return SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database);
    }

    private static Document document(Observation observation, Document id) {
        return MongoObservationStore.toDocument(observation).append("_id", id).append("pipelineId", observation.pipelineId());
    }

    private static Document keyOfFence(MongoCachePipelineAccess access, long epoch, WorkloadClaimFence fence) {
        return key(access, epoch, fence.claimGeneration(), fence.executionGeneration(), fence.topologyRevision(),
                fence.owner().nodeId(), fence.owner().bootId());
    }

    private static Document keyOfClaim(MongoCachePipelineAccess access, long epoch, Document claim) {
        return key(access, epoch, claim.getLong("claimGeneration"), claim.getLong("executionGeneration"),
                claim.getLong("topologyRevision"), claim.getString("ownerNodeId"), claim.getString("ownerBootId"));
    }

    private static Document key(MongoCachePipelineAccess access, long epoch, long claim, long execution, long topology,
            String node, String boot) {
        return new Document("scope", MongoPipelinePreparationFacts.scope(access.scope)).append("epoch", epoch)
                .append("claimGeneration", claim).append("executionGeneration", execution).append("topologyRevision", topology)
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
