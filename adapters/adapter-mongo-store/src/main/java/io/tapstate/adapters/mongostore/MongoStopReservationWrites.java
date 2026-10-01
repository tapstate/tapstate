package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Short checkpoint transactions that also write the desired and authority documents they prove. */
final class MongoStopReservationWrites {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final FindOneAndUpdateOptions RETURN_AFTER =
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    private static final Document PROVE_DESIRED = new Document("$inc", new Document("stopGuardVersion", 1L));
    private static final Document PROVE_CLAIM = new Document("$inc", new Document("fencedAppends", 1L));
    private static final Fenced FENCED = new Fenced();

    private final MongoClient client;
    private final MongoCollection<Document> states, desired, claims;

    MongoStopReservationWrites(MongoClient client, MongoCollection<Document> states,
            MongoCollection<Document> desired, MongoCollection<Document> claims) {
        this.client = Objects.requireNonNull(client, "client");
        this.states = Objects.requireNonNull(states, "states");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.claims = Objects.requireNonNull(claims, "claims");
    }

    Optional<StopReservation> reserve(CheckpointDoc expected, StopReservation proposal, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(proposal, "proposal");
        Objects.requireNonNull(at, "at");
        if (!expected.pipelineId().equals(proposal.pipelineId())
                || proposal.sourceEpoch() != expected.epoch()
                || proposal.reservedEpoch() != Math.addExact(expected.epoch(), 1)) {
            throw new IllegalArgumentException("reservation must name the checkpoint's next epoch");
        }
        Document marker = StopReservationDocument.write(proposal);
        return transact(proposal.pipelineId(), session -> {
            Document current = state(session, proposal.pipelineId());
            if (current == null || current.containsKey(StopReservationDocument.FIELD)
                    || !expected.equals(MongoStateStore.toCheckpoint(current))) { throw FENCED; }
            guardDesired(session, proposal.pipelineId(), proposal.originalDesired());
            guardAuthority(session, proposal.pipelineId(), proposal.subject(), proposal.authorityOrNull());
            Document filter = new Document("_id", proposal.pipelineId()).append("epoch", expected.epoch())
                    .append("stateJson", expected.stateJson())
                    .append(StopReservationDocument.FIELD, new Document("$exists", false));
            Document update = new Document("$set", new Document(StopReservationDocument.FIELD, marker)
                    .append("touchMillis", at.toEpochMilli()))
                    .append("$inc", new Document("epoch", 1L));
            Document next = states.findOneAndUpdate(session, filter, update, RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return StopReservationDocument.read(proposal.pipelineId(),
                    MongoStateStore.toCheckpoint(next).epoch(),
                    requireMarker(next, proposal.pipelineId()));
        });
    }

    Optional<StopReservation> rebind(StopReservation expected, StopAuthority successor, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(at, "at");
        StopReservation rebound = expected.rebind(successor, Math.addExact(expected.reservedEpoch(), 1));
        Document marker = StopReservationDocument.write(rebound);
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), expected.originalDesired());
            guardAuthority(session, expected.pipelineId(), rebound.subject(), successor);
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$set", new Document(StopReservationDocument.FIELD, marker)
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return StopReservationDocument.read(expected.pipelineId(), MongoStateStore.toCheckpoint(next).epoch(),
                    requireMarker(next, expected.pipelineId()));
        });
    }

    Optional<StopReservation> replace(StopReservation expected, StopReservation successor, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(at, "at");
        String previousCluster = switch (expected.subject()) {
            case StopReservation.NoJob absent -> absent.clusterId();
            case StopReservation.ExistingJob old -> old.oldJob().clusterId();
        };
        String successorCluster = switch (successor.subject()) {
            case StopReservation.NoJob absent -> absent.clusterId();
            case StopReservation.ExistingJob old -> old.oldJob().clusterId();
        };
        if (!expected.pipelineId().equals(successor.pipelineId())
                || expected.token().equals(successor.token())
                || successor.sourceEpoch() != expected.reservedEpoch()
                || successor.reservedEpoch() != Math.addExact(expected.reservedEpoch(), 1)
                || expected.originalDesired().equals(successor.originalDesired())
                || !previousCluster.equals(successorCluster)) {
            throw new IllegalArgumentException("a successor stop must replace the exact epoch with a fresh intent and token");
        }
        Document marker = StopReservationDocument.write(successor);
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), successor.originalDesired());
            guardAuthority(session, expected.pipelineId(), successor.subject(), successor.authorityOrNull());
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$set", new Document(StopReservationDocument.FIELD, marker)
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return StopReservationDocument.read(expected.pipelineId(), MongoStateStore.toCheckpoint(next).epoch(),
                    requireMarker(next, expected.pipelineId()));
        });
    }

    Optional<CheckpointDoc> complete(StopReservation expected, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(at, "at");
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), expected.originalDesired());
            guardAuthority(session, expected.pipelineId(), expected.subject(), expected.authorityOrNull());
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$set", new Document("stateJson", StateJson.of(PipelineState.STOPPED))
                            .append("touchMillis", at.toEpochMilli()))
                            .append("$unset", new Document(StopReservationDocument.FIELD, true))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(next);
        });
    }

    Optional<CheckpointDoc> retire(StopReservation expected, DesiredState successor,
            StopAuthority authority, Instant at) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(at, "at");
        if (!expected.pipelineId().equals(successor.pipelineId())
                || expected.originalDesired().equals(successor)
                || authority == null && !(expected.subject() instanceof StopReservation.NoJob)) {
            throw new IllegalArgumentException("retirement requires the superseding intent and current authority");
        }
        String clusterId = switch (expected.subject()) {
            case StopReservation.NoJob absent -> absent.clusterId();
            case StopReservation.ExistingJob old -> old.oldJob().clusterId();
        };
        if (authority != null && !clusterId.equals(authority.clusterId())) {
            throw new IllegalArgumentException("retirement authority belongs to another cluster");
        }
        return transact(expected.pipelineId(), session -> {
            requireCurrent(session, expected);
            guardDesired(session, expected.pipelineId(), successor);
            guardAuthority(session, expected.pipelineId(), expected.subject(), authority);
            Document next = states.findOneAndUpdate(session, exactMarker(expected),
                    new Document("$unset", new Document(StopReservationDocument.FIELD, true))
                            .append("$set", new Document("touchMillis", at.toEpochMilli()))
                            .append("$inc", new Document("epoch", 1L)), RETURN_AFTER);
            if (next == null) { throw FENCED; }
            return MongoStateStore.toCheckpoint(next);
        });
    }

    private <T> Optional<T> transact(String id, Function<ClientSession, T> action) {
        try {
            return Optional.of(StoreIo.call(id, () -> {
                try (ClientSession session = client.startSession()) {
                    return session.withTransaction(() -> action.apply(session), TRANSACTION);
                }
            }));
        } catch (Fenced lost) {
            return Optional.empty();
        }
    }

    private Document state(ClientSession session, String id) {
        return states.find(session, new Document("_id", id)).first();
    }

    private void requireCurrent(ClientSession session, StopReservation expected) {
        Document current = state(session, expected.pipelineId());
        if (current == null || MongoStateStore.toCheckpoint(current).epoch() != expected.reservedEpoch()) {
            throw FENCED;
        }
        Document marker = requireMarkerOrAbsent(current, expected.pipelineId());
        if (marker == null || !expected.equals(StopReservationDocument.read(expected.pipelineId(),
                expected.reservedEpoch(), marker))) {
            throw FENCED;
        }
    }

    private static Document exactMarker(StopReservation expected) {
        return new Document("_id", expected.pipelineId())
                .append("epoch", expected.reservedEpoch())
                .append(StopReservationDocument.FIELD + ".token", expected.token())
                .append(StopReservationDocument.FIELD + ".reservedEpoch", expected.reservedEpoch());
    }

    private static Document requireMarker(Document state, String id) {
        Document marker = requireMarkerOrAbsent(state, id);
        if (marker == null) { throw FENCED; }
        return marker;
    }

    private static Document requireMarkerOrAbsent(Document state, String id) {
        if (!state.containsKey(StopReservationDocument.FIELD)) { return null; }
        if (!(state.get(StopReservationDocument.FIELD) instanceof Document marker)) {
            throw unreadable(id, StopReservationDocument.FIELD);
        }
        return marker;
    }

    /** The guard update is a real write so a concurrent desired replacement conflicts with this txn. */
    private void guardDesired(ClientSession session, String id, DesiredState expected) {
        Document stored = desired.find(session, new Document("_id", id)).first();
        if (stored == null) { throw FENCED; }
        if (!(stored.get("_id") instanceof String)
                || !(stored.get("targetState") instanceof String)
                || !(stored.get("revision") instanceof String)
                || stored.containsKey("purgeState") && !(stored.get("purgeState") instanceof Boolean)
                || stored.containsKey("reassemble") && !(stored.get("reassemble") instanceof Boolean)
                || stored.get("assemblyRevision") != null && !(stored.get("assemblyRevision") instanceof String)
                || stored.get("rebuiltAtStateEpoch") != null
                        && !(stored.get("rebuiltAtStateEpoch") instanceof Long
                                || stored.get("rebuiltAtStateEpoch") instanceof Integer)) {
            throw unreadable(id, "pipelineDesired");
        }
        if (!MongoDesiredStore.toDesired(stored).equals(expected)) { throw FENCED; }
        Object guard = stored.get("stopGuardVersion");
        if (guard != null && !(guard instanceof Long || guard instanceof Integer)) {
            throw unreadable(id, "stopGuardVersion");
        }
        Document filter = new Document("_id", id)
                .append("targetState", stored.get("targetState"))
                .append("revision", stored.get("revision"))
                .append("purgeState", stored.get("purgeState"))
                .append("assemblyRevision", stored.get("assemblyRevision"))
                .append("reassemble", stored.get("reassemble"))
                .append("rebuiltAtStateEpoch", stored.get("rebuiltAtStateEpoch"));
        if (desired.updateOne(session, filter, PROVE_DESIRED).getMatchedCount() != 1) { throw FENCED; }
    }

    /** Current authority and this checkpoint transition write in one short Mongo transaction. */
    private void guardAuthority(ClientSession session, String id,
            StopReservation.Subject subject, StopAuthority chosen) {
        if (chosen != null) {
            guardKnownAuthority(session, id, chosen);
            return;
        }
        if (!(subject instanceof StopReservation.NoJob noJob)) {
            throw new IllegalArgumentException("a real old job requires current execution authority");
        }
        guardColdNoJob(session, id, noJob.clusterId());
    }

    private void guardKnownAuthority(ClientSession session, String id, StopAuthority authority) {
        Document key = claimId(authority.clusterId(), id);
        Document stored = claims.find(session, new Document("_id", key)).first();
        if (stored == null) { throw FENCED; }
        validateClaim(stored, id, true);
        Document filter;
        WorkloadClaimFence fence = authority.claim();
        if (fence != null) {
            if (!id.equals(fence.key().resourceId())) { throw new IllegalArgumentException("claim names another pipeline"); }
            filter = new Document("_id", key)
                    .append("ownerNodeId", fence.owner().nodeId())
                    .append("ownerBootId", fence.owner().bootId())
                    .append("claimGeneration", fence.claimGeneration())
                    .append("executionGeneration", fence.executionGeneration())
                    .append("topologyRevision", fence.topologyRevision())
                    .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        } else {
            filter = new Document("_id", key).append("executionGeneration", authority.executionGeneration())
                    .append("$or", List.of(
                            new Document("ownerNodeId", new Document("$exists", false)),
                            new Document("$and", List.of(
                                    new Document("leaseUntil", new Document("$type", "date")),
                                    new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))));
        }
        if (claims.updateOne(session, filter, PROVE_CLAIM).getMatchedCount() != 1) { throw FENCED; }
    }

    /** Creates no generation or lease: only the already used claim-write guard on the canonical id. */
    private void guardColdNoJob(ClientSession session, String id, String clusterId) {
        Document key = claimId(clusterId, id);
        Document stored = claims.find(session, new Document("_id", key)).first();
        if (stored != null) { validateClaim(stored, id, false); }
        Document filter = new Document("_id", key)
                .append("ownerNodeId", new Document("$exists", false))
                .append("$or", List.of(
                        new Document("executionGeneration", new Document("$exists", false)),
                        new Document("executionGeneration", 0L)));
        Document identity = new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .append("resourceId", id);
        Document update = new Document("$setOnInsert", identity)
                .append("$inc", new Document("fencedAppends", 1L));
        try {
            var applied = claims.updateOne(session, filter, update, new UpdateOptions().upsert(true));
            if (applied.getMatchedCount() != 1 && applied.getUpsertedId() == null) { throw FENCED; }
        } catch (MongoException race) {
            if (ErrorCategory.fromErrorCode(race.getCode()) == ErrorCategory.DUPLICATE_KEY) { throw FENCED; }
            throw race;
        }
    }

    private static Document claimId(String clusterId, String id) {
        return new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .append("resourceId", id);
    }

    private static void validateClaim(Document stored, String id, boolean requireGeneration) {
        if (!(stored.get("clusterId") instanceof String clusterId) || clusterId.isBlank()
                || !WorkloadClaimType.PIPELINE_ACTUATION.name().equals(stored.get("resourceType"))
                || !id.equals(stored.get("resourceId"))) {
            throw unreadable(id, "workloadClaims.identity");
        }
        boolean hasOwner = stored.containsKey("ownerNodeId");
        Object generation = stored.get("executionGeneration");
        if ((requireGeneration || hasOwner) && !(generation instanceof Long || generation instanceof Integer)
                || generation != null && !(generation instanceof Long || generation instanceof Integer)
                || generation instanceof Number number && number.longValue() < 0) {
            throw unreadable(id, "workloadClaims.executionGeneration");
        }
        if (hasOwner != stored.containsKey("ownerBootId")) { throw unreadable(id, "workloadClaims.owner"); }
        if (hasOwner && (!(stored.get("ownerNodeId") instanceof String ownerNode) || ownerNode.isBlank()
                || !(stored.get("ownerBootId") instanceof String ownerBoot) || ownerBoot.isBlank()
                || !(stored.get("claimGeneration") instanceof Long
                        || stored.get("claimGeneration") instanceof Integer)
                || !(stored.get("topologyRevision") instanceof Long
                        || stored.get("topologyRevision") instanceof Integer)
                || !(stored.get("leaseUntil") instanceof java.util.Date)
                || ((Number) stored.get("claimGeneration")).longValue() < 1
                || ((Number) stored.get("topologyRevision")).longValue() < 0)) {
            throw unreadable(id, "workloadClaims.owner");
        }
        if (!hasOwner && (stored.containsKey("claimGeneration")
                || stored.containsKey("topologyRevision") || stored.containsKey("leaseUntil"))) {
            throw unreadable(id, "workloadClaims.owner");
        }
        Object guard = stored.get("fencedAppends");
        if (guard != null && !(guard instanceof Long || guard instanceof Integer)
                || guard instanceof Number number && number.longValue() < 0) {
            throw unreadable(id, "workloadClaims.fencedAppends");
        }
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }

    private static final class Fenced extends RuntimeException {
        private Fenced() { super(null, null, false, false); }
    }
}
