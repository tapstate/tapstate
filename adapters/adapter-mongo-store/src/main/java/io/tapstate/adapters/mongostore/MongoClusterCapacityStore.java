package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryIntentFingerprint;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Ordinary and recovery execution demand share one profile-serialized Mongo occupancy collection. */
public final class MongoClusterCapacityStore implements ClusterCapacityStore {
    private static final FindOneAndUpdateOptions AFTER = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    final MongoClusterProfileStore profileStore;
    final MongoWorkloadClaimStore workloadStore;
    final MongoCollection<Document> occupancy;
    final MongoCollection<Document> claims;
    final MongoCollection<Document> profiles;
    final MongoCollection<Document> membership;
    final MongoCollection<Document> nodes;
    final MongoCollection<Document> desired;
    final MongoCollection<Document> states;
    final MongoCollection<Document> artifacts;

    public MongoClusterCapacityStore(MongoClusterProfileStore profileStore, MongoWorkloadClaimStore workloadStore,
            MongoCollection<Document> occupancy, MongoCollection<Document> claims, MongoCollection<Document> profiles,
            MongoCollection<Document> membership, MongoCollection<Document> nodes, MongoCollection<Document> desired,
            MongoCollection<Document> states, MongoCollection<Document> artifacts) {
        this.profileStore = Objects.requireNonNull(profileStore, "profileStore");
        this.workloadStore = Objects.requireNonNull(workloadStore, "workloadStore");
        this.occupancy = durable(occupancy);
        this.claims = durable(claims);
        this.profiles = durable(profiles);
        this.membership = durable(membership);
        this.nodes = durable(nodes);
        this.desired = durable(desired);
        this.states = durable(states);
        this.artifacts = durable(artifacts);
    }

    static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return Objects.requireNonNull(collection, "collection").withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public Result reserve(WorkloadClaim expected, ClusterExecutionProfile profile, String incarnation,
            String intentFingerprint, Map<String, ClusterCapacityDemand> demand, ClusterCapacityLimits limits, Duration ttl) {
        positive(ttl);
        Objects.requireNonNull(limits, "limits");
        Map<String, ClusterCapacityDemand> requested = Map.copyOf(demand);
        ClusterRecoveryKey key = new ClusterRecoveryKey(expected.key().clusterId(), expected.key().resourceId(), incarnation);
        return profileStore.transaction(session -> {
            if (workloadStore.conditionTouchClaim(session, WorkloadClaimFence.from(expected)) == null) {
                return result(Outcome.STALE_CLAIM, null, null, List.of());
            }
            Context context = guard(session, key, profile, intentFingerprint, true);
            if (context.outcome != Outcome.APPLIED) {
                return result(context.outcome, null, null, List.of());
            }
            Document existing = occupancy.find(session, new Document("clusterId", key.clusterId())
                    .append("pipelineId", key.pipelineId()).append("incarnationId", key.incarnation())
                    .append("intentFingerprint", intentFingerprint).append("pipelineClaim.claimGeneration", expected.claimGeneration())
                    .append("pipelineClaim.ownerNodeId", expected.owner().nodeId())
                    .append("pipelineClaim.ownerBootId", expected.owner().bootId())).first();
            if (existing != null && !fenced(session, existing, context.now)) {
                ClusterCapacityReservation previous = reservation(existing);
                Instant deadline = context.now.plus(ttl);
                if (deadline.isAfter(expected.leaseUntil())) {
                    deadline = expected.leaseUntil();
                }
                if (!deadline.isAfter(context.now)) {
                    return result(Outcome.STALE_CLAIM, previous, null, List.of());
                }
                ClusterCapacityReservation renewed = new ClusterCapacityReservation(previous.reservationId(),
                        previous.clusterId(), previous.pipelineId(), previous.incarnationId(), previous.intentFingerprint(),
                        previous.profile(), previous.pipelineClaim(), previous.demandByNode(), previous.reservedAt(), deadline,
                        previous.executionGeneration(), previous.nativeJobId());
                replace(session, existing, renewed, expected.leaseUntil());
                return result(Outcome.ALREADY_RESERVED, renewed, null, List.of());
            }
            return reserve(session, context, WorkloadClaimFence.from(expected), requested, limits, ttl, false);
        });
    }

    Result reserve(ClientSession session, Context context, WorkloadClaimFence pipelineClaim,
            Map<String, ClusterCapacityDemand> demand, ClusterCapacityLimits limits, Duration ttl, boolean recovery) {
        if (demand.isEmpty() || !context.liveNodes.equals(demand.keySet())) {
            return result(Outcome.UNKNOWN_DEMAND, null, null, List.of());
        }
        Occupied occupied = occupied(session, context.profile.clusterId(), context.now);
        if (occupied.unknown) {
            return result(Outcome.UNKNOWN_DEMAND, null, null, List.of());
        }
        List<ClusterCapacityLimits.Violation> violations = new ArrayList<>();
        for (Map.Entry<String, ClusterCapacityDemand> entry : new TreeMap<>(demand).entrySet()) {
            violations.addAll(limits.violations(occupied.demand.getOrDefault(entry.getKey(), ClusterCapacityDemand.ZERO), entry.getValue()));
        }
        if (!violations.isEmpty()) {
            return result(Outcome.CAPACITY_REFUSED, null, null, violations);
        }
        Instant deadline = context.now.plus(ttl);
        Document authority = claims.find(session, WorkloadClaimDocuments.live(pipelineClaim)).first();
        if (!recovery && authority == null) {
            return result(Outcome.STALE_CLAIM, null, null, List.of());
        }
        if (authority != null && deadline.isAfter(authority.getDate("leaseUntil").toInstant())) {
            deadline = authority.getDate("leaseUntil").toInstant();
        }
        if (!deadline.isAfter(context.now)) {
            return result(Outcome.STALE_CLAIM, null, null, List.of());
        }
        ClusterCapacityReservation reservation = new ClusterCapacityReservation(UUID.randomUUID().toString(),
                context.key.clusterId(), context.key.pipelineId(), context.key.incarnation(), context.intentFingerprint,
                context.profile, pipelineClaim, demand, context.now, deadline, null, null);
        Document stored = document(reservation).append("revision", 1L).append("recovery", recovery)
                .append("authorityUntil", authority == null ? Date.from(context.now) : authority.getDate("leaseUntil"));
        occupancy.insertOne(session, stored);
        return result(Outcome.APPLIED, reservation, null, List.of());
    }

    @Override
    public Result advanceExecution(ClusterCapacityReservation expected, WorkloadClaim pipelineClaim, Set<String> executionNodes) {
        return profileStore.transaction(session -> advanceExecution(session, expected, pipelineClaim, executionNodes));
    }

    Result advanceExecution(ClientSession session, ClusterCapacityReservation expected, WorkloadClaim pipelineClaim,
            Set<String> executionNodes) {
        Document current = exact(session, expected);
        if (current == null || expected.executionGeneration() != null) {
            return result(Outcome.STALE_EXECUTION, null, null, List.of());
        }
        if (!expected.pipelineClaim().equals(WorkloadClaimFence.from(pipelineClaim))
                || workloadStore.conditionTouchClaim(session, WorkloadClaimFence.from(pipelineClaim)) == null) {
            return result(Outcome.STALE_CLAIM, expected, null, List.of());
        }
        Context context = guard(session, key(expected), expected.profile(), expected.intentFingerprint(), true);
        if (context.outcome != Outcome.APPLIED) {
            return result(context.outcome, expected, null, List.of());
        }
        if (!expected.demandByNode().keySet().equals(executionNodes) || !context.liveNodes.equals(executionNodes)
                || !expected.deadline().isAfter(context.now)) {
            return result(Outcome.STALE_EXECUTION, expected, null, List.of());
        }
        OptionalAdvance allocated = allocate(session, pipelineClaim, context.topologyRevision, executionNodes);
        if (allocated.claim == null) {
            return result(Outcome.STALE_CLAIM, expected, null, List.of());
        }
        ClusterCapacityReservation advanced = new ClusterCapacityReservation(expected.reservationId(), expected.clusterId(),
                expected.pipelineId(), expected.incarnationId(), expected.intentFingerprint(), expected.profile(),
                WorkloadClaimFence.from(allocated.claim), expected.demandByNode(), expected.reservedAt(), expected.deadline(),
                allocated.claim.executionGeneration(), null);
        if (claims.updateOne(session, WorkloadClaimDocuments.live(WorkloadClaimFence.from(allocated.claim)),
                new Document("$set", new Document("executionIncarnation", context.key.incarnation())
                        .append("executionRevision", context.intent.getString("revision")))).getMatchedCount() != 1) {
            throw new IllegalStateException("the allocated execution changed before its artifact receipt");
        }
        WorkloadClaim recorded = MongoWorkloadClaimStore.readDocument(claims.find(session,
                WorkloadClaimDocuments.live(WorkloadClaimFence.from(allocated.claim))).first());
        replace(session, current, advanced, allocated.claim.leaseUntil());
        return result(Outcome.APPLIED, advanced, recorded, List.of());
    }

    private OptionalAdvance allocate(ClientSession session, WorkloadClaim expected, long topology, Set<String> nodes) {
        return new OptionalAdvance(workloadStore.advanceExecution(session, expected, topology, nodes).orElse(null));
    }

    @Override
    public Result submitted(ClusterCapacityReservation expected, WorkloadClaimFence pipelineClaim, String nativeJobId) {
        return profileStore.transaction(session -> submitted(session, expected, pipelineClaim, nativeJobId));
    }

    Result submitted(ClientSession session, ClusterCapacityReservation expected, WorkloadClaimFence pipelineClaim, String jobId) {
        Objects.requireNonNull(jobId, "jobId");
        Document current = exact(session, expected);
        if (current == null || expected.executionGeneration() == null || expected.nativeJobId() != null) {
            return result(Outcome.STALE_EXECUTION, expected, null, List.of());
        }
        Document authority = workloadStore.conditionTouchClaim(session, pipelineClaim);
        if (!expected.pipelineClaim().equals(pipelineClaim) || authority == null) {
            return result(Outcome.STALE_CLAIM, expected, null, List.of());
        }
        Context context = guard(session, key(expected), expected.profile(), expected.intentFingerprint(), true);
        if (context.outcome != Outcome.APPLIED) {
            return result(context.outcome, expected, null, List.of());
        }
        ClusterCapacityReservation submitted = new ClusterCapacityReservation(expected.reservationId(), expected.clusterId(),
                expected.pipelineId(), expected.incarnationId(), expected.intentFingerprint(), expected.profile(), pipelineClaim,
                expected.demandByNode(), expected.reservedAt(), expected.deadline(), expected.executionGeneration(), jobId);
        replace(session, current, submitted, authority.getDate("leaseUntil").toInstant());
        return result(Outcome.APPLIED, submitted, null, List.of());
    }

    @Override
    public Result release(ClusterCapacityReservation expected) {
        return profileStore.transaction(session -> {
            if (!profileStore.profileGuard(session, expected.clusterId(), expected.profile().generation())) {
                // A newer profile can establish fencing, but an absent profile is never that evidence.
                Document currentProfile = profiles.find(session, new Document("_id", expected.clusterId())).first();
                if (currentProfile == null || ClusterRecoveryDocuments.number(currentProfile, "generation") <= expected.profile().generation()) {
                    return result(Outcome.STALE_PROFILE, expected, null, List.of());
                }
                if (!profileStore.profileGuard(session, expected.clusterId(), ClusterRecoveryDocuments.number(currentProfile, "generation"))) {
                    return result(Outcome.STALE_PROFILE, expected, null, List.of());
                }
            }
            Document current = exact(session, expected);
            if (current == null) {
                return result(Outcome.STALE_EXECUTION, null, null, List.of());
            }
            Instant now = clock(session, expected.clusterId());
            if (!fenced(session, current, now)) {
                return result(Outcome.STALE_CLAIM, expected, null, List.of());
            }
            occupancy.deleteOne(session, new Document("_id", expected.reservationId()).append("revision", current.get("revision")));
            return result(Outcome.APPLIED, expected, null, List.of());
        });
    }

    @Override
    public Map<String, ClusterCapacityDemand> occupied(String clusterId) {
        return profileStore.transaction(session -> {
            Document profile = profiles.find(session, new Document("_id", clusterId)).first();
            if (profile == null || !profileStore.profileGuard(session, clusterId, ClusterRecoveryDocuments.number(profile, "generation"))) {
                throw new TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            }
            Occupied occupied = occupied(session, clusterId, clock(session, clusterId));
            if (occupied.unknown) {
                throw capacityError("unknown", "unknown-demand", "unknown", "unknown", "unknown");
            }
            return Map.copyOf(occupied.demand);
        });
    }

    Occupied occupied(ClientSession session, String clusterId, Instant now) {
        Map<String, ClusterCapacityDemand> total = new LinkedHashMap<>();
        List<Document> records = occupancy.find(session, new Document("clusterId", clusterId)).into(new ArrayList<>());
        Set<String> recordedExecutions = new java.util.HashSet<>();
        for (Document record : records) {
            ClusterCapacityReservation reservation = reservation(record);
            if (fenced(session, record, now) && !reservation.deadline().isAfter(now)) {
                // An unsubmitted recovery retains its authority evidence until its queue step releases it.
                if (!record.getBoolean("recovery", false) || reservation.nativeJobId() != null) {
                    occupancy.deleteOne(session, new Document("_id", record.get("_id")).append("revision", record.get("revision")));
                }
                continue;
            }
            if (reservation.executionGeneration() != null) {
                recordedExecutions.add(reservation.pipelineId() + ":" + reservation.executionGeneration());
            }
            try {
                reservation.demandByNode().forEach((node, demand) -> total.merge(node, demand, ClusterCapacityDemand::plus));
            } catch (ArithmeticException overflow) {
                return new Occupied(Map.of(), true);
            }
        }
        for (Document claim : claims.find(session, MongoClusterProfileStore.liveClaims(clusterId,
                WorkloadClaimType.PIPELINE_ACTUATION)).into(new ArrayList<>())) {
            long execution = ClusterRecoveryDocuments.number(claim, "executionGeneration");
            if (execution == 0 || recordedExecutions.contains(claim.getString("resourceId") + ":" + execution)) {
                continue;
            }
            Document actual = states.find(session, new Document("_id", claim.getString("resourceId"))).first();
            Document executionProfile = claim.get("executionProfile", Document.class);
            boolean oldProfile = executionProfile != null && ClusterRecoveryDocuments.number(executionProfile, "generation")
                    < ClusterRecoveryDocuments.number(claim, "profileGeneration");
            Document profile = profiles.find(session, new Document("_id", clusterId)).first();
            boolean legacyCold = executionProfile == null && !claim.containsKey("executionProfileVersion")
                    && profile != null && profile.getDate("legacyAuthorityRetiredAt") != null;
            if (!oldProfile && !legacyCold && actual != null && ("RUNNING".equals(actual.getString("stateJson"))
                    || "STARTING".equals(actual.getString("stateJson")))) {
                return new Occupied(Map.of(), true);
            }
        }
        return new Occupied(Map.copyOf(total), false);
    }

    Context guard(ClientSession session, ClusterRecoveryKey key, ClusterExecutionProfile profile, String fingerprint, boolean runningIntent) {
        Document currentProfile = profiles.findOneAndUpdate(session, new Document("_id", key.clusterId())
                .append("generation", profile.generation()).append("hash", profile.profile().hash()),
                List.of(new Document("$set", new Document("recoveryStoreTime", "$$NOW")
                        .append("recoveryWriteSerial", new Document("$add", List.of(new Document("$ifNull", List.of("$recoveryWriteSerial", 0L)), 1L))))), AFTER);
        if (currentProfile == null) {
            return Context.refused(Outcome.STALE_PROFILE);
        }
        Instant now = currentProfile.getDate("recoveryStoreTime").toInstant();
        Document committed = membership.find(session, new Document("_id", key.clusterId())
                .append("profileGeneration", profile.generation())).first();
        if (committed == null) {
            return Context.refused(Outcome.WAITING_QUORUM);
        }
        Set<String> committedNodes = Set.copyOf(committed.getList("activeNodeIds", String.class));
        Set<String> live = new java.util.HashSet<>();
        for (Document node : claims.find(session, MongoClusterProfileStore.liveClaims(key.clusterId(), WorkloadClaimType.NODE_SESSION)
                .append("profileGeneration", profile.generation())).into(new ArrayList<>())) {
            Document registry = nodes.find(session, new Document("clusterId", key.clusterId()).append("nodeId", node.getString("ownerNodeId"))
                    .append("bootId", node.getString("ownerBootId")).append("profileGeneration", profile.generation()).append("joined", true)).first();
            if (registry != null && committedNodes.contains(node.getString("ownerNodeId"))) {
                live.add(node.getString("ownerNodeId"));
            }
        }
        if (committedNodes.isEmpty() || live.size() < committedNodes.size() / 2 + 1) {
            return Context.refused(Outcome.WAITING_QUORUM);
        }
        membership.updateOne(session, new Document("_id", key.clusterId()).append("profileGeneration", profile.generation())
                .append("revision", committed.get("revision")), new Document("$inc", new Document("recoveryFenceSerial", 1L)));
        Document artifact = artifacts.find(session, new Document("_id", key.pipelineId())).first();
        Document intent = desired.find(session, new Document("_id", key.pipelineId())).first();
        if (artifact == null || !"pipeline".equals(artifact.getString("kind"))
                || !key.incarnation().equals(artifact.getString("incarnation")) || intent == null
                || (runningIntent && !PipelineState.RUNNING.name().equals(intent.getString("targetState")))) {
            return Context.refused(Outcome.STALE_INTENT);
        }
        String currentFingerprint = ClusterRecoveryIntentFingerprint.of(key.incarnation(), MongoDesiredStore.toDesired(intent));
        if (!io.tapstate.core.model.canonical.CanonicalHash.of(MongoArtifactStore.toResource(artifact)).equals(artifact.getString("contentHash"))) {
            throw ClusterRecoveryDocuments.unreadable(artifact, "contentHash", null);
        }
        if (!currentFingerprint.equals(fingerprint) || !Objects.equals(intent.getString("revision"), artifact.getString("contentHash"))) {
            return Context.refused(Outcome.STALE_INTENT);
        }
        Document actual = states.find(session, new Document("_id", key.pipelineId())).first();
        Document pipeline = claims.find(session, new Document("_id", claimId(key.clusterId(), key.pipelineId()))).first();
        if (actual == null || pipeline == null) {
            return Context.refused(Outcome.STALE_EXECUTION);
        }
        if (artifacts.updateOne(session, new Document("_id", key.pipelineId()).append("incarnation", key.incarnation())
                .append("contentHash", artifact.get("contentHash")), new Document("$inc", new Document("recoveryFenceSerial", 1L))).getMatchedCount() != 1
                || desired.updateOne(session, intentFilter(intent), new Document("$inc", new Document("recoveryFenceSerial", 1L))).getMatchedCount() != 1
                || states.updateOne(session, new Document("_id", key.pipelineId()).append("epoch", actual.get("epoch"))
                        .append("stateJson", actual.get("stateJson")), new Document("$inc", new Document("recoveryFenceSerial", 1L))).getMatchedCount() != 1) {
            return Context.refused(Outcome.STALE_INTENT);
        }
        return new Context(Outcome.APPLIED, key, profile, fingerprint, now, Set.copyOf(live),
                ClusterRecoveryDocuments.number(committed, "revision"), artifact, intent, actual, pipeline);
    }

    static Document intentFilter(Document intent) {
        Document filter = new Document("_id", intent.get("_id"));
        for (String field : List.of("targetState", "revision", "purgeState", "assemblyRevision", "reassemble", "rebuiltAtStateEpoch")) {
            filter.append(field, intent.containsKey(field) ? intent.get(field) : new Document("$exists", false));
        }
        return filter;
    }

    Instant clock(ClientSession session, String clusterId) {
        Document clock = profiles.findOneAndUpdate(session, new Document("_id", clusterId),
                List.of(new Document("$set", new Document("recoveryStoreTime", "$$NOW"))), AFTER);
        if (clock == null) {
            throw new IllegalStateException("guarded profile disappeared");
        }
        return clock.getDate("recoveryStoreTime").toInstant();
    }

    /** No client Boolean or elapsed permit can replace the stored exact authority and its horizon. */
    boolean fenced(ClientSession session, Document capacity, Instant now) {
        ClusterCapacityReservation reservation = reservation(capacity);
        Document profile = profiles.find(session, new Document("_id", reservation.clusterId())).first();
        if (profile == null) {
            return false;
        }
        if (ClusterRecoveryDocuments.number(profile, "generation") > reservation.profile().generation()) {
            return true;
        }
        Document current = claims.find(session, new Document("_id", claimId(reservation.clusterId(), reservation.pipelineId()))).first();
        if (current == null || claims.find(session, WorkloadClaimDocuments.live(reservation.pipelineClaim())).first() != null) {
            return false;
        }
        Date horizon = capacity.getDate("authorityUntil");
        if (horizon == null || horizon.toInstant().isAfter(now)) {
            return false;
        }
        return workloadStore.provesRetired(session, reservation.pipelineClaim());
    }

    Document exact(ClientSession session, ClusterCapacityReservation expected) {
        Document current = occupancy.find(session, new Document("_id", expected.reservationId())).first();
        return current != null && reservation(current).equals(expected) ? current : null;
    }

    ClusterCapacityReservation readReservation(ClientSession session, String id) {
        Document record = occupancy.find(session, new Document("_id", id)).first();
        return record == null ? null : reservation(record);
    }

    ClusterCapacityReservation bindRecoveryPipeline(ClientSession session, ClusterCapacityReservation expected,
            WorkloadClaim pipelineClaim) {
        Document current = exact(session, expected);
        if (current == null || expected.executionGeneration() != null
                || pipelineClaim.executionGeneration() != expected.pipelineClaim().executionGeneration()
                || pipelineClaim.profileGeneration() != expected.profile().generation()
                || workloadStore.conditionTouchClaim(session, WorkloadClaimFence.from(pipelineClaim)) == null) {
            return null;
        }
        ClusterCapacityReservation rebound = new ClusterCapacityReservation(expected.reservationId(), expected.clusterId(),
                expected.pipelineId(), expected.incarnationId(), expected.intentFingerprint(), expected.profile(),
                WorkloadClaimFence.from(pipelineClaim), expected.demandByNode(), expected.reservedAt(), expected.deadline(), null, null);
        replace(session, current, rebound, pipelineClaim.leaseUntil());
        return rebound;
    }

    boolean releaseInTransaction(ClientSession session, ClusterCapacityReservation expected, Instant now) {
        Document current = exact(session, expected);
        if (current == null || !fenced(session, current, now)) {
            return false;
        }
        return occupancy.deleteOne(session, new Document("_id", expected.reservationId())
                .append("revision", current.get("revision"))).getDeletedCount() == 1;
    }

    ClusterCapacityReservation renewRecoveryDeadline(ClientSession session, ClusterCapacityReservation expected, Instant deadline) {
        Document current = exact(session, expected);
        if (current == null || !current.getBoolean("recovery", false)) {
            return null;
        }
        ClusterCapacityReservation renewed = new ClusterCapacityReservation(expected.reservationId(), expected.clusterId(),
                expected.pipelineId(), expected.incarnationId(), expected.intentFingerprint(), expected.profile(), expected.pipelineClaim(),
                expected.demandByNode(), expected.reservedAt(), deadline, expected.executionGeneration(), expected.nativeJobId());
        Instant authority = current.getDate("authorityUntil").toInstant();
        replace(session, current, renewed, authority);
        return renewed;
    }

    private void replace(ClientSession session, Document current, ClusterCapacityReservation next, Instant authorityUntil) {
        Document replacement = document(next).append("revision", Math.addExact(ClusterRecoveryDocuments.number(current, "revision"), 1L))
                .append("recovery", current.getBoolean("recovery", false)).append("authorityUntil", Date.from(authorityUntil));
        if (occupancy.replaceOne(session, new Document("_id", next.reservationId()).append("revision", current.get("revision")), replacement).getMatchedCount() != 1) {
            throw new IllegalStateException("profile-guarded capacity reservation changed in its transaction");
        }
    }

    static Document document(ClusterCapacityReservation reservation) {
        return new Document("_id", reservation.reservationId()).append("clusterId", reservation.clusterId())
                .append("pipelineId", reservation.pipelineId()).append("incarnationId", reservation.incarnationId())
                .append("intentFingerprint", reservation.intentFingerprint()).append("profile", ClusterRecoveryDocuments.profile(reservation.profile()))
                .append("pipelineClaim", WorkloadClaimDocuments.stored(reservation.pipelineClaim()))
                .append("demandByNode", ClusterRecoveryDocuments.demands(reservation.demandByNode()))
                .append("reservedAt", Date.from(reservation.reservedAt())).append("deadline", Date.from(reservation.deadline()))
                .append("executionGeneration", reservation.executionGeneration()).append("nativeJobId", reservation.nativeJobId());
    }

    static ClusterCapacityReservation reservation(Document document) {
        try {
            Number execution = document.get("executionGeneration", Number.class);
            return new ClusterCapacityReservation(document.getString("_id"), document.getString("clusterId"), document.getString("pipelineId"),
                    document.getString("incarnationId"), document.getString("intentFingerprint"), ClusterRecoveryDocuments.profile(document.get("profile", Document.class)),
                    ClusterRecoveryDocuments.fence(document.get("pipelineClaim", Document.class)),
                    ClusterRecoveryDocuments.demands(document.getList("demandByNode", Document.class)), document.getDate("reservedAt").toInstant(),
                    document.getDate("deadline").toInstant(), execution == null ? null : execution.longValue(), document.getString("nativeJobId"));
        } catch (RuntimeException invalid) {
            if (invalid instanceof TapstateException) {
                throw invalid;
            }
            throw ClusterRecoveryDocuments.unreadable(document, "capacityReservation", invalid);
        }
    }

    static ClusterRecoveryKey key(ClusterCapacityReservation reservation) {
        return new ClusterRecoveryKey(reservation.clusterId(), reservation.pipelineId(), reservation.incarnationId());
    }

    static Document claimId(String clusterId, String pipelineId) {
        return new Document("clusterId", clusterId).append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .append("resourceId", pipelineId);
    }

    static TapstateException capacityError(String node, String resource, Object occupied, Object requested, Object limit) {
        return new TapstateException(LifecycleError.CLUSTER_CAPACITY_REFUSED,
                Map.of("node", node, "resource", resource, "occupied", occupied, "requested", requested, "limit", limit), null);
    }

    static void positive(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero() || duration.toMillis() == 0) {
            throw new IllegalArgumentException("duration must be at least one millisecond");
        }
    }

    private static Result result(Outcome outcome, ClusterCapacityReservation reservation, WorkloadClaim advanced,
            List<ClusterCapacityLimits.Violation> violations) {
        return new Result(outcome, reservation, advanced, violations);
    }

    record Context(Outcome outcome, ClusterRecoveryKey key, ClusterExecutionProfile profile, String intentFingerprint,
            Instant now, Set<String> liveNodes, long topologyRevision, Document artifact, Document intent,
            Document actual, Document pipeline) {
        static Context refused(Outcome outcome) {
            return new Context(outcome, null, null, null, null, Set.of(), 0, null, null, null, null);
        }
    }
    record Occupied(Map<String, ClusterCapacityDemand> demand, boolean unknown) {}
    private record OptionalAdvance(WorkloadClaim claim) {}
}
