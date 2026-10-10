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
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.PendingPipelineResume;
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

    @Override
    public java.util.Optional<ClusterCapacityReservation> resumeReservation(PendingPipelineResume expected) {
        Objects.requireNonNull(expected, "expected");
        return profileStore.transaction(session -> {
            Document state = states.find(session, new Document("_id", expected.originalClaim().key().resourceId())).first();
            PendingPipelineResume pending = PendingPipelineResumeDocuments.current(state).orElse(null);
            if (pending == null || !pending.sameRequestAs(expected) || pending.reservationId() == null
                    || (expected.reservationId() != null && !expected.reservationId().equals(pending.reservationId()))) {
                return java.util.Optional.empty();
            }
            Document linked = occupancy.find(session, new Document("_id", pending.reservationId())).first();
            if (linked == null || !resumeIdentity(linked, pending)) {
                throw ClusterRecoveryDocuments.unreadable(state, "resumeReservation", null);
            }
            return java.util.Optional.of(reservation(linked));
        });
    }

    @Override
    public boolean resumeAuthorityRetired(PendingPipelineResume expected, WorkloadClaim current, WorkloadClaimFence former) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(former, "former");
        return profileStore.transaction(session -> {
            if (workloadStore.conditionTouchClaim(session, WorkloadClaimFence.from(current)) == null) {
                return false;
            }
            WorkloadClaim original = expected.originalClaim();
            Context context = guard(session, new ClusterRecoveryKey(original.key().clusterId(), original.key().resourceId(),
                    original.executionIncarnation()), original.executionProfile(), expected.intentFingerprint(), true);
            PendingPipelineResume pending = context.outcome == Outcome.APPLIED
                    ? PendingPipelineResumeDocuments.current(context.actual).orElse(null) : null;
            if (pending == null || !pending.sameRequestAs(expected) || !resumeContext(pending, context)
                    || !current.key().equals(original.key()) || current.profileGeneration() != original.profileGeneration()
                    || (expected.reservationId() != null && !expected.reservationId().equals(pending.reservationId()))) {
                return false;
            }
            boolean recognized = WorkloadClaimFence.from(original).sameAuthorityAs(former);
            if (!recognized && pending.reservationId() != null) {
                Document linked = occupancy.find(session, new Document("_id", pending.reservationId())).first();
                recognized = linked != null && resumeIdentity(linked, pending)
                        && reservation(linked).pipelineClaim().sameAuthorityAs(former);
            }
            return recognized && states.updateOne(session, PendingPipelineResumeDocuments.filter(context.actual),
                    new Document("$inc", new Document("resumeFenceSerial", 1L))).getMatchedCount() == 1
                    && workloadStore.provesRetired(session, former);
        });
    }

    @Override
    public java.util.Optional<Set<String>> resumeSourceRequirements(PendingPipelineResume expected) {
        return profileStore.transaction(session -> {
            Document state = states.find(session, new Document("_id", expected.originalClaim().key().resourceId())).first();
            PendingPipelineResume pending = PendingPipelineResumeDocuments.current(state).orElse(null);
            if (pending == null || !pending.sameRequestAs(expected)
                    || (expected.reservationId() != null && !expected.reservationId().equals(pending.reservationId()))) {
                return java.util.Optional.empty();
            }
            if (pending.reservationId() == null) {
                return originalSourceRequirements(session, pending, state);
            }
            Document linked = occupancy.find(session, new Document("_id", pending.reservationId())).first();
            if (linked == null || !resumeIdentity(linked, pending)) {
                throw ClusterRecoveryDocuments.unreadable(state, "resumeReservation", null);
            }
            return sourceRequirements(linked);
        });
    }

    private java.util.Optional<Set<String>> originalSourceRequirements(ClientSession session, PendingPipelineResume pending, Document state) {
        WorkloadClaim original = pending.originalClaim();
        Document exactOriginal = new Document("clusterId", original.key().clusterId())
                .append("pipelineId", original.key().resourceId()).append("incarnationId", original.executionIncarnation())
                .append("executionGeneration", original.executionGeneration()).append("nativeJobId", pending.originalNativeJobId())
                .append("pipelineClaim.ownerNodeId", original.owner().nodeId()).append("pipelineClaim.ownerBootId", original.owner().bootId())
                .append("pipelineClaim.claimGeneration", original.claimGeneration())
                .append("pipelineClaim.executionGeneration", original.executionGeneration())
                .append("pipelineClaim.profileGeneration", original.profileGeneration());
        List<Document> rows = occupancy.find(session, exactOriginal).limit(2).into(new ArrayList<>());
        if (rows.isEmpty()) {
            return java.util.Optional.empty();
        }
        ClusterCapacityReservation prior = reservation(rows.getFirst());
        if (rows.size() != 1 || !prior.profile().equals(original.executionProfile())
                || !prior.pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(original))) {
            throw ClusterRecoveryDocuments.unreadable(state, "originalResumeReservation", null);
        }
        return sourceRequirements(rows.getFirst());
    }

    @Override
    public Result recordResumeSources(PendingPipelineResume pending, ClusterCapacityReservation expected,
            WorkloadClaimFence current, Set<String> selectedSources) {
        Objects.requireNonNull(pending, "pending");
        Set<String> requested = Set.copyOf(selectedSources);
        return profileStore.transaction(session -> recordSources(session, pending, expected, current, requested));
    }

    @Override
    public Result recordExecutionSources(ClusterCapacityReservation expected, WorkloadClaimFence current, Set<String> selectedSources) {
        Set<String> requested = Set.copyOf(selectedSources);
        return profileStore.transaction(session -> recordSources(session, null, expected, current, requested));
    }

    private Result recordSources(ClientSession session, PendingPipelineResume pending, ClusterCapacityReservation expected,
            WorkloadClaimFence current, Set<String> requested) {
            Document row = exact(session, expected);
            if (row == null || expected.executionGeneration() == null) {
                return result(Outcome.STALE_EXECUTION, expected, null, List.of());
            }
            Document authority = workloadStore.conditionTouchClaim(session, current);
            if (authority == null || !expected.pipelineClaim().sameAuthorityAs(current)) {
                return result(Outcome.STALE_CLAIM, expected, null, List.of());
            }
            Context context = guard(session, key(expected), expected.profile(), expected.intentFingerprint(), true);
            if (context.outcome != Outcome.APPLIED) {
                return result(context.outcome, expected, null, List.of());
            }
            PendingPipelineResume stored = PendingPipelineResumeDocuments.current(context.actual).orElse(null);
            if (!guardResumeLink(session, row, context) || (pending != null && (stored == null || !stored.sameRequestAs(pending)
                    || (pending.reservationId() != null && !pending.reservationId().equals(stored.reservationId()))))) {
                return result(Outcome.STALE_INTENT, expected, null, List.of());
            }
            Set<String> declared = Set.copyOf(((PipelineResource) MongoArtifactStore.toResource(context.artifact)).sourceIds());
            if (!declared.containsAll(requested) || requested.stream().anyMatch(String::isBlank)
                    || !matchesExecutionContext(authority, expected.pipelineClaim(), expected.profile(), key(expected),
                            context.intent.getString("revision"), expected.demandByNode().keySet())
                    || !expected.demandByNode().keySet().equals(context.liveNodes)
                    || !MongoWorkloadClaimStore.readDocument(authority).executionMembers()
                            .equals(liveExecutionMembers(session, expected.profile(), context.liveNodes))) {
                return result(Outcome.UNKNOWN_DEMAND, expected, null, List.of());
            }
            var previous = sourceRequirements(row);
            if (previous.isPresent()) {
                return result(previous.orElseThrow().equals(requested) ? Outcome.ALREADY_RESERVED : Outcome.UNKNOWN_DEMAND,
                        expected, null, List.of());
            }
            if (expected.nativeJobId() != null) {
                return result(Outcome.UNKNOWN_DEMAND, expected, null, List.of());
            }
            if (occupancy.updateOne(session, new Document("_id", expected.reservationId()).append("revision", row.get("revision")),
                    new Document("$set", new Document("sourceRequirementsRecorded", true)
                            .append("requiredSourceIds", requested.stream().sorted().toList()))
                            .append("$inc", new Document("revision", 1L))).getMatchedCount() != 1) {
                throw new IllegalStateException("the linked resume changed while recording its compiled source selection");
            }
            return result(Outcome.APPLIED, expected, null, List.of());
    }

    @Override
    public Result reserveResume(PendingPipelineResume expectedResume, WorkloadClaim expected,
            ClusterExecutionProfile profile, String incarnation, String fingerprint,
            Map<String, ClusterCapacityDemand> demand, ClusterCapacityLimits limits, Duration ttl) {
        Objects.requireNonNull(expectedResume, "expectedResume");
        positive(ttl);
        Objects.requireNonNull(limits, "limits");
        Map<String, ClusterCapacityDemand> requested = Map.copyOf(demand);
        ClusterRecoveryKey key = new ClusterRecoveryKey(expected.key().clusterId(), expected.key().resourceId(), incarnation);
        return profileStore.transaction(session -> {
            Document authority = workloadStore.conditionTouchClaim(session, WorkloadClaimFence.from(expected));
            if (authority == null) {
                return result(Outcome.STALE_CLAIM, null, null, List.of());
            }
            Context context = guard(session, key, profile, fingerprint, true);
            if (context.outcome != Outcome.APPLIED) {
                return result(context.outcome, null, null, List.of());
            }
            PendingPipelineResume pending = PendingPipelineResumeDocuments.current(context.actual).orElse(null);
            if (pending == null || !pending.sameRequestAs(expectedResume) || !resumeContext(pending, context)
                    || (expectedResume.reservationId() != null && !expectedResume.reservationId().equals(pending.reservationId()))) {
                return result(Outcome.STALE_INTENT, null, null, List.of());
            }
            if (states.updateOne(session, PendingPipelineResumeDocuments.filter(context.actual),
                    new Document("$inc", new Document("resumeFenceSerial", 1L))).getMatchedCount() != 1) {
                return result(Outcome.STALE_INTENT, null, null, List.of());
            }
            WorkloadClaim currentClaim = MongoWorkloadClaimStore.readDocument(authority);
            if (pending.reservationId() == null) {
                if (!PendingPipelineResume.sameExecutionContext(pending.originalClaim(), currentClaim)
                        || !workloadStore.provesRetired(session, WorkloadClaimFence.from(pending.originalClaim()))) {
                    return result(Outcome.STALE_EXECUTION, null, null, List.of());
                }
                Result reserved = reserve(session, context, WorkloadClaimFence.from(currentClaim), requested, limits, ttl,
                        false, Set.of(executionKey(currentClaim)));
                if (reserved.outcome() != Outcome.APPLIED) {
                    return reserved;
                }
                if (occupancy.updateOne(session, new Document("_id", reserved.reservation().reservationId()),
                        new Document("$set", new Document(PendingPipelineResumeDocuments.CAPACITY_EPOCH, pending.stateEpoch())))
                        .getMatchedCount() != 1) {
                    throw new IllegalStateException("the fresh resume reservation disappeared before its origin receipt");
                }
                if (states.updateOne(session, PendingPipelineResumeDocuments.filter(context.actual), new Document("$set",
                        new Document(PendingPipelineResumeDocuments.FIELD + ".reservationId", reserved.reservation().reservationId())))
                        .getMatchedCount() != 1) {
                    throw new IllegalStateException("the accepted resume changed before its atomic reservation link");
                }
                return reserved;
            }
            Document linked = occupancy.find(session, new Document("_id", pending.reservationId())).first();
            if (linked == null || !resumeIdentity(linked, pending)) {
                return result(Outcome.UNKNOWN_DEMAND, null, null, List.of());
            }
            ClusterCapacityReservation previous = reservation(linked);
            if (!previous.demandByNode().equals(requested) || !requested.keySet().equals(context.liveNodes)) {
                return result(Outcome.UNKNOWN_DEMAND, previous, null, List.of());
            }
            WorkloadClaimFence currentFence = WorkloadClaimFence.from(currentClaim);
            boolean sameAuthority = previous.pipelineClaim().sameAuthorityAs(currentFence);
            if (previous.executionGeneration() == null) {
                if (!PendingPipelineResume.sameExecutionContext(pending.originalClaim(), currentClaim)) {
                    return result(Outcome.STALE_EXECUTION, previous, null, List.of());
                }
            } else if (!matchesExecutionContext(authority, previous.pipelineClaim(), previous.profile(), key,
                    context.intent.getString("revision"), previous.demandByNode().keySet())
                    || !currentClaim.executionMembers().equals(liveExecutionMembers(session, profile, context.liveNodes))) {
                return result(Outcome.STALE_EXECUTION, previous, null, List.of());
            }
            if (!sameAuthority && !fenced(session, linked, context.now)) {
                return result(Outcome.STALE_EXECUTION, previous, null, List.of());
            }
            if (previous.nativeJobId() != null) {
                return result(sameAuthority ? Outcome.ALREADY_RESERVED : Outcome.UNKNOWN_DEMAND, previous, null, List.of());
            }
            if (!sameAuthority) {
                Occupied other = occupied(session, profile.clusterId(), context.now, true, Set.of(executionKey(currentClaim)));
                Result refusal = budgetRefusal(other, requested, limits);
                if (refusal != null) {
                    return refusal;
                }
                if (previous.executionGeneration() != null) {
                    if (currentClaim.failureClaimGeneration() > 0) {
                        return result(Outcome.UNKNOWN_DEMAND, previous, null, List.of());
                    }
                    if (claims.updateOne(session, WorkloadClaimDocuments.live(currentFence), new Document("$set",
                            new Document("executionClaimGeneration", currentClaim.claimGeneration()))).getMatchedCount() != 1) {
                        return result(Outcome.STALE_CLAIM, previous, null, List.of());
                    }
                    currentClaim = MongoWorkloadClaimStore.readDocument(claims.find(session, WorkloadClaimDocuments.live(currentFence)).first());
                }
            }
            Instant deadline = context.now.plus(ttl);
            if (deadline.isAfter(currentClaim.leaseUntil())) {
                deadline = currentClaim.leaseUntil();
            }
            if (!deadline.isAfter(context.now)) {
                return result(Outcome.STALE_CLAIM, previous, null, List.of());
            }
            WorkloadClaimFence reboundFence = previous.executionGeneration() == null ? WorkloadClaimFence.from(currentClaim)
                    : new WorkloadClaimFence(currentClaim.key(), currentClaim.owner(), currentClaim.claimGeneration(),
                            currentClaim.executionGeneration(), currentClaim.executionTopologyRevision(), currentClaim.profileGeneration());
            ClusterCapacityReservation rebound = new ClusterCapacityReservation(previous.reservationId(), previous.clusterId(),
                    previous.pipelineId(), previous.incarnationId(), previous.intentFingerprint(), previous.profile(), reboundFence,
                    previous.demandByNode(), previous.reservedAt(), deadline, previous.executionGeneration(), null);
            replace(session, linked, rebound, currentClaim.leaseUntil());
            return result(Outcome.ALREADY_RESERVED, rebound, rebound.executionGeneration() == null ? null : currentClaim, List.of());
        });
    }

    Result reserve(ClientSession session, Context context, WorkloadClaimFence pipelineClaim,
            Map<String, ClusterCapacityDemand> demand, ClusterCapacityLimits limits, Duration ttl, boolean recovery) {
        return reserve(session, context, pipelineClaim, demand, limits, ttl, recovery, Set.of());
    }

    private Result reserve(ClientSession session, Context context, WorkloadClaimFence pipelineClaim,
            Map<String, ClusterCapacityDemand> demand, ClusterCapacityLimits limits, Duration ttl, boolean recovery,
            Set<String> retiredExplicitExecutions) {
        if (demand.isEmpty() || !context.liveNodes.equals(demand.keySet())) {
            return result(Outcome.UNKNOWN_DEMAND, null, null, List.of());
        }
        Result refusal = budgetRefusal(occupied(session, context.profile.clusterId(), context.now, true, retiredExplicitExecutions), demand, limits);
        if (refusal != null) {
            return refusal;
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
        if (!guardResumeLink(session, current, context)) {
            return result(Outcome.STALE_INTENT, expected, null, List.of());
        }
        if (!expected.demandByNode().keySet().equals(executionNodes) || !context.liveNodes.equals(executionNodes)
                || !expected.deadline().isAfter(context.now)) {
            return result(Outcome.STALE_EXECUTION, expected, null, List.of());
        }
        Map<String, ClusterExecutionMember> executionMembers = liveExecutionMembers(session, expected.profile(), executionNodes);
        if (!executionMembers.keySet().equals(executionNodes)) {
            return result(Outcome.WAITING_QUORUM, expected, null, List.of());
        }
        List<Document> retiredHistory = retiredSubmittedHistory(session, pipelineClaim, context.now);
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
                        .append("executionRevision", context.intent.getString("revision"))
                        .append("executionMembers", executionMemberDocuments(executionMembers)))).getMatchedCount() != 1) {
            throw new IllegalStateException("the allocated execution changed before its artifact receipt");
        }
        WorkloadClaim recorded = MongoWorkloadClaimStore.readDocument(claims.find(session,
                WorkloadClaimDocuments.live(WorkloadClaimFence.from(allocated.claim))).first());
        replace(session, current, advanced, allocated.claim.leaseUntil());
        for (Document retired : retiredHistory) {
            occupancy.deleteOne(session, new Document("_id", retired.get("_id")).append("revision", retired.get("revision")));
        }
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
        if (!expected.pipelineClaim().sameAuthorityAs(pipelineClaim) || authority == null) {
            return result(Outcome.STALE_CLAIM, expected, null, List.of());
        }
        Context context = guard(session, key(expected), expected.profile(), expected.intentFingerprint(), true);
        if (context.outcome != Outcome.APPLIED) {
            return result(context.outcome, expected, null, List.of());
        }
        if (!guardResumeLink(session, current, context)) {
            return result(Outcome.STALE_INTENT, expected, null, List.of());
        }
        if (!matchesExecutionContext(authority, expected.pipelineClaim(), expected.profile(), key(expected),
                context.intent.getString("revision"), expected.demandByNode().keySet())) {
            return result(Outcome.STALE_EXECUTION, expected, null, List.of());
        }
        ClusterCapacityReservation submitted = new ClusterCapacityReservation(expected.reservationId(), expected.clusterId(),
                expected.pipelineId(), expected.incarnationId(), expected.intentFingerprint(), expected.profile(), expected.pipelineClaim(),
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
        return readOccupied(clusterId).map(Snapshot::occupiedByNode).orElseThrow(() ->
                new TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null));
    }

    @Override
    public java.util.Optional<Snapshot> readOccupied(String clusterId) {
        return profileStore.transaction(session -> {
            Document profile = profiles.aggregate(session, List.of(new Document("$match", new Document("_id", clusterId)),
                    new Document("$set", new Document("capacityReadTime", "$$NOW")))).first();
            if (profile == null) {
                return java.util.Optional.empty();
            }
            Instant now = profile.getDate("capacityReadTime").toInstant();
            Occupied occupied = occupied(session, clusterId, now, false);
            if (occupied.unknown) {
                throw capacityError("unknown", "unknown-demand", "unknown", "unknown", "unknown");
            }
            return java.util.Optional.of(new Snapshot(MongoClusterProfileStore.profile(profile), occupied.demand));
        });
    }

    Occupied occupied(ClientSession session, String clusterId, Instant now) {
        return occupied(session, clusterId, now, true);
    }

    private Occupied occupied(ClientSession session, String clusterId, Instant now, boolean cleanup) {
        return occupied(session, clusterId, now, cleanup, Set.of());
    }

    private Occupied occupied(ClientSession session, String clusterId, Instant now, boolean cleanup,
            Set<String> retiredExplicitExecutions) {
        Map<String, ClusterCapacityDemand> total = new LinkedHashMap<>();
        List<Document> records = occupancy.find(session, new Document("clusterId", clusterId)).into(new ArrayList<>());
        Set<String> recordedExecutions = new java.util.HashSet<>();
        Map<String, List<ClusterCapacityReservation>> retiredSubmissions = new LinkedHashMap<>();
        for (Document record : records) {
            ClusterCapacityReservation reservation = reservation(record);
            if ((cleanup ? fenced(session, record, now) : fencedAt(session, record, now)) && !reservation.deadline().isAfter(now)) {
                if (reservation.nativeJobId() != null && reservation.executionGeneration() != null) {
                    retiredSubmissions.computeIfAbsent(reservation.pipelineId() + ":" + reservation.executionGeneration(),
                            ignored -> new ArrayList<>()).add(reservation);
                }
                // An unsubmitted recovery retains its authority evidence until its queue step releases it.
                if (cleanup && (!record.getBoolean("recovery", false) || reservation.nativeJobId() != null)
                        && !retainedResume(session, record)
                        && !retiredSubmittedContext(claims.find(session,
                                new Document("_id", claimId(reservation.clusterId(), reservation.pipelineId()))).first(), List.of(reservation))) {
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
        Document live = MongoClusterProfileStore.liveClaims(clusterId, WorkloadClaimType.PIPELINE_ACTUATION);
        if (!cleanup) {
            live.put("$expr", new Document("$gt", List.of("$leaseUntil", Date.from(now))));
        }
        for (Document claim : claims.find(session, live).into(new ArrayList<>())) {
            long execution = ClusterRecoveryDocuments.number(claim, "executionGeneration");
            if (execution == 0 || recordedExecutions.contains(claim.getString("resourceId") + ":" + execution)
                    || retiredExplicitExecutions.contains(claim.getString("resourceId") + ":" + execution)) {
                continue;
            }
            if (retiredSubmittedContext(claim, retiredSubmissions.getOrDefault(claim.getString("resourceId") + ":" + execution,
                    List.of()))) {
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

    /** A submitted receipt proves history only after its exact authority and cached horizon retire. */
    private static boolean retiredSubmittedContext(Document document, List<ClusterCapacityReservation> submitted) {
        if (document == null || submitted.isEmpty()) { return false; }
        WorkloadClaim claim = MongoWorkloadClaimStore.readDocument(document);
        if (claim.executionRevision() == null || claim.executionRevision().isBlank() || claim.executionTopologyRevision() == null) {
            return false;
        }
        return submitted.stream().anyMatch(receipt -> {
            if (receipt.nativeJobId() == null || receipt.executionGeneration() == null) { return false; }
            WorkloadClaimFence authority = receipt.pipelineClaim();
            // Acquisition topology can refresh; only the retained allocator context names execution topology.
            WorkloadClaimFence original = new WorkloadClaimFence(authority.key(), authority.owner(), authority.claimGeneration(),
                    authority.executionGeneration(), claim.executionTopologyRevision(), authority.profileGeneration());
            return matchesExecutionContext(document, original, receipt.profile(), key(receipt), claim.executionRevision(),
                    receipt.demandByNode().keySet());
        });
    }

    private List<Document> retiredSubmittedHistory(ClientSession session, WorkloadClaim current, Instant now) {
        Document context = claims.find(session, new Document("_id", claimId(current.key().clusterId(), current.key().resourceId()))).first();
        return occupancy.find(session, new Document("clusterId", current.key().clusterId()).append("pipelineId", current.key().resourceId())
                .append("executionGeneration", current.executionGeneration()).append("nativeJobId", new Document("$ne", null)))
                .into(new ArrayList<>()).stream().filter(row -> {
                    ClusterCapacityReservation receipt = reservation(row);
                    return !receipt.deadline().isAfter(now) && fenced(session, row, now)
                            && retiredSubmittedContext(context, List.of(receipt));
                }).toList();
    }

    private boolean fencedAt(ClientSession session, Document record, Instant now) {
        ClusterCapacityReservation reservation = reservation(record);
        Document profile = profiles.find(session, new Document("_id", reservation.clusterId())).first();
        if (profile == null) {
            return false;
        }
        if (ClusterRecoveryDocuments.number(profile, "generation") > reservation.profile().generation()) {
            return true;
        }
        Document current = claims.find(session, new Document("_id", claimId(reservation.clusterId(), reservation.pipelineId()))).first();
        Document live = WorkloadClaimDocuments.liveAuthority(reservation.pipelineClaim());
        live.put("$expr", new Document("$gt", List.of("$leaseUntil", Date.from(now))));
        if (current == null || claims.find(session, live).first() != null) {
            return false;
        }
        Date authority = record.getDate("authorityUntil");
        if (authority == null || authority.toInstant().isAfter(now)) {
            return false;
        }
        Document retired = new Document("_id", claimId(reservation.clusterId(), reservation.pipelineId()))
                .append("$nor", List.of(live)).append("$expr", new Document("$lte", List.of(
                        new Document("$ifNull", List.of("$retiredAuthorizationUntil", new Date(Long.MAX_VALUE))), Date.from(now))));
        return claims.find(session, retired).first() != null;
    }

    Context guard(ClientSession session, ClusterRecoveryKey key, ClusterExecutionProfile profile, String fingerprint, boolean runningIntent) {
        return guard(session, key, profile, fingerprint, runningIntent, true);
    }

    /** Reporting an already-authorized execution's failure is independent of new data admission. */
    Context guardFailureFact(ClientSession session, ClusterRecoveryKey key, ClusterExecutionProfile profile, String fingerprint) {
        return guard(session, key, profile, fingerprint, true, false);
    }

    private Context guard(ClientSession session, ClusterRecoveryKey key, ClusterExecutionProfile profile,
            String fingerprint, boolean runningIntent, boolean requireDataQuorum) {
        Document currentProfile = profiles.findOneAndUpdate(session, new Document("_id", key.clusterId())
                .append("generation", profile.generation()).append("hash", profile.profile().hash()),
                List.of(new Document("$set", new Document("recoveryStoreTime", "$$NOW")
                        .append("recoveryWriteSerial", new Document("$add", List.of(new Document("$ifNull", List.of("$recoveryWriteSerial", 0L)), 1L))))), AFTER);
        if (currentProfile == null) {
            return Context.refused(Outcome.STALE_PROFILE);
        }
        Instant now = currentProfile.getDate("recoveryStoreTime").toInstant();
        Set<String> live = new java.util.HashSet<>();
        Long committedRevision = null;
        if (requireDataQuorum) {
            Document committed = membership.find(session, new Document("_id", key.clusterId())
                    .append("profileGeneration", profile.generation())).first();
            if (committed == null) {
                return Context.refused(Outcome.WAITING_QUORUM);
            }
            Set<String> committedNodes = Set.copyOf(committed.getList("activeNodeIds", String.class));
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
            committedRevision = ClusterRecoveryDocuments.number(committed, "revision");
            membership.updateOne(session, new Document("_id", key.clusterId()).append("profileGeneration", profile.generation())
                    .append("revision", committed.get("revision")), new Document("$inc", new Document("recoveryFenceSerial", 1L)));
        }
        Document artifact = artifacts.find(session, new Document("_id", key.pipelineId())).first();
        Document intent = desired.find(session, new Document("_id", key.pipelineId())).first();
        if (artifact == null || !"pipeline".equals(artifact.getString("kind"))
                || !key.incarnation().equals(artifact.getString("incarnation")) || intent == null
                || (runningIntent && !PipelineState.RUNNING.name().equals(intent.getString("targetState")))) {
            return Context.refused(Outcome.STALE_INTENT);
        }
        String currentFingerprint = intentFingerprint(key, intent);
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
                committedRevision == null ? ClusterRecoveryDocuments.number(pipeline, "topologyRevision") : committedRevision,
                artifact, intent, actual, pipeline);
    }

    Map<String, ClusterExecutionMember> liveExecutionMembers(ClientSession session, ClusterExecutionProfile profile,
            Set<String> nodeIds) {
        Map<String, ClusterExecutionMember> identities = new LinkedHashMap<>();
        for (String nodeId : nodeIds.stream().sorted().toList()) {
            Document node = claims.find(session, MongoClusterProfileStore.liveClaims(profile.clusterId(), WorkloadClaimType.NODE_SESSION)
                    .append("profileGeneration", profile.generation()).append("resourceId", nodeId)).first();
            if (node == null) {
                continue;
            }
            Document registration = nodes.find(session, new Document("clusterId", profile.clusterId()).append("nodeId", nodeId)
                    .append("profileGeneration", profile.generation()).append("bootId", node.getString("ownerBootId"))
                    .append("joined", true)).first();
            if (registration == null) {
                continue;
            }
            try {
                identities.put(nodeId, new ClusterExecutionMember(nodeId, registration.getString("bootId"), registration.getString("memberUuid")));
            } catch (RuntimeException invalid) {
                throw ClusterRecoveryDocuments.unreadable(registration, "executionMember", invalid);
            }
        }
        return Map.copyOf(identities);
    }

    static List<Document> executionMemberDocuments(Map<String, ClusterExecutionMember> identities) {
        return new TreeMap<>(identities).values().stream().map(member -> new Document("nodeId", member.nodeId())
                .append("bootId", member.bootId()).append("memberUuid", member.memberUuid())).toList();
    }

    static Document intentFilter(Document intent) {
        Document filter = new Document("_id", intent.get("_id"));
        for (String field : List.of("targetState", "revision", "purgeState", "assemblyRevision", "reassemble", "rebuiltAtStateEpoch")) {
            filter.append(field, intent.containsKey(field) ? intent.get(field) : new Document("$exists", false));
        }
        return filter;
    }

    static String intentFingerprint(ClusterRecoveryKey key, Document intent) {
        return DesiredStateFingerprint.of(MongoDesiredStore.toDesired(intent));
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
        if (current == null || claims.find(session, WorkloadClaimDocuments.liveAuthority(reservation.pipelineClaim())).first() != null) {
            return false;
        }
        Date horizon = capacity.getDate("authorityUntil");
        if (horizon == null || horizon.toInstant().isAfter(now)) {
            return false;
        }
        return workloadStore.provesRetired(session, reservation.pipelineClaim());
    }

    /** Acquisition topology may change; the allocator's immutable execution context may not. */
    static boolean matchesExecutionContext(Document document, WorkloadClaimFence original, ClusterExecutionProfile profile,
            ClusterRecoveryKey key, String revision, Set<String> executionNodes) {
        WorkloadClaim claim = MongoWorkloadClaimStore.readDocument(document);
        return claim.key().equals(original.key()) && claim.executionGeneration() == original.executionGeneration()
                && claim.contextExecutionGeneration() == original.executionGeneration()
                && claim.executionClaimGeneration() == original.claimGeneration()
                && Objects.equals(claim.executionTopologyRevision(), original.topologyRevision())
                && profile.equals(claim.executionProfile()) && key.incarnation().equals(claim.executionIncarnation())
                && Objects.equals(revision, claim.executionRevision()) && executionNodes.equals(claim.executionNodeIds())
                && executionNodes.equals(claim.executionMembers().keySet());
    }

    Document exact(ClientSession session, ClusterCapacityReservation expected) {
        Document current = occupancy.find(session, new Document("_id", expected.reservationId())).first();
        return current != null && reservation(current).equals(expected) ? current : null;
    }

    ClusterCapacityReservation readReservation(ClientSession session, String id) {
        Document record = occupancy.find(session, new Document("_id", id)).first();
        return record == null ? null : reservation(record);
    }

    boolean touchAllocation(ClientSession session, ClusterCapacityReservation expected) {
        Document current = exact(session, expected);
        return current != null && occupancy.updateOne(session, new Document("_id", expected.reservationId())
                .append("revision", current.get("revision")).append("executionGeneration", expected.executionGeneration())
                .append("pipelineClaim", WorkloadClaimDocuments.stored(expected.pipelineClaim())),
                new Document("$inc", new Document("revision", 1L))).getMatchedCount() == 1;
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
        if (current.containsKey(PendingPipelineResumeDocuments.CAPACITY_EPOCH)) {
            replacement.append(PendingPipelineResumeDocuments.CAPACITY_EPOCH, current.get(PendingPipelineResumeDocuments.CAPACITY_EPOCH));
        }
        for (String field : List.of("sourceRequirementsRecorded", "requiredSourceIds")) {
            if (current.containsKey(field)) {
                replacement.append(field, current.get(field));
            }
        }
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

    private static Result budgetRefusal(Occupied occupied, Map<String, ClusterCapacityDemand> requested, ClusterCapacityLimits limits) {
        if (occupied.unknown) {
            return result(Outcome.UNKNOWN_DEMAND, null, null, List.of());
        }
        for (Map.Entry<String, ClusterCapacityDemand> entry : new TreeMap<>(requested).entrySet()) {
            List<ClusterCapacityLimits.Violation> violations = limits.violations(
                    occupied.demand.getOrDefault(entry.getKey(), ClusterCapacityDemand.ZERO), entry.getValue());
            if (!violations.isEmpty()) {
                return new Result(Outcome.CAPACITY_REFUSED, null, null, violations, entry.getKey());
            }
        }
        return null;
    }

    private static String executionKey(WorkloadClaim claim) {
        return claim.key().resourceId() + ":" + claim.executionGeneration();
    }

    private static boolean resumeContext(PendingPipelineResume resume, Context context) {
        WorkloadClaim original = resume.originalClaim();
        return original.key().clusterId().equals(context.key.clusterId())
                && original.key().resourceId().equals(context.key.pipelineId())
                && original.executionIncarnation().equals(context.key.incarnation())
                && original.executionProfile().equals(context.profile)
                && resume.intentFingerprint().equals(context.intentFingerprint);
    }

    boolean hasPendingOriginalResume(Context context, long originalExecution) {
        PendingPipelineResume resume = PendingPipelineResumeDocuments.current(context.actual).orElse(null);
        return resume != null && resume.originalClaim().executionGeneration() == originalExecution && resumeContext(resume, context);
    }

    private static boolean resumeIdentity(Document row, PendingPipelineResume resume) {
        ClusterCapacityReservation reservation = reservation(row);
        return row.containsKey(PendingPipelineResumeDocuments.CAPACITY_EPOCH)
                && PendingPipelineResumeDocuments.integer(row, PendingPipelineResumeDocuments.CAPACITY_EPOCH) == resume.stateEpoch()
                && reservation.reservationId().equals(resume.reservationId())
                && reservation.clusterId().equals(resume.originalClaim().key().clusterId())
                && reservation.pipelineId().equals(resume.originalClaim().key().resourceId())
                && reservation.incarnationId().equals(resume.originalClaim().executionIncarnation())
                && reservation.intentFingerprint().equals(resume.intentFingerprint())
                && reservation.profile().equals(resume.originalClaim().executionProfile());
    }

    private boolean guardResumeLink(ClientSession session, Document row, Context context) {
        if (!row.containsKey(PendingPipelineResumeDocuments.CAPACITY_EPOCH)) {
            return true;
        }
        PendingPipelineResume resume = PendingPipelineResumeDocuments.current(context.actual).orElse(null);
        return resume != null && resumeContext(resume, context) && resumeIdentity(row, resume)
                && states.updateOne(session, PendingPipelineResumeDocuments.filter(context.actual),
                        new Document("$inc", new Document("resumeFenceSerial", 1L))).getMatchedCount() == 1;
    }

    private boolean retainedResume(ClientSession session, Document row) {
        if (!row.containsKey(PendingPipelineResumeDocuments.CAPACITY_EPOCH) || row.get("nativeJobId") != null) {
            return false;
        }
        PendingPipelineResume resume = PendingPipelineResumeDocuments.current(states.find(session,
                new Document("_id", row.getString("pipelineId"))).first()).orElse(null);
        return resume != null && resumeIdentity(row, resume);
    }

    private static java.util.Optional<Set<String>> sourceRequirements(Document row) {
        Object recorded = row.get("sourceRequirementsRecorded");
        if (recorded == null && !row.containsKey("requiredSourceIds")) {
            return java.util.Optional.empty();
        }
        if (!Boolean.TRUE.equals(recorded) || !(row.get("requiredSourceIds") instanceof List<?> values)
                || values.stream().anyMatch(value -> !(value instanceof String source) || source.isBlank())) {
            throw ClusterRecoveryDocuments.unreadable(row, "resumeSourceRequirements", null);
        }
        Set<String> required = Set.copyOf(row.getList("requiredSourceIds", String.class));
        if (required.size() != values.size()) {
            throw ClusterRecoveryDocuments.unreadable(row, "resumeSourceRequirements", null);
        }
        return java.util.Optional.of(required);
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
