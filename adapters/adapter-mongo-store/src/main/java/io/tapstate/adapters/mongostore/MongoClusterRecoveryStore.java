package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryDiagnostic;
import io.tapstate.spi.store.ClusterRecoveryFailureNote;
import io.tapstate.spi.store.ClusterRecoveryPipelineFence;
import io.tapstate.spi.store.CaptureStartupFailure;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryFence;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryMutation;
import io.tapstate.spi.store.ClusterRecoveryPermit;
import io.tapstate.spi.store.ClusterRecoveryStartupReceipt;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.ClusterRecoverySuccessor;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/** One durable queue, with the existing allocator and shared ordinary-start budget in each transaction. */
public final class MongoClusterRecoveryStore implements ClusterRecoveryStore {
    private static final FindOneAndUpdateOptions AFTER = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    private final MongoCollection<Document> queue;
    private final MongoClusterCapacityStore capacity;
    private final MongoSrsMetaStore meta;

    public MongoClusterRecoveryStore(MongoCollection<Document> queue, MongoClusterCapacityStore capacity) {
        this(queue, capacity, null);
    }

    public MongoClusterRecoveryStore(MongoCollection<Document> queue, MongoClusterCapacityStore capacity, MongoSrsMetaStore meta) {
        this.queue = MongoClusterCapacityStore.durable(queue);
        this.capacity = Objects.requireNonNull(capacity, "capacity");
        this.meta = meta;
    }

    @Override
    public Optional<ClusterRecoveryItem> read(ClusterRecoveryKey key) {
        return StoreIo.call(() -> Optional.ofNullable(queue.find(new Document("_id", ClusterRecoveryDocuments.id(key))).first())
                .map(ClusterRecoveryDocuments::item));
    }

    @Override
    public List<ClusterRecoveryItem> list(String clusterId, int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("queue offset must be non-negative and limit must be positive");
        }
        return StoreIo.call(() -> {
            Document includeTerminal = new Document("$and", List.of(
                    new Document("$eq", List.of(new Document("$type", "$latestTerminal"), "object")),
                    new Document("$not", List.of(new Document("$in", List.of("$status", List.of("RECOVERED", "REBUILD_FAILED", "CANCELLED")))))));
            List<Document> pipeline = List.of(new Document("$match", new Document("clusterId", clusterId)),
                    new Document("$project", new Document("rows", new Document("$concatArrays", List.of(List.of("$$ROOT"),
                            new Document("$cond", List.of(includeTerminal, List.of("$latestTerminal"), List.of())))))),
                    new Document("$unwind", "$rows"), new Document("$replaceRoot", new Document("newRoot", "$rows")),
                    new Document("$sort", new Document("enqueueSequence", 1)), new Document("$skip", offset), new Document("$limit", limit));
            return queue.aggregate(pipeline).into(new ArrayList<>()).stream().map(ClusterRecoveryDocuments::item).toList();
        });
    }

    @Override
    public Result enqueue(ClusterRecoveryEvent event, ClusterRecoveryFence expected) {
        if (!event.key().equals(expected.key()) || expected.itemRevision() != 0
                || !event.intentFingerprint().equals(expected.intentFingerprint())
                || !event.targetProfile().equals(expected.targetProfile())
                || event.originalExecutionGeneration() != expected.executionGeneration()) {
            return result(ClusterRecoveryMutation.STALE_ITEM, null);
        }
        return capacity.profileStore.transaction(session -> {
            Document recoveryClaim = capacity.workloadStore.conditionTouchClaim(session, expected.recoveryClaim());
            if (recoveryClaim == null) {
                return result(ClusterRecoveryMutation.STALE_RECOVERY_CLAIM, null);
            }
            MongoClusterCapacityStore.Context context = capacity.guard(session, event.key(), event.targetProfile(), event.intentFingerprint(), true);
            if (context.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(context.outcome()), null);
            }
            if (capacity.hasPendingOriginalResume(context, event.originalExecutionGeneration())) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, null);
            }
            Document current = queue.find(session, new Document("_id", ClusterRecoveryDocuments.id(event.key()))).first();
            if (current != null) {
                ClusterRecoveryItem item = ClusterRecoveryDocuments.item(current);
                if (item.matchesActiveAlias(event)
                        && item.executionAliases().contains(ClusterRecoveryDocuments.number(context.pipeline(), "executionGeneration"))) {
                    return result(ClusterRecoveryMutation.DUPLICATE, item);
                }
                if (!item.status().terminal()) {
                    return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
                }
                ClusterRecoveryMutation terminal = item.terminalEventCheck(event);
                if (terminal != ClusterRecoveryMutation.APPLIED) {
                    return result(terminal, item);
                }
            }
            ClusterRecoveryMutation provenance = eventProvenance(session, event, context);
            if (provenance != ClusterRecoveryMutation.APPLIED) {
                return result(provenance, current == null ? null : ClusterRecoveryDocuments.item(current));
            }
            Document counter = capacity.profiles.findOneAndUpdate(session,
                    new Document("_id", event.key().clusterId()).append("generation", event.targetProfile().generation()),
                    new Document("$inc", new Document("recoveryEnqueueSequence", 1L)), AFTER);
            ClusterRecoveryItem item = ClusterRecoveryItem.enqueued(event,
                    ClusterRecoveryDocuments.number(counter, "recoveryEnqueueSequence"), context.now(), ClusterRecoveryItem.DEFAULT_MAX_ATTEMPTS);
            Document next = ClusterRecoveryDocuments.item(item);
            if (current == null) {
                queue.insertOne(session, next);
            } else {
                next.append("itemRevision", Math.addExact(ClusterRecoveryDocuments.number(current, "itemRevision"), 1L));
                next.append("latestTerminal", terminalCopy(current));
                if (queue.replaceOne(session, itemFilter(current), next).getMatchedCount() != 1) {
                    throw new IllegalStateException("profile-guarded terminal item changed in its transaction");
                }
                item = ClusterRecoveryDocuments.item(next);
            }
            return result(ClusterRecoveryMutation.APPLIED, item);
        });
    }

    private ClusterRecoveryMutation eventProvenance(ClientSession session, ClusterRecoveryEvent event, MongoClusterCapacityStore.Context context) {
        if (originalExecutionCheck(event, context.pipeline()) != ClusterRecoveryMutation.APPLIED) {
            return ClusterRecoveryMutation.STALE_EXECUTION;
        }
        String actual = context.actual().getString("stateJson");
        if (List.of("COMPLETED", "STOPPED", "PAUSED", "DRAFT", "VALIDATED").contains(actual)) {
            return ClusterRecoveryMutation.TERMINAL;
        }
        long recordedFailure = numberOrZero(context.pipeline(), "failureClaimGeneration");
        boolean topologyFailure = Boolean.TRUE.equals(context.pipeline().getBoolean("failureAfterMemberLoss"));
        if (recordedFailure > 0 && !topologyFailure) {
            return ClusterRecoveryMutation.TERMINAL;
        }
        if ("FAILED".equals(actual) && recordedFailure == 0) {
            return ClusterRecoveryMutation.STALE_EXECUTION;
        }
        Document executionProfile = context.pipeline().get("executionProfile", Document.class);
        if (executionProfile == null) {
            if (context.pipeline().containsKey("executionProfileVersion")) {
                throw ClusterRecoveryDocuments.unreadable(context.pipeline(), "executionProfile", null);
            }
            Document cold = capacity.profiles.find(session, new Document("_id", event.key().clusterId())).first();
            if (!event.legacySourceProfile() || cold == null || cold.getDate("legacyAuthorityRetiredAt") == null
                    || event.cause() != ClusterRecoveryCause.FULL_CLUSTER_RESTART) {
                return ClusterRecoveryMutation.STALE_PROFILE;
            }
        } else {
            if (numberOrZero(executionProfile, "schemaVersion") != 1
                    || !ClusterRecoveryDocuments.profile(executionProfile).equals(event.sourceProfile())
                    || !Objects.equals(event.sourceTopologyRevision(), context.pipeline().get("executionTopologyRevision", Number.class) == null
                            ? null : ClusterRecoveryDocuments.number(context.pipeline(), "executionTopologyRevision"))) {
                return ClusterRecoveryMutation.STALE_PROFILE;
            }
        }
        if (event.cause() == ClusterRecoveryCause.MEMBER_LOSS) {
            WorkloadClaim original = MongoWorkloadClaimStore.readDocument(context.pipeline());
            Optional<Boolean> present = original.originalMembersPresent(capacity.liveExecutionMembers(session,
                    context.profile(), context.liveNodes()));
            if (numberOrZero(context.pipeline(), "contextExecutionGeneration") != event.originalExecutionGeneration()
                    || present.isEmpty() || (present.orElseThrow() && !topologyFailure)) {
                return ClusterRecoveryMutation.STALE_EXECUTION;
            }
        } else if (executionProfile != null
                && ClusterRecoveryDocuments.number(executionProfile, "generation") >= context.profile().generation()) {
            return ClusterRecoveryMutation.STALE_PROFILE;
        }
        return ClusterRecoveryMutation.APPLIED;
    }

    static ClusterRecoveryMutation originalExecutionCheck(ClusterRecoveryEvent event, Document pipeline) {
        return event.originalExecutionGeneration() == ClusterRecoveryDocuments.number(pipeline, "executionGeneration")
                && event.key().incarnation().equals(pipeline.getString("executionIncarnation"))
                && Objects.equals(event.originalExecutionRevision(), pipeline.getString("executionRevision"))
                ? ClusterRecoveryMutation.APPLIED : ClusterRecoveryMutation.STALE_EXECUTION;
    }

    @Override
    public Result retarget(ClusterRecoveryFence expected, ClusterExecutionProfile target, long topologyRevision) {
        return capacity.profileStore.transaction(session -> {
            Document current = queue.find(session, new Document("_id", ClusterRecoveryDocuments.id(expected.key()))).first();
            if (current == null) {
                return result(ClusterRecoveryMutation.NOT_FOUND, null);
            }
            ClusterRecoveryItem item = ClusterRecoveryDocuments.item(current);
            ClusterRecoveryMutation optimistic = itemExpectations(item, expected);
            if (optimistic != ClusterRecoveryMutation.APPLIED) {
                return result(optimistic, item);
            }
            if (item.status().terminal()) {
                return result(ClusterRecoveryMutation.TERMINAL, item);
            }
            if (capacity.workloadStore.conditionTouchClaim(session, expected.recoveryClaim()) == null) {
                return result(ClusterRecoveryMutation.STALE_RECOVERY_CLAIM, item);
            }
            MongoClusterCapacityStore.Context context = capacity.guard(session, expected.key(), target, expected.intentFingerprint(), true);
            if (context.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(context.outcome()), item);
            }
            if (context.topologyRevision() != topologyRevision
                    || ClusterRecoveryDocuments.number(context.pipeline(), "executionGeneration") != expected.executionGeneration()
                    || !item.event().intentFingerprint().equals(expected.intentFingerprint())) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            return save(session, current, item.retargeted(target, topologyRevision, context.now()), null);
        });
    }

    @Override
    public Result acquirePermit(ClusterRecoveryFence expected, Map<String, ClusterCapacityDemand> demandByNode,
            ClusterCapacityLimits limits, Duration ttl, Duration refusalBackoff, int maxConcurrentRebuilds) {
        MongoClusterCapacityStore.positive(ttl);
        MongoClusterCapacityStore.positive(refusalBackoff);
        if (maxConcurrentRebuilds < 1) {
            throw new IllegalArgumentException("recovery concurrency must be positive");
        }
        return write(expected, (session, context) -> {
            ClusterRecoveryItem item = context.item;
            if (item.permit() != null) {
                return result(ClusterRecoveryMutation.DUPLICATE, item);
            }
            if (!item.eligibleAt(context.facts.now())) {
                return result(ClusterRecoveryMutation.RETRY_BACKOFF, item);
            }
            if (item.successor() == null) {
                OriginalAuthority original = originalAuthority(session, item, context.facts);
                if (!original.retired()) {
                    return save(session, context.stored, item.deferredUntil(original.nextEligibleAt(), context.facts.now()),
                            null, ClusterRecoveryMutation.WAITING_PERMIT);
                }
            }
            Document earlier = queue.find(session, new Document("clusterId", expected.key().clusterId())
                    .append("enqueueSequence", new Document("$lt", item.enqueueSequence()))
                    .append("status", new Document("$in", List.of("WAITING_PERMIT", "RETRY_BACKOFF")))
                    .append("permit", null).append("$or", List.of(new Document("nextEligibleAt", null),
                            new Document("nextEligibleAt", new Document("$lte", Date.from(context.facts.now()))))))
                    .sort(new Document("enqueueSequence", 1)).first();
            if (earlier != null) {
                return result(ClusterRecoveryMutation.WAITING_PERMIT, item);
            }
            long slots = queue.countDocuments(session, new Document("clusterId", expected.key().clusterId())
                    .append("permit", new Document("$ne", null)));
            if (slots >= maxConcurrentRebuilds) {
                return result(ClusterRecoveryMutation.WAITING_PERMIT, item);
            }
            WorkloadClaim pipeline = MongoWorkloadClaimStore.readDocument(context.facts.pipeline());
            ClusterCapacityStore.Result reservation = capacity.reserve(session, context.facts,
                    WorkloadClaimFence.from(pipeline), Map.copyOf(demandByNode), limits, ttl, true);
            if (reservation.outcome() == ClusterCapacityStore.Outcome.CAPACITY_REFUSED
                    || reservation.outcome() == ClusterCapacityStore.Outcome.UNKNOWN_DEMAND) {
                ClusterRecoveryDiagnostic diagnostic = capacityDiagnostic(context, reservation);
                ClusterRecoveryItem refused = item.refused(diagnostic, refusalBackoff, context.facts.now());
                return save(session, context.stored, refused, null, ClusterRecoveryMutation.CAPACITY_REFUSED);
            }
            if (reservation.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(reservation.outcome()), item);
            }
            ClusterCapacityReservation reserved = reservation.reservation();
            Instant deadline = reserved.deadline().isBefore(context.recoveryClaim.getDate("leaseUntil").toInstant())
                    ? reserved.deadline() : context.recoveryClaim.getDate("leaseUntil").toInstant();
            ClusterRecoveryPermit permit = new ClusterRecoveryPermit(reserved.reservationId(), expected.recoveryClaim(),
                    reserved.reservedAt(), deadline, reserved.demandByNode(), 0);
            return save(session, context.stored, item.permitted(permit, context.facts.now()), null);
        });
    }

    private ClusterRecoveryDiagnostic capacityDiagnostic(QueueContext context, ClusterCapacityStore.Result refusal) {
        if (refusal.refusedNode() != null && !refusal.violations().isEmpty()) {
            ClusterCapacityLimits.Violation violation = refusal.violations().getFirst();
            return ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED,
                    MongoClusterCapacityStore.capacityError(refusal.refusedNode(), violation.resource(), violation.occupied(), violation.requested(), violation.limit()),
                    context.item.event().resumePositions(), "Correct the member capacity or pipeline demand before an explicit start");
        }
        return ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED,
                MongoClusterCapacityStore.capacityError("unknown", "unknown-demand", "unknown", "unknown", "unknown"),
                context.item.event().resumePositions(), "Restore verifiable execution demand before an explicit start");
    }

    @Override
    public Result resumePermit(ClusterRecoveryFence expected, String reservationId, Duration ttl) {
        MongoClusterCapacityStore.positive(ttl);
        return write(expected, (session, context) -> {
            ClusterRecoveryPermit previous = context.item.permit();
            if (previous == null || !previous.reservationId().equals(reservationId)) {
                return result(ClusterRecoveryMutation.STALE_ITEM, context.item);
            }
            Instant deadline = context.facts.now().plus(ttl);
            Instant authority = context.recoveryClaim.getDate("leaseUntil").toInstant();
            if (deadline.isAfter(authority)) {
                deadline = authority;
            }
            ClusterCapacityReservation reservation = capacity.readReservation(session, reservationId);
            if (reservation == null || !reservation.profile().equals(context.item.targetProfile())) {
                return result(ClusterRecoveryMutation.STALE_PROFILE, context.item);
            }
            if (capacity.renewRecoveryDeadline(session, reservation, deadline) == null) {
                return result(ClusterRecoveryMutation.STALE_ITEM, context.item);
            }
            ClusterRecoveryPermit resumed = new ClusterRecoveryPermit(previous.reservationId(), expected.recoveryClaim(),
                    previous.reservedAt(), deadline, previous.demandByNode(), previous.transferredExecutionGeneration());
            return save(session, context.stored, context.item.permitted(resumed, context.facts.now()), null);
        });
    }

    @Override
    public Result advanceExecution(ClusterRecoveryFence expected, WorkloadClaim expectedPipelineClaim, Set<String> executionNodeIds) {
        return advance(expected, expectedPipelineClaim, executionNodeIds, null);
    }

    @Override
    public Result advanceExecution(ClusterRecoveryFence expected, WorkloadClaim expectedPipelineClaim,
            Set<String> executionNodeIds, Set<String> requiredSourceIds) {
        return advance(expected, expectedPipelineClaim, executionNodeIds, Set.copyOf(requiredSourceIds));
    }

    private Result advance(ClusterRecoveryFence expected, WorkloadClaim expectedPipelineClaim,
            Set<String> executionNodeIds, Set<String> selectedSources) {
        return write(expected, (session, context) -> {
            ClusterRecoveryItem item = context.item;
            Set<String> artifactSources = Set.copyOf(((PipelineResource) MongoArtifactStore.toResource(context.facts.artifact())).sourceIds());
            Set<String> requiredSources = selectedSources == null ? artifactSources : selectedSources;
            if (!artifactSources.containsAll(requiredSources) || requiredSources.stream().anyMatch(String::isBlank)) {
                return result(ClusterRecoveryMutation.STALE_INTENT, item);
            }
            if (item.permit() == null || !item.permit().deadline().isAfter(context.facts.now())
                    || !item.permit().recoveryClaim().equals(expected.recoveryClaim())) {
                return result(ClusterRecoveryMutation.WAITING_PERMIT, item);
            }
            if (item.successor() == null && !originalAuthority(session, item, context.facts).retired()) {
                return result(ClusterRecoveryMutation.WAITING_PERMIT, item);
            }
            if (item.successor() != null && !retired(session, item.successor().pipelineClaim(), item.successor().profile())) {
                return result(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED, item);
            }
            if (item.permit().transferredExecutionGeneration() != 0 || expectedPipelineClaim.executionGeneration() != item.executionFrontier()) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            ClusterCapacityReservation reservation = capacity.readReservation(session, item.permit().reservationId());
            if (reservation == null) {
                return result(ClusterRecoveryMutation.STALE_ITEM, item);
            }
            reservation = capacity.bindRecoveryPipeline(session, reservation, expectedPipelineClaim);
            if (reservation == null) {
                return result(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM, item);
            }
            ClusterCapacityStore.Result advanced = capacity.advanceExecution(session, reservation, expectedPipelineClaim, executionNodeIds);
            if (advanced.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(advanced.outcome()), item);
            }
            WorkloadClaim claim = advanced.advancedPipelineClaim();
            ClusterRecoverySuccessor successor = new ClusterRecoverySuccessor(WorkloadClaimFence.from(claim), item.targetProfile(),
                    executionNodeIds, requiredSources, context.facts.now(), null, null, Map.of(), null);
            return save(session, context.stored, item.advanced(successor, context.facts.now()), claim);
        });
    }

    @Override
    public Result recordSubmission(ClusterRecoveryFence expected, WorkloadClaimFence pipelineClaim, String nativeJobId) {
        return write(expected, (session, context) -> {
            ClusterRecoveryItem item = context.item;
            if (!item.hasAllocatedSuccessor() || !item.successor().pipelineClaim().sameAuthorityAs(pipelineClaim)
                    || item.successor().failureNote() != null) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            ClusterCapacityReservation reservation = capacity.readReservation(session, item.permit().reservationId());
            if (reservation == null) {
                return result(ClusterRecoveryMutation.STALE_ITEM, item);
            }
            ClusterCapacityStore.Result transferred = capacity.submitted(session, reservation, pipelineClaim, nativeJobId);
            if (transferred.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(transferred.outcome()), item);
            }
            return save(session, context.stored, item.submitted(nativeJobId, context.facts.now()), null);
        });
    }

    @Override
    public Result recordStartup(ClusterRecoveryFence expected, WorkloadClaimFence pipelineClaim, ClusterRecoveryStartupReceipt receipt) {
        return write(expected, (session, context) -> {
            ClusterRecoverySuccessor successor = context.item.successor();
            if (!context.item.hasAllocatedSuccessor() || !successor.pipelineClaim().sameAuthorityAs(pipelineClaim)
                    || !originalOrCurrent(receipt.pipelineClaim(), successor.pipelineClaim(), pipelineClaim)) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, context.item);
            }
            Document authority = capacity.workloadStore.conditionTouchClaim(session, pipelineClaim);
            if (authority == null) {
                return result(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM, context.item);
            }
            if (!matchesContext(context.item, context.facts, authority)) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, context.item);
            }
            ClusterRecoveryStartupReceipt originalReceipt = receiptWithClaim(receipt, successor.pipelineClaim());
            ClusterRecoveryMutation evidence = context.item.startupReceiptCheck(originalReceipt);
            if (evidence != ClusterRecoveryMutation.APPLIED) {
                return result(evidence, context.item);
            }
            ClusterRecoveryMutation liveExecution = startupExecutionCheck(context.item.successor(), context.facts.pipeline(),
                    context.facts.actual(), originalReceipt);
            if (liveExecution != ClusterRecoveryMutation.APPLIED) {
                return result(liveExecution, context.item);
            }
            if (!guardSources(session, successor, originalReceipt, pipelineClaim)) {
                return result(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT, context.item);
            }
            return save(session, context.stored, context.item.initialized(originalReceipt, context.facts.now()), null);
        });
    }

    @Override
    public Result complete(ClusterRecoveryFence expected) {
        return write(expected, (session, context) -> {
            ClusterRecoveryMutation evidence = context.item.completionCheck();
            if (evidence != ClusterRecoveryMutation.APPLIED) {
                return result(evidence, context.item);
            }
            ClusterRecoverySuccessor successor = context.item.successor();
            WorkloadClaimFence current = WorkloadClaimFence.from(MongoWorkloadClaimStore.readDocument(context.facts.pipeline()));
            if (!successor.pipelineClaim().sameAuthorityAs(current) || !matchesContext(context.item, context.facts, context.facts.pipeline())) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, context.item);
            }
            ClusterRecoveryMutation liveExecution = startupExecutionCheck(successor, context.facts.pipeline(),
                    context.facts.actual(), successor.startupReceipt());
            if (liveExecution != ClusterRecoveryMutation.APPLIED) {
                return result(liveExecution, context.item);
            }
            if (capacity.workloadStore.conditionTouchClaim(session, current) == null) {
                return result(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM, context.item);
            }
            if (!guardSources(session, successor, successor.startupReceipt(), current)) {
                return result(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT, context.item);
            }
            ClusterCapacityReservation reservation = capacity.readReservation(session, context.item.permit().reservationId());
            if (reservation == null || reservation.nativeJobId() == null
                    || !reservation.nativeJobId().equals(context.item.successor().nativeJobId())) {
                return result(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT, context.item);
            }
            return save(session, context.stored, context.item.recovered(context.facts.now()), null);
        });
    }

    static ClusterRecoveryMutation startupExecutionCheck(ClusterRecoverySuccessor successor, Document pipeline,
            Document actual, ClusterRecoveryStartupReceipt receipt) {
        if (ClusterRecoveryDocuments.number(pipeline, "executionGeneration") != successor.executionGeneration()
                || numberOrZero(pipeline, "contextExecutionGeneration") != successor.executionGeneration()
                || numberOrZero(pipeline, "failureClaimGeneration") != 0 || "FAILED".equals(actual.getString("stateJson"))) {
            return ClusterRecoveryMutation.STALE_EXECUTION;
        }
        return "RUNNING".equals(actual.getString("stateJson"))
                || ("COMPLETED".equals(actual.getString("stateJson")) && receipt.executionCompleted())
                ? ClusterRecoveryMutation.APPLIED : ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT;
    }

    private boolean guardSources(ClientSession session, ClusterRecoverySuccessor successor, ClusterRecoveryStartupReceipt receipt,
            WorkloadClaimFence current) {
        if (!successor.matchesStartup(receipt)) {
            return false;
        }
        if (!successor.requiredSourceIds().isEmpty() && meta == null) {
            return false;
        }
        for (String source : successor.requiredSourceIds()) {
            if (!meta.guardPreparedStartup(session, current, receipt.preparedWitnesses().get(source),
                    receipt.requestedPositions().get(source), successor.requiredSourceIds())) {
                return false;
            }
        }
        return true;
    }

    private static boolean originalOrCurrent(WorkloadClaimFence observed, WorkloadClaimFence original, WorkloadClaimFence current) {
        return original.equals(observed) || current.equals(observed);
    }

    private static ClusterRecoveryStartupReceipt receiptWithClaim(ClusterRecoveryStartupReceipt receipt, WorkloadClaimFence claim) {
        return new ClusterRecoveryStartupReceipt(claim, receipt.nativeJobId(), receipt.nativeInitializedAt(), receipt.preparedWitnesses(),
                receipt.requestedPositions(), receipt.acceptedPositions(), receipt.positionsAcceptedAt(), receipt.executionCompleted());
    }

    private static boolean matchesContext(ClusterRecoveryItem item, MongoClusterCapacityStore.Context facts, Document authority) {
        ClusterRecoverySuccessor successor = item.successor();
        return successor != null && MongoClusterCapacityStore.matchesExecutionContext(authority, successor.pipelineClaim(),
                successor.profile(), item.event().key(), facts.intent().getString("revision"), successor.executionNodeIds());
    }

    /** An older allocation marker is repaired only from the reservation created by the real issuer. */
    private ClusterRecoveryItem recordedAllocation(ClientSession session, ClusterRecoveryItem item, MongoClusterCapacityStore.Context facts) {
        if (item.hasAllocatedSuccessor() || item.permit() == null || item.successor() == null
                || item.permit().transferredExecutionGeneration() != 0 || item.successor().nativeJobId() != null
                || !item.successor().sourceRequirementsRecorded()) {
            return item;
        }
        ClusterCapacityReservation reservation = capacity.readReservation(session, item.permit().reservationId());
        if (reservation == null || reservation.executionGeneration() == null
                || reservation.executionGeneration() != item.successor().executionGeneration()
                || !reservation.pipelineClaim().equals(item.successor().pipelineClaim())
                || !reservation.profile().equals(item.successor().profile())
                || !reservation.incarnationId().equals(item.event().key().incarnation())
                || !reservation.intentFingerprint().equals(item.event().intentFingerprint())
                || !reservation.demandByNode().equals(item.permit().demandByNode()) || reservation.nativeJobId() != null
                || !matchesContext(item, facts, facts.pipeline())
                || !Set.copyOf(((PipelineResource) MongoArtifactStore.toResource(facts.artifact())).sourceIds())
                        .containsAll(item.successor().requiredSourceIds())
                || !capacity.touchAllocation(session, reservation)) {
            return item;
        }
        return item.allocationRecorded(reservation.executionGeneration(), facts.now());
    }

    private OriginalAuthority originalAuthority(ClientSession session, ClusterRecoveryItem item, MongoClusterCapacityStore.Context facts) {
        Document profile = capacity.profiles.find(session, new Document("_id", item.event().key().clusterId())).first();
        return originalAuthority(item.event(), facts.profile(), facts.pipeline(), profile, facts.now());
    }

    static OriginalAuthority originalAuthority(ClusterRecoveryEvent event, ClusterExecutionProfile target,
            Document pipeline, Document profile, Instant now) {
        if (event.legacySourceProfile()) {
            Instant cold = horizon(profile, "legacyAuthorityRetiredAt");
            if (!pipeline.containsKey("executionProfileVersion") && pipeline.get("executionProfile") == null
                    && cold != null && !cold.isAfter(now)) {
                return new OriginalAuthority(true, null);
            }
        } else if (numberOrZero(pipeline, "contextExecutionGeneration") == event.originalExecutionGeneration()
                && target.generation() > event.sourceProfile().generation()) {
            return new OriginalAuthority(true, null);
        } else if (numberOrZero(pipeline, "contextExecutionGeneration") == event.originalExecutionGeneration()
                && numberOrZero(pipeline, "executionClaimGeneration") > 0
                && ClusterRecoveryDocuments.number(pipeline, "claimGeneration") > numberOrZero(pipeline, "executionClaimGeneration")) {
            Instant retired = horizon(pipeline, "retiredAuthorizationUntil");
            if (retired != null && !retired.isAfter(now)) {
                return new OriginalAuthority(true, null);
            }
        }
        Instant eligible = horizon(pipeline, "retiredAuthorizationUntil");
        Instant current = horizon(pipeline, "leaseUntil");
        if (ClusterRecoveryDocuments.number(pipeline, "claimGeneration") <= numberOrZero(pipeline, "executionClaimGeneration")
                && current != null && (eligible == null || current.isAfter(eligible))) {
            eligible = current;
        }
        if (eligible == null || !eligible.isAfter(now)) {
            eligible = now.plusSeconds(1);
        }
        return new OriginalAuthority(false, eligible);
    }

    private static Instant horizon(Document document, String field) {
        Object value = document == null ? null : document.get(field);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Date date)) {
            throw ClusterRecoveryDocuments.unreadable(document, field, null);
        }
        return date.toInstant();
    }

    record OriginalAuthority(boolean retired, Instant nextEligibleAt) { }

    @Override
    public Result recordFailureNote(ClusterRecoveryPipelineFence expected, ClusterRecoveryDiagnostic diagnostic,
            FailureStage stage, CaptureStartupFailure sourceFailure) {
        return capacity.profileStore.transaction(session -> {
            Document stored = queue.find(session, new Document("_id", ClusterRecoveryDocuments.id(expected.key()))).first();
            if (stored == null) {
                return result(ClusterRecoveryMutation.NOT_FOUND, null);
            }
            ClusterRecoveryItem item = ClusterRecoveryDocuments.item(stored);
            if (item.itemRevision() != expected.itemRevision()) {
                return result(ClusterRecoveryMutation.STALE_ITEM, item);
            }
            if (item.status().terminal()) {
                return result(ClusterRecoveryMutation.TERMINAL, item);
            }
            if (!item.event().intentFingerprint().equals(expected.intentFingerprint())) {
                return result(ClusterRecoveryMutation.STALE_INTENT, item);
            }
            if (!item.targetProfile().equals(expected.targetProfile())) {
                return result(ClusterRecoveryMutation.STALE_PROFILE, item);
            }
            var facts = capacity.guardFailureFact(session, expected.key(), expected.targetProfile(), expected.intentFingerprint());
            if (facts.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(facts.outcome()), item);
            }
            if (!List.of("RUNNING", "FAILED").contains(facts.actual().getString("stateJson"))) {
                return result(ClusterRecoveryMutation.TERMINAL, item);
            }
            item = recordedAllocation(session, item, facts);
            if (!item.hasAllocatedSuccessor() || !item.successor().pipelineClaim().sameAuthorityAs(expected.pipelineClaim())
                    || item.executionFrontier() != expected.pipelineClaim().executionGeneration()) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            Document authority = capacity.workloadStore.conditionTouchClaim(session, expected.pipelineClaim());
            if (authority == null) {
                return result(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM, item);
            }
            if (!matchesContext(item, facts, authority)) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            if (item.successor().failureNote() != null) {
                return result(ClusterRecoveryMutation.DUPLICATE, item);
            }
            boolean qualified;
            if (sourceFailure == null) {
                qualified = numberOrZero(facts.pipeline(), "contextExecutionGeneration") == item.executionFrontier()
                        && numberOrZero(facts.pipeline(), "failureClaimGeneration") == expected.pipelineClaim().claimGeneration();
            } else {
                String source = sourceFailure.witness().sourceId();
                if (!originalOrCurrent(sourceFailure.pipelineClaim(), item.successor().pipelineClaim(), expected.pipelineClaim())) {
                    return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
                }
                var rebound = new CaptureStartupFailure(expected.pipelineClaim(), sourceFailure.witness(),
                        sourceFailure.requestedPosition(), sourceFailure.preparedAt(), sourceFailure.readerState());
                qualified = meta != null
                        && item.successor().sourceRequirementsRecorded() && item.successor().requiredSourceIds().contains(source)
                        && diagnostic.code().equals(sourceFailure.code()) && diagnostic.params().equals(sourceFailure.params())
                        && (sourceFailure.requestedPosition() == null ? !diagnostic.positions().containsKey(source)
                                : sourceFailure.requestedPosition().equals(diagnostic.positions().get(source)))
                        && meta.guardStartupFailure(session, rebound, expected.pipelineClaim(), item.successor().requiredSourceIds());
            }
            if (!qualified) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            ClusterRecoveryFailureNote note = new ClusterRecoveryFailureNote(item.successor().pipelineClaim(), stage, diagnostic, facts.now());
            return save(session, stored, item.failureNoted(note, facts.now()), null);
        });
    }

    @Override
    public Result fail(ClusterRecoveryFence expected, WorkloadClaimFence pipelineClaim,
            ClusterRecoveryDiagnostic diagnostic, FailureStage stage, Duration backoff) {
        MongoClusterCapacityStore.positive(backoff);
        return write(expected, (session, context) -> {
            ClusterRecoveryItem item = context.item;
            ClusterRecoveryFailureNote note = item.hasAllocatedSuccessor() ? item.successor().failureNote() : null;
            FailureStage effectiveStage = note == null ? stage : note.stage();
            ClusterRecoveryDiagnostic effectiveDiagnostic = note == null ? diagnostic : note.diagnostic();
            if ((effectiveStage == FailureStage.ALLOCATED_EXECUTION || effectiveStage == FailureStage.SOURCE_POSITION_REJECTION)
                    && (!item.hasAllocatedSuccessor() || !item.successor().pipelineClaim().sameAuthorityAs(pipelineClaim))) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            if (effectiveStage == FailureStage.SOURCE_POSITION_REJECTION
                    && effectiveDiagnostic.reason() != ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED) {
                return result(ClusterRecoveryMutation.STALE_EXECUTION, item);
            }
            if (!releaseReservation(session, item, context.facts.now())) {
                return result(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED, item);
            }
            ClusterRecoveryItem failed = (effectiveStage == FailureStage.ALLOCATED_EXECUTION || effectiveStage == FailureStage.SOURCE_POSITION_REJECTION)
                    ? item.executionFailed(effectiveDiagnostic, backoff, context.facts.now()) : item.refused(effectiveDiagnostic, backoff, context.facts.now());
            return save(session, context.stored, failed, null);
        });
    }

    @Override
    public Result cancel(ClusterRecoveryFence expected) {
        return capacity.profileStore.transaction(session -> {
            Document current = queue.find(session, new Document("_id", ClusterRecoveryDocuments.id(expected.key()))).first();
            if (current == null) {
                return result(ClusterRecoveryMutation.NOT_FOUND, null);
            }
            ClusterRecoveryItem item = ClusterRecoveryDocuments.item(current);
            ClusterRecoveryMutation optimistic = itemExpectations(item, expected);
            if (optimistic != ClusterRecoveryMutation.APPLIED) {
                return result(optimistic, item);
            }
            if (item.status().terminal()) {
                return result(ClusterRecoveryMutation.TERMINAL, item);
            }
            if (capacity.workloadStore.conditionTouchClaim(session, expected.recoveryClaim()) == null) {
                return result(ClusterRecoveryMutation.STALE_RECOVERY_CLAIM, item);
            }
            Document artifact = capacity.artifacts.find(session, new Document("_id", expected.key().pipelineId())).first();
            Document intent = capacity.desired.find(session, new Document("_id", expected.key().pipelineId())).first();
            Document actual = capacity.states.find(session, new Document("_id", expected.key().pipelineId())).first();
            Document pipeline = capacity.claims.find(session, new Document("_id",
                    MongoClusterCapacityStore.claimId(expected.key().clusterId(), expected.key().pipelineId()))).first();
            boolean sameIncarnation = artifact != null && expected.key().incarnation().equals(artifact.getString("incarnation"));
            boolean sameExecution = pipeline != null && item.executionAliases().contains(ClusterRecoveryDocuments.number(pipeline, "executionGeneration"));
            boolean userTerminal = actual == null || List.of("COMPLETED", "STOPPED", "PAUSED", "DRAFT", "VALIDATED").contains(actual.getString("stateJson"));
            if (sameIncarnation && intent != null && "RUNNING".equals(intent.getString("targetState"))
                    && sameExecution && !userTerminal
                    && item.event().intentFingerprint().equals(MongoClusterCapacityStore.intentFingerprint(expected.key(), intent))) {
                return result(ClusterRecoveryMutation.STALE_INTENT, item);
            }
            if (artifact != null) {
                capacity.artifacts.updateOne(session, new Document("_id", artifact.get("_id")).append("incarnation", artifact.get("incarnation")),
                        new Document("$inc", new Document("recoveryFenceSerial", 1L)));
            }
            if (intent != null) {
                capacity.desired.updateOne(session, MongoClusterCapacityStore.intentFilter(intent),
                        new Document("$inc", new Document("recoveryFenceSerial", 1L)));
            }
            if (actual != null) {
                capacity.states.updateOne(session, new Document("_id", actual.get("_id")).append("epoch", actual.get("epoch"))
                        .append("stateJson", actual.get("stateJson")), new Document("$inc", new Document("recoveryFenceSerial", 1L)));
            }
            Instant now = capacity.clock(session, expected.key().clusterId());
            if (!releaseReservation(session, item, now)) {
                return result(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED, item);
            }
            return save(session, current, item.cancelled(now), null);
        });
    }

    @Override
    public Result releaseExpiredPermit(ClusterRecoveryFence expected) {
        return write(expected, (session, context) -> {
            ClusterRecoveryItem item = context.item;
            if (item.permit() == null || item.permit().deadline().isAfter(context.facts.now())) {
                return result(ClusterRecoveryMutation.WAITING_PERMIT, item);
            }
            if (!releaseReservation(session, item, context.facts.now())) {
                return result(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED, item);
            }
            if (item.hasAllocatedSuccessor() && item.successor().failureNote() != null) {
                return save(session, context.stored, item.executionFailed(item.successor().failureNote().diagnostic(),
                        Duration.ofMillis(1), context.facts.now()), null);
            }
            if (item.attempt() >= item.maxAttempts() && item.hasAllocatedSuccessor()) {
                ClusterRecoveryDiagnostic cause = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                        new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null),
                        item.event().resumePositions(), "Confirm the prior execution is stopped before starting the pipeline explicitly");
                return save(session, context.stored, item.executionFailed(cause, Duration.ofMillis(1), context.facts.now()), null);
            }
            ClusterRecoveryItem waiting = new ClusterRecoveryItem(item.schemaVersion(), item.event(), Math.addExact(item.itemRevision(), 1),
                    item.enqueueSequence(), item.enqueuedAt(), context.facts.now(), item.targetProfile(), item.targetTopologyRevision(),
                    ClusterRecoveryStatus.WAITING_PERMIT, item.attempt(), item.maxAttempts(), context.facts.now(), item.executionAliases(),
                    null, item.successor(), item.diagnostic());
            return save(session, context.stored, waiting, null);
        });
    }

    @Override
    public Result removeDeleted(ClusterRecoveryFence expected) {
        return capacity.profileStore.transaction(session -> {
            Document current = queue.find(session, new Document("_id", ClusterRecoveryDocuments.id(expected.key()))).first();
            if (current == null) {
                return result(ClusterRecoveryMutation.NOT_FOUND, null);
            }
            ClusterRecoveryItem item = ClusterRecoveryDocuments.item(current);
            ClusterRecoveryMutation optimistic = itemExpectations(item, expected);
            if (optimistic != ClusterRecoveryMutation.APPLIED) {
                return result(optimistic, item);
            }
            if (capacity.workloadStore.conditionTouchClaim(session, expected.recoveryClaim()) == null) {
                return result(ClusterRecoveryMutation.STALE_RECOVERY_CLAIM, item);
            }
            Document artifact = capacity.artifacts.find(session, new Document("_id", expected.key().pipelineId())).first();
            if (artifact != null && expected.key().incarnation().equals(artifact.getString("incarnation"))) {
                return result(ClusterRecoveryMutation.STALE_INTENT, item);
            }
            Instant now = capacity.clock(session, expected.key().clusterId());
            if (!releaseReservation(session, item, now)
                    || (item.successor() != null && !retired(session, item.successor().pipelineClaim(), item.successor().profile()))) {
                return result(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED, item);
            }
            queue.deleteOne(session, itemFilter(current));
            return result(ClusterRecoveryMutation.APPLIED, item);
        });
    }

    private boolean releaseReservation(ClientSession session, ClusterRecoveryItem item, Instant now) {
        if (item.permit() == null) {
            return item.successor() == null || retired(session, item.successor().pipelineClaim(), item.successor().profile());
        }
        ClusterCapacityReservation reservation = capacity.readReservation(session, item.permit().reservationId());
        return reservation != null ? capacity.releaseInTransaction(session, reservation, now)
                : item.successor() != null && retired(session, item.successor().pipelineClaim(), item.successor().profile());
    }

    private boolean retired(ClientSession session, WorkloadClaimFence claim, ClusterExecutionProfile profile) {
        Document current = capacity.profiles.find(session, new Document("_id", profile.clusterId())).first();
        return current != null && (ClusterRecoveryDocuments.number(current, "generation") > profile.generation()
                || capacity.workloadStore.provesRetired(session, claim));
    }

    private Result write(ClusterRecoveryFence expected, BiFunction<ClientSession, QueueContext, Result> operation) {
        return capacity.profileStore.transaction(session -> {
            Document current = queue.find(session, new Document("_id", ClusterRecoveryDocuments.id(expected.key()))).first();
            if (current == null) {
                return result(ClusterRecoveryMutation.NOT_FOUND, null);
            }
            ClusterRecoveryItem item = ClusterRecoveryDocuments.item(current);
            if (item.itemRevision() != expected.itemRevision()) {
                return result(ClusterRecoveryMutation.STALE_ITEM, item);
            }
            if (item.status().terminal()) {
                return result(ClusterRecoveryMutation.TERMINAL, item);
            }
            Document recovery = capacity.workloadStore.conditionTouchClaim(session, expected.recoveryClaim());
            if (recovery == null) {
                return result(ClusterRecoveryMutation.STALE_RECOVERY_CLAIM, item);
            }
            MongoClusterCapacityStore.Context facts = capacity.guard(session, expected.key(), expected.targetProfile(), expected.intentFingerprint(), true);
            if (facts.outcome() != ClusterCapacityStore.Outcome.APPLIED) {
                return result(mutation(facts.outcome()), item);
            }
            ClusterRecoveryMutation optimistic = item.check(expected, facts.intentFingerprint(), facts.profile(),
                    ClusterRecoveryDocuments.number(facts.pipeline(), "executionGeneration"));
            if (optimistic != ClusterRecoveryMutation.APPLIED) {
                return result(optimistic, item);
            }
            return operation.apply(session, new QueueContext(current, recordedAllocation(session, item, facts), facts, recovery));
        });
    }

    private Result save(ClientSession session, Document current, ClusterRecoveryItem next, WorkloadClaim claim) {
        return save(session, current, next, claim, ClusterRecoveryMutation.APPLIED);
    }

    private Result save(ClientSession session, Document current, ClusterRecoveryItem next, WorkloadClaim claim, ClusterRecoveryMutation outcome) {
        Document replacement = ClusterRecoveryDocuments.item(next);
        Document terminal = next.status().terminal() ? ClusterRecoveryDocuments.item(next) : current.get("latestTerminal", Document.class);
        if (terminal != null) {
            replacement.append("latestTerminal", terminal);
        }
        if (queue.replaceOne(session, itemFilter(current), replacement).getMatchedCount() != 1) {
            throw new IllegalStateException("profile-guarded recovery item changed in its transaction");
        }
        return new Result(outcome, next, claim);
    }

    private static Document itemFilter(Document current) {
        return new Document("_id", current.get("_id")).append("itemRevision", current.get("itemRevision"));
    }

    private static ClusterRecoveryMutation itemExpectations(ClusterRecoveryItem item, ClusterRecoveryFence expected) {
        if (!item.event().key().equals(expected.key()) || item.itemRevision() != expected.itemRevision()) {
            return ClusterRecoveryMutation.STALE_ITEM;
        }
        if (!item.event().intentFingerprint().equals(expected.intentFingerprint())) {
            return ClusterRecoveryMutation.STALE_INTENT;
        }
        if (!item.targetProfile().equals(expected.targetProfile())) {
            return ClusterRecoveryMutation.STALE_PROFILE;
        }
        return item.executionFrontier() == expected.executionGeneration()
                ? ClusterRecoveryMutation.APPLIED : ClusterRecoveryMutation.STALE_EXECUTION;
    }

    private static Document terminalCopy(Document current) {
        Document terminal = new Document(current);
        terminal.remove("latestTerminal");
        return terminal;
    }

    private static ClusterRecoveryMutation mutation(ClusterCapacityStore.Outcome outcome) {
        return switch (outcome) {
            case APPLIED -> ClusterRecoveryMutation.APPLIED;
            case ALREADY_RESERVED -> ClusterRecoveryMutation.DUPLICATE;
            case CAPACITY_REFUSED, UNKNOWN_DEMAND -> ClusterRecoveryMutation.CAPACITY_REFUSED;
            case STALE_CLAIM -> ClusterRecoveryMutation.STALE_PIPELINE_CLAIM;
            case STALE_PROFILE -> ClusterRecoveryMutation.STALE_PROFILE;
            case STALE_INTENT -> ClusterRecoveryMutation.STALE_INTENT;
            case STALE_EXECUTION -> ClusterRecoveryMutation.STALE_EXECUTION;
            case WAITING_QUORUM -> ClusterRecoveryMutation.WAITING_QUORUM;
        };
    }

    private static Result result(ClusterRecoveryMutation outcome, ClusterRecoveryItem item) {
        return new Result(outcome, item, null);
    }

    private static long numberOrZero(Document document, String field) {
        Number number = document.get(field, Number.class);
        return number == null ? 0 : number.longValue();
    }

    private record QueueContext(Document stored, ClusterRecoveryItem item, MongoClusterCapacityStore.Context facts,
            Document recoveryClaim) {}
}
