package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ClusterRecoveryDiagnostic;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;

/** Joins durable recovery facts through read-only ports; it never acquires or retires authority. */
public final class ClusterRecoveryProjection {
    private static final int PAGE_SIZE = 100;
    private final ArtifactStore artifacts;
    private final ClusterRecoveryStore queue;
    private final ClusterProfileStore profiles;
    private final ClusterCapacityStore capacity;
    private final WorkloadClaimStore claims;

    public ClusterRecoveryProjection(ArtifactStore artifacts, ClusterRecoveryStore queue,
            ClusterProfileStore profiles, ClusterCapacityStore capacity, WorkloadClaimStore claims) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.profiles = Objects.requireNonNull(profiles, "profiles");
        this.capacity = Objects.requireNonNull(capacity, "capacity");
        this.claims = Objects.requireNonNull(claims, "claims");
    }

    /** Quorum is supplied by the existing gate; null means it was not observed by this endpoint. */
    public ClusterRecoveryView cluster(String clusterId, Boolean quorumReady) {
        Reading<QueueRows> queued = read(() -> queueRows(clusterId));
        List<Row> rows = queued.value() == null ? List.of() : queued.value().rows();
        Reading<Optional<ClusterExecutionProfile>> installed = read(() -> profiles.profile(clusterId));
        ClusterExecutionProfile profile = installed.value() == null ? null : installed.value().orElse(null);
        List<WorkloadClaimKey> keys = new ArrayList<>();
        WorkloadClaimKey coordinator = new WorkloadClaimKey(clusterId, WorkloadClaimType.CLUSTER_RECOVERY, clusterId);
        keys.add(coordinator);
        rows.stream().map(row -> new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION,
                row.item().event().key().pipelineId())).distinct().forEach(keys::add);
        Reading<Map<WorkloadClaimKey, WorkloadClaimReading>> held = read(() -> claims.readAll(keys));
        Map<WorkloadClaimKey, WorkloadClaimReading> heldClaims = held.value() == null ? Map.of() : held.value();
        List<ClusterRecoveryItemView> items = rows.stream().map(row -> item(row, quorumReady,
                heldClaims.get(new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION,
                        row.item().event().key().pipelineId())), held.unavailable())).toList();
        return new ClusterRecoveryView(clusterId, quorumReady, state(items, queued.unavailable()), causes(items), items,
                profile(profile), installed.unavailable(), claimReading(heldClaims.get(coordinator)), held.unavailable(),
                capacity(clusterId, profile), queued.unavailable());
    }

    /** Keeps the active item and latest terminal result for this pipeline's actual current incarnation. */
    public ClusterPipelineRecoveryView pipeline(String clusterId, String pipelineId, Boolean quorumReady) {
        ClusterRecoveryView all = cluster(clusterId, quorumReady);
        Reading<Optional<ArtifactIdentity>> current = read(() -> artifacts.identity(pipelineId));
        ArtifactIdentity identity = current.value() == null ? null : current.value().orElse(null);
        ClusterRecoveryReadFailure unavailable = current.unavailable() == null
                ? all.queueUnavailable() : current.unavailable();
        List<ClusterRecoveryItemView> items = identity == null ? List.of() : all.items().stream()
                .filter(item -> pipelineId.equals(item.pipelineId()) && identity.incarnation().equals(item.incarnation())).toList();
        return new ClusterPipelineRecoveryView(pipelineId, identity == null ? null : identity.incarnation(),
                state(items, unavailable), causes(items), items, unavailable, all.capacity());
    }

    private QueueRows queueRows(String clusterId) {
        Map<Long, ClusterRecoveryItem> observed = new LinkedHashMap<>();
        for (int offset = 0; ; offset = Math.addExact(offset, PAGE_SIZE)) {
            List<ClusterRecoveryItem> page = queue.list(clusterId, offset, PAGE_SIZE);
            for (ClusterRecoveryItem item : page) {
                if (!clusterId.equals(item.event().key().clusterId())) {
                    throw new IllegalStateException("recovery store returned another cluster's item");
                }
                observed.merge(item.enqueueSequence(), item,
                        (first, second) -> first.itemRevision() >= second.itemRevision() ? first : second);
            }
            if (page.size() < PAGE_SIZE) { break; }
        }
        Map<String, Optional<ArtifactIdentity>> identities = new LinkedHashMap<>();
        List<Row> rows = new ArrayList<>();
        int activePosition = 0;
        for (ClusterRecoveryItem item : observed.values().stream()
                .sorted(Comparator.comparingLong(ClusterRecoveryItem::enqueueSequence)).toList()) {
            String pipeline = item.event().key().pipelineId();
            ArtifactIdentity current = identities.computeIfAbsent(pipeline, artifacts::identity).orElse(null);
            if (current == null || !current.incarnation().equals(item.event().key().incarnation())) { continue; }
            Integer position = item.status().terminal() ? null : (activePosition = Math.incrementExact(activePosition));
            rows.add(new Row(item, current, position));
        }
        return new QueueRows(List.copyOf(rows));
    }

    private ClusterCapacityView capacity(String clusterId, ClusterExecutionProfile installed) {
        ClusterExecutionProfile observedProfile = installed;
        try {
            Optional<ClusterCapacityStore.Snapshot> read = capacity.readOccupied(clusterId);
            if (read.isEmpty()) {
                return new ClusterCapacityView("PROFILE_ABSENT", "mongo-capacity-profile-absent", null, null, null, null);
            }
            ClusterCapacityStore.Snapshot snapshot = read.get();
            observedProfile = snapshot.profile();
            ClusterResourceCounts ceilings = ceilings(snapshot.profile());
            Map<String, ClusterResourceCounts> occupied = new TreeMap<>();
            snapshot.occupiedByNode().forEach((node, demand) -> occupied.put(node, counts(demand)));
            return new ClusterCapacityView("AVAILABLE", "mongo-capacity-snapshot", profile(snapshot.profile()),
                    ceilings, occupied, null);
        } catch (TapstateException failure) {
            ClusterResourceCounts ceilings = null;
            if (observedProfile != null) {
                try { ceilings = ceilings(observedProfile); }
                catch (TapstateException unreadableCeilings) { /* The first read diagnostic remains authoritative. */ }
            }
            return new ClusterCapacityView("UNAVAILABLE", "mongo-capacity-unavailable", profile(observedProfile), ceilings,
                    null, ClusterRecoveryReadFailure.from(failure));
        }
    }

    private static ClusterResourceCounts ceilings(ClusterExecutionProfile profile) {
        return new ClusterResourceCounts(ceiling(profile, "capacityProcessors"), ceiling(profile, "capacityBlockingProcessors"),
                ceiling(profile, "capacityWriters"), ceiling(profile, "capacityConnectorInstances"),
                ceiling(profile, "capacityBufferedRecords"), ceiling(profile, "capacityEdgeQueueRecords"));
    }

    private static long ceiling(ClusterExecutionProfile profile, String name) {
        String value = profile.profile().attributes().get(name);
        try {
            long count = Long.parseLong(value);
            if (count > 0) { return count; }
        } catch (NumberFormatException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", profile.clusterId(), "field", name), invalid);
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", profile.clusterId(), "field", name), null);
    }

    private static ClusterRecoveryItemView item(Row row, Boolean quorumReady, WorkloadClaimReading current,
            ClusterRecoveryReadFailure claimUnavailable) {
        ClusterRecoveryItem item = row.item();
        var event = item.event();
        var permit = item.permit();
        var successor = item.successor();
        var receipt = successor == null ? null : successor.startupReceipt();
        var note = successor == null ? null : successor.failureNote();
        Map<String, ClusterResourceCounts> demand = new TreeMap<>();
        if (permit != null) { permit.demandByNode().forEach((node, value) -> demand.put(node, counts(value))); }
        return new ClusterRecoveryItemView(event.key().pipelineId(), event.key().incarnation(), row.artifact().contentHash(),
                event.intentFingerprint(), event.cause().name(), item.status().name(),
                Boolean.FALSE.equals(quorumReady) && !item.status().terminal() ? "WAITING_QUORUM" : item.status().name(),
                item.enqueueSequence(), item.enqueuedAt(), item.updatedAt(), row.position(), item.attempt(), item.maxAttempts(),
                item.nextEligibleAt(), event.originalExecutionGeneration(), event.originalExecutionRevision(),
                event.sourceTopologyRevision(), profile(event.sourceProfile()), event.legacySourceProfile(),
                profile(item.targetProfile()), item.targetTopologyRevision(), item.executionFrontier(), positions(event.resumePositions()),
                permit == null ? null : new ClusterRecoveryItemView.Permit(permit.reservationId(), permit.reservedAt(), permit.deadline(),
                        claim(permit.recoveryClaim()), permit.transferredExecutionGeneration(), demand),
                successor == null ? null : new ClusterRecoveryItemView.Successor(claim(successor.pipelineClaim()),
                        profile(successor.profile()), successor.executionNodeIds().stream().sorted().toList(),
                        successor.requiredSourceIds().stream().sorted().toList(), successor.sourceRequirementsRecorded(),
                        successor.allocatedAt(), successor.nativeJobId(), successor.submittedAt(),
                        receipt == null ? null : receipt.nativeInitializedAt(), receipt == null ? null : receipt.positionsAcceptedAt(),
                        receipt == null ? null : receipt.executionCompleted(), positions(successor.requestedPositions()),
                        receipt == null ? Map.of() : positions(receipt.acceptedPositions()),
                        note == null ? null : new ClusterRecoveryItemView.FailureNote(claim(note.pipelineClaim()),
                                note.stage().name(), diagnostic(note.diagnostic()), note.recordedAt())),
                diagnostic(item.diagnostic()), claimReading(current), claimUnavailable);
    }

    private static ClusterRecoveryItemView.Profile profile(ClusterExecutionProfile profile) {
        return profile == null ? null : new ClusterRecoveryItemView.Profile(profile.generation(), profile.profile().formatVersion(),
                profile.profile().hash(), profile.profile().attributes());
    }

    private static ClusterResourceCounts counts(ClusterCapacityDemand demand) {
        return new ClusterResourceCounts(demand.processors(), demand.blockingProcessors(), demand.writers(),
                demand.connectorInstances(), demand.bufferedRecords(), demand.edgeQueueRecords());
    }

    private static ClusterRecoveryItemView.Claim claim(WorkloadClaimFence claim) {
        return new ClusterRecoveryItemView.Claim(claim.key().clusterId(), claim.key().type().name(), claim.key().resourceId(),
                claim.owner().nodeId(), claim.owner().bootId(), claim.claimGeneration(), claim.executionGeneration(),
                claim.topologyRevision(), claim.profileGeneration());
    }

    private static ClusterRecoveryItemView.ClaimReading claimReading(WorkloadClaimReading reading) {
        return reading == null ? null : new ClusterRecoveryItemView.ClaimReading(claim(WorkloadClaimFence.from(reading.claim())),
                reading.claim().leaseUntil(), reading.leaseRemaining().toMillis(), reading.leased());
    }

    private static ClusterRecoveryItemView.Diagnostic diagnostic(ClusterRecoveryDiagnostic diagnostic) {
        return diagnostic == null ? null : new ClusterRecoveryItemView.Diagnostic(diagnostic.reason().name(), diagnostic.code(),
                diagnostic.params(), positions(diagnostic.positions()), diagnostic.disposition());
    }

    private static Map<String, ClusterRecoveryItemView.Position> positions(Map<String, ClusterRecoveryPosition> positions) {
        Map<String, ClusterRecoveryItemView.Position> projected = new TreeMap<>();
        positions.forEach((source, position) -> {
            var point = position.position();
            var order = point == null ? null : point.order();
            projected.put(source, new ClusterRecoveryItemView.Position(position.connectorId(), position.captureId(), position.kind().name(),
                    order == null ? null : order.epoch(), order == null ? null : order.seq(), point == null ? null : point.token(),
                    position.provenance(), position.durableStateReference()));
        });
        return projected;
    }

    private static String state(List<ClusterRecoveryItemView> items, ClusterRecoveryReadFailure unavailable) {
        if (unavailable != null) { return null; }
        return items.stream().anyMatch(item -> item.queuePosition() != null) ? "RECOVERING" : "IDLE";
    }

    private static List<String> causes(List<ClusterRecoveryItemView> items) {
        return items.stream().filter(item -> item.queuePosition() != null).map(ClusterRecoveryItemView::cause).distinct().sorted().toList();
    }

    private static <T> Reading<T> read(Supplier<T> reader) {
        try { return new Reading<>(reader.get(), null); }
        catch (TapstateException failure) { return new Reading<>(null, ClusterRecoveryReadFailure.from(failure)); }
    }

    private record Reading<T>(T value, ClusterRecoveryReadFailure unavailable) {}
    private record Row(ClusterRecoveryItem item, ArtifactIdentity artifact, Integer position) {}
    private record QueueRows(List<Row> rows) {}
}
