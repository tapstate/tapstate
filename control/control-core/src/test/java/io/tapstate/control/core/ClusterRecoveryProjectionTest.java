package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryPermit;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStartupReceipt;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.ClusterRecoverySuccessor;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;

class ClusterRecoveryProjectionTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final ClusterExecutionProfile ORIGINAL = profile(1, ceilings());
    private static final ClusterExecutionProfile CURRENT = profile(2, ceilings());

    @Test
    void pagesCurrentIncarnationsAndRanksOnlyActiveItemsWithTheSamePipelineMapper() {
        Fixture fixture = new Fixture();
        fixture.add(item("p0", "inc0", 3, 1).cancelled(START.plusSeconds(1)));
        for (int index = 0; index < 101; index++) {
            fixture.add(item("p" + index, "inc" + index, 4, index + 2));
        }
        fixture.rows.add(item("p0", "deleted-incarnation", 4, 103));
        fixture.rows.add(item("deleted", "deleted-incarnation", 4, 104));
        ClusterRecoveryView cluster = fixture.projection().cluster("east", false);
        assertThat(fixture.pageRequests).containsExactly(0, 100);
        assertThat(cluster.items()).hasSize(102);
        assertThat(cluster.recoveryState()).isEqualTo("RECOVERING");
        assertThat(cluster.causes()).containsExactly("FULL_CLUSTER_RESTART");
        assertThat(cluster.items().getFirst().queuePosition()).isNull();
        assertThat(cluster.items().getFirst().status()).isEqualTo("CANCELLED");
        assertThat(cluster.items().get(1).queuePosition()).isEqualTo(1);
        assertThat(cluster.items().getLast().queuePosition()).isEqualTo(101);
        assertThat(cluster.items().get(1).persistedStatus()).isEqualTo("WAITING_PERMIT");
        assertThat(cluster.items().get(1).status()).isEqualTo("WAITING_QUORUM");
        var pipeline = fixture.projection().pipeline("east", "p0", false);
        assertThat(pipeline.currentIncarnation()).isEqualTo("inc0");
        assertThat(pipeline.items()).containsExactlyElementsOf(cluster.items().subList(0, 2));
        assertThat(pipeline.capacity()).isEqualTo(cluster.capacity());
    }

    @Test
    void keepsTheArchivedPointDistinctFromTheSuccessorsActualPreparedAndAcceptedPoint() {
        Fixture fixture = new Fixture();
        WorkloadClaimFence recovery = fence(WorkloadClaimType.CLUSTER_RECOVERY, "east", 0);
        WorkloadClaimFence pipeline = fence(WorkloadClaimType.PIPELINE_ACTUATION, "pipeline", 5);
        var permit = new ClusterRecoveryPermit("reservation", recovery, START, START.plusSeconds(60),
                Map.of("node", new ClusterCapacityDemand(3, 2, 1, 2, 90, 30)), 0);
        var successor = new ClusterRecoverySuccessor(pipeline, CURRENT, Set.of("node"), Set.of("source"),
                START.plusSeconds(1), null, null, Map.of(), null);
        ClusterRecoveryItem rebuilding = item("pipeline", "inc", 4, 1).permitted(permit, START)
                .advanced(successor, START.plusSeconds(1)).submitted("native-actual", START.plusSeconds(2));
        ChainPosition point = new ChainPosition(new SourceOrder(2, 8), "confirmed-prepared");
        CaptureResumeWitness witness = new CaptureResumeWitness("source", "mongo", "chain", "consumer",
                ReadMode.CDC_ONLY, false, List.of("orders"), true, 2, point, false, true, List.of(), null, 0,
                ConsumerProgressKind.DIRECT_SOURCE, point, Map.of());
        var actual = position(point);
        var receipt = new ClusterRecoveryStartupReceipt(pipeline, "native-actual", START.plusSeconds(3),
                Map.of("source", witness), Map.of("source", actual), Map.of("source", actual), START.plusSeconds(4), false);
        fixture.add(rebuilding.initialized(receipt, START.plusSeconds(4)));
        var view = fixture.projection().cluster("east", true).items().getFirst();
        assertThat(view.originalPositions().get("source").token()).isEqualTo("original");
        assertThat(view.originalPositions().get("source").epoch()).isEqualTo(1);
        assertThat(view.successor().requestedPositions().get("source").token()).isEqualTo("confirmed-prepared");
        assertThat(view.successor().acceptedPositions().get("source").sequence()).isEqualTo(8);
        assertThat(view.originalProfile().generation()).isEqualTo(1);
        assertThat(view.targetProfile().generation()).isEqualTo(2);
        assertThat(view.successor().pipelineClaim().executionGeneration()).isEqualTo(5);
        assertThat(view.successor().nativeJobId()).isEqualTo("native-actual");
        assertThat(view.successor().sourceRequirementsRecorded()).isTrue();
        assertThat(view.successor().sourcesAcceptedAt()).isEqualTo(START.plusSeconds(4));
        assertThat(view.currentPipelineClaim().leased()).isFalse();
        assertThat(view.currentPipelineClaim().leaseRemainingMillis()).isEqualTo(-3000);
    }

    @Test
    void anUnreadableQueueHasNoInventedIdleStateAndDoesNotHideAvailableCapacity() {
        Fixture fixture = new Fixture();
        fixture.queueFailure = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "queue unreachable"), null);
        ClusterRecoveryView view = fixture.projection().cluster("east", null);
        assertThat(view.recoveryState()).isNull();
        assertThat(view.quorumReady()).isNull();
        assertThat(view.items()).isEmpty();
        assertThat(view.queueUnavailable().code()).isEqualTo(IoError.STORE_UNAVAILABLE.code());
        assertThat(view.queueUnavailable().params()).containsEntry("detail", "queue unreachable");
        assertThat(view.capacity().availability()).isEqualTo("AVAILABLE");
        assertThat(view.capacity().configuredLimits().processors()).isEqualTo(11);
    }

    @Test
    void unknownDemandRetainsTheOriginalRefusalAndNeverBecomesZeroOccupancy() {
        Fixture fixture = new Fixture();
        fixture.add(item("pipeline", "inc", 4, 1));
        fixture.capacityFailure = new TapstateException(LifecycleError.CLUSTER_CAPACITY_UNPROVEN,
                Map.of("pipeline", "pipeline", "reason", "unrecorded execution demand"), null);
        ClusterRecoveryView view = fixture.projection().cluster("east", true);
        assertThat(view.recoveryState()).isEqualTo("RECOVERING");
        assertThat(view.capacity().availability()).isEqualTo("UNAVAILABLE");
        assertThat(view.capacity().occupiedByNode()).isNull();
        assertThat(view.capacity().unavailable().code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_UNPROVEN.code());
        assertThat(view.capacity().unavailable().params()).containsEntry("reason", "unrecorded execution demand");
        assertThat(view.capacity().configuredLimits().writers()).isEqualTo(5);
    }

    @Test
    void missingImmutableProfileCeilingsAreCodedUnavailableWithoutAnEndpointDefault() {
        Fixture fixture = new Fixture();
        Map<String, String> inputs = new LinkedHashMap<>(ceilings());
        inputs.remove("capacityWriters");
        fixture.current = profile(3, inputs);
        ClusterCapacityView capacity = fixture.projection().cluster("east", true).capacity();
        assertThat(capacity.configuredLimits()).isNull();
        assertThat(capacity.occupiedByNode()).isNull();
        assertThat(capacity.profile().generation()).isEqualTo(3);
        assertThat(capacity.unavailable().code()).isEqualTo(IoError.DOCUMENT_UNREADABLE.code());
        assertThat(capacity.unavailable().params()).containsEntry("field", "capacityWriters");
    }

    @Test
    void aKnownAbsentCapacityProfileDoesNotInventAZeroDemandSnapshot() {
        Fixture fixture = new Fixture();
        fixture.capacityAbsent = true;
        ClusterCapacityView capacity = fixture.projection().cluster("east", true).capacity();
        assertThat(capacity.availability()).isEqualTo("PROFILE_ABSENT");
        assertThat(capacity.profile()).isNull();
        assertThat(capacity.configuredLimits()).isNull();
        assertThat(capacity.occupiedByNode()).isNull();
    }

    @Test
    void anInvalidNewerSnapshotCannotSubstituteTheEarlierProfilesCeilings() {
        Fixture fixture = new Fixture();
        Map<String, String> inputs = new LinkedHashMap<>(ceilings());
        inputs.remove("capacityWriters");
        fixture.snapshotProfile = profile(3, inputs);
        ClusterRecoveryView view = fixture.projection().cluster("east", true);
        assertThat(view.currentProfile().generation()).isEqualTo(2);
        assertThat(view.capacity().profile().generation()).isEqualTo(3);
        assertThat(view.capacity().configuredLimits()).isNull();
        assertThat(view.capacity().occupiedByNode()).isNull();
        assertThat(view.capacity().unavailable().params()).containsEntry("field", "capacityWriters");
    }

    @Test
    void aCurrentFailureNoteIdentifiesItsSuccessorWhileTheReservationIsRetiring() {
        Fixture fixture = new Fixture();
        var old = io.tapstate.spi.store.ClusterRecoveryDiagnostic.from(
                io.tapstate.spi.store.ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED,
                new TapstateException(LifecycleError.CLUSTER_CAPACITY_UNPROVEN,
                        Map.of("pipeline", "pipeline", "reason", "old-demand-unproven"), null),
                Map.of("source", position(new ChainPosition(new SourceOrder(1, 5), "original"))), "retry-after-capacity");
        var pipeline = fence(WorkloadClaimType.PIPELINE_ACTUATION, "pipeline", 5);
        var permit = new ClusterRecoveryPermit("reservation", fence(WorkloadClaimType.CLUSTER_RECOVERY, "east", 0),
                START.plusSeconds(2), START.plusSeconds(60), Map.of("node", new ClusterCapacityDemand(3, 2, 1, 2, 90, 30)), 0);
        var successor = new ClusterRecoverySuccessor(pipeline, CURRENT, Set.of("node"), Set.of("source"),
                START.plusSeconds(2), null, null, Map.of(), null);
        var item = item("pipeline", "inc", 4, 1).refused(old, Duration.ofSeconds(1), START)
                .permitted(permit, START.plusSeconds(2)).advanced(successor, START.plusSeconds(2))
                .submitted("native-actual", START.plusSeconds(3));
        var current = new io.tapstate.spi.store.ClusterRecoveryDiagnostic(
                io.tapstate.spi.store.ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                "capture.start-from-outside-window", Map.of("requested", "attempt-point", "earliest", "head", "retention", "2h"),
                Map.of("source", position(new ChainPosition(new SourceOrder(2, 9), "attempt-point"))), "stop-and-clear-source-state");
        var note = new io.tapstate.spi.store.ClusterRecoveryFailureNote(pipeline,
                ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, current, START.plusSeconds(4));
        fixture.add(item);
        var previous = fixture.projection().cluster("east", false).items().getFirst();
        assertThat(previous.diagnostic().code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_UNPROVEN.code());
        assertThat(previous.successor().failureNote()).isNull();
        fixture.rows.clear();
        fixture.add(item.failureNoted(note, START.plusSeconds(4)));
        var projected = fixture.projection().cluster("east", false).items().getFirst();
        assertThat(projected.diagnostic().code()).isEqualTo(current.code());
        assertThat(projected.successor().failureNote().diagnostic().code()).isEqualTo(current.code());
        assertThat(projected.successor().failureNote().diagnostic().positions().get("source").token()).isEqualTo("attempt-point");
        assertThat(projected.successor().failureNote().pipelineClaim().executionGeneration()).isEqualTo(5);
        assertThat(projected.successor().failureNote().recordedAt()).isEqualTo(START.plusSeconds(4));
        assertThat(projected.status()).isEqualTo("WAITING_QUORUM");
        assertThat(projected.permit()).isNotNull();
        assertThat(projected.attempt()).isEqualTo(2);
    }

    private static ClusterRecoveryItem item(String pipeline, String incarnation, long original, long sequence) {
        var key = new ClusterRecoveryKey("east", pipeline, incarnation);
        var event = new ClusterRecoveryEvent(key, ClusterRecoveryCause.FULL_CLUSTER_RESTART, original, "original-hash", 1L,
                ORIGINAL, CURRENT, 2, "intent", Map.of("source", position(new ChainPosition(new SourceOrder(1, 5), "original"))));
        return ClusterRecoveryItem.enqueued(event, sequence, START, 3);
    }

    private static ClusterRecoveryPosition position(ChainPosition point) {
        return new ClusterRecoveryPosition("source", "mongo", "capture", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                point, "mongo-confirmed-consumer", "consumer");
    }

    private static ClusterExecutionProfile profile(long generation, Map<String, String> inputs) {
        return new ClusterExecutionProfile("east", generation, new ExecutionProfile(1, inputs));
    }

    private static Map<String, String> ceilings() {
        return Map.of("capacityProcessors", "11", "capacityBlockingProcessors", "7", "capacityWriters", "5",
                "capacityConnectorInstances", "3", "capacityBufferedRecords", "200", "capacityEdgeQueueRecords", "40");
    }

    private static WorkloadClaimFence fence(WorkloadClaimType type, String resource, long execution) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", type, resource), new WorkloadOwner("node", "boot"), 1, execution, 2, 2);
    }

    private static final class Fixture {
        private final List<ClusterRecoveryItem> rows = new ArrayList<>();
        private final Map<String, ArtifactIdentity> identities = new LinkedHashMap<>();
        private final List<Integer> pageRequests = new ArrayList<>();
        private ClusterExecutionProfile current = CURRENT;
        private ClusterExecutionProfile snapshotProfile;
        private TapstateException queueFailure;
        private TapstateException capacityFailure;
        private boolean capacityAbsent;

        void add(ClusterRecoveryItem item) {
            rows.add(item);
            identities.put(item.event().key().pipelineId(), new ArtifactIdentity(item.event().key().pipelineId(),
                    item.event().key().incarnation(), "current-hash"));
        }

        ClusterRecoveryProjection projection() {
            ArtifactStore artifacts = port(ArtifactStore.class, (proxy, method, args) -> {
                if (method.getName().equals("identity")) { return Optional.ofNullable(identities.get(args[0])); }
                throw unexpected(method.getName());
            });
            ClusterRecoveryStore queue = port(ClusterRecoveryStore.class, (proxy, method, args) -> {
                if (!method.getName().equals("list")) { throw unexpected(method.getName()); }
                if (queueFailure != null) { throw queueFailure; }
                int offset = (int) args[1];
                int limit = (int) args[2];
                pageRequests.add(offset);
                assertThat(limit).isEqualTo(100);
                return rows.subList(Math.min(offset, rows.size()), Math.min(offset + limit, rows.size()));
            });
            ClusterProfileStore profiles = port(ClusterProfileStore.class, (proxy, method, args) -> {
                if (method.getName().equals("profile")) { return Optional.of(current); }
                throw unexpected(method.getName());
            });
            ClusterCapacityStore capacity = port(ClusterCapacityStore.class, (proxy, method, args) -> {
                if (!method.getName().equals("readOccupied")) { throw unexpected(method.getName()); }
                if (capacityFailure != null) { throw capacityFailure; }
                return capacityAbsent ? Optional.empty() : Optional.of(new ClusterCapacityStore.Snapshot(snapshotProfile == null ? current : snapshotProfile,
                        Map.of("node", new ClusterCapacityDemand(3, 2, 1, 2, 90, 30))));
            });
            WorkloadClaimStore claims = port(WorkloadClaimStore.class, (proxy, method, args) -> {
                if (!method.getName().equals("readAll")) { throw unexpected(method.getName()); }
                Map<WorkloadClaimKey, WorkloadClaimReading> result = new LinkedHashMap<>();
                for (Object key : (Iterable<?>) args[0]) {
                    var claimKey = (WorkloadClaimKey) key;
                    WorkloadClaim claim = new WorkloadClaim(claimKey, new WorkloadOwner("current-owner", "current-boot"),
                            2, 5, 7, START.plus(Duration.ofDays(3650)), 5, 2, Set.of("node"), 0, false, 2);
                    result.put(claimKey, new WorkloadClaimReading(claim, Duration.ofSeconds(-3)));
                }
                return result;
            });
            return new ClusterRecoveryProjection(artifacts, queue, profiles, capacity, claims);
        }

        private static AssertionError unexpected(String method) {
            return new AssertionError("a read projection called a non-read port method: " + method);
        }

        private static <T> T port(Class<T> type, InvocationHandler handler) {
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
        }
    }
}
