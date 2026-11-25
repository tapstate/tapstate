package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SharedSrsRecoverySnapshotTest {
    private static final String HASH = "a".repeat(64);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster", 1, "cache");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final CapturePreparation PREVIOUS = new CapturePreparation(BUSINESS,
            CapturePreparation.Mode.SHARED_SRS, "chain", "capture", "mc-" + HASH, UUID.randomUUID().toString());
    private static final WorkloadClaimFence CAPTURE = claim("cluster", WorkloadClaimType.CAPTURE, "capture", 3);

    @Test void ordinaryConstructorsKeepTheirOriginalSelectionAndManifestContract() {
        var selection = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.PIPELINE, Set.of("a"));
        var manifest = new StorageRecoveryCommands.Manifest(selection, Set.of("a"), Map.of("a", HASH), HASH, 0);
        assertThat(selection.canonicalChain()).isNull();
        assertThat(selection.physicalChain()).isNull();
        assertThat(manifest.sharedSrs()).isNull();
        assertThatThrownBy(() -> new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER,
                Set.of(), "chain", "chain")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS, Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void theObservedClosureIsSortedAndImmutableWithoutRequiringTheOldCommandOwnerToBeCurrent() {
        var consumers = new ArrayList<>(List.of(consumer("b", "source", 1, false), consumer("a", "second", 1, false),
                consumer("a", "first", 1, false)));
        var pipelines = new LinkedHashMap<String, SharedSrsRecoverySnapshot.Pipeline>();
        pipelines.put("b", pipeline("b", 1, BUSINESS));
        pipelines.put("a", pipeline("a", 1, BUSINESS));
        var snapshot = snapshot(consumers, pipelines);
        consumers.clear(); pipelines.clear();
        assertThat(snapshot.pipelines().keySet()).containsExactly("a", "b");
        assertThat(snapshot.consumers()).extracting(value -> value.context().key().consumerId()).isSorted();
        assertThat(snapshot.consumers()).hasSize(3);
        assertThat(snapshot.pipelines().get("a").claim().executionGeneration()).isZero();
        assertThatThrownBy(() -> snapshot.consumers().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> snapshot.pipelines().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void sharedManifestRequiresTheExactObservedPhysicalSelectionAndPipelineClosure() {
        var snapshot = snapshot(List.of(consumer("a", "source", 1, false)), Map.of("a", pipeline("a", 1, BUSINESS)));
        var selection = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,
                Set.of(), "chain", "chain");
        assertThat(new StorageRecoveryCommands.Manifest(selection, Set.of("a"), Map.of("a", HASH), HASH, 0, snapshot).sharedSrs())
                .isSameAs(snapshot);
        assertThatThrownBy(() -> new StorageRecoveryCommands.Manifest(selection, Set.of("a", "b"),
                Map.of("a", HASH, "b", HASH), HASH, 0, snapshot)).isInstanceOf(IllegalArgumentException.class);
        var wrongChain = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,
                Set.of(), "chain", "other");
        assertThatThrownBy(() -> new StorageRecoveryCommands.Manifest(wrongChain, Set.of("a"), Map.of("a", HASH), HASH, 0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        var ordinary = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of());
        assertThatThrownBy(() -> new StorageRecoveryCommands.Manifest(ordinary, Set.of("a"), Map.of("a", HASH), HASH, 0, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void detachedDuplicateOrWrongGenerationConsumersCannotBecomeAnActualClosure() {
        var pipelines = Map.of("a", pipeline("a", 1, BUSINESS));
        var consumer = consumer("a", "source", 1, false);
        assertInvalid(List.of(consumer, consumer), pipelines);
        assertInvalid(List.of(consumer("a", "source", 1, true)), pipelines);
        assertInvalid(List.of(consumer("a", "source", 2, false)), pipelines);
        assertInvalid(List.of(), pipelines);
        assertInvalid(List.of(consumer), Map.of("a", pipeline("a", 1, BUSINESS), "b", pipeline("b", 1, BUSINESS)));
    }

    @Test void anEmptyObservedClosureCarriesNoInventedPipelineOrConsumer() {
        var observed=snapshot(List.of(),Map.of());
        assertThat(observed.consumers()).isEmpty();assertThat(observed.pipelines()).isEmpty();
    }

    @Test void oldPhysicalCoordinatesAndBothClaimClustersMustMatch() {
        var wrongChain = new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("other", "a", "source"), 1),
                1, false, Set.of("table"), claim("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "a", 1));
        var wrongCluster = new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("chain", "a", "source"), 1),
                1, false, Set.of("table"), claim("other", WorkloadClaimType.PIPELINE_ACTUATION, "a", 1));
        var pipelines = Map.of("a", pipeline("a", 1, BUSINESS));
        assertInvalid(List.of(wrongChain), pipelines);
        assertInvalid(List.of(wrongCluster), pipelines);
        assertInvalid(List.of(consumer("a", "source", 1, false)),
                Map.of("a", pipeline("a", 1, new CacheBusinessScope("cluster", 1, "other"))));
        assertThatThrownBy(() -> new SharedSrsRecoverySnapshot(PREVIOUS,
                claim("cluster", WorkloadClaimType.CAPTURE, "other", 1), HASH,
                List.of(consumer("a", "source", 1, false)), pipelines)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aSharedPlanCannotUseAnAnnexFromAnotherBusinessScope() {
        var snapshot = snapshot(List.of(consumer("a", "source", 1, false)), Map.of("a", pipeline("a", 1, BUSINESS)));
        var selection = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,
                Set.of(), "chain", "chain");
        var manifest = new StorageRecoveryCommands.Manifest(selection, Set.of("a"), Map.of("a", HASH), HASH, 0, snapshot);
        var current = new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1);
        assertThat(new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(), null, current, manifest,
                HASH, 1, StorageRecoveryCommands.Phase.PLANNED).originalScope()).isEqualTo(current);
        var other = new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "other", 1);
        assertThatThrownBy(() -> new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(), null, other, manifest,
                HASH, 1, StorageRecoveryCommands.Phase.PLANNED)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void exhaustedCurrentClaimCountersCannotDescribeAReplacement() {
        var exhausted = new WorkloadClaimFence(CAPTURE.key(), OWNER, Long.MAX_VALUE - 1, 1, 0);
        assertThatThrownBy(() -> new SharedSrsRecoverySnapshot(PREVIOUS, exhausted, HASH,
                List.of(consumer("a", "source", 1, false)), Map.of("a", pipeline("a", 1, BUSINESS))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static SharedSrsRecoverySnapshot snapshot(List<SrsConsumerMembership> consumers,
            Map<String, SharedSrsRecoverySnapshot.Pipeline> pipelines) {
        return new SharedSrsRecoverySnapshot(PREVIOUS, CAPTURE, HASH, consumers, pipelines);
    }
    private static void assertInvalid(List<SrsConsumerMembership> consumers,
            Map<String, SharedSrsRecoverySnapshot.Pipeline> pipelines) {
        assertThatThrownBy(() -> snapshot(consumers, pipelines)).isInstanceOf(IllegalArgumentException.class);
    }
    private static SharedSrsRecoverySnapshot.Pipeline pipeline(String id, long generation, CacheBusinessScope business) {
        return new SharedSrsRecoverySnapshot.Pipeline(new PipelineIntentScope(business.clusterId(), business.bindingVersion(),
                business.cacheIncarnation(), id, generation), claim(business.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, id, 0));
    }
    private static SrsConsumerMembership consumer(String id, String source, long generation, boolean detached) {
        return new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("chain", id, source), generation), 1,
                detached, detached ? Set.of() : Set.of("table"), claim("cluster", WorkloadClaimType.PIPELINE_ACTUATION, id, 1));
    }
    private static WorkloadClaimFence claim(String cluster, WorkloadClaimType type, String id, long execution) {
        return new WorkloadClaimFence(new WorkloadClaimKey(cluster, type, id), OWNER, 3, execution, 1);
    }
}
