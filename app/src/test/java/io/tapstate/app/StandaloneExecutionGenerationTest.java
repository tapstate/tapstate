package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StandaloneExecutionGenerationTest {

    private static final String CLUSTER = "cluster-a";
    private static final String PIPELINE = "orders";
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final WorkloadClaimKey KEY =
            new WorkloadClaimKey(CLUSTER, WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);

    @Test
    void rebuildingTheOwnerAndChangingModesNeverReuseAnExecutionGeneration() {
        InMemoryWorkloadClaimStore store = new InMemoryWorkloadClaimStore();
        ExecutionFence first = PipelineActuationOwnership.single(CLUSTER, store).beginExecution(PIPELINE).fence();
        ExecutionFence rebuilt = PipelineActuationOwnership.single(CLUSTER, store).beginExecution(PIPELINE).fence();
        assertThat(first.claimGeneration()).isZero();
        assertThat(first.executionGeneration()).isEqualTo(1);
        assertThat(rebuilt.executionGeneration()).isEqualTo(2);
        assertThat(store.read(KEY)).as("standalone created no claim or lease").isEmpty();

        ClusterMembershipGate gate = eligibleGate();
        PipelineActuationOwnership clusterOwner = owner(store, gate, "node-a", "boot-1");
        assertThat(clusterOwner.permit(PIPELINE).granted()).isTrue();
        WorkloadClaim oldClaim = store.read(KEY).orElseThrow().claim();
        assertThat(oldClaim.executionGeneration()).isEqualTo(2);
        ExecutionFence clusterRun = clusterOwner.beginExecution(PIPELINE).fence();
        assertThat(clusterRun.claimGeneration()).isEqualTo(1);
        assertThat(clusterRun.executionGeneration()).isEqualTo(3);
        assertThatThrownBy(() -> PipelineActuationOwnership.single(CLUSTER, store).beginExecution(PIPELINE))
                .as("a live cluster owner must exclude standalone submission")
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(ActuationError.EXECUTION_GENERATION_UNAVAILABLE));
        assertThat(store.executionGeneration(KEY)).isEqualTo(3);

        store.elapse(TTL.plusSeconds(1));
        PipelineActuationOwnership successor = owner(store, gate, "node-b", "boot-2");
        assertThat(successor.permit(PIPELINE).granted()).isTrue();
        ExecutionFence takeover = successor.beginExecution(PIPELINE).fence();
        assertThat(takeover.claimGeneration()).isEqualTo(2);
        assertThat(takeover.executionGeneration()).isEqualTo(4);
        assertThat(store.advanceUnderClaim(oldClaim, 7)).isEmpty();

        assertThat(store.release(store.read(KEY).orElseThrow().claim())).isTrue();
        ExecutionFence backToStandalone = PipelineActuationOwnership.single(CLUSTER, store)
                .beginExecution(PIPELINE).fence();
        assertThat(backToStandalone.claimGeneration()).isZero();
        assertThat(backToStandalone.executionGeneration()).isEqualTo(5);
    }

    @Test
    void storeFailureIsCodedButAnInvalidStoreResultCrashesAsAnInvariant() {
        ExecutionGenerationStore unavailable = generations(() -> {
            throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "unreachable"), null);
        });
        assertThatThrownBy(() -> PipelineActuationOwnership.single(CLUSTER, unavailable)
                .beginExecution(PIPELINE))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ActuationError.EXECUTION_GENERATION_UNAVAILABLE);
                    assertThat(failure.args()).containsEntry("pipeline", PIPELINE);
                });

        ExecutionGenerationStore invalid = generations(() -> OptionalLong.of(0));
        assertThatThrownBy(() -> PipelineActuationOwnership.single(CLUSTER, invalid).beginExecution(PIPELINE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nonpositive generation");
    }

    @Test
    void standaloneReusesThePersistentClusterIdentityAndRefusesAConflictingConfiguredId() {
        MemoryIdentityStore identities = new MemoryIdentityStore();
        ClusterProperties properties = new ClusterProperties();
        String first = DataPlaneActuationConfiguration.standaloneClusterId(properties, identities);
        assertThat(first).isNotBlank();
        assertThat(DataPlaneActuationConfiguration.standaloneClusterId(properties, identities)).isEqualTo(first);

        properties.setId("another-cluster");
        assertThatThrownBy(() -> DataPlaneActuationConfiguration.standaloneClusterId(properties, identities))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(BootError.CLUSTER_ID_MISMATCH));
        properties.setId(first);
        assertThat(DataPlaneActuationConfiguration.standaloneClusterId(properties, identities)).isEqualTo(first);
    }

    private static PipelineActuationOwnership owner(
            InMemoryWorkloadClaimStore store, ClusterMembershipGate gate, String nodeId, String bootId) {
        return new PipelineActuationOwnership(
                CLUSTER, new WorkloadOwner(nodeId, bootId), gate, new ClusterWorkloadClaims(store, gate),
                TTL, Duration.ofSeconds(10), () -> 0L);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership(CLUSTER, 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static ExecutionGenerationStore generations(java.util.function.Supplier<OptionalLong> standalone) {
        return new ExecutionGenerationStore() {
            @Override
            public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long revision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OptionalLong advanceStandalone(String clusterId, String pipelineId) {
                return standalone.get();
            }
        };
    }

    private static final class MemoryIdentityStore implements ClusterIdentityStore {
        private ClusterIdentity identity;

        @Override
        public Optional<ClusterIdentity> find() {
            return Optional.ofNullable(identity);
        }

        @Override
        public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
            if (identity == null) {
                identity = proposed;
            }
            return identity;
        }
    }
}
