package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Production factories must honor an admitted single node's original claim instead of bypassing it. */
class SingleNodeOwnershipWiringTest {
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final DataPlaneActuationConfiguration CONFIGURATION = new DataPlaneActuationConfiguration();

    @Test void anAdmittedSingleNodeAcquiresAnActualCaptureClaim() {
        var f = fixture();
        var ownership = CONFIGURATION.captureOwnership(f.member(), f.properties(), f.gate(), new ClusterWorkloadClaims(f.claims(), f.gate()));
        var permit = ownership.acquire(new CaptureId("capture-a"));
        assertThat(permit.acquired()).isTrue(); assertThat(permit.fence()).isNotNull();
        assertThat(permit.fence().owner()).isEqualTo(OWNER);
        assertThat(f.claims().read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a"))).isPresent();
    }

    @Test void anAdmittedSingleNodeAllocatesExecutionUnderItsOriginalPipelineClaim() {
        var f = fixture();
        var ownership = CONFIGURATION.pipelineActuationOwnership(f.member(), f.properties(), f.gate(), new ClusterWorkloadClaims(f.claims(), f.gate()));
        var permit = ownership.permit("orders");
        assertThat(permit.granted()).isTrue(); assertThat(permit.claim()).isNotNull();
        var execution = ownership.beginExecution("orders");
        assertThat(execution.allowed()).isTrue(); assertThat(execution.fence()).isNotNull();
        assertThat(execution.fence().executionGeneration()).isPositive();
        assertThat(f.claims().read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"))
                .orElseThrow().claim().owner()).isEqualTo(OWNER);
    }

    @Test void aSingleNodeWithoutAPipelineClaimCannotAuthorizeExternalCalls() {
        var f = fixture();
        var original = (io.tapstate.spi.store.WorkloadClaim) f.member().getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
        try (var lease = new NodeSessionLease(f.claims(), original, System.nanoTime(), f.properties().getNodeSessionTtl(),
                f.properties().getNodeSessionRenewInterval(), () -> {});
             var authorization = CONFIGURATION.executionAuthorization(f.member(), f.properties(), f.claims(), lease, f.gate())) {
            assertThatThrownBy(() -> authorization.require(new ExecutionFence("orders", 1, 1)))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
        }
    }

    @Test void anUnboundNodeSessionCannotAcquireCaptureOrPipelineClaims() {
        var f = fixture(false);
        var claims = new ClusterWorkloadClaims(f.claims(), f.gate());
        assertThat(CONFIGURATION.captureOwnership(f.member(), f.properties(), f.gate(), claims)
                .acquire(new CaptureId("capture-a")).acquired()).isFalse();
        assertThat(CONFIGURATION.pipelineActuationOwnership(f.member(), f.properties(), f.gate(), claims)
                .permit("orders").granted()).isFalse();
        assertThat(f.claims().read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a"))).isEmpty();
        assertThat(f.claims().read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"))).isEmpty();
    }

    @Test void aReleasedOriginalNodeSessionCannotAdmitAnotherCapture() {
        var f = fixture();
        var original = (io.tapstate.spi.store.WorkloadClaim) f.member().getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
        assertThat(f.claims().release(original)).isTrue();
        assertThat(CONFIGURATION.captureOwnership(f.member(), f.properties(), f.gate(), new ClusterWorkloadClaims(f.claims(), f.gate()))
                .acquire(new CaptureId("capture-a")).acquired()).isFalse();
        assertThat(f.claims().read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a"))).isEmpty();
    }

    private static Fixture fixture() { return fixture(true); }

    private static Fixture fixture(boolean bound) {
        var properties = new ClusterProperties(); properties.setId("cluster-a"); properties.setNodeId(OWNER.nodeId());
        var claims = new InMemoryWorkloadClaimStore();
        var node = claims.acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, OWNER.nodeId()),
                OWNER, 0, properties.getNodeSessionTtl()).claim();
        var member = mock(HazelcastInstance.class); var context = new ConcurrentHashMap<String, Object>();
        context.put(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY, node); when(member.getUserContext()).thenReturn(context);
        var lifecycle = mock(com.hazelcast.core.LifecycleService.class); when(lifecycle.isRunning()).thenReturn(true); when(member.getLifecycleService()).thenReturn(lifecycle);
        var gate = new ClusterMembershipGate(properties); gate.install(new ClusterMembership("cluster-a", 1, Set.of(OWNER.nodeId())));
        if (bound) gate.observeNodeSession(() -> claims.read(node.key())
                .filter(reading -> reading.leased() && reading.claim().equals(node)).isPresent());
        gate.canCommit(Set.of(OWNER.nodeId()));
        return new Fixture(properties, claims, member, gate);
    }
    private record Fixture(ClusterProperties properties, InMemoryWorkloadClaimStore claims, HazelcastInstance member, ClusterMembershipGate gate) {}
}
