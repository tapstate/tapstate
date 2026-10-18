package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreparationTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache-a", "flow", 1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadClaimFence NODE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node-a"), OWNER, 1, 0, 0);

    @Test void anUnassignedStatusCarriesNoReconstructibleCreationAuthority() {
        var unassigned = new PipelinePreparation("flow", 1, PipelinePreparation.Phase.UNASSIGNED, null, null, null);
        assertThat(unassigned.scope()).isNull();
        assertThatThrownBy(() -> new PipelinePreparation("flow", 1, PipelinePreparation.Phase.UNASSIGNED,
                SCOPE, UUID.randomUUID().toString(), NODE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FirstPipelineCreationResult.AlreadyIssued(unassigned)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void anIssuedStatusCannotChangeItsPipelineGenerationOrOriginalNodeRole() {
        String id = UUID.randomUUID().toString();
        assertThat(new PipelinePreparation("flow", 1, PipelinePreparation.Phase.ISSUED, SCOPE, id, NODE).issuerNode()).isEqualTo(NODE);
        assertThatThrownBy(() -> new PipelinePreparation("other", 1, PipelinePreparation.Phase.ISSUED, SCOPE, id, NODE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PipelinePreparation("flow", 2, PipelinePreparation.Phase.ISSUED, SCOPE, id, NODE))
                .isInstanceOf(IllegalArgumentException.class);
        var capture = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "source"), OWNER, 1, 0, 0);
        assertThatThrownBy(() -> new PipelinePreparation("flow", 1, PipelinePreparation.Phase.ISSUED, SCOPE, id, capture))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PipelinePreparation("flow", 1, PipelinePreparation.Phase.ISSUED, SCOPE, "not-a-uuid", NODE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void unsupportedActuationReadsDoNotFallBackToAuthoringLoads() {
        AtomicInteger authoringLoads = new AtomicInteger();
        PipelineIntentStore authoringOnly = new PipelineIntentStore() {
            @Override public PipelineIntentStore guardedBy(Runnable active) { return this; }
            @Override public Optional<PipelineIntentSnapshot> load(String id) { authoringLoads.incrementAndGet(); return Optional.empty(); }
            @Override public boolean compareAndSet(PipelineIntentScope scope, long revision, DesiredState desired) { throw new AssertionError("not used"); }
            @Override public boolean remove(PipelineIntentScope scope, long revision) { throw new AssertionError("not used"); }
            @Override public List<String> pipelineIds() { return List.of("flow"); }
        };
        assertThatThrownBy(() -> authoringOnly.loadForActuation("flow")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        assertThat(authoringLoads).hasValue(0);
    }
}
