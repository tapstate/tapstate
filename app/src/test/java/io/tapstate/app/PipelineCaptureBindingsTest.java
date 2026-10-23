package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PipelineCaptureBindingsTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final WorkloadClaimFence CLAIM=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),3,8,2);
    private static final ExecutionFence ORIGINAL=new ExecutionFence(SCOPE,CLAIM);
    private static PipelineCaptureBindings.SnapshotOnlySource bounded(long epoch) { return new PipelineCaptureBindings.SnapshotOnlySource("same-chain",Set.of("orders"),epoch); }
    @Test void distinctSourceNodesOnTheSamePhysicalChainHaveSeparateOriginalHandoffs() {
        var bindings=new PipelineCaptureBindings(ORIGINAL,Map.of(),Map.of(),Map.of("left",bounded(11),"right",bounded(11)),true);
        assertThat(bindings.bufferKey("left")).isNotEqualTo(bindings.bufferKey("right"));
        assertThat(bindings.requireSnapshot("left","same-chain","orders").epoch()).isEqualTo(11);
        assertThatThrownBy(() -> bindings.bufferKey("foreign")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> bindings.requireSnapshot("left","other-chain","orders")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> bindings.requireSnapshot("left","same-chain","other-table")).isInstanceOf(TapstateException.class);
    }
    @Test void oneBoundedPipelineCannotMixOrdersOrBorrowSharedConsumerAuthority() {
        assertThatThrownBy(() -> new PipelineCaptureBindings(ORIGINAL,Map.of(),Map.of(),Map.of("left",bounded(11),"right",bounded(12)),true)).isInstanceOf(TapstateException.class);
        var membership=new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("same-chain","flow","left"),7),1,false,Set.of("orders"),CLAIM);
        var source=new PipelineCaptureBindings.LoggedSource(SrsConsumerRun.of(SCOPE,membership),11,Map.of("orders",-1L));
        assertThatThrownBy(() -> new PipelineCaptureBindings(ORIGINAL,Map.of("left",source),Map.of(),Map.of("right",bounded(11)),true)).isInstanceOf(TapstateException.class);
    }
}
