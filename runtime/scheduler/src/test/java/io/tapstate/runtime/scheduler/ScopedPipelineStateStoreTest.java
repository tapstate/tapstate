package io.tapstate.runtime.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ScopedPipelineStateStoreTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","p",1);
    private static final WorkloadClaimFence ORIGINAL = claim("boot-a",1);
    private static final Instant NOW = Instant.parse("2026-10-23T00:00:00Z");

    @Test void completionUsesItsOriginalClaimEvenIfAnInterveningGuardChangesTheSupplier() {
        var state = new InMemoryStateStore(); state.create("p",StateJson.of(PipelineState.RUNNING),NOW);
        var supplied = new AtomicReference<>(ORIGINAL); var opened = new ArrayList<WorkloadClaimFence>();
        var scoped = new ScopedPipelineStateStore(SCOPE,supplied::get,views(state,opened),
                () -> supplied.set(claim("boot-b",2)));
        assertThat(scoped.compareAndSwap("p",0,StateJson.of(PipelineState.COMPLETED),NOW,ORIGINAL))
                .isInstanceOf(CasOutcome.Applied.class);
        assertThat(opened).containsExactly(ORIGINAL);
        assertThat(state.read("p").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.COMPLETED));
    }

    @Test void aDifferentCompletionClaimIsRefusedBeforeOpeningAnyWriteView() {
        var state = new InMemoryStateStore(); var opened = new ArrayList<WorkloadClaimFence>();
        var scoped = new ScopedPipelineStateStore(SCOPE,() -> ORIGINAL,views(state,opened),() -> {});
        assertThatThrownBy(() -> scoped.compareAndSwap("p",0,StateJson.of(PipelineState.COMPLETED),NOW,claim("boot-b",2)))
                .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(opened).isEmpty();
    }

    private static PipelineStoreViews views(StateStore state,List<WorkloadClaimFence> opened) {
        return new PipelineStoreViews() {
            @Override public StateStore pipelineStates(PipelineIntentScope scope) { throw new AssertionError("unbound read"); }
            @Override public StateStore pipelineStates(PipelineIntentScope scope,WorkloadClaimFence original,Runnable active) {
                assertThat(scope).isEqualTo(SCOPE); opened.add(original); return state;
            }
            @Override public ObservationStore pipelineObservations(PipelineIntentScope scope) { throw new AssertionError("unexpected observation"); }
            @Override public ObservationStore pipelineObservations(PipelineIntentScope scope,WorkloadClaimFence original,Runnable active) {
                throw new AssertionError("unexpected observation");
            }
        };
    }

    private static WorkloadClaimFence claim(String boot,long generation) {
        return new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),
                new WorkloadOwner("node",boot),generation,1,1);
    }
}
