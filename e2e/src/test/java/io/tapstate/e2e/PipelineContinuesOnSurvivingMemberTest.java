package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PipelineContinuesOnSurvivingMemberTest {

    @Test
    void waitsForRunningWhenThePostKillRowPrecedesTheRecoveredObservation() {
        AtomicInteger reads = new AtomicInteger();

        // The post-kill row has satisfied the witness's data wait. Its next status read can still
        // describe the failed run; the rebuilt run's RUNNING observation arrives on the next read.
        // Exercise the witness's own assertion, so copying a corrected assertion here cannot pass.
        PipelineContinuesOnSurvivingMemberIT.assertRunningAgain(() ->
                ControlPlane.interpretState(200, reads.getAndIncrement() == 0
                        ? "{\"state\":\"FAILED\"}"
                        : "{\"state\":\"RUNNING\"}", "failover_pipe"));

        assertThat(reads.get())
                .describedAs("the recovered observation was read after the stale FAILED observation")
                .isGreaterThanOrEqualTo(2);
    }
}
