package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class InitialLoadTest {
    private InitialLoad load(String mode, InitialLoad.Stage... stages) {
        Map<String, InitialLoad.Table> rows = new java.util.HashMap<>();
        for (int i = 0; i < stages.length; i++) rows.put("table" + i, new InitialLoad.Table(stages[i], null, null));
        return new InitialLoad("run", Instant.EPOCH, mode, rows);
    }

    @Test
    void onlyTargetConfirmationAdvancesThePhase() {
        assertThat(load("snapshot_and_cdc", InitialLoad.Stage.READING, InitialLoad.Stage.LANDING)
                .phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.SNAPSHOTTING);
        assertThat(load("snapshot_and_cdc", InitialLoad.Stage.LANDED, InitialLoad.Stage.READING)
                .phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.MIXED);
        assertThat(load("snapshot_and_cdc", InitialLoad.Stage.LANDED, InitialLoad.Stage.LANDED)
                .phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.STREAMING);
    }

    @Test
    void boundedCompletionIsNotStreamingAndAnEmptyInventoryIsNotCompletion() {
        assertThat(load("snapshot_only", InitialLoad.Stage.LANDED).phase(PipelineState.COMPLETED))
                .isEqualTo(InitialLoad.Phase.COMPLETED);
        assertThat(load("snapshot_only", InitialLoad.Stage.LANDED).phase(PipelineState.RUNNING))
                .isEqualTo(InitialLoad.Phase.SNAPSHOTTING);
        assertThat(load("snapshot_and_cdc").phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.UNKNOWN);
        assertThat(load("cdc_only").phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.STREAMING);
        assertThat(load("cdc_only").phase(PipelineState.FAILED)).isEqualTo(InitialLoad.Phase.UNKNOWN);
    }
}
