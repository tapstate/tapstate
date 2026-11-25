package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.core.lifecycle.InitialLoad;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;

class PipelineSnapshotResponseTest {
    @Test
    void legacyResponseKeepsItsGoldenShapeAndDoesNotInventInitialLoad() throws Exception {
        assertThat(JsonMapper.builder().build().writeValueAsString(
                PipelineSnapshotResponse.of(new PipelineSnapshot("p", Map.of()))))
                .isEqualTo("{\"pipelineId\":\"p\",\"snapshot\":{}}");
    }

    @Test
    void readProjectionReturnsServerComputedPhaseAndTargetConfirmedCounts() {
        var load = new InitialLoad("execution:p:2:at", Instant.EPOCH, "snapshot_and_cdc", Map.of(
                "a", new InitialLoad.Table(InitialLoad.Stage.LANDED, 20L, null),
                "b", new InitialLoad.Table(InitialLoad.Stage.READING, 5L, 100L)));
        var response = PipelineSnapshotResponse.of(new PipelineSnapshot("p", Map.of(), load, InitialLoad.Phase.MIXED));
        assertThat(response.initialLoad().landedTables()).isEqualTo(1);
        assertThat(response.initialLoad().totalTables()).isEqualTo(2);
        assertThat(response.initialLoad().phase()).isEqualTo(InitialLoad.Phase.MIXED);
    }
}
