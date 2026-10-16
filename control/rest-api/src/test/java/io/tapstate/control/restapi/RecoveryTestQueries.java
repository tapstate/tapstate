package io.tapstate.control.restapi;

import io.tapstate.control.core.ClusterPipelineRecoveryView;
import io.tapstate.control.core.ClusterRecoveryQueries;
import io.tapstate.control.core.ClusterRecoveryView;
import io.tapstate.core.common.JsonReader;
import java.nio.charset.StandardCharsets;
import tools.jackson.databind.ObjectMapper;

/** One shared typed wire fixture behind the real existing HTTP query services. */
final class RecoveryTestQueries implements ClusterRecoveryQueries {
    private ClusterRecoveryView reading;

    void clear() { reading = null; }

    void enable(String clusterId, String pipelineId) throws Exception {
        try (var input = getClass().getResourceAsStream("/golden/cluster/recovery.golden.json")) {
            if (input == null) { throw new AssertionError("missing shared recovery fixture"); }
            String json = new String(input.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("\"cluster-a\"", "\"" + clusterId + "\"")
                    .replace("\"orders\"", "\"" + pipelineId + "\"");
            reading = new ObjectMapper().readValue(json, ClusterRecoveryView.class);
        }
    }

    @Override public ClusterRecoveryView cluster() { return reading; }

    @Override public ClusterPipelineRecoveryView pipeline(String pipelineId) {
        if (reading == null || !reading.items().getFirst().pipelineId().equals(pipelineId)) { return null; }
        return new ClusterPipelineRecoveryView(pipelineId, reading.items().getFirst().incarnation(),
                reading.recoveryState(), reading.causes(), reading.items(), reading.queueUnavailable(), reading.capacity());
    }

    Object pipelineJson(String pipelineId) {
        return JsonReader.parse(new ObjectMapper().writeValueAsString(pipeline(pipelineId)));
    }
}
