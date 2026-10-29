package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.TableSnapshot;
import java.time.Instant;
import java.util.Map;

/** The existing snapshot read plus optional current-run display facts. */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineSnapshotResponse(String pipelineId, Map<String, TableSnapshot> snapshot, Load initialLoad) {
    record Load(String runKey, Instant observedAt, String readMode, InitialLoad.Phase phase,
            long landedTables, int totalTables, Map<String, InitialLoad.Table> tables) { }

    static PipelineSnapshotResponse of(PipelineSnapshot snapshot) {
        InitialLoad load = snapshot.initialLoad();
        return new PipelineSnapshotResponse(snapshot.pipelineId(), snapshot.snapshot(), load == null ? null
                : new Load(load.runKey(), load.observedAt(), load.readMode(), snapshot.phase(),
                        load.landedTables(), load.tables().size(), load.tables()));
    }
}
