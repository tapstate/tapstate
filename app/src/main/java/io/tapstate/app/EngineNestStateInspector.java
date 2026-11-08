package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.control.core.NestStateInspector;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.nest.NestInspector;
import io.tapstate.runtime.engine.nest.NestTopology;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads what a running nest step holds for one root key off this member's engine: the step's tree is compiled
 * from the stored definition, exactly as a start compiles it, and the state is read through the maps the run
 * keeps it in. A pipeline with no live job here has nothing to read and says so.
 */
final class EngineNestStateInspector implements NestStateInspector {

    private final Engine engine;
    private final DagSource dags;
    private final HazelcastInstance member;

    EngineNestStateInspector(Engine engine, DagSource dags, HazelcastInstance member) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.dags = Objects.requireNonNull(dags, "dags");
        this.member = Objects.requireNonNull(member, "member");
    }

    @Override
    public Result inspect(String pipelineId, String step, List<String> rootKey) {
        if (!engine.hasLiveJob(pipelineId)) {
            return new Result.NotRunning();
        }
        Optional<NestTopology> topology = dags.nestTopology(pipelineId, step);
        if (topology.isEmpty()) {
            return new Result.NotRunning();
        }
        return switch (NestInspector.inspect(member, pipelineId, step, topology.get(), rootKey)) {
            case NestInspector.Outcome.Held held -> new Result.Held(held.inspection());
            case NestInspector.Outcome.WrongArity wrong -> new Result.WrongWidth(wrong.columns());
            case NestInspector.Outcome.NotHeld none -> new Result.NotHeld(none.columns());
            case NestInspector.Outcome.NotLive ignored -> new Result.NotRunning();
        };
    }
}
