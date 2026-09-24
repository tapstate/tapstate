package io.tapstate.core.dsl;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Every input a join's alias map names has to be a source table, never another step of the pipeline.
 *
 * <p>A join resolves the columns its SQL names against the discovered model of each input table, and
 * files every row it holds under the driving table's primary key. A step's output has neither: its
 * columns are whatever the step emits and its rows carry no declared key. Accepted here, such a join
 * passes validate and apply and then fails at every start, because no column of that input can be
 * resolved - so the refusal belongs here, while the author is still looking at what they wrote.
 *
 * <p>A reference resolves the way the closure resolves it: a bare token that is a step id of the
 * pipeline names that step. A step id cannot shadow a table name, so the two never compete.
 */
final class JoinInputRules {

    private JoinInputRules() {
    }

    /** Validates every join step in the batch against the steps of its own pipeline. */
    static void validate(Map<String, Resource> byId) {
        for (Resource resource : byId.values()) {
            if (resource instanceof PipelineResource pipeline) {
                validatePipeline(pipeline, byId);
            }
        }
    }

    private static void validatePipeline(PipelineResource pipeline, Map<String, Resource> byId) {
        List<Step> steps = pipeline.transforms();
        if (steps == null) {
            return;
        }
        Set<String> stepIds = new HashSet<>();
        for (Step step : steps) {
            stepIds.add(step.id());
        }
        for (int i = 0; i < steps.size(); i++) {
            Step step = steps.get(i);
            if (!(bodyOf(step, byId) instanceof TransformBody.Join)
                    || !(step.from() instanceof FromClause.Aliases wiring)) {
                continue;
            }
            for (Map.Entry<String, FromRef> alias : wiring.aliases().entrySet()) {
                if (alias.getValue() instanceof FromRef.Literal literal && stepIds.contains(literal.ref())) {
                    throw new DslException(DslError.JOIN_INPUT_NOT_A_TABLE,
                            "transforms[" + i + "].from." + alias.getKey(), 0, 0, null,
                            Map.of("step", step.id(), "alias", alias.getKey(), "ref", literal.ref()));
                }
            }
        }
    }

    /** The body a step runs: its own, or the one the definition it reuses declares. */
    private static TransformBody bodyOf(Step step, Map<String, Resource> byId) {
        return switch (step) {
            case Step.Inline inline -> inline.body();
            case Step.Use use -> byId.get(use.use()) instanceof TransformResource definition
                    ? definition.body() : null;
        };
    }
}
