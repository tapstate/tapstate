package io.tapstate.core.dsl;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The memory budget a nest step writes down belongs to the whole pipeline, and a pipeline may hold more
 * than one nest step. Two of them naming different numbers is expressible and cannot be honoured: what
 * the budget bounds is memory those levels share, so there is one answer to give and two were written.
 *
 * <p>Taking one of the two silently is what this refuses. Whichever were taken, the pipeline would run
 * on a number its author wrote down and then contradicted, and nothing would report which won - the
 * state would simply settle at a size neither of them asked for, which reads as the substrate being
 * approximate rather than as a pipeline being wrong.
 *
 * <p>The per-document limit is deliberately not checked here. It is applied per nest, so two nests
 * holding different limits are two answers to two questions; sweeping both knobs into one rule would
 * refuse a pipeline that is correct.
 */
final class NestCapacityRules {

    private NestCapacityRules() {
    }

    /** Validates that no pipeline in the batch names its memory budget twice over, differently. */
    static void validate(Collection<Resource> batch) {
        for (Resource resource : batch) {
            if (resource instanceof PipelineResource pipeline) {
                validatePipeline(pipeline);
            }
        }
    }

    private static void validatePipeline(PipelineResource pipeline) {
        List<Step> steps = pipeline.transforms();
        if (steps == null) {
            return;
        }
        Integer agreed = null;
        for (int i = 0; i < steps.size(); i++) {
            Integer written = budgetOf(steps.get(i));
            if (written == null) {
                continue;
            }
            if (agreed != null && !agreed.equals(written)) {
                // Reported at the step that disagrees, and carrying both numbers. Told only that a
                // conflict exists, an author has to go and find the other one to know what to change.
                throw new DslException(DslError.COMPOSITION,
                        "transforms[" + i + "].entries_in_memory", 0, 0, null,
                        Map.of("detail", "a pipeline holds one memory budget for all of its nests, and "
                                + "this one names two: " + agreed + " and " + written));
            }
            agreed = written;
        }
    }

    /** The budget this step writes down, or null where it is not a nest or writes none. */
    private static Integer budgetOf(Step step) {
        return step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest
                ? nest.entriesInMemory()
                : null;
    }
}
