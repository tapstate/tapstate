package io.tapstate.core.dsl;

import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceResource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Reports what a batch asks of how its nodes run that will not happen as written.
 *
 * <p>Two things, both reported rather than refused. A pipeline's {@code settings.parallelism} and
 * {@code settings.batch_size} still parse - removing a field from the grammar is a breaking change - but
 * how wide a node runs and how it batches are decided by the {@code execution} block on the node, and
 * nothing reads the pipeline-level fields. And a source asking for more than one processor is read by
 * one: splitting a read needs parts that never overlap and each keep their own progress, and no connector
 * has declared that it can do so.
 *
 * <p>Refusing either would turn away artifacts that run correctly. What makes them worth a line is that
 * nothing downstream says so - a setting with no effect and one that worked read the same in the artifact.
 */
public final class ExecutionRules {

    private ExecutionRules() {
    }

    /** The findings over {@code batch}, in the order its resources are given. */
    public static List<Advisory> review(Collection<Resource> batch) {
        List<Advisory> findings = new ArrayList<>();
        for (Resource resource : batch) {
            if (resource instanceof PipelineResource pipeline && pipeline.settings() != null) {
                Settings settings = pipeline.settings();
                if (settings.parallelism() != null) {
                    findings.add(new Advisory(ExecutionAdvisoryError.SETTING_HAS_NO_EFFECT, Map.of(
                            "pipeline", pipeline.id(),
                            "setting", "settings.parallelism",
                            "instead", "execution.parallelism")));
                }
                if (settings.batchSize() != null) {
                    findings.add(new Advisory(ExecutionAdvisoryError.SETTING_HAS_NO_EFFECT, Map.of(
                            "pipeline", pipeline.id(),
                            "setting", "settings.batch_size",
                            "instead", "execution.batch.max_records")));
                }
            }
            if (resource instanceof SourceResource source) {
                ExecutionSpec execution = source.execution();
                if (execution != null && execution.parallelism() != null && execution.parallelism() > 1) {
                    findings.add(new Advisory(ExecutionAdvisoryError.SOURCE_READ_NOT_SPLIT, Map.of(
                            "source", source.id(),
                            "requested", execution.parallelism())));
                }
            }
        }
        return List.copyOf(findings);
    }
}
