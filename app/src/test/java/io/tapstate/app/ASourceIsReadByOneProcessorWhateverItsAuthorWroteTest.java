package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.runtime.engine.ExecutionShape;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A source is read by one processor whatever its author wrote. Splitting a read needs parts that never overlap
 * and each keep their own progress, and no connector has declared that it can; so a wider target is held to one
 * and says why, rather than refused - more readers would only be slower to reason about, never wrong in a way
 * one reader is not.
 */
class ASourceIsReadByOneProcessorWhateverItsAuthorWroteTest {

    @Test
    void aSourceWrittenNoWidthIsReadByOneProcessorAtItsDefault() {
        NodeParallelism orders = shape(null).nodes().get("orders_src");

        assertThat(orders.scope()).isEqualTo(NodeParallelism.Scope.TOTAL_ONE);
        assertThat(orders.requested()).isEqualTo(1);
        assertThat(orders.origin()).isEqualTo(NodeParallelism.Origin.NODE_DEFAULT);
    }

    @Test
    void aSourceAskedForMoreIsStillReadByOneAndSaysWhy() {
        NodeParallelism orders = shape(new ExecutionSpec(4, null)).nodes().get("orders_src");

        assertThat(orders.scope()).isEqualTo(NodeParallelism.Scope.TOTAL_ONE);
        assertThat(orders.effective()).isEqualTo(1);
        assertThat(orders.requested()).isEqualTo(4);
        assertThat(orders.origin()).isEqualTo(NodeParallelism.Origin.EXPLICIT);
        assertThat(orders.reasons()).contains("source-reads-not-split");
    }

    private static ExecutionShape shape(ExecutionSpec sourceExecution) {
        PipelineResource pipeline = new PipelineResource("p", null, List.of(SourceRef.bare("orders_src")), List.of(),
                null, null, null, null);
        ExecutionShapes.Graph graph = new ExecutionShapes.Graph(ref -> List.of(((FromRef.Literal) ref).ref()),
                Map.of("orders_src", "orders"), Map.of("orders", List.of("id")), Map.of(), Map.of(),
                sourceExecution == null ? Map.of() : Map.of("orders_src", sourceExecution));
        return ExecutionShapes.of("p", pipeline, 3, ParallelismBudget.DEFAULTS, graph, List.of());
    }
}
