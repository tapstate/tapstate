package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ExecutionShape;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A nest or a join is worked out like any step: one processor for the whole cluster where its author wrote no
 * width, and otherwise the nearest width the members taking part can run - never the engine's own answer, which
 * is the member's core count. What bounds it is what it costs: every vertex of a nest holds a thread of its own
 * for the life of a run, and all of them run as wide as the nest does, so they count against what a member may
 * run of such threads.
 */
class ANestOrAJoinIsWorkedOutLikeAnyStepTest {

    @Test
    void aNestOrAJoinItsAuthorWroteNoWidthForRunsAsOneProcessorForTheCluster() {
        ExecutionShape shape = shape(3, nest(null), join(null), Map.of("doc", 2));

        for (String node : List.of("doc", "j")) {
            NodeParallelism parallelism = shape.nodes().get(node);
            assertThat(parallelism.scope()).as(node).isEqualTo(NodeParallelism.Scope.TOTAL_ONE);
            assertThat(parallelism.requested()).as(node).isEqualTo(1);
            assertThat(parallelism.origin()).as(node).isEqualTo(NodeParallelism.Origin.NODE_DEFAULT);
        }
    }

    @Test
    void aWrittenWidthIsWorkedOutForTheMembersTakingPart() {
        ExecutionShape shape = shape(3, nest(new ExecutionSpec(8, null)), join(new ExecutionSpec(4, null)),
                Map.of("doc", 2));

        assertThat(shape.localOf("doc")).isEqualTo(3);
        assertThat(shape.effectiveOf("doc")).isEqualTo(9);
        assertThat(shape.localOf("j")).as("four on three members is nearest at one each - three in all, "
                + "still one on every member rather than one for the cluster").isEqualTo(1);
        assertThat(shape.effectiveOf("j")).isEqualTo(3);
    }

    @Test
    void aNestsVerticesCountAgainstTheThreadsAMemberMayHoldForThem() {
        // Fifty vertices each holding a thread, four wide on one member, is two hundred threads - past the 128 a
        // member may hold; two wide is a hundred, within it. A join holds none of its own.
        assertThatThrownBy(() -> shape(1, nest(new ExecutionSpec(4, null)), join(null), Map.of("doc", 50)))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ActuationError.NO_SAFE_PARALLELISM);
                    assertThat(refused.args()).containsEntry("node", "doc")
                            .containsEntry("candidates", "4 per member breaks max-blocking-processors-per-member");
                });

        assertThat(shape(1, nest(new ExecutionSpec(2, null)), join(new ExecutionSpec(16, null)), Map.of("doc", 50))
                .localOf("j")).isEqualTo(16);
    }

    private static Step nest(ExecutionSpec execution) {
        TransformBody body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of()));
        return Step.inline("doc", FromClause.aliases(Map.of("o", FromRef.literal("orders"))), body, execution,
                null);
    }

    private static Step join(ExecutionSpec execution) {
        TransformBody body = new TransformBody.Join(JoinEngine.BUILTIN,
                "SELECT o.id FROM orders o JOIN customers c ON o.cust_id = c.id");
        return Step.inline("j", FromClause.aliases(Map.of("o", FromRef.literal("orders"),
                "c", FromRef.literal("customers"))), body, execution, null);
    }

    private static ExecutionShape shape(int members, Step nest, Step join, Map<String, Integer> blocking) {
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("orders"), SourceRef.bare("customers")), List.of(nest, join),
                null, null, null, null);
        ExecutionShapes.Graph graph = new ExecutionShapes.Graph(ref -> List.of(((FromRef.Literal) ref).ref()),
                Map.of("orders", "orders", "customers", "customers"),
                Map.of("orders", List.of("id"), "customers", List.of("id")),
                Map.of("doc", List.of("id"), "j", List.of("id")),
                blocking);
        return ExecutionShapes.of("p", pipeline, members, ParallelismBudget.DEFAULTS, graph, List.of());
    }
}
