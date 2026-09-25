package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ExecutionShape;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A step asked to run on several processors routes its input by the key of the rows reaching it, so it can
 * only run that way where those rows carry one - and which columns make up the key is a property of the stream
 * at that point of the graph, not of the table it started from. A projection can move the key to a new name
 * or take it away; an unwind's rows are elements rather than the rows the key identified.
 *
 * <p>Each case below changes only what sits between the source and the wide step, so a walk that read the
 * table's key and ignored the steps in between reddens on the rename, the drop and the unwind alike.
 */
class AStepRunsWideOnlyWhereItsRowsCarryAKeyTest {

    private static Step wide(String id, String from) {
        return Step.inline(id, FromClause.list(FromRef.literal(from)), new TransformBody.Js("row"),
                new ExecutionSpec(4, null), null);
    }

    private static Step plain(String id, String from, TransformBody body) {
        return Step.inline(id, FromClause.list(FromRef.literal(from)), body, null);
    }

    private static PipelineResource pipeline(Step... steps) {
        return new PipelineResource("p", null, List.of(SourceRef.bare("src")), List.of(steps),
                null, null, null, null);
    }

    private static ExecutionShapes.Graph graph(Map<String, List<String>> tableKeys, List<String> stepIds) {
        return new ExecutionShapes.Graph(
                ref -> {
                    String name = ((FromRef.Literal) ref).ref();
                    return name.equals("orders") ? List.of("src") : List.of(name);
                },
                Map.of("src", "orders"),
                tableKeys,
                Map.of());
    }

    private static ExecutionShape shape(Map<String, List<String>> tableKeys, Step... steps) {
        return ExecutionShapes.of("p", pipeline(steps), 2, ParallelismBudget.DEFAULTS,
                graph(tableKeys, java.util.Arrays.stream(steps).map(Step::id).toList()));
    }

    @Test
    void aStepReadingAKeyedTableRunsWideAndRoutesByTheTablesKey() {
        ExecutionShape shape = shape(Map.of("orders", List.of("id")), wide("w", "orders"));

        assertThat(shape.isNative("w")).isTrue();
        assertThat(shape.localOf("w")).isEqualTo(2);
        assertThat(shape.inputKeysOf("w")).isEqualTo(Map.of("orders", List.of("id")));
        assertThat(shape.nodes().get("w").origin()).isEqualTo(NodeParallelism.Origin.EXPLICIT);
    }

    @Test
    void aKeyMovedByAProjectionIsRoutedUnderItsNewName() {
        Map<String, FieldRule> rename = new LinkedHashMap<>();
        rename.put("order_id", FieldRule.rename("id"));
        ExecutionShape shape = shape(Map.of("orders", List.of("id")),
                plain("moved", "orders", new TransformBody.MapProjection(rename)),
                wide("w", "moved"));

        assertThat(shape.inputKeysOf("w")).isEqualTo(Map.of("orders", List.of("order_id")));
    }

    @Test
    void aKeyTakenAwayByAProjectionLeavesNothingToRouteBy() {
        assertRefused(shape -> shape(Map.of("orders", List.of("id")),
                plain("dropped", "orders", new TransformBody.MapProjection(Map.of("id", FieldRule.drop()))),
                wide("w", "dropped")));
        assertRefused(shape -> shape(Map.of("orders", List.of("id")),
                plain("overwritten", "orders",
                        new TransformBody.MapProjection(Map.of("id", FieldRule.literal(7)))),
                wide("w", "overwritten")));
    }

    @Test
    void anUnwindsRowsAreNotTheRowsTheKeyIdentified() {
        assertRefused(shape -> shape(Map.of("orders", List.of("id")),
                plain("lines", "orders", new TransformBody.Unwind("items", null, null, null, null)),
                wide("w", "lines")));
    }

    @Test
    void aKeylessTableRefusesAWrittenWidthButRunsAtItsDefault() {
        assertRefused(shape -> shape(Map.of("orders", List.of()), wide("w", "orders")));

        ExecutionShape byDefault = shape(Map.of("orders", List.of()), plain("d", "orders", new TransformBody.Js("row")));
        assertThat(byDefault.isNative("d")).isFalse();
        assertThat(byDefault.nodes().get("d").scope()).isEqualTo(NodeParallelism.Scope.TOTAL_ONE);
        assertThat(byDefault.nodes().get("d").origin()).isEqualTo(NodeParallelism.Origin.NODE_DEFAULT);
    }

    @Test
    void aWidthNoBudgetAllowsIsRefusedNamingEachCandidate() {
        Step tooWide = Step.inline("w", FromClause.list(FromRef.literal("orders")), new TransformBody.Js("row"),
                new ExecutionSpec(100, null), null);

        assertThatThrownBy(() -> ExecutionShapes.of("p", pipeline(tooWide), 1, ParallelismBudget.DEFAULTS,
                graph(Map.of("orders", List.of("id")), List.of("w"))))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ActuationError.NO_SAFE_PARALLELISM);
                    assertThat(refused.args()).containsEntry("node", "w").containsEntry("requested", 100)
                            .containsEntry("members", 1)
                            .containsEntry("candidates", "100 per member breaks max-local-parallelism");
                });
    }

    private static void assertRefused(java.util.function.Function<Void, ExecutionShape> build) {
        assertThatThrownBy(() -> build.apply(null))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ActuationError.PARALLELISM_NEEDS_A_KEY);
                    assertThat(refused.args()).containsEntry("node", "w").containsEntry("requested", 4)
                            .containsEntry("reason", "key-not-derivable");
                });
    }
}
