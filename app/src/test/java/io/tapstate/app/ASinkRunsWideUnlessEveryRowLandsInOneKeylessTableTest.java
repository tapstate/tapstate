package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.SinkTarget;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A sink its author wrote no width for runs four writers across the cluster - unlike a step, which runs one -
 * routing each row by the table it lands in and its key there. It is held to one writer only where that cannot
 * work: every row lands in one table with no key, or a stream reaches it that nothing says the landing of. A
 * sink writing several tables is not held to one by a keyless table among them, and two streams written into
 * one table are routed by that table's key, not by the streams they came in on.
 */
class ASinkRunsWideUnlessEveryRowLandsInOneKeylessTableTest {

    private static final SinkTarget ORDERS = new SinkTarget("orders", List.of("id"));
    private static final SinkTarget AUDIT = new SinkTarget("audit", List.of());
    private static final SinkTarget EVENTS = new SinkTarget("events", List.of());

    @Test
    void aSinkWrittenNoWidthRunsItsDefaultOfFourRoutedByWhereItsRowsLand() {
        ExecutionShape shape = shape(2, sink(null, Map.of("orders", ORDERS)));

        assertThat(shape.isNative("serve.s")).isTrue();
        assertThat(shape.localOf("serve.s")).isEqualTo(2);
        assertThat(shape.effectiveOf("serve.s")).isEqualTo(4);
        assertThat(shape.nodes().get("serve.s").requested()).isEqualTo(4);
        assertThat(shape.nodes().get("serve.s").origin()).isEqualTo(NodeParallelism.Origin.NODE_DEFAULT);
        assertThat(shape.sinkTargetsOf("serve.s")).isEqualTo(Map.of("orders", ORDERS));
    }

    @Test
    void aWidthItsAuthorWroteReplacesTheDefault() {
        ExecutionShape eight = shape(3, sink(new ExecutionSpec(8, null), Map.of("orders", ORDERS)));
        ExecutionShape one = shape(3, sink(new ExecutionSpec(1, null), Map.of("orders", ORDERS)));

        assertThat(eight.localOf("serve.s")).isEqualTo(3);
        assertThat(eight.effectiveOf("serve.s")).isEqualTo(9);
        assertThat(eight.nodes().get("serve.s").origin()).isEqualTo(NodeParallelism.Origin.EXPLICIT);
        assertThat(one.isNative("serve.s")).isFalse();
        assertThat(one.nodes().get("serve.s").scope()).isEqualTo(NodeParallelism.Scope.TOTAL_ONE);
    }

    @Test
    void everyRowLandingInOneKeylessTableHoldsTheSinkToOneWriterAndRefusesAWrittenWidth() {
        ExecutionShape byDefault = shape(2, sink(null, Map.of("audit", AUDIT)));

        assertThat(byDefault.isNative("serve.s")).isFalse();
        assertThat(byDefault.nodes().get("serve.s").reasons()).contains("single-target-keyless");
        assertRefused(new ExecutionSpec(8, null), Map.of("audit", AUDIT), "single-target-keyless");
    }

    @Test
    void twoStreamsWrittenIntoOneTableAreRoutedByThatTablesKey() {
        Map<String, SinkTarget> consolidated = new LinkedHashMap<>();
        consolidated.put("eu_orders", ORDERS);
        consolidated.put("us_orders", ORDERS);
        Map<String, SinkTarget> keyless = new LinkedHashMap<>();
        keyless.put("eu_audit", AUDIT);
        keyless.put("us_audit", AUDIT);

        ExecutionShape keyed = shape(2, sink(null, consolidated));

        assertThat(keyed.isNative("serve.s")).isTrue();
        assertThat(keyed.sinkTargetsOf("serve.s")).isEqualTo(consolidated);
        assertThat(shape(2, sink(null, keyless)).isNative("serve.s")).isFalse();
        assertRefused(new ExecutionSpec(8, null), keyless, "single-target-keyless");
    }

    @Test
    void aKeylessTableAmongSeveralLeavesTheSinkWideAndOnlyThatTableSerial() {
        Map<String, SinkTarget> tables = new LinkedHashMap<>();
        tables.put("orders", ORDERS);
        tables.put("audit", AUDIT);
        Map<String, SinkTarget> keylessTables = new LinkedHashMap<>();
        keylessTables.put("audit", AUDIT);
        keylessTables.put("events", EVENTS);

        ExecutionShape mixed = shape(2, sink(new ExecutionSpec(8, null), tables));
        ExecutionShape allKeyless = shape(2, sink(new ExecutionSpec(8, null), keylessTables));

        assertThat(mixed.effectiveOf("serve.s")).isEqualTo(8);
        assertThat(mixed.sinkTargetsOf("serve.s")).isEqualTo(tables);
        assertThat(allKeyless.effectiveOf("serve.s")).isEqualTo(8);
        assertThat(allKeyless.sinkTargetsOf("serve.s")).isEqualTo(keylessTables);
    }

    @Test
    void aStreamNothingSaysTheLandingOfHoldsTheSinkToOneWriter() {
        Map<String, SinkTarget> unknown = new HashMap<>();
        unknown.put("orders", ORDERS);
        unknown.put("mystery", null);

        assertThat(shape(2, sink(null, unknown)).isNative("serve.s")).isFalse();
        assertThat(shape(2, sink(null, Map.of())).isNative("serve.s")).isFalse();
        assertRefused(new ExecutionSpec(8, null), unknown, "key-not-derivable");
    }

    @Test
    void theBatchAWriterFormsCountsAgainstWhatAMemberMayBuffer() {
        // Four writers on one member, each holding up to two batches of 65536 rows, is more than a member buffers.
        ExecutionSpec large = new ExecutionSpec(null, new BatchSpec(65536, null));

        assertThatThrownBy(() -> shape(1, sink(large, Map.of("orders", ORDERS))))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ActuationError.NO_SAFE_PARALLELISM);
                    assertThat(refused.args()).containsEntry("node", "serve.s").containsEntry("requested", 4);
                    assertThat((String) refused.args().get("candidates"))
                            .contains("max-buffered-records-per-member");
                });
        assertThat(shape(1, sink(null, Map.of("orders", ORDERS))).effectiveOf("serve.s")).isEqualTo(4);
    }

    private static ExecutionShapes.Sink sink(ExecutionSpec execution, Map<String, SinkTarget> targets) {
        return new ExecutionShapes.Sink("serve.s", execution, targets);
    }

    private static ExecutionShape shape(int members, ExecutionShapes.Sink sink) {
        PipelineResource pipeline = new PipelineResource("p", null, List.of(SourceRef.bare("src")), List.of(),
                null, null, null, null);
        ExecutionShapes.Graph graph = new ExecutionShapes.Graph(ref -> List.of(((FromRef.Literal) ref).ref()),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
        return ExecutionShapes.of("p", pipeline, members, ParallelismBudget.DEFAULTS, graph, List.of(sink));
    }

    private static void assertRefused(ExecutionSpec execution, Map<String, SinkTarget> targets, String reason) {
        assertThatThrownBy(() -> shape(2, sink(execution, targets)))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ActuationError.PARALLELISM_NEEDS_A_KEY);
                    assertThat(refused.args()).containsEntry("node", "serve.s").containsEntry("requested", 8)
                            .containsEntry("reason", reason);
                });
    }
}
