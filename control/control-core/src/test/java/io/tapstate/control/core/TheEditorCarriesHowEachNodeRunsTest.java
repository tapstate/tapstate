package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.ViewBlock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The editor's face carries each node's execution block both ways. A payload that lost it on the way in
 * would save a pipeline that runs at its defaults while the editor showed a different width; one that lost
 * it on the way out would show defaults for a pipeline that runs otherwise.
 *
 * <p>The payload is a map, so a key this face does not read would otherwise be dropped or folded into the
 * transform body without a word - which is why a per-member count or an unknown batch limit is refused here
 * by name, the same as in the authored form.
 */
class TheEditorCarriesHowEachNodeRunsTest {

    private final PipelineRepresentation representation = new PipelineRepresentation();

    private PipelineResource pipeline() {
        return (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: kept
                    type: filter
                    from: [orders]
                    expr: "op != 'd'"
                    execution: { parallelism: 8, batch: { max_records: 512, max_wait: 50ms } }
                  - { id: masked, use: mask, from: [kept], execution: { parallelism: 1 } }
                view:
                  id: v
                  from: masked
                  primary_key: order_id
                  execution: { parallelism: 6 }
                serve:
                  from: masked
                  sync:
                    - { id: out, source: tgt, execution: { batch: { max_wait: 1s } } }
                    - { id: plain, source: tgt }
                """);
    }

    @Test
    void eachNodesBlockSurvivesTheRoundTrip() {
        PipelineResource pipeline = pipeline();

        PipelineView view = representation.toView(
                pipeline, "e".repeat(64), List.of(new PipelineSourceSummary("src_a", null, "mysql")));

        assertThat(view.transforms().get(0).get("execution")).isEqualTo(Map.of(
                "parallelism", 8,
                "batch", Map.of("maxRecords", 512, "maxWait", "50ms")));
        // Absent stays absent: which default applies is the runtime's to report, beside whether the
        // author wrote the value at all.
        Map<String, Object> useStep = view.transforms().get(1);
        @SuppressWarnings("unchecked")
        Map<String, Object> useExecution = (Map<String, Object>) useStep.get("execution");
        assertThat(useExecution).containsEntry("parallelism", 1).containsEntry("batch", null);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sync = (List<Map<String, Object>>) view.serve().get("sync");
        assertThat(sync.get(1).get("execution")).isNull();

        PipelineResource roundTripped = representation.toModel(new PipelineInput(
                view.id(), view.metadata(), new ArrayList<Object>(view.sources()), view.transforms(),
                view.view(), view.serve(), view.settings(), view.experimental()), pipeline);

        assertThat(roundTripped).isEqualTo(pipeline);
        assertThat(((Step.Use) roundTripped.transforms().get(1)).execution())
                .isEqualTo(new ExecutionSpec(1, null));
        assertThat(((ViewBlock.Inline) roundTripped.view()).execution().parallelism()).isEqualTo(6);
        assertThat(((ServeBlock.Inline) roundTripped.serve()).sync().get(0).execution().batch())
                .isEqualTo(new BatchSpec(null, "1s"));
    }

    @Test
    void theAuthoredSpellingOfBatchLimitsIsReadToo() {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("id", "kept");
        step.put("type", "filter");
        step.put("from", List.of("orders"));
        step.put("expr", "op != 'd'");
        step.put("execution", Map.of("batch", Map.of("max_records", 256, "max_wait", "2s")));

        PipelineResource model = representation.toModel(new PipelineInput(
                "p", null, List.of("src_a"), List.of(step), null, null, null, null), null);

        assertThat(model.transforms().get(0).execution()).isEqualTo(
                new ExecutionSpec(null, new BatchSpec(256, "2s")));
    }

    @Test
    void aPerMemberCountIsRefusedByNameRatherThanDropped() {
        assertRefused(Map.of("local_parallelism", 3), "local_parallelism is not a field of execution");
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "0", "1025", "2.5"})
    void theParallelismIsAWholeNumberFromOneToTheLimit(String written) {
        Object value = switch (written) {
            case "auto" -> "auto";
            case "2.5" -> 2.5;
            default -> Integer.valueOf(written);
        };
        assertRefused(Map.of("parallelism", value), "must be a whole number from 1 to 1024");
    }

    @Test
    void aBareWaitIsRefusedBecauseItsUnitIsAGuess() {
        assertRefused(Map.of("batch", Map.of("maxWait", "50")), "must be a duration from 0ms to 60s");
    }

    @Test
    void anExecutionOnAViewReferenceIsRefusedRatherThanLost() {
        // A reference carries no execution of its own: the view it names does. Accepting one here would
        // save it nowhere, and the view would run at whatever its definition says.
        assertThatThrownBy(() -> representation.toModel(new PipelineInput(
                "p", null, List.of("src_a"), null,
                Map.of("use", "shared_view", "from", "orders", "execution", Map.of("parallelism", 2)),
                null, null, null), null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(String.valueOf(error.args().get("reason")))
                            .contains("belongs to the view definition");
                });
    }

    private void assertRefused(Map<String, Object> execution, String reason) {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("id", "kept");
        step.put("type", "filter");
        step.put("from", List.of("orders"));
        step.put("expr", "op != 'd'");
        step.put("execution", execution);

        assertThatThrownBy(() -> representation.toModel(new PipelineInput(
                "p", null, List.of("src_a"), List.of(step), null, null, null, null), null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(String.valueOf(error.args().get("reason"))).contains(reason);
                });
    }
}
