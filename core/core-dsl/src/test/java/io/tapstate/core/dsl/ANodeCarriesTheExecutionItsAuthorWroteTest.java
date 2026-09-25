package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * How wide a node runs, and in what batches, is written on the node itself and nowhere else.
 *
 * <p>The number an author writes is a total for the whole cluster. The per-member count it becomes is worked
 * out when a run is built, from how many members take part, so it is never something an author can write -
 * and a word asking for a guess is refused for the same reason: a guess that moves with a restart or a resize
 * would change how wide a node runs with nobody having changed its artifact.
 *
 * <p>Absent is not a default written out. A node that says nothing takes its node type's default, and the
 * difference is reported when the node runs, so the parse keeps the absence and the canonical form keeps a
 * written default.
 */
class ANodeCarriesTheExecutionItsAuthorWroteTest {

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    /** A pipeline whose first step carries {@code execution}, indented as a key of that step. */
    private static String pipelineWithStepExecution(String execution) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: kept
                    type: filter
                    from: [orders]
                    expr: "op != 'd'"
                """
                + execution
                + """
                serve:
                  from: kept
                  sync: [ { id: out, source: tgt } ]
                """;
    }

    private ExecutionSpec stepExecution(String execution) {
        PipelineResource p = (PipelineResource) parser.parse(pipelineWithStepExecution(execution));
        return p.transforms().get(0).execution();
    }

    @Test
    void everyPartOfAWrittenBlockIsCarried() {
        ExecutionSpec execution = stepExecution("""
                    execution:
                      parallelism: 8
                      batch: { max_records: 512, max_wait: 50ms }
                """);

        assertThat(execution).isEqualTo(new ExecutionSpec(8, new BatchSpec(512, "50ms")));
        assertThat(execution.batch().effectiveMaxWaitMillis()).isEqualTo(50L);
    }

    @Test
    void aNodeThatSaysNothingCarriesNothing() {
        // Absent rather than the default resolved early: which default applies depends on the kind of node,
        // and whether it was written is itself reported when the node runs.
        assertThat(stepExecution("")).isNull();
        assertThat(stepExecution("    execution: {}\n")).isNull();
        assertThat(stepExecution("    execution: { batch: {} }\n")).isNull();
    }

    @Test
    void theBlockIsAcceptedOnEveryKindOfNodeThatRuns() {
        PipelineResource pipeline = (PipelineResource) parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - { id: masked, use: mask, from: [orders], execution: { parallelism: 2 } }
                view:
                  id: v
                  from: masked
                  primary_key: order_id
                  execution: { parallelism: 6 }
                serve:
                  from: masked
                  sync:
                    - { id: out, source: tgt, execution: { batch: { max_wait: 1s } } }
                """);

        assertThat(((Step.Use) pipeline.transforms().get(0)).execution().parallelism()).isEqualTo(2);
        assertThat(((ViewBlock.Inline) pipeline.view()).execution().parallelism()).isEqualTo(6);
        assertThat(((ServeBlock.Inline) pipeline.serve()).sync().get(0).execution().batch().maxWait())
                .isEqualTo("1s");

        SourceResource source = (SourceResource) parser.parse("""
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                config: { host: h }
                mode: cdc
                tables: [ orders ]
                execution: { parallelism: 4 }
                """);
        // Accepted: one processor reads a source today whatever is written, and saying so is the
        // runtime's job, where the member count and the reason are known - not a parse error.
        assertThat(source.execution().parallelism()).isEqualTo(4);

        ViewResource view = (ViewResource) parser.parse("""
                version: tapstate/v1
                kind: view
                id: v
                primary_key: order_id
                execution: { parallelism: 3 }
                """);
        assertThat(view.execution().parallelism()).isEqualTo(3);
    }

    @Test
    void aTransformDefinitionCarriesNoExecution() {
        // A definition body is pure logic; how it runs belongs to the step that uses it, and the same body
        // may run wide in one pipeline and narrow in another.
        Throwable refused = catchThrowable(() -> parser.parse("""
                version: tapstate/v1
                kind: transform
                id: mask
                type: map
                fields: { notes: false }
                execution: { parallelism: 2 }
                """));

        assertThat(refused).isInstanceOf(DslException.class);
        assertThat(((DslException) refused).code()).isEqualTo(DslError.UNKNOWN_FIELD);
    }

    @Test
    void aPerMemberCountIsNotSomethingAnAuthorWrites() {
        Throwable refused = catchThrowable(() -> stepExecution("""
                    execution: { local_parallelism: 3 }
                """));

        assertThat(refused).isInstanceOf(DslException.class);
        DslException error = (DslException) refused;
        assertThat(error.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(error.args()).containsEntry("field", "local_parallelism");
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "0", "-1", "1025", "2.5", "'8'"})
    void theParallelismIsAWholeNumberFromOneToTheLimit(String written) {
        Throwable refused = catchThrowable(() -> stepExecution("    execution: { parallelism: " + written + " }\n"));

        assertThat(refused).isInstanceOf(DslException.class);
        assertThat(((DslException) refused).code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(((DslException) refused).args().get("expected")).isEqualTo("a whole number from 1 to 1024");
    }

    @Test
    void theParallelismLimitItselfIsAccepted() {
        assertThat(stepExecution("    execution: { parallelism: 1024 }\n").parallelism()).isEqualTo(1024);
        assertThat(stepExecution("    execution: { parallelism: 1 }\n").parallelism()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "65537", "-4", "'512'"})
    void theRowLimitIsAWholeNumberFromOneToTheLimit(String written) {
        Throwable refused = catchThrowable(
                () -> stepExecution("    execution: { batch: { max_records: " + written + " } }\n"));

        assertThat(refused).isInstanceOf(DslException.class);
        assertThat(((DslException) refused).code()).isEqualTo(DslError.ILLEGAL_VALUE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"50", "0", "61s", "2m", "50 ms", "50MS", "1h", "-5ms", "05ms"})
    void theWaitIsAWholeNumberWithAUnitAndAtMostAMinute(String written) {
        // A bare number is refused rather than read in some unit: 50 could mean milliseconds or seconds.
        Throwable refused = catchThrowable(
                () -> stepExecution("    execution: { batch: { max_wait: " + written + " } }\n"));

        assertThat(refused).as(written).isInstanceOf(DslException.class);
        assertThat(((DslException) refused).code()).isEqualTo(DslError.ILLEGAL_VALUE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0ms", "50ms", "2s", "60s", "1m", "60000ms"})
    void everyWaitUpToAMinuteIsAccepted(String written) {
        ExecutionSpec execution = stepExecution("    execution: { batch: { max_wait: " + written + " } }\n");

        assertThat(execution.batch().maxWait()).isEqualTo(written);
        assertThat(execution.batch().effectiveMaxWaitMillis()).isBetween(0L, 60_000L);
    }

    @Test
    void theCanonicalFormKeepsAWrittenDefaultAndDropsNothingElse() {
        String canonical = writer.write(parser.parse(pipelineWithStepExecution("""
                    execution:
                      parallelism: 1
                      batch: { max_records: 1024, max_wait: 0ms }
                """)));

        // Written defaults stay written: whether a value came from the author or from the node type is
        // reported when the node runs, and the canonical form is where that answer is kept.
        assertThat(canonical).contains("""
                    execution:
                      parallelism: 1
                      batch:
                        max_records: 1024
                        max_wait: 0ms
                """);
        assertThat(parser.parse(canonical)).isEqualTo(parser.parse(pipelineWithStepExecution("""
                    execution:
                      parallelism: 1
                      batch: { max_records: 1024, max_wait: 0ms }
                """)));
    }
}
