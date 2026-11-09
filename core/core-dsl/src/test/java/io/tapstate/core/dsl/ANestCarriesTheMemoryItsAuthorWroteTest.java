package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

/**
 * A nest step says how much memory its state may take in bytes, written as a size with a unit, and the
 * artifact keeps one spelling for one size.
 */
class ANestCarriesTheMemoryItsAuthorWroteTest {

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    private static String pipeline(String memory) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: doc
                    type: nest
                    from: { customer: customers, policy: policies }
                """
                + memory
                + """
                    root:
                      from: customer
                      key: [customer_id]
                      embed:
                        - from: policy
                          on: { customer_id: customer_id }
                          as: array
                          path: policies
                          arrayKey: [policy_id]
                serve:
                  from: doc
                  query: [ { type: rest } ]
                """;
    }

    private TransformBody.Nest nestOf(String memory) {
        PipelineResource p = (PipelineResource) parser.parse(pipeline(memory));
        return (TransformBody.Nest) ((Step.Inline) p.transforms().get(0)).body();
    }

    @Test
    void aNestThatWritesNoMemoryCarriesNone() {
        assertThat(nestOf("").memory()).isNull();
        assertThat(nestOf("").memoryBytes()).isNull();
        assertThat(writer.write(parser.parse(pipeline("")))).doesNotContain("memory");
    }

    @Test
    void aSizeWithAUnitIsCarriedInBytes() {
        TransformBody.Nest nest = nestOf("    memory: 256MiB\n");
        assertThat(nest.memory()).isEqualTo("256MiB");
        assertThat(nest.memoryBytes()).isEqualTo(256L * 1024 * 1024);
    }

    @Test
    void oneSizeHasOneSpellingWhicheverWayItWasWritten() {
        assertThat(nestOf("    memory: 1024KiB\n").memory()).isEqualTo("1MiB");
        assertThat(nestOf("    memory: 2097152\n").memory()).isEqualTo("2MiB");
        assertThat(nestOf("    memory: 1500B\n").memory()).isEqualTo("1500B");
        assertThat(writer.write(parser.parse(pipeline("    memory: 4096MiB\n")))).contains("memory: 4GiB");
    }

    @Test
    void whatTheAuthorWroteSurvivesBeingWrittenBackOut() {
        String canonical = writer.write(parser.parse(pipeline("    memory: 64MiB\n")));
        assertThat(((TransformBody.Nest) ((Step.Inline) ((PipelineResource) parser.parse(canonical))
                .transforms().get(0)).body()).memory()).isEqualTo("64MiB");
    }

    @Test
    void anythingButAPositiveSizeIsRefusedWithACode() {
        for (String written : new String[] {"memory: 0", "memory: 0MiB", "memory: -1", "memory: lots",
                "memory: 256MB", "memory: 1.5GiB", "memory: 99999999999999999999GiB"}) {
            Throwable thrown = catchThrowable(() -> nestOf("    " + written + "\n"));
            assertThat(thrown).as(written).isInstanceOf(DslException.class);
            assertThat(((DslException) thrown).code()).as(written).isEqualTo(DslError.ILLEGAL_VALUE);
        }
    }
}
