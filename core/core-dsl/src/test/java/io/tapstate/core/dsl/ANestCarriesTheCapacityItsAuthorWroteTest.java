package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

/**
 * What a nest is allowed to hold is the author's to say, in the pipeline that holds the nest.
 *
 * <p>Both numbers existed before this and neither could be reached: they were settings the process was
 * built with, and nothing a deployment could write set them to anything but their default. Putting them
 * where the nest is written puts them where the shape they bound is - one pipeline's tree is deep and
 * narrow and another's is shallow and wide, and a single number covering a whole process is a number
 * chosen for neither.
 *
 * <p>Absent is not zero. A nest that says nothing takes what the process was started with, which is what
 * keeps every pipeline written before these existed meaning exactly what it meant.
 */
class ANestCarriesTheCapacityItsAuthorWroteTest {

    /** Both knobs written, indented as step keys of the nest the template builds. */
    private static final String BOTH = """
                entries_in_memory: 50000
                max_elements_per_document: 2000
            """;

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    /** The same nest every time, with {@code capacity} the only thing that differs. */
    private static String pipeline(String capacity) {
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
                + capacity
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

    private TransformBody.Nest nestOf(String capacity) {
        PipelineResource p = (PipelineResource) parser.parse(pipeline(capacity));
        return (TransformBody.Nest) ((Step.Inline) p.transforms().get(0)).body();
    }

    @Test
    void aNestThatWritesNeitherKnobCarriesNeither() {
        TransformBody.Nest nest = nestOf("");

        // Not zero and not the default resolved early: absent, so that what the process was started with
        // is what applies. Resolving the default here would freeze it into the artifact, and a deployment
        // that later changed its own number would find pipelines written before the change ignoring it.
        assertThat(nest.entriesInMemory()).isNull();
        assertThat(nest.maxElementsPerDocument()).isNull();
    }

    @Test
    void aNestCarriesBothKnobsItsAuthorWrote() {
        TransformBody.Nest nest = nestOf(BOTH);

        assertThat(nest.entriesInMemory()).isEqualTo(50_000);
        assertThat(nest.maxElementsPerDocument()).isEqualTo(2_000);
    }

    @Test
    void eachKnobIsCarriedWithoutTheOther() {
        assertThat(nestOf("    entries_in_memory: 50000\n").maxElementsPerDocument()).isNull();
        assertThat(nestOf("    max_elements_per_document: 2000\n").entriesInMemory()).isNull();
    }

    @Test
    void whatTheAuthorWroteSurvivesBeingWrittenBackOut() {
        String canonical = writer.write(parser.parse(pipeline(BOTH)));

        // Both on the step and ahead of root, which is where the emitted order puts every scalar of a
        // body: a knob emitted after the tree would sit past the block it applies to.
        assertThat(canonical).contains("""
                    type: nest
                    from:
                      customer: customers
                      policy: policies
                    entries_in_memory: 50000
                    max_elements_per_document: 2000
                    root:
                """);
    }

    @Test
    void aNestThatWroteNeitherEmitsNeither() {
        String canonical = writer.write(parser.parse(pipeline("")));

        // An absent knob emits nothing rather than its default. Emitting it would make every pipeline
        // that never mentioned capacity start carrying a number nobody chose.
        assertThat(canonical).doesNotContain("entries_in_memory");
        assertThat(canonical).doesNotContain("max_elements_per_document");
    }

    @Test
    void aKnobThatIsNotAWholeNumberIsRefusedWithACode() {
        Throwable thrown = catchThrowable(() -> nestOf("    entries_in_memory: plenty\n"));

        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).code()).isEqualTo(DslError.ILLEGAL_VALUE);
    }

    @Test
    void aKnobThatWouldLetNothingThroughIsRefusedWithACode() {
        // Zero and below are refused where they are written rather than where they are enforced. A budget
        // of nothing holds nothing in memory and a document allowed no elements can never be assembled -
        // both fail every run rather than some, so the author hears about it while writing.
        for (String written : new String[] {"entries_in_memory: 0", "entries_in_memory: -1",
                "max_elements_per_document: 0", "max_elements_per_document: -1"}) {
            Throwable thrown = catchThrowable(() -> nestOf("    " + written + "\n"));

            assertThat(thrown).as(written).isInstanceOf(DslException.class);
            assertThat(((DslException) thrown).code()).as(written)
                    .isEqualTo(DslError.ILLEGAL_VALUE);
        }
    }
}
