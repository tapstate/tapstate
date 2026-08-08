package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The memory budget is one number for the pipeline, while it is written on a nest step - and a pipeline
 * may hold more than one nest step. Two of them naming different numbers is therefore expressible and
 * cannot be honoured: what the budget bounds is the memory the whole pipeline's levels share.
 *
 * <p>Silently taking one of the two is the failure this refuses. Whichever were taken, the pipeline
 * would run with a number its author wrote down and then contradicted, and nothing would say which
 * one won - the state would simply sit at a size neither of them asked for.
 *
 * <p>The per-document limit is not like this and must not be treated as though it were: it is applied
 * per nest, so two nests holding different limits is two answers to two questions rather than two
 * answers to one.
 */
class OnePipelineHoldsOneMemoryBudgetTest {

    private static final String SOURCES = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: mysql
            mode: cdc
            tables: [ customers, policies ]
            """;

    /** Two nest steps in one pipeline, each with whatever capacity the case gives it. */
    private static String pipeline(String firstCapacity, String secondCapacity) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: doc_a
                    type: nest
                    from: { customer: customers, policy: policies }
                """
                + firstCapacity
                + """
                    root:
                      from: customer
                      key: [customer_id]
                      embed:
                        - { from: policy, on: { customer_id: customer_id }, as: array, path: p,
                            arrayKey: [policy_id] }
                  - id: doc_b
                    type: nest
                    from: { holder: customers, cover: policies }
                """
                + secondCapacity
                + """
                    root:
                      from: holder
                      key: [customer_id]
                      embed:
                        - { from: cover, on: { customer_id: customer_id }, as: array, path: c,
                            arrayKey: [policy_id] }
                serve:
                  from: doc_b
                  sync: [ { id: s, source: src_a } ]
                """;
    }

    private static void batch(String first, String second) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(SOURCES, pipeline(first, second)).map(parser::parse).toList());
    }

    private static String budget(int entries) {
        return "    entries_in_memory: " + entries + "\n";
    }

    @Test
    @DisplayName("two nest steps agreeing on the budget load clean")
    void twoNestsMayNameTheSameBudget() {
        assertThatCode(() -> batch(budget(50_000), budget(50_000))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("only one nest step naming the budget is the pipeline's budget")
    void oneNestMayNameTheBudgetForThePipeline() {
        // Not a conflict and not a requirement to repeat: a pipeline states its budget once, on whichever
        // of its nests the author wrote it. Demanding it on every nest step would make adding a second
        // nest to a pipeline an edit to the first one.
        assertThatCode(() -> batch(budget(50_000), "")).doesNotThrowAnyException();
        assertThatCode(() -> batch("", budget(50_000))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("no nest step naming the budget loads clean")
    void neitherNestNeedNameTheBudget() {
        assertThatCode(() -> batch("", "")).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("two nest steps naming different budgets is refused with a code")
    void twoNestsMayNotNameDifferentBudgets() {
        Throwable thrown = catchThrowable(() -> batch(budget(50_000), budget(70_000)));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.COMPOSITION);
        // Located on the second of the two, which is where the disagreement becomes visible, and naming
        // both numbers: told only that they disagree, an author has to go and find the other one.
        assertThat(ex.path()).isEqualTo("transforms[1].entries_in_memory");
        assertThat(ex.args().get("detail").toString()).contains("50000").contains("70000");
    }

    @Test
    @DisplayName("two nest steps may hold different per-document limits")
    void thePerDocumentLimitIsPerNestAndMayDiffer() {
        // The discriminating case: the same shape that is refused for the budget is correct here. A rule
        // that swept both knobs together would pass every test above and still be wrong.
        assertThatCode(() -> batch("    max_elements_per_document: 2000\n",
                "    max_elements_per_document: 9000\n")).doesNotThrowAnyException();
    }
}
