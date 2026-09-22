package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

/** The database holding durable state is an optional choice made by one nest, not by every nest. */
class ANestCarriesItsStateDatabaseTest {

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    @Test
    void anExplicitDatabaseSurvivesCanonicalizationInsideTheStateBlock() {
        String canonical = writer.write(parser.parse(pipeline("""
                    state:
                      database: order_doc_state
                """)));

        assertThat(canonical).contains("""
                    type: nest
                    from:
                      i: order_items
                      o: orders
                    state:
                      database: order_doc_state
                    root:
                """);
    }

    @Test
    void anUnconfiguredNestDoesNotFreezeTheDeploymentDefaultIntoItsArtifact() {
        String canonical = writer.write(parser.parse(pipeline("")));

        assertThat(canonical).doesNotContain("state:");
        assertThat(canonical).doesNotContain("database:");
    }

    @Test
    void anUnknownStatePlacementKeyIsRefusedWhereItWasWritten() {
        Throwable thrown = catchThrowable(() -> parser.parse(pipeline("""
                    state:
                      collection: operator_state_b
                """)));

        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).path()).isEqualTo("transforms[0].state.collection");
    }

    private static String pipeline(String state) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: src
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, i: order_items }
                """
                + state
                + """
                    root:
                      from: o
                      key: [id]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items }
                """;
    }
}
