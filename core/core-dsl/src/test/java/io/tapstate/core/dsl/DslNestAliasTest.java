package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A nest body names its streams by the aliases the step declares, and nothing checked that those names
 * existed. A dangling one passes validate today and only surfaces once the pipeline is built, as a
 * stream that never arrives - which reads as a source that is merely slow.
 *
 * <p>The alias map itself is batch wiring and is resolved elsewhere; this is the other half, where the
 * body points back at it.
 */
class DslNestAliasTest {

    private static final String SOURCES = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: mysql
            mode: cdc
            tables: [ customers, policies, claims ]
            """;

    private static String pipeline(String rootFrom, String policyFrom, String claimFrom) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: doc
                    from: { customer: customers, policy: policies, claim: claims }
                    type: nest
                    root:
                      from: %s
                      key: [customer_id]
                      embed:
                        - from: %s
                          on: { customer_id: customer_id }
                          as: array
                          path: policies
                          arrayKey: [policy_id]
                          embed:
                            - from: %s
                              on: { policy_id: policy_id }
                              as: array
                              path: claims
                              arrayKey: [claim_id]
                serve:
                  from: doc
                  sync: [ { id: s, source: src_a } ]
                """.formatted(rootFrom, policyFrom, claimFrom);
    }

    private static void batch(String... yamls) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(yamls).map(parser::parse).toList());
    }

    private static DslException refused(String rootFrom, String policyFrom, String claimFrom) {
        Throwable thrown = catchThrowable(() -> batch(SOURCES, pipeline(rootFrom, policyFrom, claimFrom)));
        assertThat(thrown).isInstanceOf(DslException.class);
        return (DslException) thrown;
    }

    @Test
    @DisplayName("a nest body naming only declared aliases loads clean")
    void everyAliasTheBodyNamesIsOneTheStepDeclared() {
        assertThatCode(() -> batch(SOURCES, pipeline("customer", "policy", "claim")))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("root.from naming an undeclared alias is missing-reference")
    void rootPointingAtAnUndeclaredAliasIsRefused() {
        DslException ex = refused("shopper", "policy", "claim");

        assertThat(ex.code()).isEqualTo(DslError.MISSING_REFERENCE);
        assertThat(ex.path()).isEqualTo("transforms[0].root.from");
        assertThat(ex.args()).containsEntry("ref", "shopper");
    }

    @Test
    @DisplayName("embed.from naming an undeclared alias is missing-reference at that embed")
    void anEmbedPointingAtAnUndeclaredAliasIsRefused() {
        DslException ex = refused("customer", "coverage", "claim");

        assertThat(ex.code()).isEqualTo(DslError.MISSING_REFERENCE);
        assertThat(ex.path()).isEqualTo("transforms[0].root.embed[0].from");
        assertThat(ex.args()).containsEntry("ref", "coverage");
    }

    @Test
    @DisplayName("the path locates a dangling alias however deep in the tree it sits")
    void anEmbedDeepInTheTreeIsLocatedByItsOwnPath() {
        DslException ex = refused("customer", "policy", "incident");

        assertThat(ex.code()).isEqualTo(DslError.MISSING_REFERENCE);
        assertThat(ex.path()).isEqualTo("transforms[0].root.embed[0].embed[0].from");
        assertThat(ex.args()).containsEntry("ref", "incident");
    }

    @Test
    @DisplayName("an alias the step declares but the body never names is not an error")
    void anUnusedAliasIsNotRefused() {
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: doc
                    from: { customer: customers, policy: policies, spare: claims }
                    type: nest
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
                  sync: [ { id: s, source: src_a } ]
                """;

        assertThatCode(() -> batch(SOURCES, pipe)).doesNotThrowAnyException();
    }
}
