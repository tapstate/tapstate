package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.jupiter.api.Test;

/** A view must not accept a schema policy that no runtime or control-plane path enforces. */
class AViewCannotPromiseASchemaPolicyNobodyEnforcesTest {

    @Test
    void aViewRefusesASchemaPolicyThatHasNoRuntimeMeaning() {
        String yaml = """
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: orders_src
                view:
                  id: order_state
                  from: widen
                  primary_key: order_id
                  schema:
                    enforce: true
                    evolution: additive
                """;

        Throwable thrown = catchThrowable(() -> new DslParser().parse(yaml));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException refused = (DslException) thrown;
        assertThat(refused.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(refused.path()).isEqualTo("view.schema");
    }
}
