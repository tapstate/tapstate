package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import org.junit.jupiter.api.Test;

class DslFlatEmbedTest {

    private static String pipeline(String embedBody) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src
                transforms:
                  - id: document
                    type: nest
                    from: { customer: customers, profile: profiles }
                    root:
                      from: customer
                      key: [id]
                      embed:
                %s
                """.formatted(embedBody.indent(8));
    }

    @Test
    void flatIsAFirstClassShapeWithNoTargetPath() {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse(pipeline("""
                - from: profile
                  on: { customer_id: id }
                  as: flat
                """));

        Step.Inline step = (Step.Inline) pipeline.transforms().getFirst();
        Embed embed = ((TransformBody.Nest) step.body()).root().embed().getFirst();
        assertThat(embed.as()).isEqualTo(EmbedAs.FLAT);
        assertThat(embed.path()).isNull();
        assertThat(embed.arrayKey()).isNull();
    }

    @Test
    void aFlatEmbedRefusesAPathAtTheAuthoredField() {
        assertThatThrownBy(() -> new DslParser().parse(pipeline("""
                - from: profile
                  on: { customer_id: id }
                  as: flat
                  path: profile
                """)))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.FORBIDDEN_FIELD);
                    assertThat(error.path()).isEqualTo("transforms[0].root.embed[0].path");
                });
    }

    @Test
    void aFlatEmbedRefusesAnArrayKeyAtTheAuthoredField() {
        assertThatThrownBy(() -> new DslParser().parse(pipeline("""
                - from: profile
                  on: { customer_id: id }
                  as: flat
                  arrayKey: [id]
                """)))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.FORBIDDEN_FIELD);
                    assertThat(error.path()).isEqualTo("transforms[0].root.embed[0].arrayKey");
                });
    }

    @Test
    void objectAndArrayEmbedsStillRequireTheirPath() {
        for (String shape : new String[] {"object", "array"}) {
            assertThatThrownBy(() -> new DslParser().parse(pipeline("""
                    - from: profile
                      on: { customer_id: id }
                      as: %s
                    """.formatted(shape))))
                    .isInstanceOfSatisfying(DslException.class, error -> {
                        assertThat(error.code()).isEqualTo(DslError.MISSING_FIELD);
                        assertThat(error.path()).isEqualTo("transforms[0].root.embed[0].path");
                    });
        }
    }
}
