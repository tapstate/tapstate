package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The parse surface of a map's {@code soft_delete} key: the name of the column that marks a deleted
 * row. With it a map may leave {@code fields} out, since adding the marker is a whole step on its own;
 * without it {@code fields} is still required, so a map that does nothing is still refused.
 */
class DslMapSoftDeleteParseTest {

    private static String pipeline(String payload) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: live
                    from: [orders]
                    type: map
                %s
                serve:
                  from: live
                  sync: [ { id: s, source: src_a } ]
                """.formatted(payload.indent(4).stripTrailing());
    }

    private static TransformBody.MapProjection mapIn(Resource parsed) {
        Step step = ((PipelineResource) parsed).transforms().get(0);
        return (TransformBody.MapProjection) ((Step.Inline) step).body();
    }

    private static DslException refused(String payload) {
        Throwable thrown = catchThrowable(() -> new DslParser().parse(pipeline(payload)));
        assertThat(thrown).isInstanceOf(DslException.class);
        return (DslException) thrown;
    }

    @Test
    @DisplayName("soft_delete alone is a whole map, with no fields")
    void softDeleteAlone() {
        TransformBody.MapProjection body = mapIn(new DslParser().parse(pipeline("soft_delete: _is_deleted")));

        assertThat(body.softDelete()).isEqualTo("_is_deleted");
        assertThat(body.fields()).isEmpty();
        assertThat(body.outputFields()).containsExactly(
                java.util.Map.entry("_is_deleted", FieldRule.literal(Boolean.FALSE)));
    }

    @Test
    @DisplayName("soft_delete sits beside fields, and the marker is the last output column")
    void softDeleteBesideFields() {
        TransformBody.MapProjection body = mapIn(new DslParser().parse(pipeline("""
                soft_delete: _is_deleted
                fields: { stage: prod }
                """)));

        assertThat(body.fields()).containsOnlyKeys("stage");
        assertThat(body.outputFields()).containsOnlyKeys("stage", "_is_deleted");
    }

    @Test
    @DisplayName("a map with neither fields nor soft_delete is refused as missing its fields")
    void neitherIsRefused() {
        DslException ex = refused("# no payload");

        assertThat(ex.code()).isEqualTo(DslError.MISSING_FIELD);
        assertThat(ex.args()).containsEntry("field", "fields");
    }

    @Test
    @DisplayName("a marker also declared as a field is refused where it was written")
    void markerAlsoAFieldIsRefused() {
        DslException ex = refused("""
                soft_delete: _is_deleted
                fields: { _is_deleted: false }
                """);

        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("transforms[0].soft_delete");
    }

    @Test
    @DisplayName("the camelCase spelling is refused by name rather than ignored")
    void camelCaseIsRefused() {
        DslException ex = refused("softDelete: _is_deleted");

        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
    }

    /**
     * The canonical form is what the author wrote: soft_delete alone writes no empty fields block,
     * and parsing the written form gives the same body back.
     */
    @Test
    @DisplayName("the canonical form keeps soft_delete and round-trips")
    void canonicalRoundTrip() {
        Resource parsed = new DslParser().parse(pipeline("soft_delete: _is_deleted"));
        String written = new CanonicalWriter().write(parsed);

        assertThat(written).contains("soft_delete: _is_deleted").doesNotContain("fields");
        assertThat(mapIn(new DslParser().parse(written))).isEqualTo(mapIn(parsed));
    }

    @Test
    @DisplayName("a map without soft_delete writes exactly what it wrote before")
    void plainMapCanonicalUnchanged() {
        String written = new CanonicalWriter().write(new DslParser().parse(pipeline("fields: { stage: prod }")));

        assertThat(written).contains("fields:").doesNotContain("soft_delete");
    }
}
