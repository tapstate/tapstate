package io.tapstate.core.dsl;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.WriteMode;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A view declares the same two write settings a sync element does, under the same names and with the
 * same defaults -- and a view that declares neither is exactly the document it was before views could.
 */
class ViewWriteSettingsDslTest {

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: leads
            source: crm
            view:
              id: lead
              from: crm
              primary_key: id
            """;

    private static final String DEFINITION = """
            version: tapstate/v1
            kind: view
            id: v_lead
            primary_key: id
            """;

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    @Test
    void anInlineViewKeepsBothSettingsThroughItsCanonicalForm() {
        String declared = PIPELINE + "  write_mode: append\n  on_full_load: clear\n";
        PipelineResource pipeline = (PipelineResource) parser.parse(declared);
        ViewBlock.Inline view = (ViewBlock.Inline) pipeline.view();
        assertThat(view.writeMode()).isEqualTo(WriteMode.APPEND);
        assertThat(view.onFullLoad()).isEqualTo(OnFullLoad.CLEAR);

        String canonical = writer.write(pipeline);
        assertThat(canonical).isEqualTo(declared);
        assertThat(writer.write(parser.parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void theSettingsSitAfterTheKeyAndBeforeTheStorage() {
        String declared = PIPELINE
                + "  storage: { warm: { collection: leads } }\n"
                + "  on_full_load: fail\n";
        assertThat(writer.write(parser.parse(declared))).isEqualTo(PIPELINE
                + "  on_full_load: fail\n"
                + "  storage:\n    warm:\n      collection: leads\n");
    }

    @Test
    void aViewDefinitionKeepsBothSettingsThroughItsCanonicalForm() {
        String declared = DEFINITION + "write_mode: append\non_full_load: clear\n";
        ViewResource definition = (ViewResource) parser.parse(declared);
        assertThat(definition.writeMode()).isEqualTo(WriteMode.APPEND);
        assertThat(definition.onFullLoad()).isEqualTo(OnFullLoad.CLEAR);
        assertThat(writer.write(definition)).isEqualTo(declared);
    }

    @Test
    void aViewThatDeclaresNeitherIsTheDocumentItAlwaysWas() {
        // The canonical text is what the content hash is taken over, so pinning it byte for byte pins the
        // hash of every view written before views could say either.
        assertThat(writer.write(parser.parse(PIPELINE))).isEqualTo(PIPELINE);
        assertThat(writer.write(parser.parse(DEFINITION))).isEqualTo(DEFINITION);
    }

    @Test
    void writingTheDefaultsOutChangesNothing() {
        String explicit = PIPELINE + "  write_mode: upsert\n  on_full_load: append\n";
        assertThat(writer.write(parser.parse(explicit))).isEqualTo(PIPELINE);
        assertThat(CanonicalHash.of(parser.parse(explicit))).isEqualTo(CanonicalHash.of(parser.parse(PIPELINE)));

        String definition = DEFINITION + "write_mode: upsert\non_full_load: append\n";
        assertThat(CanonicalHash.of(parser.parse(definition))).isEqualTo(CanonicalHash.of(parser.parse(DEFINITION)));
    }

    @Test
    void aUseReferenceRefusesThemTheWayAServeReferenceDoes() {
        String viewUse = """
                version: tapstate/v1
                kind: pipeline
                id: leads
                source: crm
                view:
                  use: v_lead
                  from: crm
                  on_full_load: clear
                """;
        String serveUse = """
                version: tapstate/v1
                kind: pipeline
                id: leads
                source: crm
                serve:
                  use: s_lead
                  from: crm
                  on_full_load: clear
                """;
        assertThatThrownBy(() -> parser.parse(viewUse))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.UNKNOWN_FIELD);
                    assertThat(error.path()).isEqualTo("view.on_full_load");
                });
        assertThatThrownBy(() -> parser.parse(serveUse))
                .isInstanceOfSatisfying(DslException.class,
                        error -> assertThat(error.code()).isEqualTo(DslError.UNKNOWN_FIELD));
    }

    @Test
    void anUnknownPolicyIsRefusedRatherThanReadAsKeepingTheRows() {
        assertThatThrownBy(() -> parser.parse(PIPELINE + "  on_full_load: truncate\n"))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.ILLEGAL_VALUE);
                    assertThat(error.path()).isEqualTo("view.on_full_load");
                });
    }
}
