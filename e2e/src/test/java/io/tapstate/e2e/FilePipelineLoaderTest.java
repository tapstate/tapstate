package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code pipeline:} reference is resolved by the product's own parser. A second, test-local
 * reading of the DSL would be a copy free to drift from the grammar it claims to exercise, and a
 * specification that drifted from the product would still pass while testing nothing real.
 */
class FilePipelineLoaderTest {

    private static final String PIPELINE =
            """
            version: tapstate/v1
            kind: pipeline
            id: mongo2mongo
            source: src_mongo
            serve:
              from: /.*/
              sync:
                - { source: tgt_mongo, write_mode: upsert }
            """;

    private static final String SOURCE =
            """
            version: tapstate/v1
            kind: source
            id: src_mongo
            connector: mongodb
            config: { uri: "mongodb://127.0.0.1:27017/demo" }
            """;

    @TempDir Path workspace;

    @Test
    void resolvesThePipelineIdByParsingTheReferencedProductFile() throws IOException {
        write("mongo2mongo.tap.yml", PIPELINE);

        String id = new FilePipelineLoader(workspace).resolvePipelineId("mongo2mongo.tap.yml");

        assertThat(id).isEqualTo("mongo2mongo");
    }

    @Test
    void namesTheKindItFoundWhenTheReferenceIsNotAPipeline() throws IOException {
        write("src_mongo.tap.yml", SOURCE);

        assertThatThrownBy(() -> new FilePipelineLoader(workspace).resolvePipelineId("src_mongo.tap.yml"))
                .isInstanceOf(EnvelopeException.class)
                // The kind is the computed part: asserting only the boilerplate would pass even if
                // the parser reported no kind at all.
                .hasMessage("src_mongo.tap.yml must declare a pipeline, found kind: source");
    }

    @Test
    void rejectsAReferenceToAMissingFile() {
        assertThatThrownBy(() -> new FilePipelineLoader(workspace).resolvePipelineId("absent.tap.yml"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("absent.tap.yml");
    }

    @Test
    void carriesTheProductDiagnosticIntoTheAuthoringError() throws IOException {
        write("broken.tap.yml", "version: tapstate/v1\nkind: pipeline\nid: broken\nnonsense: true\n");

        assertThatThrownBy(() -> new FilePipelineLoader(workspace).resolvePipelineId("broken.tap.yml"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("broken.tap.yml does not parse")
                // The product's own coded diagnostic must survive into the message, otherwise the
                // author is told only that something, somewhere, is wrong.
                .hasMessageContaining("TS0010002")
                .hasMessageContaining("nonsense");
    }

    private void write(String name, String content) throws IOException {
        Files.writeString(workspace.resolve(name), content);
    }
}
