package io.tapstate.adapters.pdk;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.model.SourceResource;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URISyntaxException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PdkTargetPreviewRendererTest {

    private static final String OBJECT_ID = "64f0c0de0011223344556677";

    @Test
    void restoresTargetCodecTypesAndWritesMongoExtendedJson(@TempDir Path directory) throws Exception {
        Path connectorJar = Synthetic.mongoPreviewTarget(directory);
        Path bsonJar = bsonJar();
        SourceResource target = new SourceResource(
                "mongo_target", null, "mongodb", Map.of(), null, null, null, null);
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> new ConnectorRef(
                List.of(connectorJar, bsonJar), "synthetic.MongoPreviewTarget", null, null));
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("_id", new ConvertedValue(OBJECT_ID, "_id"));
        document.put("created_at", new ConvertedValue(
                new io.tapdata.entity.schema.value.DateTime(Instant.parse("2026-10-02T00:00:00Z")), "date"));
        document.put("details", Map.of("amount", new BigDecimal("12.30")));
        document.put("nullable", null);

        PdkTargetPreviewRenderer.Result result = renderer.render(target, List.of(document), () -> { });

        assertThat(result.format()).isEqualTo(PdkTargetPreviewRenderer.MONGO_EXTENDED_JSON);
        assertThat(result.documents()).hasSize(1);
        assertThat(result.documents().getFirst().get("_id")).isEqualTo(Map.of("$oid", OBJECT_ID));
        assertThat(result.documents().getFirst().get("created_at")).isInstanceOf(Map.class)
                .satisfies(date -> assertThat(((Map<?, ?>) date).containsKey("$date")).isTrue());
        Map<?, ?> details = (Map<?, ?>) result.documents().getFirst().get("details");
        assertThat(details.get("amount")).isEqualTo(Map.of("$numberDecimal", "12.30"));
        assertThat(result.documents().getFirst()).containsEntry("nullable", null);
    }

    @Test
    void unresolvedNonMongoTargetsUseExplicitLogicalJson(@TempDir Path directory) {
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> {
            throw new AssertionError("non-Mongo target should not be opened");
        });
        SourceResource target = new SourceResource(
                "sql_target", null, "postgres", Map.of(), null, null, null, null);

        PdkTargetPreviewRenderer.Result result = renderer.render(
                target, List.of(Map.of("_id", new ConvertedValue(OBJECT_ID, "_id"))), () -> { });

        assertThat(result.format()).isEqualTo(PdkTargetPreviewRenderer.LOGICAL_JSON);
        assertThat(result.documents()).containsExactly(Map.of("_id", OBJECT_ID));
    }

    private static Path bsonJar() throws URISyntaxException {
        return Path.of(Document.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }
}
