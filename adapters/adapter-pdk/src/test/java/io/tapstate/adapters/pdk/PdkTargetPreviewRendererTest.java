package io.tapstate.adapters.pdk;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.RowValueError;
import io.tapstate.core.model.SourceResource;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URISyntaxException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.AbstractMap;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void unresolvedMongoTargetFallsBackToLogicalJson() {
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> {
            throw new io.tapstate.core.common.TapstateException(
                    ConnectorError.CLASS_NOT_FOUND, Map.of("connector", id, "class", "missing"), null);
        });
        SourceResource target = new SourceResource(
                "mongo_target", null, "mongodb", Map.of(), null, null, null, null);

        PdkTargetPreviewRenderer.Result result = renderer.render(
                target, List.of(Map.of("name", new ConvertedValue("orders", "string"))), () -> { });

        assertThat(result.format()).isEqualTo(PdkTargetPreviewRenderer.LOGICAL_JSON);
        assertThat(result.documents()).containsExactly(Map.of("name", "orders"));
    }

    @Test
    void logicalJsonNormalizesPortableValuesAndNestedContainers() {
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> {
            throw new AssertionError("logical rendering must not resolve a connector");
        });
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("wrapped", new ConvertedValue(List.of("a", 2), "array"));
        nested.put("date", new io.tapdata.entity.schema.value.DateTime(
                Instant.parse("2026-10-02T00:00:00Z")));
        nested.put("primitive_array", new int[] {3, 5});
        nested.put("object_array", new Object[] {"x", new ConvertedValue("y", "string")});
        nested.put("binary", new byte[] {1, 2});
        AtomicInteger checkpoints = new AtomicInteger();

        PdkTargetPreviewRenderer.Result result = renderer.render(
                null, List.of(nested, Map.of("empty", List.of())), checkpoints::incrementAndGet);

        assertThat(result.format()).isEqualTo(PdkTargetPreviewRenderer.LOGICAL_JSON);
        assertThat(checkpoints).hasValue(2);
        assertThat(result.documents()).hasSize(2);
        Map<String, Object> normalized = result.documents().getFirst();
        assertThat(normalized.get("wrapped")).isEqualTo(List.of("a", 2));
        assertThat(normalized.get("date")).isEqualTo("2026-10-02T00:00:00Z");
        assertThat(normalized.get("primitive_array")).isEqualTo(List.of(3, 5));
        assertThat(normalized.get("object_array")).isEqualTo(List.of("x", "y"));
        assertThat(((List<?>) normalized.get("binary")).stream()
                .map(Number.class::cast).map(Number::intValue).toList()).containsExactly(1, 2);
        assertThat(result.documents().get(1)).containsEntry("empty", List.of());
    }

    @Test
    void logicalJsonRejectsNonStringFieldsAndExcessiveNesting() {
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> {
            throw new AssertionError("logical rendering must not resolve a connector");
        });
        Map<Object, Object> invalid = new LinkedHashMap<>();
        invalid.put(1, "value");
        Map<String, Object> tooDeep = new LinkedHashMap<>();
        Map<String, Object> cursor = tooDeep;
        for (int depth = 0; depth < 130; depth++) {
            Map<String, Object> child = new LinkedHashMap<>();
            cursor.put("child", child);
            cursor = child;
        }

        assertThatThrownBy(() -> renderer.render(null, List.of(Map.of("nested", invalid)), () -> { }))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                    assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("path", "$/nested", "keyType", "java.lang.Integer"));
                });
        assertThatThrownBy(() -> renderer.render(null, List.of(tooDeep), () -> { }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting limit");
    }

    @Test
    void mongoPreviewRejectsTheSameKeyBeforeResolvingTheTarget() {
        AtomicInteger resolutions = new AtomicInteger();
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> {
            resolutions.incrementAndGet();
            throw new AssertionError("invalid rows must not open a target connector");
        });
        SourceResource target = new SourceResource(
                "mongo_target", null, "mongodb", Map.of(), null, null, null, null);
        assertThatThrownBy(() -> renderer.render(target,
                List.of(Map.of("dict", Map.of(7, "number-key", "7", "text-key"))), () -> { }))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                    assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("path", "$/dict", "keyType", "java.lang.Integer"));
                });
        assertThat(resolutions).hasValue(0);
    }

    @Test
    void rejectsExcessiveDepthBeforeVisitingItsContainer() {
        Map<String, Object> guarded = new AbstractMap<>() {
            @Override
            public Set<Entry<String, Object>> entrySet() {
                throw new AssertionError("preview traversed beyond its nesting limit");
            }
        };
        Map<String, Object> document = guarded;
        for (int index = 0; index < 129; index++) {
            document = Map.of("nested", document);
        }
        Map<String, Object> tooDeep = document;
        PdkTargetPreviewRenderer renderer = new PdkTargetPreviewRenderer(id -> {
            throw new AssertionError("over-depth rows must not open a connector");
        });
        SourceResource mongo = new SourceResource(
                "mongo_target", null, "mongodb", Map.of(), null, null, null, null);
        for (SourceResource target : new SourceResource[] {null, mongo}) {
            assertThatThrownBy(() -> renderer.render(target, List.of(tooDeep), () -> { }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nesting limit");
        }
    }

    private static Path bsonJar() throws URISyntaxException {
        return Path.of(Document.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }
}
