package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ValidationTaskStore;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import static org.assertj.core.api.Assertions.*;

class MongoValidationTaskCodecTest {
    @Test void countRoundTripsWithoutKeysModelsOrCodecsAndKeepsNativeIntegerWidths() {
        var spec = spec(false); var settings = ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT, 1_000);
        var encoded = MongoValidationTaskCodec.definition(spec, settings); var binary = new RawBsonDocument(encoded, new DocumentCodec());
        assertThat(binary.getDocument("full").getInt32("historyLimit").getValue()).isEqualTo(10);
        assertThat(binary.getDocument("incremental").getInt64("firstCheckDelayMillis").getValue()).isEqualTo(1_000);
        assertThat(binary.getDocument("comparison").getArray("mappings").getFirst().asDocument().get("keys").isNull()).isTrue();
        assertThat(MongoValidationTaskCodec.definition(binary.decode(new DocumentCodec())))
                .isEqualTo(new MongoValidationTaskCodec.Definition(spec, settings));
    }

    @Test void confirmedPhysicalKeysAndIndependentSideBindingsRoundTripExactly() {
        var spec = spec(true); var settings = ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN, 5_000);
        var encoded = copy(MongoValidationTaskCodec.definition(spec, settings));
        var decoded = MongoValidationTaskCodec.definition(encoded);
        assertThat(decoded.comparison()).isEqualTo(spec);
        assertThat(decoded.comparison().mappings().getFirst().source().codecBinding()).isEqualTo("source-codec");
        assertThat(decoded.comparison().mappings().getFirst().target().codecBinding()).isEqualTo("target-codec");
        assertThat(MongoValidationTaskCodec.definition(decoded.comparison(), decoded.settings())).isEqualTo(encoded);
    }

    @Test void literalResourceNamesRemainValuesRatherThanBsonFieldPaths() {
        var spec = new ValidationTaskSpec("flow.name", Map.of("flow.name", "a".repeat(64), "source.with.dots", "b".repeat(64), "target$literal", "c".repeat(64)),
                1, "rules-v1", List.of(new ValidationTaskSpec.Mapping("map", ValidationTaskSpec.OutputKind.SYNC, "output",
                        new ValidationTaskSpec.Table("input", "source.with.dots", "db", "", "source.table", null, null),
                        new ValidationTaskSpec.Table("output", "target$literal", "db", null, "target.table", null, null), null)));
        var encoded = MongoValidationTaskCodec.comparison(spec);
        assertThat(encoded.get("dependencies")).isInstanceOf(List.class);
        assertThat(MongoValidationTaskCodec.comparison(copy(encoded))).isEqualTo(spec);
        assertThat(MongoValidationTaskCodec.comparison(copy(encoded)).mappings().getFirst().source().schema()).isEmpty();
        assertThat(MongoValidationTaskCodec.comparison(copy(encoded)).mappings().getFirst().target().schema()).isNull();
    }

    @Test void schedulesKeepTheOriginalAnchorPrecisionAndExplicitRulesVersion() {
        var spec = spec(false); var anchor = Instant.parse("2026-10-28T00:00:00.000000001Z");
        for (var schedule : List.of(new ValidationSchedule.Interval(2, ValidationSchedule.Unit.MINUTES, anchor, "UTC"),
                new ValidationSchedule.Cron("0 9 * * 1-5", "Asia/Shanghai"))) {
            var settings = new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT, 1, schedule, true, 10),
                    new ValidationTaskStore.Incremental(1_000, 10));
            var encoded = MongoValidationTaskCodec.definition(spec, settings);
            assertThat(encoded.get("full", Document.class).get("schedule", Document.class)).containsEntry("rulesVersion", ValidationSchedule.RULES_VERSION);
            assertThat(MongoValidationTaskCodec.definition(copy(encoded)).settings()).isEqualTo(settings);
            var incompatible = copy(encoded); incompatible.get("full", Document.class).get("schedule", Document.class).put("rulesVersion", "unknown");
            unreadable(() -> MongoValidationTaskCodec.definition(incompatible));
        }
    }

    @Test void missingExtraWrongWidthAndUnknownVersionFactsNeverDecodeAsDefaults() {
        var original = MongoValidationTaskCodec.definition(spec(true), ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN, 1_000));
        List<Consumer<Document>> corruptions = List.of(
                row -> row.remove("comparison"), row -> row.put("unknown", true), row -> row.put("formatVersion", 2),
                row -> row.get("full", Document.class).put("historyLimit", 10L),
                row -> row.get("incremental", Document.class).put("firstCheckDelayMillis", 1_000),
                row -> row.get("incremental", Document.class).remove("historyLimit"),
                row -> row.get("comparison", Document.class).put("mappingRevision", 1.0),
                row -> row.get("comparison", Document.class).put("fingerprint", "0".repeat(64)),
                row -> row.get("comparison", Document.class).getList("dependencies", Document.class).add(new Document("id", "source").append("hash", "b".repeat(64))),
                row -> row.get("comparison", Document.class).getList("mappings", Document.class).getFirst().get("keys", Document.class)
                        .getList("source", Document.class).getFirst().put("logicalTypes", List.of("integer", "integer")),
                row -> row.get("comparison", Document.class).getList("mappings", Document.class).getFirst().get("source", Document.class).put("unknown", "value"));
        for (var corruption : corruptions) {
            var changed = copy(original); corruption.accept(changed); unreadable(() -> MongoValidationTaskCodec.definition(changed));
        }
    }

    @Test void runSnapshotCannotChangeMethodOrModeBehindItsOriginalFingerprint() {
        var original = new ValidationRunSpec(spec(true), ValidationMode.FULL, ValidationMethod.UNIQUE_KEY);
        var row = MongoValidationTaskCodec.run(original);
        assertThat(MongoValidationTaskCodec.run(copy(row))).isEqualTo(original);
        var changedMethod = copy(row); changedMethod.put("method", "FULL_COLUMN"); unreadable(() -> MongoValidationTaskCodec.run(changedMethod));
        var changedMode = copy(row); changedMode.put("mode", "INCREMENTAL"); unreadable(() -> MongoValidationTaskCodec.run(changedMode));
    }

    private static void unreadable(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static Document copy(Document row) { return new RawBsonDocument(row, new DocumentCodec()).decode(new DocumentCodec()); }
    private static ValidationTaskSpec spec(boolean keys) {
        var source = new ValidationTaskSpec.Table("input", "source", "db", null, "orders", keys ? "source-model" : null, keys ? "source-codec" : null);
        var target = new ValidationTaskSpec.Table("output", "views", "db", null, "renamed", keys ? "target-model" : null, keys ? "target-codec" : null);
        var key = new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("integer", "decimal"));
        return new ValidationTaskSpec("flow", Map.of("flow", "a".repeat(64), "source", "b".repeat(64), "views", "c".repeat(64)), 1, "rules-v1",
                List.of(new ValidationTaskSpec.Mapping("map", ValidationTaskSpec.OutputKind.SYNC, "output", source, target,
                        keys ? new ValidationTaskSpec.Keys("confirmed", List.of(key), List.of(key)) : null)));
    }
}
