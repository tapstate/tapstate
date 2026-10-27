package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.bson.types.Binary;

/** Explicit portable observation encoding; no reflective serialization or database type coercion. */
final class MongoValidationObservationCodec {
    private MongoValidationObservationCodec() {}

    static Document encode(ValidationRootCheck check) {
        // Bound existing owned facts before allocating encoded copies. Exact BSON is checked afterward.
        if (check.accountedBytes() > 256 * 1024) throw limit();
        var checks = new Document(); check.checks().forEach((key, value) -> checks.put(key.name(), value.name()));
        var counts = new Document(); check.counts().forEach((key, value) -> counts.put(key.name(), value));
        return new Document("method", check.method().name()).append("participation", check.participation().name())
                .append("targetPresence", check.targetPresence().name()).append("checks", checks).append("counts", counts)
                .append("findings", findings(check))
                .append("notCompared", check.notCompared().stream().map(item -> new Document("check", item.check().name())
                        .append("location", location(item.location())).append("cause", finding(item.cause()))).toList())
                .append("notApplicableScopes", check.notApplicableScopes().stream().map(item -> new Document("check", item.check().name())
                        .append("location", location(item.location())).append("cause", finding(item.cause()))).toList())
                .append("reads", check.reads().stream().map(read -> new Document("side", read.side().name())
                        .append("readId", read.boundary().readId()).append("contractVersion", read.boundary().contractVersion())
                        .append("startedAt", instant(read.boundary().startedAt())).append("finishedAt", instant(read.boundary().finishedAt()))).toList())
                .append("samples", check.samples().stream().map(sample -> new Document("finding", finding(sample.finding()))
                        .append("source", sample.source().map(MongoValidationObservationCodec::sample).orElse(null))
                        .append("target", sample.target().map(MongoValidationObservationCodec::sample).orElse(null))).toList())
                .append("samplesTruncated", check.samplesTruncated()).append("confirmation", "LOCAL_OBSERVATION");
    }

    private static List<Document> findings(ValidationRootCheck check) {
        var ordered = new TreeMap<String, Document>();
        for (var entry : check.findings().entrySet()) {
            var encoded = finding(entry.getKey()).append("count", entry.getValue());
            if (ordered.putIfAbsent(encoded.toJson(), encoded) != null) throw invalid();
        }
        return List.copyOf(ordered.values());
    }

    static Document root(ValidationRoot root) {
        return new Document("mappingId", root.mappingId()).append("outputId", root.outputId()).append("key", key(root.rootKey()))
                .append("ancestors", root.ancestors().stream().map(ancestor -> new Document("fieldPath", ancestor.fieldPath()).append("key", key(ancestor.key()))).toList());
    }
    private static List<Document> key(ValidationKey key) {
        return key.parts().stream().map(part -> new Document("kind", part.kind().name()).append("binarySubtype", part.binarySubtype())
                .append("value", part.value() instanceof byte[] bytes ? new Binary(bytes) : value(part.value(), 0))).toList();
    }
    private static Document location(ValidationRootCheck.Location location) {
        return new Document("scope", root(location.scope())).append("fieldPath", location.fieldPath());
    }
    private static Document finding(ValidationRootCheck.Finding finding) {
        return new Document("kind", finding.kind().name()).append("location", location(finding.location()))
                .append("multiplicity", finding.multiplicity().map(value -> new Document("occurrences", value.occurrences()).append("exact", value.exact())).orElse(null));
    }
    private static Document sample(ValidationRootCheck.SampledSide side) {
        var identity = side.identity();
        return new Document("identity", new Document("sourceId", identity.sourceId()).append("database", identity.database())
                .append("schema", identity.schema()).append("table", identity.table()).append("keyFields", identity.keyFields())
                .append("actualKey", key(identity.actualKey())))
                .append("readId", side.readId()).append("row", value(side.row().fields(), 0));
    }
    static Document value(Object value, int depth) {
        if (depth > 64) throw limit();
        if (value == null) return new Document("kind", "NULL");
        if (value instanceof ValidationValues.Typed typed) return new Document("kind", "TYPED").append("logicalType", typed.logicalType())
                .append("nativeType", typed.nativeType()).append("codecBinding", typed.codecBinding()).append("conversionOrigin", typed.conversionOrigin())
                .append("value", value(typed.value(), depth + 1));
        if (value instanceof ValidationValues.Binary binary) return new Document("kind", "BINARY").append("subtype", binary.subtype()).append("value", new Binary(binary.bytes()));
        if (value instanceof BigDecimal number) return new Document("kind", "DECIMAL").append("unscaled", number.unscaledValue().toString()).append("scale", number.scale());
        if (value instanceof BigInteger number) return new Document("kind", "INTEGER").append("value", number.toString());
        if (value instanceof Byte number) return scalar("BYTE", number.intValue());
        if (value instanceof Short number) return scalar("SHORT", number.intValue());
        if (value instanceof Integer number) return scalar("INT", number);
        if (value instanceof Long number) return scalar("LONG", number);
        if (value instanceof Float number) return scalar("FLOAT_BITS", Float.floatToRawIntBits(number));
        if (value instanceof Double number) return scalar("DOUBLE_BITS", Double.doubleToRawLongBits(number));
        if (value instanceof String text) return scalar("TEXT", text(text));
        if (value instanceof Boolean flag) return scalar("BOOLEAN", flag);
        if (value instanceof Instant time) return new Document("kind", "INSTANT").append("value", instant(time));
        if (value instanceof LocalDate date) return scalar("DATE", date.toString());
        if (value instanceof LocalTime time) return scalar("TIME", time.toString());
        if (value instanceof LocalDateTime time) return scalar("LOCAL_DATE_TIME", time.toString());
        if (value instanceof Map<?, ?> object) {
            var entries = new ArrayList<Document>();
            var names = new TreeSet<String>();
            for (var name : object.keySet()) {
                if (!(name instanceof String field)) throw invalid(); names.add(text(field));
            }
            // User field names stay values, so dots, dollar signs and missing/null cannot become Mongo paths.
            for (String name : names) entries.add(new Document("name", name).append("value", value(object.get(name), depth + 1)));
            return new Document("kind", "OBJECT").append("entries", entries);
        }
        if (value instanceof List<?> list) return new Document("kind", "ARRAY").append("values", list.stream().map(item -> value(item, depth + 1)).toList());
        throw invalid();
    }
    private static String text(String value) {
        for (int i = 0; i < value.length(); i++) {
            char unit = value.charAt(i);
            if (Character.isHighSurrogate(unit)) {
                if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) throw invalidText();
            } else if (Character.isLowSurrogate(unit)) throw invalidText();
        }
        return value;
    }
    private static TapstateException invalidText() {
        return new TapstateException(ValidationError.VALUE_TYPE_UNSUPPORTED, Map.of("type", "malformed-utf16"), null);
    }
    private static Document scalar(String kind, Object value) { return new Document("kind", kind).append("value", value); }
    private static Document instant(Instant time) { return new Document("seconds", time.getEpochSecond()).append("nanos", time.getNano()); }
    private static TapstateException invalid() { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "stored-observation"), null); }
    static TapstateException limit() { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "result-bytes"), null); }
}
