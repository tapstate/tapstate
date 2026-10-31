package io.tapstate.core.validation;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Immutable comparison inputs. A stored hash or this descriptor alone is never admission authority. */
public record ValidationTaskSpec(String pipelineId, Map<String, String> dependencyHashes, long mappingRevision,
                                 String comparisonRulesVersion, List<Mapping> mappings) {
    public static final int MAX_MAPPINGS = 1024;
    public static final long MAX_METADATA_BYTES = 1024 * 1024;
    public enum OutputKind { VIEW, SYNC }

    /** Resource references and actual physical names only; never credentials, driver objects or row values. */
    public record Table(String nodeId, String sourceId, String database, String schema, String table,
                        String modelVersion, String codecBinding) {
        public Table {
            nodeId = id(nodeId, "spec.node"); sourceId = id(sourceId, "spec.source"); table = id(table, "spec.table");
            if (database != null) ValidationError.fieldName(database, "spec.database");
            if (schema != null) ValidationError.fieldName(schema, "spec.schema");
            if (modelVersion != null) modelVersion = id(modelVersion, "spec.model");
            if (codecBinding != null) codecBinding = id(codecBinding, "spec.codec");
        }
    }

    /** Ordered user-confirmed fields; independent model and codec bindings belong to each physical side. */
    public record Keys(String confirmationId, List<ValidationKeyBinding.Component> source,
                       List<ValidationKeyBinding.Component> target) {
        public Keys {
            confirmationId = id(confirmationId, "spec.confirmation");
            source = keyFields(source); target = keyFields(target);
            if (source.size() != target.size()) throw ValidationError.invalid("spec.keyArity");
            for (int i = 0; i < source.size(); i++) {
                if (source.get(i).domain() != target.get(i).domain() || source.get(i).numericPolicy() != target.get(i).numericPolicy())
                    throw ValidationError.invalid("spec.keyDomains");
            }
        }
        private static List<ValidationKeyBinding.Component> keyFields(List<ValidationKeyBinding.Component> fields) {
            if (fields == null || fields.isEmpty() || fields.size() > ValidationKey.MAX_COMPONENTS) throw ValidationError.invalid("spec.keyFields");
            var paths = new HashSet<List<String>>();
            for (var field : fields) if (!paths.add(Objects.requireNonNull(field).path())) throw ValidationError.invalid("spec.duplicateKeyField");
            return List.copyOf(fields);
        }
    }

    public record Mapping(String mappingId, OutputKind outputKind, String outputId, Table source, Table target, Keys keys) {
        public Mapping {
            mappingId = id(mappingId, "spec.mapping"); outputId = id(outputId, "spec.output");
            Objects.requireNonNull(outputKind); Objects.requireNonNull(source); Objects.requireNonNull(target);
        }
    }

    public ValidationTaskSpec {
        pipelineId = id(pipelineId, "spec.pipeline"); comparisonRulesVersion = id(comparisonRulesVersion, "spec.rules");
        if (mappingRevision < 1 || mappingRevision == Long.MAX_VALUE || dependencyHashes == null
                || dependencyHashes.isEmpty() || dependencyHashes.size() > 1024 || !dependencyHashes.containsKey(pipelineId)
                || mappings == null || mappings.isEmpty() || mappings.size() > MAX_MAPPINGS) throw ValidationError.invalid("spec.shape");
        long bytes = 256 + textBytes(pipelineId) + textBytes(comparisonRulesVersion);
        for (var entry : dependencyHashes.entrySet()) {
            id(entry.getKey(), "spec.dependency");
            if (entry.getValue() == null || !entry.getValue().matches("[a-f0-9]{64}")) throw ValidationError.invalid("spec.dependencyHash");
            bytes += 64 + textBytes(entry.getKey()) + textBytes(entry.getValue());
        }
        var ids = new HashSet<String>(); var physicalPairs = new HashSet<List<Object>>();
        for (var mapping : mappings) {
            Objects.requireNonNull(mapping);
            if (!ids.add(mapping.mappingId()) || !physicalPairs.add(List.of(mapping.outputKind(), mapping.outputId(),
                    mapping.source().sourceId(), tableIdentity(mapping.source()), mapping.target().sourceId(), tableIdentity(mapping.target()))))
                throw ValidationError.invalid("spec.duplicateMapping");
            if (!dependencyHashes.containsKey(mapping.source().sourceId()) || !dependencyHashes.containsKey(mapping.target().sourceId()))
                throw ValidationError.invalid("spec.unpinnedSource");
            bytes += 256 + textBytes(mapping.mappingId()) + textBytes(mapping.outputId()) + tableBytes(mapping.source()) + tableBytes(mapping.target());
            if (mapping.keys() != null) {
                bytes += textBytes(mapping.keys().confirmationId());
                for (var side : List.of(mapping.keys().source(), mapping.keys().target())) for (var field : side) {
                    bytes += 128;
                    for (var segment : field.path()) bytes += textBytes(segment);
                    for (var type : field.logicalTypes()) bytes += textBytes(type);
                }
            }
            if (bytes > MAX_METADATA_BYTES) throw limit();
        }
        if (bytes > MAX_METADATA_BYTES) throw limit();
        dependencyHashes = Collections.unmodifiableMap(new TreeMap<>(dependencyHashes)); mappings = List.copyOf(mappings);
    }

    /** COUNT needs an exact count-capability preflight, but never row keys, value models or codecs. */
    public void requireFor(ValidationMethod method, ValidationMode mode) {
        Objects.requireNonNull(method); Objects.requireNonNull(mode);
        if (!method.supports(mode)) throw ValidationError.invalid("spec.methodMode");
        if (method == ValidationMethod.COUNT) return;
        for (var mapping : mappings) {
            if (mapping.keys() == null || mapping.source().modelVersion() == null || mapping.target().modelVersion() == null
                    || mapping.source().codecBinding() == null || mapping.target().codecBinding() == null)
                throw ValidationError.invalid("spec.keyModelCodecBinding");
        }
    }

    /** Versioned, length-delimited SHA-256 over every immutable field, independent of map/set iteration order. */
    public String fingerprint() {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            try (var data = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
                text(data, "validation-task-spec-v1"); text(data, pipelineId); data.writeLong(mappingRevision); text(data, comparisonRulesVersion);
                data.writeInt(dependencyHashes.size());
                for (var entry : new TreeMap<>(dependencyHashes).entrySet()) { text(data, entry.getKey()); text(data, entry.getValue()); }
                data.writeInt(mappings.size());
                for (var mapping : mappings) {
                    text(data, mapping.mappingId()); text(data, mapping.outputKind().name()); text(data, mapping.outputId());
                    table(data, mapping.source()); table(data, mapping.target()); data.writeBoolean(mapping.keys() != null);
                    if (mapping.keys() != null) {
                        text(data, mapping.keys().confirmationId()); keys(data, mapping.keys().source()); keys(data, mapping.keys().target());
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException | IOException impossible) { throw new IllegalStateException("validation fingerprint could not be encoded", impossible); }
    }

    private static void table(DataOutputStream data, Table table) throws IOException {
        text(data, table.nodeId()); text(data, table.sourceId()); text(data, table.database()); text(data, table.schema());
        text(data, table.table()); text(data, table.modelVersion()); text(data, table.codecBinding());
    }
    private static void keys(DataOutputStream data, List<ValidationKeyBinding.Component> keys) throws IOException {
        data.writeInt(keys.size());
        for (var key : keys) {
            data.writeInt(key.path().size()); for (var field : key.path()) text(data, field);
            text(data, key.domain().name()); text(data, key.numericPolicy().name()); data.writeInt(key.logicalTypes().size());
            for (var type : key.logicalTypes().stream().sorted().toList()) text(data, type);
        }
    }
    private static void text(DataOutputStream data, String value) throws IOException {
        if (value == null) { data.writeInt(-1); return; }
        var bytes = value.getBytes(StandardCharsets.UTF_8); data.writeInt(bytes.length); data.write(bytes);
    }
    private static List<Object> tableIdentity(Table table) { return java.util.Arrays.asList(table.database(), table.schema(), table.table()); }
    private static long tableBytes(Table table) {
        return 128 + textBytes(table.nodeId()) + textBytes(table.sourceId()) + textBytes(table.database()) + textBytes(table.schema())
                + textBytes(table.table()) + textBytes(table.modelVersion()) + textBytes(table.codecBinding());
    }
    private static long textBytes(String value) { return value == null ? 8 : 64L + 2L * value.length(); }
    private static String id(String value, String field) { return ValidationError.identifier(value, field); }
    private static io.tapstate.core.common.TapstateException limit() {
        return new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation.specMetadata"), null);
    }
}
