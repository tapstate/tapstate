package io.tapstate.e2e.benchmark;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** The MongoDB source workload resolved from one versioned catalog case. */
public final class WorkloadRecipe {
    static final long KEY_LANE = 1L << 40;

    public record Mix(int insert, int update, int delete) {
        public Mix {
            if (insert < 0 || update < 0 || delete < 0 || (long) insert + update + delete != 100) {
                throw new IllegalArgumentException("I/U/D percentages must be nonnegative and sum to 100");
            }
        }
    }

    enum Changes { NONE, BASIC, SPECIAL, STEADY }

    record Input(String name, int rows, int share, long firstKey, boolean receivesChanges,
                 Map<String, Kind> required, Set<String> protectedFields,
                 String array, int elements, String elementKey, String elementOrder, String elementValue,
                 String relationField, String reference, boolean nested, int fanout, int matchPercent) {
    }

    enum Kind {
        STRING("s"), INTEGER("i"), DECIMAL("d"), BOOLEAN("b"), DATETIME("t"), ARRAY("a");

        final String prefix;

        Kind(String prefix) {
            this.prefix = prefix;
        }
    }

    record Scalar(int fields, int rowBytes, int tolerance, int nullPercent,
                  Map<Kind, Integer> fieldCounts, int stringMinPercent, int stringMaxPercent,
                  int minInt, int maxInt, long minCents, long maxCents, int decimalScale,
                  int truePercent, long dateStart, long dateEnd) {
    }

    public record Identity(String caseId, int caseVersion, String definitionSha256, int contractVersion,
                           int recipeVersion, int specificationVersion, String profile, int profileVersion, long seed) {
    }

    final Identity identity;
    final Scalar scalar;
    final List<Input> inputs;
    final Changes changeMode;
    final Mix mix;
    final int updatePercent;
    final boolean preImages;
    final boolean allowMixOverride;

    private WorkloadRecipe(Identity identity, Scalar scalar, List<Input> inputs, Changes changeMode,
                           Mix mix, int updatePercent, boolean preImages, boolean allowMixOverride) {
        this.identity = identity;
        this.scalar = scalar;
        this.inputs = List.copyOf(inputs);
        this.changeMode = changeMode;
        this.mix = mix;
        this.updatePercent = updatePercent;
        this.preImages = preImages;
        this.allowMixOverride = allowMixOverride;
    }

    /**
     * The caller resolves stage-specific row counts and, for the paired Nest scenario, the arm's fanout.
     * A null fanout uses the scenario's configured baseline.
     */
    public static WorkloadRecipe fromCatalog(BsonDocument selected, Map<String, Integer> rows, Integer fanout) {
        if (!"mongodb".equals(selected.getString("source").getValue())) {
            throw new IllegalArgumentException("The workload generator needs a MongoDB source case");
        }
        var definition = selected.getDocument("workload_spec");
        var spec = definition.getDocument("scenario");
        var profile = definition.getDocument("profile");
        var generation = definition.getDocument("generation");
        var identity = new Identity(text(selected, "id"), number(selected, "case_version"),
                text(selected, "definition_sha256"), number(definition, "contract_version"),
                number(generation, "version"), number(spec, "version"), text(selected, "profile"),
                number(profile, "version"), generation.getNumber("seed").longValue());
        if (identity.recipeVersion() != 1) {
            throw new IllegalArgumentException("Unsupported MongoDB recipe version " + identity.recipeVersion());
        }
        var counts = new java.util.EnumMap<Kind, Integer>(Kind.class);
        int fields = number(profile, "fields");
        generation.getDocument("scalar_fields_percent").forEach((kind, value) ->
                counts.put(kind(kind), fields * value.asNumber().intValue() / 100));
        var strings = generation.getDocument("strings").getArray("uniform_length_percent");
        var integers = generation.getDocument("integers").getArray("uniform_inclusive");
        var decimal = generation.getDocument("decimals");
        var cents = decimal.getArray("uniform_inclusive_cents");
        var dates = generation.getDocument("datetime_utc");
        var scalar = new Scalar(fields, number(profile, "logical_row_bytes"),
                number(generation, "logical_row_size_tolerance_percent"),
                number(generation, "nullable_field_values_percent"), Map.copyOf(counts),
                strings.get(0).asNumber().intValue(), strings.get(1).asNumber().intValue(),
                integers.get(0).asNumber().intValue(), integers.get(1).asNumber().intValue(),
                cents.get(0).asNumber().longValue(), cents.get(1).asNumber().longValue(), number(decimal, "scale"),
                number(generation, "boolean_true_percent"),
                Instant.parse(text(dates, "start_inclusive")).toEpochMilli(),
                Instant.parse(text(dates, "end_exclusive")).toEpochMilli());

        var parameters = spec.getDocument("parameters");
        var tables = spec.getDocument("inputs");
        if (!rows.keySet().equals(tables.keySet()) || rows.values().stream().anyMatch(value -> value < 1)) {
            throw new IllegalArgumentException("Provide positive row counts for exactly " + tables.keySet());
        }
        List<Input> inputs = new ArrayList<>();
        for (String name : tables.keySet().stream().sorted().toList()) {
            var table = tables.getDocument(name);
            String join = optional(table, "join_key");
            String foreign = optional(table, "foreign_key");
            String reference = optional(table, "reference_input");
            if (reference == null) {
                reference = join != null ? "dimensions" : foreign != null ? "roots" : null;
            }
            if (reference != null && !tables.containsKey(reference)) {
                throw new IllegalArgumentException("Unknown referenced collection " + reference);
            }
            int ratio = reference != null && foreign != null
                    ? tables.getDocument(reference).getInt32("children_per_root").getValue() : 1;
            if (foreign != null && fanout != null) {
                ratio = fanout;
            }
            if (foreign != null && (ratio < 1 || (long) rows.get(reference) * ratio != rows.get(name))) {
                throw new IllegalArgumentException("Initial child count must equal root count times fanout");
            }
            Map<String, Kind> required = new java.util.LinkedHashMap<>();
            var requiredFields = table.getDocument("required_fields", new BsonDocument());
            requiredFields.forEach((field, type) -> required.put(mongoField(field),
                    kind(type.asString().getValue().split(" ", 2)[0])));
            Set<String> protectedFields = new java.util.LinkedHashSet<>();
            var protectedFieldsSpec = parameters.getArray("protected_fields", new org.bson.BsonArray());
            protectedFieldsSpec.forEach(value -> protectedFields.add(mongoField(value.asString().getValue())));
            inputs.add(new Input(name, rows.get(name), table.getInt32("share_percent", new org.bson.BsonInt32(100)).getValue(),
                    1 + inputs.size() * KEY_LANE, table.getBoolean("receives_changes").getValue(),
                    Map.copyOf(required), Set.copyOf(protectedFields),
                    optional(parameters, "array_field"), table.getInt32("element_count", new org.bson.BsonInt32(0)).getValue(),
                    optional(table, "element_key"), optional(table, "order_field"),
                    table.getString("element_value", new org.bson.BsonString("value")).getValue(),
                    join != null ? join : foreign, reference, foreign != null, ratio, parameters.getInt32("match_percent", new org.bson.BsonInt32(100)).getValue()));
        }

        var incremental = definition.getDocument("incremental", null);
        if (incremental == null) {
            return new WorkloadRecipe(identity, scalar, inputs, Changes.NONE, null, 0, false, false);
        }
        var source = incremental.getDocument("mongo_source");
        if (!source.getBoolean("pre_images").getValue() || !"majority".equals(text(source, "write_concern"))) {
            throw new IllegalArgumentException("CDC source recipes require pre-images and majority acknowledgement");
        }
        int updatePercent = number(incremental, "update_mutable_fields_percent");
        if (updatePercent < 1 || updatePercent > 100) {
            throw new IllegalArgumentException("Update field percentage must be within 1-100");
        }
        var percentages = incremental.getDocument("mix_percent", null);
        if (percentages != null) {
            return new WorkloadRecipe(identity, scalar, inputs, Changes.BASIC,
                    new Mix(number(percentages, "insert"), number(percentages, "update"), number(percentages, "delete")),
                    updatePercent, true, incremental.getBoolean("allow_manual_mix_override").getValue());
        }
        List<String> operations = incremental.getArray("operations").stream().map(BsonValue::asString)
                .map(org.bson.BsonString::getValue).toList();
        if (operations.equals(List.of("update existing keys only"))) {
            return new WorkloadRecipe(identity, scalar, inputs, Changes.STEADY, new Mix(0, 100, 0), updatePercent, true, false);
        }
        if (operations.equals(List.of("update", "delete"))) {
            return new WorkloadRecipe(identity, scalar, inputs, Changes.SPECIAL, null, updatePercent, true, false);
        }
        throw new IllegalArgumentException("Unknown incremental operations " + operations);
    }

    private static Kind kind(String name) {
        return Kind.valueOf(name.toUpperCase(java.util.Locale.ROOT));
    }

    private static String mongoField(String name) {
        return name.equals("id") ? "_id" : name;
    }

    static int number(BsonDocument object, String key) {
        return object.getNumber(key).intValue();
    }

    static String text(BsonDocument object, String key) {
        return object.getString(key).getValue();
    }

    private static String optional(BsonDocument object, String key) {
        return object.containsKey(key) ? text(object, key) : null;
    }

    public Identity identity() {
        return identity;
    }
}
