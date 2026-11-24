package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.TargetTable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Idempotent Mongo document mutations, over the original target connector and target table. */
final class PdkMongoDdl {
    private static final Set<String> MONGO = Set.of("mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb");
    record Mutation(Map<String,Object> filter, Map<String,Object> update) { }
    record Plan(String table, String action, String field, String after, List<Mutation> mutations) { }
    private PdkMongoDdl() { }

    static Plan prepare(String connectorId, Envelope event, TargetTable target, boolean sourceColumns) {
        if (target == null) throw unsupported("DDL has no resolved target table for " + event.src());
        Object value = event.schema() == null ? null : event.schema().get(PdkDdlCommand.KEY);
        if (!(value instanceof Map<?,?> ddl) || !(ddl.get("format") instanceof Number version) || version.intValue() != 1)
            throw unsupported("DDL apply requires a typed captured event for " + target.name() + "; legacy origin SQL is not executed");
        String action = String.valueOf(ddl.get("action"));
        if (action.equals("physical-table-rename")) return new Plan(target.name(), action, null, null, List.of());
        if (!MONGO.contains(connectorId)) throw unsupported("DDL apply is not implemented for target connector " + connectorId);
        if (Boolean.TRUE.equals(ddl.get("primary"))) throw unsupported("primary-key evolution requires explicit target preparation for " + target.name());
        if (!action.equals("attributes") && !sourceColumns)
            throw unsupported("column DDL cannot be applied through a view or transformed row shape for " + target.name());
        var mutations = new ArrayList<Mutation>();
        String field = null, after = null;
        switch (action) {
            case "rename" -> {
                field = field(ddl.get("before")); after = field(ddl.get("after"));
                mutations.add(new Mutation(exists(field, true), Map.of("$rename", Map.of(field, after))));
            }
            case "drop" -> {
                field = field(ddl.get("field"));
                mutations.add(new Mutation(exists(field, true), Map.of("$unset", Map.of(field, ""))));
            }
            case "add" -> {
                if (!(ddl.get("fields") instanceof List<?> fields) || fields.isEmpty()) throw unsupported("ADD has no typed fields");
                for (var raw : fields) {
                    if (!(raw instanceof Map<?,?> column)) throw unsupported("ADD has an invalid field");
                    String name = field(column.get("name"));
                    if (Boolean.TRUE.equals(column.get("primary"))) throw unsupported("adding a primary-key field requires explicit target preparation");
                    var set = new LinkedHashMap<String,Object>(); set.put(name, defaultValue(column));
                    mutations.add(new Mutation(exists(name, false), Map.of("$set", set)));
                }
            }
            case "attributes" -> field = field(ddl.get("field")); // Mongo has no SQL type/nullability column to alter.
            default -> throw unsupported("unsupported typed DDL action for " + target.name() + ": " + action);
        }
        return new Plan(target.name(), action, field, after, List.copyOf(mutations));
    }

    static void apply(PdkConnector connector, Plan plan) throws Throwable {
        for (var mutation : plan.mutations()) {
            var execute = connector.functions().getExecuteCommandFunction();
            if (execute == null) throw unsupported("target DDL mutation function is unavailable for " + plan.table());
            var params = new LinkedHashMap<String,Object>();
            params.put("op", "update"); params.put("collection", plan.table());
            params.put("multi", true); params.put("upsert", false);
            params.put("filter", mutable(mutation.filter())); params.put("opObject", mutable(mutation.update()));
            var answered = new AtomicBoolean(); var failure = new AtomicReference<Throwable>();
            connector.requireActive();
            execute.execute(connector.context(), TapExecuteCommand.create().command("execute").params(params), result -> {
                if (result == null) return;
                if (result.getError() != null) failure.set(result.getError());
                else if (result.getResult() instanceof Number count && count.longValue() >= 0) answered.set(true);
                else failure.set(new IllegalStateException("target DDL mutation returned an invalid acknowledgement"));
            });
            connector.requireActive();
            if (failure.get() != null) throw failure.get();
            if (!answered.get()) throw new IllegalStateException("target DDL mutation returned no acknowledgement");
        }
    }

    static Object defaultValue(Map<?,?> field) {
        if (field.containsKey("defaultUnsupported") || field.containsKey("functionUnsupported"))
            throw unsupported("ADD default is not a portable literal for " + field.get("name"));
        boolean function = !field.containsKey("default") && field.containsKey("function");
        Object raw = field.containsKey("default") ? field.get("default") : field.get("function");
        if (raw == null) return null;
        String text = raw.toString().trim(), kind = String.valueOf(field.get("kind"));
        if (kind.equals("STRING") && !function) {
            if (Boolean.TRUE.equals(field.get("fixedText"))) throw unsupported("fixed-width text defaults require a source-coerced literal");
            // defaultValue is already decoded; only defaultFunction contains SQL expression syntax.
            return raw.toString();
        }
        if (text.equalsIgnoreCase("NULL")) return null;
        try {
            if (kind.equals("STRING")) {
                if (Boolean.TRUE.equals(field.get("fixedText"))) throw unsupported("fixed-width text defaults require a source-coerced literal");
                if (text.length() >= 2 && text.startsWith("'") && text.endsWith("'")) return text.substring(1, text.length()-1).replace("''", "'");
                if (!function) return raw.toString();
            } else if (kind.equals("INT64")) {
                // The event boundary promotes every source integer to Long, including SMALLINT/INTEGER.
                return new BigDecimal(text).longValueExact();
            } else if (kind.equals("DECIMAL")) {
                var number = new BigDecimal(text);
                return field.get("scale") instanceof Number scale ? number.setScale(scale.intValue(), RoundingMode.UNNECESSARY) : number;
            } else if (kind.equals("DOUBLE")) {
                double number = Double.parseDouble(text);
                if (Double.isFinite(number)) return number;
            } else if (kind.equals("BOOLEAN")) {
                if (text.equalsIgnoreCase("true") || text.equals("1")) return true;
                if (text.equalsIgnoreCase("false") || text.equals("0")) return false;
            }
        } catch (NumberFormatException | ArithmeticException invalid) {
            throw unsupported("ADD default cannot be represented exactly for " + field.get("name"));
        }
        throw unsupported("ADD default is not a supported literal for " + field.get("name"));
    }

    private static String field(Object raw) {
        if (!(raw instanceof String name) || name.isBlank() || name.equals("_id") || name.startsWith("$")
                || name.indexOf('.') >= 0 || name.indexOf('\0') >= 0) throw unsupported("target DDL requires a literal non-identity field name");
        return name;
    }
    private static Map<String,Object> exists(String field, boolean value) { return Map.of(field, Map.of("$exists", value)); }
    private static Object mutable(Object value) {
        if (value instanceof Map<?,?> map) {
            var copy = new LinkedHashMap<Object,Object>(); map.forEach((key, item) -> copy.put(key, mutable(item))); return copy;
        }
        if (value instanceof List<?> list) { var copy = new ArrayList<Object>(); list.forEach(item -> copy.add(mutable(item))); return copy; }
        return value;
    }
    private static UnsupportedOperationException unsupported(String reason) { return new UnsupportedOperationException(reason); }
}
