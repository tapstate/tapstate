package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.schema.TapField;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceSchemaChange;
import io.tapstate.spi.store.SourceTable;
import java.util.*;

/** Portable instructions derived from the same ordered event as the source model receipt. */
final class PdkDdlCommand {
    static final String KEY = "tapstateDdl";
    private PdkDdlCommand() { }

    static Map<String,Object> encode(TapDDLEvent event, SourceSchemaChange model) {
        var result = new LinkedHashMap<String,Object>();
        result.put("format", 1);
        if (event instanceof TapNewFieldEvent added) {
            result.put("action", "add");
            var fields = new ArrayList<Map<String,Object>>();
            for (var field : added.getNewFields()) {
                var source = model.after().fields().stream().filter(value -> value.name().equals(field.getName())).findFirst().orElseThrow();
                var encoded = new LinkedHashMap<String,Object>();
                encoded.put("name", field.getName()); encoded.put("kind", source.type().name());
                encoded.put("primary", model.after().primaryKey().contains(field.getName()));
                if (source.numericType() != null) {
                    if (source.numericType().bit() != null) encoded.put("bit", source.numericType().bit());
                    if (source.numericType().scale() != null) encoded.put("scale", source.numericType().scale());
                }
                if (source.stringType() != null) encoded.put("fixedText", Boolean.TRUE.equals(source.stringType().fixed()));
                literal(encoded, "default", field.getDefaultValue());
                literal(encoded, "function", field.getDefaultFunction());
                fields.add(Collections.unmodifiableMap(encoded));
            }
            result.put("fields", List.copyOf(fields));
        } else if (event instanceof TapAlterFieldNameEvent renamed) {
            result.put("action", "rename");
            result.put("before", renamed.getNameChange().getBefore()); result.put("after", renamed.getNameChange().getAfter());
            result.put("primary", model.before().primaryKey().contains(renamed.getNameChange().getBefore()));
        } else if (event instanceof TapDropFieldEvent dropped) {
            result.put("action", "drop"); result.put("field", dropped.getFieldName());
            result.put("primary", model.before().primaryKey().contains(dropped.getFieldName()));
        } else if (event instanceof TapAlterFieldAttributesEvent altered) {
            result.put("action", "attributes"); result.put("field", altered.getFieldName());
            result.put("primary", !model.before().primaryKey().equals(model.after().primaryKey())
                    || altered.getDataTypeChange() != null && model.before().primaryKey().contains(altered.getFieldName()));
            result.put("typeChanged", altered.getDataTypeChange() != null);
        } else if (event instanceof TapRenameTableEvent
                && io.tapstate.spi.store.SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(model.before(), model.after())
                && io.tapstate.spi.store.SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(model.after(), model.before())) {
            // Physical source aliases do not change the selected logical stream or authored target.
            result.put("action", "physical-table-rename");
        } else result.put("action", "unsupported");
        return Collections.unmodifiableMap(result);
    }

    private static void literal(Map<String,Object> into, String key, Object value) {
        if (value == null) return;
        if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean || value instanceof Character)
            into.put(key, value.toString());
        else into.put(key + "Unsupported", value.getClass().getName());
    }
}
