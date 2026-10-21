package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import java.util.LinkedHashMap;
import java.util.Map;

/** The complete discovered fields and their declared names, prepared once per source table. */
record PdkRowSchema(Map<String, TapField> fields, Map<String, String> declared) {
    static PdkRowSchema of(TapTable table) {
        Map<String, TapField> fields = table == null || table.getNameFieldMap() == null
                ? Map.of() : table.getNameFieldMap();
        Map<String, String> names = new LinkedHashMap<>();
        fields.forEach((name, field) -> {
            if (field != null && field.getDataType() != null) {
                names.put(name, field.getDataType());
            }
        });
        return new PdkRowSchema(fields, names);
    }

    static PdkRowSchema declared(Map<String, String> names) {
        Map<String, TapField> fields = new LinkedHashMap<>();
        names.forEach((name, type) -> fields.put(name, new TapField(name, type)));
        return new PdkRowSchema(fields, names);
    }
}
