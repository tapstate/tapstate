package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** The stable discovered schema and its serialized type descriptions, prepared once per table. */
record PdkRowSchema(Map<String, TapField> fields, Map<String, String> declared,
        Map<TapType, PdkTypeDescription> descriptions) {
    static final PdkRowSchema EMPTY = new PdkRowSchema(Map.of(), Map.of(), Map.of());

    static PdkRowSchema of(TapTable table) {
        Map<String, TapField> fields = table == null || table.getNameFieldMap() == null
                ? Map.of() : table.getNameFieldMap();
        if (fields.isEmpty()) return EMPTY;
        Map<String, String> names = new LinkedHashMap<>();
        Map<TapType, PdkTypeDescription> descriptions = new IdentityHashMap<>();
        fields.forEach((name, field) -> {
            if (field != null) {
                if (field.getDataType() != null) names.put(name, field.getDataType());
                if (field.getTapType() != null) descriptions.computeIfAbsent(field.getTapType(), PdkTypeDescription::new).json();
            }
        });
        return new PdkRowSchema(fields, names, Collections.unmodifiableMap(descriptions));
    }

    static PdkRowSchema declared(Map<String, String> names) {
        Objects.requireNonNull(names, "columnTypes");
        if (names.isEmpty()) return EMPTY;
        Map<String, TapField> fields = new LinkedHashMap<>();
        names.forEach((name, type) -> fields.put(name, new TapField(name, type)));
        return new PdkRowSchema(fields, names, Map.of());
    }

    String description(TapType type) {
        if (type == null) return null;
        PdkTypeDescription cached = descriptions.get(type);
        // A codec can supply a different type for one value. Never replace it with column metadata.
        return cached == null ? PdkTypeDescription.describe(type) : cached.json();
    }

}
