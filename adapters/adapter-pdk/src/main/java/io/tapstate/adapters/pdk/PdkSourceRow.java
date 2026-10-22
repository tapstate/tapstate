package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.value.TapValue;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Per-image provenance survives the PDK's in-place conversion of custom containers. */
final class PdkSourceRow extends LinkedHashMap<String, Object> {
    private static final long serialVersionUID = 1L;
    private transient Map<TapValue<?, ?>, Object> originals;

    PdkSourceRow(Map<String, Object> row) {
        super(row);
    }

    void converted(TapValue<?, ?> value, Object original) {
        if (originals == null) originals = new IdentityHashMap<>();
        originals.put(value, original);
    }

    boolean wasConverted(TapValue<?, ?> value) {
        return originals != null && originals.containsKey(value);
    }

    Object original(TapValue<?, ?> value, Object fallback) {
        return wasConverted(value) ? originals.get(value) : fallback;
    }
}
