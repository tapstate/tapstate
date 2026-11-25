package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.value.TapValue;
import java.util.IdentityHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-image provenance survives the PDK's in-place conversion of custom containers.
 * Closing restores temporary host writes and releases bookkeeping; the portable row stays readable.
 */
final class PdkSourceRow extends LinkedHashMap<String, Object> implements AutoCloseable {
    private static final long serialVersionUID = 1L;
    private transient Map<TapValue<?, ?>, Object> originals;
    private transient List<Runnable> restorations;

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

    void changedEntry(Map<Object, Object> map, Object key, Object previous, Object converted) {
        restoreLater(() -> {
            // Keep deliberate codec replacements. Only undo a slot still holding our wrapper.
            if (map.get(key) == converted && map.containsKey(key)) map.put(key, previous);
        });
    }

    void changedCollection(Collection<Object> collection, List<Object> previous, Collection<?> converted) {
        Map<Object, Object> replacements = new IdentityHashMap<>();
        int index = 0;
        for (Object value : converted) {
            Object original = previous.get(index++);
            if (value != original) replacements.put(value, original);
        }
        restoreLater(() -> {
            // Restore our remaining values even when a codec changed or reordered other slots.
            Collection<?> restored = PdkContainerValues.collection(collection, (key, position, value) ->
                    replacements.containsKey(value) ? replacements.get(value) : value);
            if (restored != collection) {
                collection.clear();
                collection.addAll(restored);
            }
        });
    }

    private void restoreLater(Runnable restoration) {
        if (restorations == null) restorations = new ArrayList<>();
        restorations.add(restoration);
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        if (restorations != null) {
            for (int index = restorations.size() - 1; index >= 0; index--) {
                try {
                    restorations.get(index).run();
                } catch (RuntimeException restoreFailed) {
                    if (failure == null) failure = restoreFailed;
                    else if (failure != restoreFailed) failure.addSuppressed(restoreFailed);
                }
            }
        }
        originals = null;
        restorations = null;
        if (failure != null) throw failure;
    }

}
