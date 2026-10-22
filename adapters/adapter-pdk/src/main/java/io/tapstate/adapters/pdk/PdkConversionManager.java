package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.EntryFilter;
import io.tapdata.entity.codec.filter.StopFilterException;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import java.util.Collection;
import java.util.Map;

/** Keeps codec selection in the PDK while adapting its container traversal to portable rows. */
final class PdkConversionManager extends TapCodecsFilterManager {
    PdkConversionManager(TapCodecsRegistry codecs) {
        super(codecs);
        mapIteratorToTapValue = (map, filter) -> {
            PdkSourceRow originals = map instanceof PdkSourceRow row ? row : null;
            try {
                replace(map, (key, index, value) -> to(value,
                        PdkContainerValues.childPath("", key, index), false, filter, originals));
            } catch (StopFilterException stopped) {
                // Honor the PDK iterator's early-stop contract.
            }
        };
        mapIteratorFromTapValue = (map, filter) -> replace(map, (key, index, value) -> from(value,
                PdkContainerValues.childPath("", key, index), false, filter));
    }

    private Object to(Object value, String path, boolean nested, EntryFilter filter, PdkSourceRow originals) {
        boolean customContainer = (value instanceof Map<?, ?> || value instanceof Collection<?>)
                && codecsRegistry.getCustomToTapValueCodec(value.getClass()) != null;
        Object traversed = children(value, path, filter, customContainer, originals);
        Object converted = filter.filter(path, traversed, nested);
        if (originals != null && converted instanceof io.tapdata.entity.schema.value.TapValue<?, ?> tap) {
            originals.converted(tap, value);
        }
        return converted == null ? traversed : converted;
    }

    private Object children(Object value, String path, EntryFilter filter, boolean customContainer,
            PdkSourceRow originals) {
        PdkContainerValues.Child child = (key, index, item) ->
                to(item, PdkContainerValues.childPath(path, key, index), true, filter, originals);
        if (value instanceof Map<?, ?> map) {
            if (customContainer) {
                // PDK codecs receive their registered driver class, with its original mutation
                // semantics. Replacing a driver map with a plain map would select another codec.
                replace(map, child);
                return map;
            }
            return PdkContainerValues.map(map, child);
        }
        if (value instanceof Collection<?> collection) {
            Collection<?> converted = PdkContainerValues.collection(collection, child);
            if (customContainer) {
                replaceCollection(collection, converted);
                return collection;
            }
            return converted;
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static void replaceCollection(Collection<?> original, Collection<?> converted) {
        if (original == converted) return;
        Collection<Object> mutable = (Collection<Object>) original;
        mutable.clear();
        mutable.addAll(converted);
    }

    private static Object from(Object value, String path, boolean nested, EntryFilter filter) {
        Object converted = filter.filter(path, value, nested);
        Object result = converted == null ? value : converted;
        if (value instanceof TapMapValue && result instanceof Map<?, ?> map) {
            return PdkContainerValues.map(map, (key, index, child) ->
                    from(child, PdkContainerValues.childPath(path, key, index), true, filter));
        }
        if (value instanceof TapArrayValue && result instanceof Collection<?> collection) {
            return PdkContainerValues.collection(collection, (key, index, child) ->
                    from(child, path + ".#" + index, true, filter));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void replace(Map<?, ?> values, PdkContainerValues.Child child) {
        Map<Object, Object> mutable = (Map<Object, Object>) values;
        int index = 0;
        for (Map.Entry<Object, Object> entry : mutable.entrySet()) {
            Object next = child.apply(entry.getKey(), index++, entry.getValue());
            if (next != entry.getValue()) entry.setValue(next);
        }
    }
}
