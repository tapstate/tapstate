package io.tapstate.adapters.pdk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Container traversal that retains key objects and copies only when a child changes. */
final class PdkContainerValues {
    @FunctionalInterface
    interface Child {
        Object apply(Object key, int index, Object value);
    }

    private PdkContainerValues() { }

    static Map<?, ?> map(Map<?, ?> source, Child child) {
        Map<Object, Object> changed = null;
        int index = 0;
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object next = child.apply(entry.getKey(), index++, entry.getValue());
            if (next != entry.getValue()) {
                if (changed == null) changed = new LinkedHashMap<>(source);
                changed.put(entry.getKey(), next);
            }
        }
        return changed == null ? source : changed;
    }

    static Collection<?> collection(Collection<?> source, Child child) {
        List<Object> changed = source instanceof List<?> ? null : new ArrayList<>(source);
        int index = 0;
        for (Object value : source) {
            Object next = child.apply(null, index, value);
            if (next != value) {
                if (changed == null) changed = new ArrayList<>(source);
                changed.set(index, next);
            }
            index++;
        }
        return changed == null ? source : changed;
    }

    static String childPath(String path, Object key, int index) {
        // Non-string keys have no named schema field; use the PDK's positional convention.
        String segment = key instanceof String name ? name : "#" + index;
        return path.isEmpty() ? segment : path + "." + segment;
    }

}
