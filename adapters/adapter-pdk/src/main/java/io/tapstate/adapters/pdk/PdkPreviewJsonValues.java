package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.value.DateTime;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.EventJsonValues;
import java.lang.reflect.Array;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/** Lazily projects PDK values so bounded byte accounting stops before visiting later containers. */
final class PdkPreviewJsonValues {

    private PdkPreviewJsonValues() {
    }

    static long encodedSize(Object value, long limit) {
        return EventJsonValues.encodedSize(project(value, 0), limit);
    }

    private static Object project(Object value, int depth) {
        if (depth > 128) {
            throw new IllegalArgumentException("preview document exceeds the JSON nesting limit");
        }
        return switch (value) {
            case null -> null;
            case ConvertedValue converted -> project(converted.value(), depth + 1);
            case DateTime dateTime -> dateTime.toInstant().toString();
            case Map<?, ?> map -> new AbstractMap<Object, Object>() {
                @Override
                public Set<Entry<Object, Object>> entrySet() {
                    return new AbstractSet<>() {
                        @Override
                        public int size() { return map.size(); }

                        @Override
                        public Iterator<Entry<Object, Object>> iterator() {
                            Iterator<? extends Entry<?, ?>> entries = map.entrySet().iterator();
                            return new Iterator<>() {
                                @Override
                                public boolean hasNext() { return entries.hasNext(); }

                                @Override
                                public Entry<Object, Object> next() {
                                    Entry<?, ?> entry = entries.next();
                                    return new Entry<>() {
                                        @Override
                                        public Object getKey() { return entry.getKey(); }

                                        @Override
                                        public Object getValue() { return project(entry.getValue(), depth + 1); }

                                        @Override
                                        public Object setValue(Object ignored) {
                                            throw new UnsupportedOperationException("preview projection is read-only");
                                        }
                                    };
                                }
                            };
                        }
                    };
                }
            };
            case Collection<?> collection -> new AbstractCollection<Object>() {
                @Override
                public int size() { return collection.size(); }

                @Override
                public Iterator<Object> iterator() {
                    Iterator<?> values = collection.iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() { return values.hasNext(); }

                        @Override
                        public Object next() { return project(values.next(), depth + 1); }
                    };
                }
            };
            default -> value.getClass().isArray() ? new AbstractList<Object>() {
                @Override
                public int size() { return Array.getLength(value); }

                @Override
                public Object get(int index) { return project(Array.get(value, index), depth + 1); }
            } : value;
        };
    }
}
