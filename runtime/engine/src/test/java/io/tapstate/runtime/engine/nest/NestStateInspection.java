package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a nest namespace's state for a test, wherever the selected carrier keeps it: the namespace's map, or,
 * on the state session, what the session committed to the store behind it. A test written against this
 * observes the state rather than the carrier holding it.
 *
 * <p>Keys are the ones the carrier stores under: the vertex's own key on a map, its stored name on the
 * session. {@link #keyOf} turns a key a test knows into that form, so a test compares like with like.
 */
public final class NestStateInspection {

    private NestStateInspection() {}

    /** Whether state is kept on the state session rather than in maps. */
    public static boolean onSession() {
        return NestStateSession.selected();
    }

    /** {@code key} as the selected carrier stores it. */
    public static Object keyOf(Object key) {
        return onSession() ? NestStateKeys.nameOf(key) : key;
    }

    /** Every entry of {@code namespace} on {@code member}, by stored key. */
    @SuppressWarnings("unchecked")
    public static <V> Map<Object, V> entries(HazelcastInstance member, String namespace) {
        Map<Object, V> entries = new LinkedHashMap<>();
        if (onSession()) {
            NestStateSession.unbacked(member, namespace)
                    .forEach((name, bytes) -> entries.put(name, (V) NestStateSession.fromBytes(bytes)));
            return entries;
        }
        IMap<Object, V> map = member.getMap(namespace);
        for (Object key : map.keySet()) {
            V value = map.get(key);
            if (value != null) entries.put(key, value);
        }
        return entries;
    }

    /** The entry of {@code namespace} under {@code key}, a key as the vertex knows it; null when there is none. */
    public static <V> V get(HazelcastInstance member, String namespace, Object key) {
        Map<Object, V> entries = entries(member, namespace);
        return entries.get(keyOf(key));
    }

    /** The namespaces holding state whose names start with {@code prefix}, sorted. */
    public static List<String> namespaces(HazelcastInstance member, String prefix) {
        List<String> names = new ArrayList<>();
        if (onSession()) {
            for (String name : NestStateSession.unbackedNamespaces(member)) {
                if (name.startsWith(prefix)) names.add(name);
            }
        } else {
            member.getDistributedObjects().forEach(object -> {
                if (object instanceof IMap && object.getName().startsWith(prefix)) names.add(object.getName());
            });
        }
        Collections.sort(names);
        return names;
    }
}
