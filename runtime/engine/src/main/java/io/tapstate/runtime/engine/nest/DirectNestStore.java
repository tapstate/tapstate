package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestReleaseStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nest state kept by the operator itself, in front of the keyed state store, instead of in a Hazelcast
 * map whose MapStore writes through per key.
 *
 * <p>Experimental, selected with {@code -Dtapstate.nest.store=direct}. A mutation applies to a resident
 * copy and marks the key dirty; nothing reaches the store until the processor calls {@link #commit()},
 * which it does before anything leaves it. One commit writes every dirty key of the namespace in one
 * {@link KeyedStateStore#saveAll} call, so the store pays its per-transaction cost once per group rather
 * than once per key, and a crash loses only mutations whose inputs were never acknowledged downstream.
 *
 * <p>The resident copy is shared by every processor on the member that names the namespace, as the map
 * was, so a row a lookup files is seen by the assembler reading it. Values are held serialized, so every
 * load returns a copy the caller may change freely, exactly as a map read did. A key read and found
 * absent is remembered as absent: the namespace has one writer, and that writer goes through here.
 */
final class DirectNestStore<S> implements NestStore<S> {

    private static final long serialVersionUID = 1L;

    /** System property that selects this store in place of the map-backed one. */
    static final String MODE_PROPERTY = "tapstate.nest.store";

    /** Resident entries per namespace; 0 or absent means unbounded. Dirty entries are never evicted. */
    static final String RESIDENT_PROPERTY = "tapstate.nest.direct.resident";

    private static final String USER_CONTEXT_KEY = "tapstate.nest.direct-stores";
    private static final byte[] ABSENT = new byte[0];

    private final transient Shared shared;

    private DirectNestStore(Shared shared) {
        this.shared = shared;
    }

    static boolean selected() {
        return "direct".equals(System.getProperty(MODE_PROPERTY));
    }

    @SuppressWarnings("unchecked")
    static <S> NestStore<S> on(HazelcastInstance member, String namespace) {
        Map<String, Shared> registry = (Map<String, Shared>) member.getUserContext()
                .computeIfAbsent(USER_CONTEXT_KEY, ignored -> new ConcurrentHashMap<String, Shared>());
        return new DirectNestStore<>(registry.computeIfAbsent(namespace, name -> new Shared(name, resolve(member, name))));
    }

    /** Resolves the backing store the same way the MapStore for this namespace would have. */
    private static KeyedStateStore resolve(HazelcastInstance member, String namespace) {
        MapConfig config = member.getConfig().findMapConfig(namespace);
        if (config.getMapStoreConfig() == null || !config.getMapStoreConfig().isEnabled()) {
            // A map configured with nothing behind it kept its state in memory only; so does this.
            return new InMemory();
        }
        Properties properties = config.getMapStoreConfig().getProperties();
        String database = properties.getProperty(NestMaps.STATE_DATABASE_PROPERTY);
        boolean required = OperatorMapBindings.required(properties);
        OperatorMapBindings bindings = OperatorMapBindings.resolve(member, required);
        if (bindings != null) {
            return bindings.nest(namespace, database);
        }
        var stores = NestStateMapStoreFactory.boundTo(member);
        return stores.inDatabase(database == null ? stores.defaultDatabase() : database).state();
    }

    @Override
    @SuppressWarnings("unchecked")
    public S load(Object key) {
        return (S) shared.load(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, S> loadAll(Collection<Object> keys) {
        return (Map<Object, S>) (Map<Object, ?>) shared.loadAll(keys);
    }

    @Override
    public void save(Object key, S state) {
        shared.put(key, state);
    }

    @Override
    public void saveAll(Map<Object, S> states, Set<Object> firstFilings) {
        states.forEach(shared::put);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(Object key, Object element) {
        synchronized (shared) {
            Set<Object> held = (Set<Object>) shared.load(key);
            Set<Object> grown = held == null ? new LinkedHashSet<>() : held;
            if (grown.add(element)) shared.put(key, grown);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addAll(Map<Object, Set<Object>> additions) {
        synchronized (shared) {
            Map<Object, Object> held = shared.loadAll(additions.keySet());
            additions.forEach((key, elements) -> {
                Set<Object> current = (Set<Object>) held.get(key);
                Set<Object> grown = current == null ? new LinkedHashSet<>() : current;
                if (grown.addAll(elements)) shared.put(key, grown);
            });
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void remove(Object key, Object element) {
        synchronized (shared) {
            Set<Object> held = (Set<Object>) shared.load(key);
            if (held == null || !held.remove(element)) return;
            if (held.isEmpty()) shared.delete(key);
            else shared.put(key, held);
        }
    }

    @Override
    public void remove(Object key) {
        shared.delete(key);
    }

    @Override
    public long count() {
        return shared.count();
    }

    @Override
    public void commit() {
        shared.commit();
    }

    @Override
    public boolean readsAhead() {
        return true;
    }

    /** One namespace's resident state and its uncommitted changes. */
    private static final class Shared {
        private final String namespace;
        private final KeyedStateStore store;
        private final long bound;
        private final LinkedHashMap<String, byte[]> resident = new LinkedHashMap<>(16, 0.75f, true);
        private final Map<String, Object> keysByName = new LinkedHashMap<>();
        private final LinkedHashMap<String, byte[]> dirty = new LinkedHashMap<>();
        private final Set<String> releasing = new LinkedHashSet<>();
        private long present;

        Shared(String namespace, KeyedStateStore store) {
            this.namespace = Objects.requireNonNull(namespace, "namespace");
            this.store = Objects.requireNonNull(store, "store");
            this.bound = Long.getLong(RESIDENT_PROPERTY, 0L);
        }

        synchronized Object load(Object key) {
            String name = NestStateKeys.nameOf(key);
            byte[] bytes = resident.get(name);
            if (bytes == null) {
                bytes = store.load(namespace, name).orElse(ABSENT);
                remember(name, key, bytes);
            }
            return bytes == ABSENT ? null : fromBytes(bytes);
        }

        synchronized Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<String, Object> names = new LinkedHashMap<>();
            Map<String, byte[]> held = new LinkedHashMap<>();
            for (Object key : keys) {
                String name = NestStateKeys.nameOf(key);
                names.put(name, key);
                byte[] bytes = resident.get(name);
                if (bytes != null) held.put(name, bytes);
            }
            List<String> missing = names.keySet().stream().filter(name -> !held.containsKey(name)).toList();
            if (!missing.isEmpty()) {
                Map<String, byte[]> found = store.loadAll(namespace, missing);
                for (String name : missing) {
                    byte[] bytes = found.getOrDefault(name, ABSENT);
                    held.put(name, bytes);
                    remember(name, names.get(name), bytes);
                }
            }
            Map<Object, Object> loaded = new LinkedHashMap<>();
            names.forEach((name, key) -> {
                byte[] bytes = held.get(name);
                if (bytes != ABSENT) loaded.put(key, fromBytes(bytes));
            });
            return loaded;
        }

        synchronized void put(Object key, Object state) {
            String name = NestStateKeys.nameOf(key);
            byte[] bytes = toBytes(state);
            remember(name, key, bytes);
            dirty.put(name, bytes);
            if (state instanceof ResolverState resolver && store instanceof NestReleaseStore
                    && !resolver.releases().isEmpty()) releasing.add(name);
            else releasing.remove(name);
        }

        synchronized void delete(Object key) {
            String name = NestStateKeys.nameOf(key);
            remember(name, key, ABSENT);
            dirty.put(name, ABSENT);
            releasing.remove(name);
        }

        synchronized long count() {
            return present;
        }

        synchronized void commit() {
            if (dirty.isEmpty()) return;
            Map<String, byte[]> writes = new LinkedHashMap<>();
            List<String> deletes = new ArrayList<>();
            dirty.forEach((name, bytes) -> {
                if (bytes == ABSENT) deletes.add(name);
                else if (!releasing.contains(name)) writes.put(name, bytes);
            });
            if (!writes.isEmpty()) store.saveAll(namespace, writes);
            for (String name : releasing) {
                byte[] bytes = dirty.get(name);
                if (bytes != null && bytes != ABSENT) {
                    ((NestReleaseStore) store).saveWithRelease(namespace, name, bytes, toBytes(keysByName.get(name)));
                }
            }
            for (String name : deletes) store.delete(namespace, name);
            dirty.clear();
            releasing.clear();
            evict();
        }

        private void remember(String name, Object key, byte[] bytes) {
            byte[] before = resident.put(name, bytes);
            if (before != null && before != ABSENT) present--;
            if (bytes != ABSENT) present++;
            keysByName.put(name, key);
            evict();
        }

        private void evict() {
            if (bound <= 0 || resident.size() <= bound) return;
            Iterator<Map.Entry<String, byte[]>> oldest = resident.entrySet().iterator();
            while (resident.size() > bound && oldest.hasNext()) {
                Map.Entry<String, byte[]> entry = oldest.next();
                if (dirty.containsKey(entry.getKey())) continue;
                if (entry.getValue() != ABSENT) present--;
                keysByName.remove(entry.getKey());
                oldest.remove();
            }
        }
    }

    /** State held in this process only, for a namespace whose map had no store behind it. */
    private static final class InMemory implements KeyedStateStore {
        private final Map<String, byte[]> held = new ConcurrentHashMap<>();
        @Override public java.util.Optional<byte[]> load(String namespace, String key) {
            return java.util.Optional.ofNullable(held.get(key));
        }
        @Override public void save(String namespace, String key, byte[] state) { held.put(key, state); }
        @Override public java.util.Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return java.util.Optional.ofNullable(held.putIfAbsent(key, state));
        }
        @Override public void delete(String namespace, String key) { held.remove(key); }
        @Override public void dropNamespace(String namespace) { held.clear(); }
        @Override public long count(String namespace) { return held.size(); }
    }

    private static byte[] toBytes(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException("nest state of type " + value.getClass().getName()
                    + " cannot be written to the state layer");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException cause) {
            throw new IllegalStateException("could not write nest state", cause);
        }
        return bytes.toByteArray();
    }

    private static Object fromBytes(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException malformed) {
            throw new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", "nest-state", "field", "operator-state"), null);
        }
    }
}
