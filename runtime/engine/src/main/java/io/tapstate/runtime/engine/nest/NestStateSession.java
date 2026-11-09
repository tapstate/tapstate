package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.spi.store.GroupCommitStateStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestReleaseStore;
import io.tapstate.spi.store.StateGroup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * The state of one vertex instance: every namespace it writes, kept resident within a byte budget, with every
 * change staged until the instance commits them, all namespaces together, in one store transaction.
 *
 * <p>Selected with {@code -Dtapstate.nest.store=direct}; without it a vertex keeps its state in a Hazelcast
 * map per namespace, written through per key.
 *
 * <p><b>Proof before the first read.</b> The session opens its commit group on the first call that touches
 * state, and opening proves the instance's authority in the store, so nothing this instance reads can predate
 * that proof. The namespaces are fixed by then: every view is taken when the instance's processor is built.
 *
 * <p><b>Groups.</b> Mutations are staged in each namespace's resident values. The processor commits when
 * {@link #due() the group is due} - at {@link NestCommitBounds} keys, bytes or age - and before anything that
 * says a change has been made durable leaves it. A single value over the byte bound is refused with
 * {@code nest.state-value-too-large}. The bounds are checked where the processor asks, between drains, never
 * inside one: a drain's changes to one key and to the keys it moves between always land in the same group.
 *
 * <p><b>Failure.</b> A commit that fails leaves nothing of its group in the store. The session then fails
 * closed: what was staged is dropped and every later call fails the same way, so the vertex stops and replays
 * from the frontier rather than carrying on over state the store does not hold.
 *
 * <p>Values are held serialized, exactly as the map carrier stores them, so every load hands back a copy the
 * caller may change freely and a pipeline can switch carriers on restart without rewriting its state.
 *
 * <p>Not thread-safe: one vertex instance owns it and uses it from the thread running its processor.
 */
public final class NestStateSession {

    /** System property that selects this carrier in place of the map-backed one. */
    public static final String MODE_PROPERTY = "tapstate.nest.store";

    private static final Counters COUNTERS = new Counters();

    /**
     * Namespaces with no store behind them, one per cluster and namespace. A map with no MapStore kept its
     * state in cluster memory, so its stand-in is shared by every instance reaching that namespace, which on
     * members sharing one process is the case tests run in, the only place a nest runs without a store.
     */
    private static final Map<String, Unbacked> UNBACKED = new ConcurrentHashMap<>();

    private final String vertexInstance;
    private final Function<String, KeyedStateStore> stores;
    private final NestCommitBounds bounds;
    private final Map<String, Namespace> namespaces = new LinkedHashMap<>();
    private StateGroup group;
    private boolean opened;
    private RuntimeException failed;
    private int stagedKeys;
    private long stagedBytes;
    private long stagedSince;

    NestStateSession(String vertexInstance, Function<String, KeyedStateStore> stores, NestCommitBounds bounds) {
        this.vertexInstance = Objects.requireNonNull(vertexInstance, "vertexInstance");
        this.stores = Objects.requireNonNull(stores, "stores");
        this.bounds = Objects.requireNonNull(bounds, "bounds");
    }

    /** Whether this carrier is selected for the vertices built in this process. */
    public static boolean selected() {
        return "direct".equals(System.getProperty(MODE_PROPERTY));
    }

    /** A session whose namespaces are the stores the member's map configuration names for them. */
    static NestStateSession on(HazelcastInstance member, String vertexInstance) {
        Objects.requireNonNull(member, "member");
        return new NestStateSession(vertexInstance, namespace -> resolve(member, namespace), NestCommitBounds.configured());
    }

    /** What the sessions in this process have done since the last {@link Counters#reset()}. */
    public static Counters counters() {
        return COUNTERS;
    }

    /**
     * What an unbacked namespace of {@code member}'s cluster holds, by stored key name, for a test to inspect
     * the way it would read a map. Empty when nothing was ever committed there.
     */
    static Map<String, byte[]> unbacked(HazelcastInstance member, String namespace) {
        Unbacked held = UNBACKED.get(member.getConfig().getClusterName() + "/" + namespace);
        return held == null ? Map.of() : Map.copyOf(held.held);
    }

    /** The unbacked namespaces of {@code member}'s cluster that anything was committed to. */
    static Set<String> unbackedNamespaces(HazelcastInstance member) {
        String prefix = member.getConfig().getClusterName() + "/";
        Set<String> names = new LinkedHashSet<>();
        UNBACKED.forEach((name, held) -> {
            if (name.startsWith(prefix) && !held.held.isEmpty()) names.add(name.substring(prefix.length()));
        });
        return names;
    }

    /** Forgets every unbacked namespace, so one test's state does not reach the next. */
    public static void forgetUnbacked() {
        UNBACKED.clear();
    }

    /**
     * The view of one namespace as a typed store. Every view must be taken before the session first touches
     * state; a view asked for later would be a namespace the proof did not cover.
     */
    <S> NestStore<S> view(String namespace, long residentBytes) {
        return view(namespace, residentBytes, null, NestStateGauge.NONE);
    }

    /**
     * The same view, leaving the namespace's readings where the run's statistics are collected: every reach
     * for the state counted as the map carrier counts it, every read the store had to serve counted as a
     * backfill, and how many keys the instance holds in memory as the namespace's entries.
     */
    <S> NestStore<S> view(String namespace, long residentBytes, NestStateStats stats, NestStateGauge gauge) {
        if (opened) throw new IllegalStateException("a state session's namespaces are fixed once it is open: " + namespace);
        Namespace held = namespaces.computeIfAbsent(Objects.requireNonNull(namespace, "namespace"),
                name -> new Namespace(name, residentBytes, stats, Objects.requireNonNull(gauge, "gauge")));
        return new View<>(held);
    }

    /**
     * A read-only view of a namespace another vertex writes, for the assembler rendering referenced rows.
     * Every read goes to the store and nothing is kept: the only copy that is always current is the store's,
     * and the writing vertex commits before it sends the word that makes a document read it again.
     */
    @SuppressWarnings("unchecked")
    static <S> NestStore<S> readThrough(HazelcastInstance member, String namespace) {
        return (NestStore<S>) new ReadThrough(namespace, resolve(member, namespace), NestStateStats.of(member));
    }

    /** Whether anything is staged and not yet committed. */
    public boolean hasStaged() {
        return stagedKeys > 0;
    }

    /** Whether the open group has reached one of its bounds and should commit now. */
    public boolean due() {
        return stagedKeys > 0 && (stagedKeys >= bounds.maxKeys() || stagedBytes >= bounds.maxBytes()
                || System.nanoTime() - stagedSince >= bounds.maxDelayNanos());
    }

    /**
     * Commits everything staged, in every namespace, in one store transaction. Nothing staged commits nothing
     * and opens no transaction.
     */
    public void commit() {
        requireUsable();
        if (stagedKeys == 0) return;
        open();
        Map<String, StateGroup.Changes> changes = new LinkedHashMap<>();
        for (Namespace namespace : namespaces.values()) {
            if (namespace.values.hasStaged()) changes.put(namespace.name, namespace.changes());
        }
        try {
            for (Map<String, StateGroup.Changes> part : split(changes, bounds)) {
                if (group != null) group.commit(part);
                else commitWithoutGroup(part);
                COUNTERS.commits.increment();
            }
        } catch (RuntimeException failure) {
            failed = failure;
            namespaces.values().forEach(Namespace::abandon);
            namespaces.values().forEach(Namespace::publish);
            resetGroup();
            throw failure;
        }
        namespaces.values().forEach(Namespace::committed);
        namespaces.values().forEach(Namespace::publish);
        COUNTERS.keysCommitted.add(stagedKeys);
        resetGroup();
    }

    /**
     * What was staged, as groups that each fit the bounds, in staging order. Staging can pass a bound inside one
     * drain, because the bounds are only checked between drains; a group past them is split here rather than
     * sent as one transaction too large for the store. A key's value is never split, and a value can never be
     * over the byte bound alone, because one that is was refused when it was staged.
     *
     * <p>Splitting is safe because every value is written whole and carries the order it came from: if the
     * member dies between two of the parts, the replay rewrites the first part's keys with the same bytes. The
     * output that depends on the parts is still held until the last of them has committed.
     */
    static List<Map<String, StateGroup.Changes>> split(Map<String, StateGroup.Changes> changes, NestCommitBounds bounds) {
        List<Map<String, StateGroup.Changes>> parts = new ArrayList<>();
        Part current = new Part();
        for (Map.Entry<String, StateGroup.Changes> entry : changes.entrySet()) {
            String namespace = entry.getKey();
            StateGroup.Changes all = entry.getValue();
            for (Map.Entry<String, byte[]> write : all.writes().entrySet()) {
                byte[] routing = all.releaseKeys().get(write.getKey());
                long weight = write.getValue().length + (routing == null ? 0 : routing.length);
                if (current.full(weight, bounds)) {
                    parts.add(current.changes());
                    current = new Part();
                }
                current.write(namespace, write.getKey(), write.getValue(), routing, weight);
            }
            for (String removed : all.removals()) {
                if (current.full(0, bounds)) {
                    parts.add(current.changes());
                    current = new Part();
                }
                current.remove(namespace, removed);
            }
        }
        if (current.keys > 0) parts.add(current.changes());
        return parts;
    }

    /** One group being filled by {@link #split}. */
    private static final class Part {
        private final Map<String, Map<String, byte[]>> writes = new LinkedHashMap<>();
        private final Map<String, Map<String, byte[]>> releases = new LinkedHashMap<>();
        private final Map<String, Set<String>> removals = new LinkedHashMap<>();
        private int keys;
        private long bytes;

        boolean full(long weight, NestCommitBounds bounds) {
            return keys > 0 && (keys + 1 > bounds.maxKeys() || bytes + weight > bounds.maxBytes());
        }

        void write(String namespace, String key, byte[] value, byte[] routing, long weight) {
            writes.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>()).put(key, value);
            if (routing != null) releases.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>()).put(key, routing);
            keys++;
            bytes += weight;
        }

        void remove(String namespace, String key) {
            removals.computeIfAbsent(namespace, ignored -> new LinkedHashSet<>()).add(key);
            keys++;
        }

        Map<String, StateGroup.Changes> changes() {
            Map<String, StateGroup.Changes> out = new LinkedHashMap<>();
            Set<String> names = new LinkedHashSet<>(writes.keySet());
            names.addAll(removals.keySet());
            for (String name : names) {
                out.put(name, new StateGroup.Changes(writes.getOrDefault(name, Map.of()),
                        releases.getOrDefault(name, Map.of()), removals.getOrDefault(name, Set.of())));
            }
            return out;
        }
    }

    private void resetGroup() {
        stagedKeys = 0;
        stagedBytes = 0;
        stagedSince = 0;
    }

    /**
     * A namespace store that cannot group, which is only ever a store with nothing durable behind it: each
     * namespace is written on its own. Nothing here is atomic across namespaces, and nothing needs it to be.
     */
    private void commitWithoutGroup(Map<String, StateGroup.Changes> changes) {
        changes.forEach((name, part) -> {
            KeyedStateStore store = namespaces.get(name).store;
            Map<String, byte[]> plain = new LinkedHashMap<>(part.writes());
            plain.keySet().removeAll(part.releaseKeys().keySet());
            if (!plain.isEmpty()) store.saveAll(name, plain);
            part.releaseKeys().forEach((key, routing) -> {
                if (store instanceof NestReleaseStore releases) releases.saveWithRelease(name, key, part.writes().get(key), routing);
                else store.save(name, key, part.writes().get(key));
            });
            part.removals().forEach(key -> store.delete(name, key));
        });
    }

    private void open() {
        requireUsable();
        if (opened) return;
        opened = true;
        List<GroupCommitStateStore.GroupNamespace> grouped = new ArrayList<>();
        boolean groupable = !namespaces.isEmpty();
        for (Namespace namespace : namespaces.values()) {
            namespace.store = stores.apply(namespace.name);
            if (namespace.store instanceof GroupCommitStateStore store) grouped.add(new GroupCommitStateStore.GroupNamespace(store, namespace.name));
            else groupable = false;
        }
        if (groupable) {
            try {
                group = grouped.get(0).store().openGroup(vertexInstance, grouped);
            } catch (RuntimeException failure) {
                failed = failure;
                throw failure;
            }
            COUNTERS.groupsOpened.increment();
        }
    }

    private void requireUsable() {
        if (failed != null) {
            throw failed instanceof TapstateException coded
                    ? new TapstateException(coded.code(), coded.args(), coded)
                    : new IllegalStateException("this vertex's state session failed closed", failed);
        }
    }

    /** One namespace of the session: its store, its resident values and what a commit has to know of its keys. */
    private final class Namespace {
        private final String name;
        private final ResidentValues<String> values;
        private final Map<String, Object> keysByName = new HashMap<>();
        private final Set<String> releasing = new LinkedHashSet<>();
        private final NestStateStats stats;
        private final NestStateGauge gauge;
        private KeyedStateStore store;

        Namespace(String name, long residentBytes, NestStateStats stats, NestStateGauge gauge) {
            this.name = name;
            this.values = new ResidentValues<>(residentBytes);
            this.stats = stats;
            this.gauge = gauge;
        }

        /** One reach for the state, over {@code keys} keys, counted as the map carrier counts one. */
        void access(long keys) {
            if (stats == null) return;
            stats.access(name);
            stats.accessKeys(name, keys);
        }

        private void backfilled(long began, long keys) {
            if (stats == null) return;
            stats.backfill(name, System.nanoTime() - began);
            stats.coldKeys(name, keys);
        }

        void holding(long pending) {
            if (stats != null) stats.holding(name, pending);
        }

        /** Leaves the namespace's current readings, after whatever changed them, as the map carrier does. */
        void publish() {
            if (stats == null) return;
            stats.held(name, vertexInstance, values.presentCount());
            NestStateStats.Counted counted = stats.counted(name);
            gauge.reading(name, stats.held(name), counted.accesses(), counted.backfills(),
                    counted.backfillNanos() / 1_000_000L, counted.pendingHighWater());
            gauge.keyReading(name, counted.keyAccesses(), counted.coldKeys());
        }

        Object load(Object key) {
            open();
            String id = NestStateKeys.nameOf(key);
            Optional<Optional<byte[]>> resident = values.get(id);
            if (resident.isPresent()) {
                COUNTERS.residentHits.increment();
                return resident.get().map(NestStateSession::fromBytes).orElse(null);
            }
            COUNTERS.storeReads.increment();
            long began = System.nanoTime();
            byte[] found = store.load(name, id).orElse(null);
            backfilled(began, 1);
            values.putClean(id, found);
            return found == null ? null : fromBytes(found);
        }

        Map<Object, Object> loadAll(Collection<Object> keys) {
            open();
            Map<String, Object> ids = new LinkedHashMap<>();
            for (Object key : keys) ids.put(NestStateKeys.nameOf(key), key);
            Map<String, byte[]> held = new LinkedHashMap<>();
            List<String> missing = new ArrayList<>();
            ids.forEach((id, key) -> {
                Optional<Optional<byte[]>> resident = values.get(id);
                if (resident.isEmpty()) missing.add(id);
                else resident.get().ifPresent(bytes -> held.put(id, bytes));
            });
            if (!missing.isEmpty()) {
                COUNTERS.storeReads.increment();
                long began = System.nanoTime();
                Map<String, byte[]> found = store.loadAll(name, missing);
                backfilled(began, missing.size());
                for (String id : missing) {
                    byte[] bytes = found.get(id);
                    values.putClean(id, bytes);
                    if (bytes != null) held.put(id, bytes);
                }
            }
            Map<Object, Object> loaded = new LinkedHashMap<>();
            ids.forEach((id, key) -> {
                byte[] bytes = held.get(id);
                if (bytes != null) loaded.put(key, fromBytes(bytes));
            });
            return loaded;
        }

        void save(Object key, Object state) {
            open();
            String id = NestStateKeys.nameOf(key);
            byte[] bytes = toBytes(state);
            if (bytes.length > bounds.maxBytes()) {
                throw new TapstateException(NestError.STATE_VALUE_TOO_LARGE,
                        Map.<String, Object>of("namespace", name, "bytes", bytes.length, "limit", bounds.maxBytes()), null);
            }
            stage(id, key);
            values.stage(id, bytes);
            stagedBytes += bytes.length;
            if (state instanceof ResolverState resolver && !resolver.releases().isEmpty()) releasing.add(id);
            else releasing.remove(id);
        }

        void remove(Object key) {
            open();
            String id = NestStateKeys.nameOf(key);
            stage(id, key);
            values.stageRemoval(id);
            releasing.remove(id);
        }

        private void stage(String id, Object key) {
            requireUsable();
            if (!values.isStaged(id)) {
                if (stagedKeys == 0) stagedSince = System.nanoTime();
                stagedKeys++;
            }
            keysByName.put(id, key);
        }

        StateGroup.Changes changes() {
            Map<String, byte[]> writes = new LinkedHashMap<>();
            Map<String, byte[]> releases = new LinkedHashMap<>();
            Set<String> removals = new LinkedHashSet<>();
            values.staged().forEach((id, value) -> {
                if (value.isEmpty()) {
                    removals.add(id);
                    return;
                }
                writes.put(id, value.get());
                if (releasing.contains(id)) releases.put(id, toBytes(keysByName.get(id)));
            });
            return new StateGroup.Changes(writes, releases, removals);
        }

        void committed() {
            values.committed();
            releasing.clear();
            keysByName.clear();
        }

        void abandon() {
            values.abandonStaged();
            releasing.clear();
            keysByName.clear();
        }

        long count() {
            open();
            return store.count(name);
        }
    }

    /** One namespace as the vertex sees it: a store whose writes wait for the session's commit. */
    private final class View<S> implements NestStore<S> {
        private static final long serialVersionUID = 1L;
        private final transient Namespace namespace;

        View(Namespace namespace) {
            this.namespace = namespace;
        }

        @Override
        @SuppressWarnings("unchecked")
        public S load(Object key) {
            namespace.access(1);
            S state = (S) namespace.load(key);
            namespace.publish();
            return state;
        }

        /** One reach for however many keys, as the map carrier counts a batch read. */
        @Override
        @SuppressWarnings("unchecked")
        public Map<Object, S> loadAll(Collection<Object> keys) {
            if (keys.isEmpty()) return Map.of();
            Set<Object> distinct = new LinkedHashSet<>(keys);
            namespace.access(distinct.size());
            Map<Object, S> loaded = (Map<Object, S>) (Map<Object, ?>) namespace.loadAll(distinct);
            namespace.publish();
            return loaded;
        }

        @Override
        public void save(Object key, S state) {
            namespace.access(1);
            namespace.save(key, state);
            namespace.publish();
        }

        @Override
        public void saveAll(Map<Object, S> states, Set<Object> firstFilings) {
            if (states.isEmpty()) return;
            states.forEach((key, state) -> {
                namespace.access(1);
                namespace.save(key, state);
            });
            namespace.publish();
        }

        /** One reach, as the map carrier counts growing a set at its key. */
        @Override
        @SuppressWarnings("unchecked")
        public void add(Object key, Object element) {
            namespace.access(1);
            Set<Object> held = (Set<Object>) namespace.load(key);
            Set<Object> grown = held == null ? new LinkedHashSet<>() : held;
            if (grown.add(element)) namespace.save(key, grown);
            namespace.publish();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void addAll(Map<Object, Set<Object>> additions) {
            if (additions.isEmpty()) return;
            additions.keySet().forEach(ignored -> namespace.access(1));
            Map<Object, Object> held = namespace.loadAll(additions.keySet());
            additions.forEach((key, elements) -> {
                Set<Object> current = (Set<Object>) held.get(key);
                Set<Object> grown = current == null ? new LinkedHashSet<>() : current;
                if (grown.addAll(elements)) namespace.save(key, grown);
            });
            namespace.publish();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void remove(Object key, Object element) {
            namespace.access(1);
            Set<Object> held = (Set<Object>) namespace.load(key);
            if (held != null && held.remove(element)) {
                if (held.isEmpty()) namespace.remove(key);
                else namespace.save(key, held);
            }
            namespace.publish();
        }

        @Override
        public void remove(Object key) {
            namespace.access(1);
            namespace.remove(key);
            namespace.publish();
        }

        /** The trip behind memory is counted as a backfill; the reach is counted when a row makes it. */
        @Override
        public void readAhead(Collection<Object> keys) {
            if (keys.isEmpty()) return;
            namespace.loadAll(new LinkedHashSet<>(keys));
            namespace.publish();
        }

        @Override
        public void holding(long pending) {
            namespace.holding(pending);
        }

        @Override
        public long count() {
            return namespace.count();
        }

        @Override
        public void commit() {
            NestStateSession.this.commit();
        }

        @Override
        public boolean hasUncommitted() {
            return hasStaged();
        }

        @Override
        public boolean commitDue() {
            return due();
        }

        @Override
        public boolean readsAhead() {
            return true;
        }
    }

    /**
     * A namespace another vertex owns, read for rendering only. Nothing is resident and nothing may be written:
     * the owning vertex is the only writer of its namespaces.
     */
    private static final class ReadThrough implements NestStore<Object> {
        private static final long serialVersionUID = 1L;
        private final String namespace;
        private final transient KeyedStateStore store;
        private final transient NestStateStats stats;

        ReadThrough(String namespace, KeyedStateStore store, NestStateStats stats) {
            this.namespace = Objects.requireNonNull(namespace, "namespace");
            this.store = Objects.requireNonNull(store, "store");
            this.stats = stats;
        }

        @Override
        public Object load(Object key) {
            return loadAll(List.of(key)).get(key);
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<String, Object> ids = new LinkedHashMap<>();
            for (Object key : keys) ids.put(NestStateKeys.nameOf(key), key);
            if (ids.isEmpty()) return Map.of();
            COUNTERS.readThroughCalls.increment();
            // One reach, and always a trip behind memory: nothing read here is kept.
            if (stats != null) {
                stats.access(namespace);
                stats.accessKeys(namespace, ids.size());
            }
            long began = System.nanoTime();
            Map<String, byte[]> found = store.loadAll(namespace, List.copyOf(ids.keySet()));
            if (stats != null) {
                stats.backfill(namespace, System.nanoTime() - began);
                stats.coldKeys(namespace, ids.size());
            }
            Map<Object, Object> loaded = new LinkedHashMap<>();
            ids.forEach((id, key) -> {
                byte[] bytes = found.get(id);
                if (bytes != null) loaded.put(key, fromBytes(bytes));
            });
            return loaded;
        }

        @Override
        public void save(Object key, Object state) {
            throw new IllegalStateException(namespace + " is read here, never written: its owner writes it");
        }

        @Override
        public void remove(Object key) {
            throw new IllegalStateException(namespace + " is read here, never written: its owner writes it");
        }

        @Override
        public long count() {
            return store.count(namespace);
        }

        @Override
        public boolean readsAhead() {
            return true;
        }
    }

    /** Resolves the store behind a namespace the way the namespace's MapStore would have. */
    static KeyedStateStore resolve(HazelcastInstance member, String namespace) {
        MapConfig config = member.getConfig().findMapConfig(namespace);
        if (config.getMapStoreConfig() == null || !config.getMapStoreConfig().isEnabled()) {
            return UNBACKED.computeIfAbsent(member.getConfig().getClusterName() + "/" + namespace, ignored -> new Unbacked());
        }
        var properties = config.getMapStoreConfig().getProperties();
        String database = properties.getProperty(NestMaps.STATE_DATABASE_PROPERTY);
        OperatorMapBindings bindings = OperatorMapBindings.resolve(member, OperatorMapBindings.required(properties));
        if (bindings != null) return bindings.nest(namespace, database);
        var stores = NestStateMapStoreFactory.boundTo(member);
        return stores.inDatabase(database == null ? stores.defaultDatabase() : database).state();
    }

    /** Counts of what the sessions did, process-wide; for measurement, not for decisions. */
    public static final class Counters {
        private final LongAdder commits = new LongAdder();
        private final LongAdder keysCommitted = new LongAdder();
        private final LongAdder groupsOpened = new LongAdder();
        private final LongAdder residentHits = new LongAdder();
        private final LongAdder storeReads = new LongAdder();
        private final LongAdder readThroughCalls = new LongAdder();

        public void reset() {
            commits.reset();
            keysCommitted.reset();
            groupsOpened.reset();
            residentHits.reset();
            storeReads.reset();
            readThroughCalls.reset();
        }

        public long commits() { return commits.sum(); }
        public long keysCommitted() { return keysCommitted.sum(); }
        public long groupsOpened() { return groupsOpened.sum(); }
        public long residentHits() { return residentHits.sum(); }
        public long storeReads() { return storeReads.sum(); }
        public long readThroughCalls() { return readThroughCalls.sum(); }
    }

    /** State held in this process only, for a namespace whose map had no store behind it. */
    private static final class Unbacked implements KeyedStateStore {
        private final Map<String, byte[]> held = new ConcurrentHashMap<>();

        @Override public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(held.get(key)); }
        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            Map<String, byte[]> loaded = new LinkedHashMap<>();
            for (String key : keys) {
                byte[] state = held.get(key);
                if (state != null) loaded.put(key, state);
            }
            return loaded;
        }
        @Override public void saveAll(String namespace, Map<String, byte[]> states) { held.putAll(states); }
        @Override public void save(String namespace, String key, byte[] state) { held.put(key, state); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return Optional.ofNullable(held.putIfAbsent(key, state));
        }
        @Override public void delete(String namespace, String key) { held.remove(key); }
        @Override public void dropNamespace(String namespace) { held.clear(); }
        @Override public long count(String namespace) { return held.size(); }
    }

    /** The same encoding the map carrier stores, so a pipeline can switch carriers without rewriting state. */
    static byte[] toBytes(Object value) {
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

    static Object fromBytes(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException malformed) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "nest-state", "field", "operator-state"), null);
        }
    }
}
