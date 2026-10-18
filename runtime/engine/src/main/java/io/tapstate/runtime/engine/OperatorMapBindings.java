package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.OperatorPlacement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Member-local bindings installed before a job creates its maps. Each cold position retains one latest
 * execution slot, including a small retired marker. A MapStore captures that slot's handle once and
 * never resolves a later execution. Its hot namespace is translated to the original stable cold one.
 */
public final class OperatorMapBindings implements AutoCloseable {
    public static final String SCOPED_PROPERTY = "tapstate.operator.scoped";
    private static final String USER_CONTEXT_KEY = "tapstate.operator.map-bindings";

    @FunctionalInterface public interface Provider {
        /** Opens a fixed handle on the existing CACHE owner, retaining both supplied guards. */
        KeyedStateStore open(OperatorMapBinding original, Runnable requireBindingActive);
    }
    private final class Bound {
        final OperatorMapBinding binding;
        final AtomicBoolean active = new AtomicBoolean(true);
        KeyedStateStore store;
        Bound(OperatorMapBinding binding) { this.binding = binding; }
        void initialize() { store = forMap(this, Objects.requireNonNull(provider.open(binding, this::requireActive), "operator state handle")); }
        void requireActive() { if (closed || !active.get()) throw fenced(); }
        void retire() { active.set(false); store = null; }
    }
    private HazelcastInstance member;
    private final Provider provider;
    private final Map<String, Bound> slots = new HashMap<>();
    private final Map<String, Bound> hotMaps = new HashMap<>();
    private final Map<String, PendingStore> initializingMaps = new HashMap<>();
    private volatile boolean closed;

    private OperatorMapBindings(HazelcastInstance member, Provider provider) {
        this.member = member; this.provider = Objects.requireNonNull(provider, "provider");
    }

    /** A member has one immutable CACHE owner; another assembly cannot replace it. */
    public static OperatorMapBindings bindTo(HazelcastInstance member, Provider provider) {
        var created = new OperatorMapBindings(Objects.requireNonNull(member, "member"), provider);
        if (member.getUserContext().putIfAbsent(USER_CONTEXT_KEY, created) != null) throw fenced();
        return created;
    }

    /** A joining member can initialize migrated maps before its application assembly returns. */
    public static OperatorMapBindings prepare(Config config, Provider provider) {
        var created = new OperatorMapBindings(null, provider);
        if (Objects.requireNonNull(config, "config").getUserContext().putIfAbsent(USER_CONTEXT_KEY, created) != null) throw fenced();
        return created;
    }

    private synchronized void attach(HazelcastInstance candidate) {
        if (candidate.getUserContext().get(USER_CONTEXT_KEY) != this) throw fenced();
        if (member == null) member = candidate;
        else if (member != candidate && !member.getName().equals(candidate.getName())) throw fenced();
    }

    /** Absence is allowed only for the existing explicitly unscoped map configurations. */
    public static OperatorMapBindings resolve(HazelcastInstance member, boolean required) {
        Object value = member.getUserContext().get(USER_CONTEXT_KEY);
        if (value == null && !required) return null;
        if (!(value instanceof OperatorMapBindings bindings)) throw fenced();
        bindings.attach(member); return bindings;
    }
    public static boolean required(Properties properties) {
        String value = properties.getProperty(SCOPED_PROPERTY);
        if (value == null) return false;
        if (!"true".equals(value)) throw fenced();
        return true;
    }

    /** All candidates on this member publish together, after callbacks and map-liveness rechecks. */
    public synchronized void install(Collection<OperatorMapBinding> candidates) { publish(candidates, false); }

    /** Restores only original descriptors for scoped handles already initialized on this member. */
    public synchronized void attachExisting(Collection<OperatorMapBinding> originals) { publish(originals, true); }

    private void publish(Collection<OperatorMapBinding> candidates, boolean existingMaps) {
        List<OperatorMapBinding> captured = List.copyOf(candidates); requireOpen();
        Map<String, Bound> before = new LinkedHashMap<>(); Map<String, Bound> next = new LinkedHashMap<>();
        Map<String, PendingStore> pending = new LinkedHashMap<>();
        for (var original : captured) {
            String cold = original.placement().mapName(); if (before.containsKey(cold)) throw fenced();
            Bound current = slots.get(cold); before.put(cold, current);
            PendingStore waiting = initializingMaps.get(original.mapName()); pending.put(original.mapName(), waiting);
            if (existingMaps && waiting == null) throw fenced();
            if (waiting != null) waiting.requireCoordinates(original);
            if (current != null && current.binding.equals(original) && !current.active.get()) throw fenced();
        }
        for (var original : captured) {
            Bound current = before.get(original.placement().mapName());
            if (!existingMaps && (current == null || !current.binding.equals(original))) requireNoMap(original.mapName());
        }
        List<Bound> created = new ArrayList<>(); boolean published = false;
        try {
            for (var original : captured) {
                String cold = original.placement().mapName(); Bound current = before.get(cold);
                if (current != null && current.binding.equals(original)) {
                    Objects.requireNonNull(provider.open(original, current::requireActive), "operator state handle");
                    next.put(cold, current);
                } else {
                    Bound candidate = new Bound(original); created.add(candidate); candidate.initialize(); next.put(cold, candidate);
                }
            }
            before.forEach((cold, original) -> {
                if (!existingMaps && original != next.get(cold)) requireNoMap(next.get(cold).binding.mapName());
            });
            // Finish all external callbacks before the final, purely local publication checks.
            requireOpen();
            before.forEach((cold, original) -> { if (slots.get(cold) != original) throw fenced(); });
            next.values().forEach(Bound::requireActive);
            Map<String, Bound> byHotName = new HashMap<>(); next.values().forEach(bound -> byHotName.put(bound.binding.mapName(), bound));
            pending.forEach((name, original) -> {
                if (initializingMaps.get(name) != original) throw fenced();
                if (original != null) original.requireBinding(byHotName.get(name));
            });
            next.forEach((cold, candidate) -> {
                Bound previous = slots.put(cold, candidate);
                if (previous != null && previous != candidate) {
                    previous.retire(); hotMaps.remove(previous.binding.mapName(), previous); initializingMaps.remove(previous.binding.mapName());
                }
                hotMaps.put(candidate.binding.mapName(), candidate);
            });
            pending.forEach((name, waiting) -> { if (waiting != null) waiting.bound = byHotName.get(name); });
            published = true;
        } finally { if (!published) created.forEach(Bound::retire); }
    }

    public synchronized KeyedStateStore nest(String namespace, String configuredDatabase) {
        Bound original = require(namespace);
        if (original.binding.placement().resource() != OperatorPlacement.Resource.NEST_STATE
                || (configuredDatabase != null && !configuredDatabase.equals(original.binding.placement().database()))) throw fenced();
        return original.store;
    }
    public synchronized KeyedStateStore join(String namespace) {
        Bound original = require(namespace);
        boolean join = switch (original.binding.placement().resource()) {
            case JOIN_FACTS, JOIN_DIMENSIONS, JOIN_INDEXES, JOIN_WRITERS -> true;
            case NEST_STATE -> false;
        };
        if (!join) throw fenced();
        return original.store;
    }

    public synchronized KeyedStateStore initializingNest(String namespace, String configuredDatabase) {
        return initializing(namespace, true, configuredDatabase);
    }
    public synchronized KeyedStateStore initializingJoin(String namespace) { return initializing(namespace, false, null); }
    public synchronized Set<String> awaitingMapNames() {
        requireOpen(); return initializingMaps.entrySet().stream().filter(entry -> entry.getValue().bound == null)
                .map(Map.Entry::getKey).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
    private KeyedStateStore initializing(String name, boolean nest, String database) {
        requireOpen(); PendingStore existing = initializingMaps.get(name);
        if (existing != null) {
            if (existing.nest != nest || !Objects.equals(existing.database, database)) throw fenced();
            return existing;
        }
        PendingStore created = new PendingStore(name, nest, database); Bound admitted = hotMaps.get(name);
        if (admitted != null) { created.requireCoordinates(admitted.binding); admitted.requireActive(); created.bound = admitted; }
        initializingMaps.put(name, created); return created;
    }

    /** A name selects this local placeholder, but never supplies a cold location or authorization. */
    private final class PendingStore implements KeyedStateStore {
        final String name, database;
        final boolean nest;
        Bound bound;
        PendingStore(String name, boolean nest, String database) {
            this.name = Objects.requireNonNull(name, "namespace"); this.nest = nest; this.database = database;
        }
        void requireCoordinates(OperatorMapBinding original) {
            if (!name.equals(original.mapName()) || nest != (original.placement().resource() == OperatorPlacement.Resource.NEST_STATE)
                    || (database != null && !database.equals(original.placement().database()))) throw fenced();
        }
        void requireBinding(Bound original) { requireCoordinates(original.binding); if (bound != null && bound != original) throw fenced(); }
        private KeyedStateStore delegate(String namespace) {
            synchronized (OperatorMapBindings.this) {
                requireOpen(); if (!name.equals(namespace) || bound == null) throw fenced(); bound.requireActive(); return bound.store;
            }
        }
        @Override public Optional<byte[]> load(String namespace, String key) { return delegate(namespace).load(namespace, key); }
        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) { return delegate(namespace).loadAll(namespace, keys); }
        @Override public void save(String namespace, String key, byte[] value) { delegate(namespace).save(namespace, key, value); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) { return delegate(namespace).saveIfAbsent(namespace, key, value); }
        @Override public void delete(String namespace, String key) { delegate(namespace).delete(namespace, key); }
        @Override public void dropNamespace(String namespace) { delegate(namespace).dropNamespace(namespace); }
        @Override public long count(String namespace) { return delegate(namespace).count(namespace); }
    }

    /** The caller drains and destroys the original hot map before releasing its exact binding. */
    public synchronized boolean release(OperatorMapBinding original) {
        requireOpen(); Bound held = slots.get(original.placement().mapName());
        if (held == null || !held.binding.equals(original) || !held.active.get()) return false;
        requireNoMap(original.mapName()); held.retire(); hotMaps.remove(original.mapName(), held); initializingMaps.remove(original.mapName()); return true;
    }

    private KeyedStateStore forMap(Bound original, KeyedStateStore cold) {
        String hotName = original.binding.mapName(); String coldName = original.binding.placement().mapName();
        return new KeyedStateStore() {
            private void check(String namespace) { if (!hotName.equals(namespace)) throw fenced(); original.requireActive(); }
            @Override public Optional<byte[]> load(String namespace, String key) { check(namespace); return cold.load(coldName, key); }
            @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) { check(namespace); return cold.loadAll(coldName, keys); }
            @Override public void save(String namespace, String key, byte[] value) { check(namespace); cold.save(coldName, key, value); }
            @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) { check(namespace); return cold.saveIfAbsent(coldName, key, value); }
            @Override public void delete(String namespace, String key) { check(namespace); cold.delete(coldName, key); }
            @Override public void dropNamespace(String namespace) { check(namespace); cold.dropNamespace(coldName); }
            @Override public long count(String namespace) { check(namespace); return cold.count(coldName); }
        };
    }
    private Bound require(String name) { requireOpen(); Bound held = hotMaps.get(name); if (held == null) throw fenced(); held.requireActive(); return held; }
    private void requireNoMap(String name) {
        if (member == null) throw fenced();
        for (DistributedObject object : member.getDistributedObjects()) if (object instanceof IMap<?, ?> && name.equals(object.getName())) throw fenced();
    }
    private void requireOpen() { if (closed) throw fenced(); }
    /** Keep the closed sentinel bound, so a late factory cannot fall back to an unscoped store. */
    @Override public synchronized void close() { closed = true; slots.values().forEach(Bound::retire); hotMaps.clear(); slots.clear(); initializingMaps.clear(); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
