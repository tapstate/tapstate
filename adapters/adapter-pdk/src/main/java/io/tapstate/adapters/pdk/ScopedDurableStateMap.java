package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteValue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** The unchanged PDK map over captured configuration scopes, without namespace resolution in callbacks. */
final class ScopedDurableStateMap implements KVMap<Object> {
    private final RequiredNoteStore store;
    private final AtomicReference<RequiredNoteScope> scope;
    private final List<RequiredNoteScope> carriedFrom;
    private final boolean shared;
    private final Runnable requireActive;

    ScopedDurableStateMap(RequiredNoteStore store, RequiredNoteScope scope, List<RequiredNoteScope> carriedFrom,
            boolean shared, Runnable requireActive) {
        this.store = Objects.requireNonNull(store, "store");
        this.scope = new AtomicReference<>(Objects.requireNonNull(scope, "scope"));
        this.carriedFrom = List.copyOf(carriedFrom);
        this.shared = shared;
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (!shared && !carriedFrom.isEmpty()) throw new IllegalArgumentException("only shared notes carry earlier scopes");
    }

    @Override
    public void init(String mapKey, Class<Object> valueClass) {
        // Physical identity is captured before the connector is constructed.
    }

    @Override
    public void put(String key, Object value) {
        RequiredNoteScope captured = capture();
        RequiredNoteValue observed = store.read(captured, key);
        byte[] bytes = value == null ? null : ConnectorStateCodec.encode(value);
        if (!store.compareAndSet(captured, key, observed.revision(), bytes, shared && value == null)) {
            throw conflict();
        }
    }

    @Override
    public Object get(String key) {
        return decoded(observe(capture(), key));
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        RequiredNoteScope captured = capture();
        RequiredNoteValue current = observe(captured, key);
        if (current.present() || value == null) return decoded(current);
        if (store.compareAndSet(captured, key, current.revision(), ConnectorStateCodec.encode(value), false)) return null;
        // A competing successful insert supplies the incumbent, without another write attempt.
        RequiredNoteValue winner = store.read(captured, key);
        if (!winner.present()) throw conflict();
        return decoded(winner);
    }

    @Override
    public Object remove(String key) {
        RequiredNoteScope captured = capture();
        RequiredNoteValue current = observe(captured, key);
        Object previous = decoded(current);
        if (!store.compareAndSet(captured, key, current.revision(), null, shared)) throw conflict();
        return previous;
    }

    @Override
    public void clear() {
        RequiredNoteScope captured = capture();
        RequiredNoteScope next = store.clear(captured, shared);
        if (!scope.compareAndSet(captured, next)) throw unavailable();
        // Publication succeeded even if physical cleanup fails; this holder keeps its returned token.
        store.reclaimBefore(next);
    }

    @Override
    public void reset() {
        clear();
    }

    private RequiredNoteScope capture() {
        requireActive.run();
        return scope.get();
    }

    private RequiredNoteValue observe(RequiredNoteScope captured, String key) {
        RequiredNoteValue own = store.read(captured, key);
        if (own.present() || own.revision() > 0 || carriedFrom.isEmpty() || store.carryBlocked(captured, key)) return own;
        byte[] candidate = null;
        for (RequiredNoteScope earlier : carriedFrom) {
            RequiredNoteValue retained = store.read(earlier, key);
            if (!retained.present()) continue;
            byte[] value = retained.bytes();
            if (candidate != null && !Arrays.equals(candidate, value)) {
                throw new TapstateException(ConnectorError.STATE_UNREADABLE,
                        Map.of("detail", "earlier connector notes disagree; verify their retained scopes before resuming"), null);
            }
            candidate = value;
        }
        if (candidate == null) return own;
        // Unreadable private bytes must not become authoritative shared notes.
        ConnectorStateCodec.decode(candidate);
        if (store.compareAndSet(captured, key, own.revision(), candidate, false)) {
            return new RequiredNoteValue(own.revision() + 1, candidate);
        }
        RequiredNoteValue winner = store.read(captured, key);
        if (!winner.present()) throw conflict();
        return winner;
    }

    private static Object decoded(RequiredNoteValue value) {
        return value.present() ? ConnectorStateCodec.decode(value.bytes()) : null;
    }

    private static TapstateException conflict() {
        return new TapstateException(IoError.NOTE_WRITE_CONFLICT, Map.of(), null);
    }

    private static TapstateException unavailable() {
        return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
    }
}
