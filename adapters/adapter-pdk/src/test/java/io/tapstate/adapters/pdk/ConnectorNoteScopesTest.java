package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConnectorNoteScopesTest {
    private static final RequiredNoteScope ORIGINAL = new RequiredNoteScope(
            RequiredNoteOwner.forNode(1, new PipelineNode("pipeline", "source"), PipelineNode.Purpose.CAPTURE, 1), 1);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void composingGuardsChecksEachBoundaryOnceWithoutMultiplyingTheOriginalProof(boolean readStore) {
        var f = new Fixture();
        if (readStore) assertThat(f.scopes.store().origin(ORIGINAL)).isSameAs(f.origin);
        else assertThat(f.scopes.open(ORIGINAL.owner().namespace())).isSameAs(ORIGINAL);
        assertThat(f.calls).containsExactly("original", "release", "connector", "access", "original", "release", "connector");
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2})
    void everyOriginalOrAdditionalRevocationStillPreventsBothKindsOfAccess(int revoked) {
        var f = new Fixture();
        f.active.get(revoked).set(false);
        assertThatThrownBy(() -> f.scopes.open(ORIGINAL.owner().namespace())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> f.scopes.store().origin(ORIGINAL)).isInstanceOf(TapstateException.class);
        assertThat(f.accesses).hasValue(0);
    }

    @Test void aGuardRevokedByTheReadStillPreventsPublishingItsResult() {
        var f = new Fixture();
        f.afterAccess = () -> f.active.get(1).set(false);
        assertThatThrownBy(() -> f.scopes.store().origin(ORIGINAL)).isInstanceOf(TapstateException.class);
        assertThat(f.accesses).hasValue(1);
    }

    @Test void pinningRetainsTheOriginalScopeWithoutReopeningItOrDuplicatingItsGuards() {
        var f = new Fixture();
        var pinned = BoundConnectorNoteScopes.pin(f.scopes, ORIGINAL);
        assertThat(pinned.open(ORIGINAL.owner().namespace())).isSameAs(ORIGINAL);
        assertThat(f.accesses).hasValue(0);
        assertThat(f.calls).containsExactly("original", "release", "connector", "original", "release", "connector");
        f.active.get(1).set(false);
        assertThatThrownBy(() -> pinned.open(ORIGINAL.owner().namespace())).isInstanceOf(TapstateException.class);
    }

    @Test void anOpenerThatRevokesItsGuardCannotPublishTheResolvedScope() {
        var f = new Fixture();
        f.afterAccess = () -> f.active.get(2).set(false);
        assertThatThrownBy(() -> f.scopes.open(ORIGINAL.owner().namespace())).isInstanceOf(TapstateException.class);
        assertThat(f.accesses).hasValue(1);
    }

    @Test void pinningACustomProviderPreservesItsGuardAndOtherNamespaceResolver() {
        var f = new Fixture();
        var global = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.global-state", RequiredNoteOwner.Kind.GLOBAL, "cluster", 0), 1);
        var opened = new AtomicInteger();
        ConnectorNoteScopes custom = new ConnectorNoteScopes() {
            @Override public RequiredNoteStore store() { return f.scopes.store(); }
            @Override public void requireActive() { f.scopes.requireActive(); }
            @Override public RequiredNoteScope open(String namespace) {
                opened.incrementAndGet();
                assertThat(namespace).isEqualTo(global.owner().namespace());
                return global;
            }
        };
        var pinned = BoundConnectorNoteScopes.pin(custom, ORIGINAL);
        assertThat(pinned.open(ORIGINAL.owner().namespace())).isSameAs(ORIGINAL);
        assertThat(opened).hasValue(0);
        assertThat(pinned.open(global.owner().namespace())).isSameAs(global);
        assertThat(opened).hasValue(1);
        f.active.get(0).set(false);
        assertThatThrownBy(() -> pinned.open(ORIGINAL.owner().namespace())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> pinned.store().origin(ORIGINAL)).isInstanceOf(TapstateException.class);
        assertThat(f.accesses).hasValue(0);
    }

    private static final class Fixture {
        final List<String> calls = new ArrayList<>();
        final List<AtomicBoolean> active = List.of(new AtomicBoolean(true), new AtomicBoolean(true), new AtomicBoolean(true));
        final AtomicInteger accesses = new AtomicInteger();
        final RequiredNoteOrigin origin = ConnectorNoteOrigin.capture("connector", "a".repeat(64), Map.of("value", "original"));
        Runnable afterAccess = () -> {};
        final ConnectorNoteScopes scopes = ConnectorNoteScopes.of(store(List.of()), namespace -> {
            access(); return ORIGINAL;
        }, guard(0, "original")).guardedBy(guard(1, "release")).guardedBy(guard(2, "connector"));

        private Runnable guard(int id, String name) {
            return () -> {
                calls.add(name);
                if (!active.get(id).get()) throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
            };
        }
        private void access() {
            calls.add("access"); accesses.incrementAndGet(); afterAccess.run();
        }
        // The fake preserves nested store guards, unlike a no-op guardedBy test double.
        private RequiredNoteStore store(List<Runnable> guards) {
            return (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                    new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("guardedBy")) {
                            var next = new ArrayList<>(guards); next.add((Runnable) args[0]); return store(List.copyOf(next));
                        }
                        if (method.getName().equals("origin")) {
                            guards.forEach(Runnable::run);
                            assertThat(args[0]).isEqualTo(ORIGINAL);
                            access();
                            guards.forEach(Runnable::run);
                            return origin;
                        }
                        if (method.getName().equals("toString")) return "guard-aware-note-store";
                        throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
