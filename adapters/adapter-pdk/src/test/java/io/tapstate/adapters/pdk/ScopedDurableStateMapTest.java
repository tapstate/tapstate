package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Host-side command sequencing; database atomicity is witnessed in the Mongo adapter's real tests. */
class ScopedDurableStateMapTest {
    private static final RequiredNoteScope PRIMARY = chain("capture", 1);
    private static final RequiredNoteScope EARLIER = pipeline("p1", "source");

    @Test
    void aWriteThatEnteredBeforeClearKeepsItsOriginalToken() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RequiredNoteScope next = new RequiredNoteScope(PRIMARY.owner(), 2);
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> {
                if (args[0].equals(PRIMARY)) {
                    entered.countDown();
                    assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                } else {
                    assertThat(args[0]).isEqualTo(next);
                }
                yield new RequiredNoteValue(0, null);
            }
            case "clear" -> { assertThat(args[0]).isEqualTo(PRIMARY); yield next; }
            case "reclaimBefore" -> { assertThat(args[0]).isEqualTo(next); yield 0L; }
            case "compareAndSet" -> {
                assertThat(args[0]).isEqualTo(PRIMARY);
                throw unavailable();
            }
            default -> throw unexpected(method);
        });
        var map = map(store, PRIMARY, List.of(), true);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var old = executor.submit(() -> map.put("key", "late"));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                map.clear();
                release.countDown();
                assertThatThrownBy(() -> old.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                assertThat(map.get("key")).isNull();
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void aConflictingPutDoesNotRefreshItsReadOrRetryItsWrite() {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> {
                assertThat(reads.incrementAndGet()).isEqualTo(1);
                yield value(5, "old");
            }
            case "compareAndSet" -> {
                assertThat(args[0]).isEqualTo(PRIMARY);
                assertThat(args[2]).isEqualTo(5L);
                assertThat(writes.incrementAndGet()).isEqualTo(1);
                yield false;
            }
            default -> throw unexpected(method);
        });
        assertThatThrownBy(() -> map(store, PRIMARY, List.of(), false).put("key", "new"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.NOTE_WRITE_CONFLICT));
        assertThat(reads.get()).isEqualTo(1);
        assertThat(writes.get()).isEqualTo(1);
    }

    @Test
    void aDeletionTombstoneDoesNotCarryAnEarlierPrivateValue() {
        RequiredNoteStore store = scripted((method, args) -> {
            assertThat(method).isEqualTo("read");
            assertThat(args[0]).isEqualTo(PRIMARY);
            return new RequiredNoteValue(2, null);
        });
        assertThat(map(store, PRIMARY, List.of(EARLIER), true).get("key")).isNull();
    }

    @Test
    void conflictingEarlierScopesRefuseBeforePublishingAnyCarry() {
        var another = pipeline("p2", "source");
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "carryBlocked" -> false;
            case "read" -> args[0].equals(PRIMARY) ? new RequiredNoteValue(0, null)
                    : value(1, args[0].equals(EARLIER) ? "left" : "right");
            default -> throw unexpected(method);
        });
        assertThatThrownBy(() -> map(store, PRIMARY, List.of(EARLIER, another), true).get("key"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ConnectorError.STATE_UNREADABLE));
    }

    @Test
    void putIfAbsentReportsACompetingIncumbentWithoutAnotherWrite() {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> reads.incrementAndGet() == 1 ? new RequiredNoteValue(0, null) : value(1, "incumbent");
            case "compareAndSet" -> {
                assertThat(writes.incrementAndGet()).isEqualTo(1);
                assertThat(args[2]).isEqualTo(0L);
                yield false;
            }
            default -> throw unexpected(method);
        });
        assertThat(map(store, PRIMARY, List.of(), false).putIfAbsent("key", "candidate")).isEqualTo("incumbent");
        assertThat(reads.get()).isEqualTo(2);
        assertThat(writes.get()).isEqualTo(1);
    }

    @Test
    void aDeleteWinningPutIfAbsentIsNotReportedAsTheCandidateBeingStored() {
        AtomicInteger reads = new AtomicInteger();
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> new RequiredNoteValue(reads.getAndIncrement(), null);
            case "compareAndSet" -> false;
            default -> throw unexpected(method);
        });
        assertThatThrownBy(() -> map(store, PRIMARY, List.of(), false).putIfAbsent("key", "candidate"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.NOTE_WRITE_CONFLICT));
    }

    @Test
    void cleanupFailureDoesNotPutTheSuccessfulClearHolderBackOnItsOldToken() {
        var next = new RequiredNoteScope(PRIMARY.owner(), 2);
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "clear" -> { assertThat(args[0]).isEqualTo(PRIMARY); assertThat(args[1]).isEqualTo(true); yield next; }
            case "reclaimBefore" -> throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "fixture"), null);
            case "carryBlocked" -> true;
            case "read" -> {
                if (!args[0].equals(next)) throw unavailable();
                yield new RequiredNoteValue(0, null);
            }
            default -> throw unexpected(method);
        });
        var holder = map(store, PRIMARY, List.of(EARLIER), true);
        var otherOldHandle = map(store, PRIMARY, List.of(), true);
        assertThatThrownBy(holder::clear).isInstanceOf(TapstateException.class);
        assertThat(holder.get("key")).isNull();
        assertThatThrownBy(() -> otherOldHandle.get("key")).isInstanceOf(TapstateException.class);
    }

    @Test
    void sharedRemoveKeepsTheObservedRevisionAndRequestsAnAtomicCarryBarrier() {
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> value(3, "previous");
            case "compareAndSet" -> {
                assertThat(args[0]).isEqualTo(PRIMARY);
                assertThat(args[2]).isEqualTo(3L);
                assertThat(args[3]).isNull();
                assertThat(args[4]).isEqualTo(true);
                yield true;
            }
            default -> throw unexpected(method);
        });
        assertThat(map(store, PRIMARY, List.of(EARLIER), true).remove("key")).isEqualTo("previous");
    }

    @Test
    @SuppressWarnings("unchecked")
    void returnedValuesRemainDetachedSnapshots() {
        byte[] encoded = ConnectorStateCodec.encode(new ArrayList<>(List.of("original")));
        RequiredNoteStore store = scripted((method, args) -> {
            assertThat(method).isEqualTo("read");
            return new RequiredNoteValue(1, encoded);
        });
        var map = map(store, PRIMARY, List.of(), false);
        List<Object> edited = (List<Object>) map.get("key");
        edited.add("local");
        assertThat(map.get("key")).isEqualTo(List.of("original"));
    }

    @Test
    void unreadableEarlierNotesRefuseBeforePublishingCarry() {
        AtomicInteger writes = new AtomicInteger();
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> args[0].equals(PRIMARY) ? new RequiredNoteValue(0, null)
                    : new RequiredNoteValue(1, new byte[]{0});
            case "carryBlocked" -> false;
            case "compareAndSet" -> { writes.incrementAndGet(); yield true; }
            default -> throw unexpected(method);
        });
        assertThatThrownBy(() -> map(store, PRIMARY, List.of(EARLIER), true).get("key"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ConnectorError.STATE_UNREADABLE));
        assertThat(writes.get()).isZero();
    }

    @Test
    void unreadableCurrentNotesRefuseBeforeDeletion() {
        AtomicInteger writes = new AtomicInteger();
        RequiredNoteStore store = scripted((method, args) -> switch (method) {
            case "read" -> new RequiredNoteValue(1, new byte[]{0});
            case "compareAndSet" -> { writes.incrementAndGet(); yield true; }
            default -> throw unexpected(method);
        });
        assertThatThrownBy(() -> map(store, PRIMARY, List.of(), false).remove("key"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ConnectorError.STATE_UNREADABLE));
        assertThat(writes.get()).isZero();
    }

    @Test
    void requiredScopesAreResolvedBeforeAnyPluginClassCanBeLoaded(@TempDir Path dir) {
        var reference = new ConnectorRef(List.of(dir.resolve("absent.jar")), "absent.Plugin", "2.0.8", null);
        var scopes = ConnectorNoteScopes.of(scripted((method, args) -> { throw unexpected(method); }),
                namespace -> { throw unavailable(); }, () -> {});
        assertThatThrownBy(() -> PdkConnector.open("demo", reference, Map.of(), new PipelineNode("p1", "source"),
                null, null, scopes)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));
    }

    @Test
    void theConfiguredContextUsesFixedScopesAndRefusesNotesAfterClose(@TempDir Path dir) {
        var reference = new ConnectorRef(List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);
        var local = pipeline("p1", "source");
        var global = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.global-state",
                RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0), 1);
        List<String> opened = new ArrayList<>();
        AtomicInteger reads = new AtomicInteger();
        var store = scripted((method, args) -> {
            assertThat(method).isEqualTo("read");
            reads.incrementAndGet();
            return value(1, args[0].equals(local) ? "local" : "global");
        });
        var scopes = ConnectorNoteScopes.of(store, namespace -> {
            opened.add(namespace);
            return namespace.equals(local.owner().namespace()) ? local : global;
        }, () -> {});
        KeyedStateStore cache = (KeyedStateStore) Proxy.newProxyInstance(KeyedStateStore.class.getClassLoader(),
                new Class<?>[]{KeyedStateStore.class}, (proxy, method, args) -> { throw new AssertionError("notes reached cache"); });
        PdkConnector connector = PdkConnector.open("demo", reference, Map.of(), new PipelineNode("p1", "source"),
                cache, null, scopes);
        var notes = connector.context().getStateMap();
        var globalNotes = connector.context().getGlobalStateMap();
        try {
            assertThat(notes).isSameAs(connector.context().getStateMap());
            assertThat(notes.get("key")).isEqualTo("local");
            assertThat(globalNotes.get("key")).isEqualTo("global");
            assertThat(opened).containsExactly(local.owner().namespace(), "pdk.global-state");
        } finally {
            connector.close();
        }
        int beforeClosedReads = reads.get();
        assertThatThrownBy(() -> notes.get("key")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> globalNotes.get("key")).isInstanceOf(TapstateException.class);
        assertThat(reads.get()).isEqualTo(beforeClosedReads);
        assertThat(opened).hasSize(2);
    }

    private static ScopedDurableStateMap map(RequiredNoteStore store, RequiredNoteScope scope,
            List<RequiredNoteScope> earlier, boolean shared) {
        return new ScopedDurableStateMap(store, scope, earlier, shared, () -> {});
    }

    private static RequiredNoteScope chain(String id, long generation) {
        return new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.chain." + id, RequiredNoteOwner.Kind.CHAIN, id, 0), generation);
    }

    private static RequiredNoteScope pipeline(String id, String node) {
        return new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.state." + id + "." + node,
                RequiredNoteOwner.Kind.PIPELINE, id, 1), 1);
    }

    private static RequiredNoteValue value(long revision, Object value) {
        return new RequiredNoteValue(revision, ConnectorStateCodec.encode(value));
    }

    private static TapstateException unavailable() {
        return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
    }

    private static AssertionError unexpected(String method) {
        return new AssertionError("unexpected note operation: " + method);
    }

    private static RequiredNoteStore scripted(Script script) {
        return (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> {
                    // Database retry checks are witnessed against Mongo; these scripts inspect PDK commands.
                    if (method.getName().equals("guardedBy")) return proxy;
                    return script.call(method.getName(), args);
                });
    }

    @FunctionalInterface
    private interface Script { Object call(String method, Object[] args) throws Exception; }
}
