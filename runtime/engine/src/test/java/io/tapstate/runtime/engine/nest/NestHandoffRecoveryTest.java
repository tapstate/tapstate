package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestStateStore;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

import static io.tapstate.runtime.engine.nest.NestFixtures.*;
import static org.assertj.core.api.Assertions.*;

class NestHandoffRecoveryTest {
    @Test void pagingStopsAtBackpressureAndPreservesEveryOriginalKey() throws Exception {
        var rows = new ArrayList<NestStateStore.Pending>();
        for (int i = 0; i < 70; i++) rows.add(new NestStateStore.Pending(String.format("%03d", i),
                NestHandoffRecovery.encodeKey(List.of("key-" + i))));
        var reads = new ArrayList<Integer>();
        var source = new NestHandoffRecovery((after, limit) -> {
            reads.add(limit);
            return rows.stream().filter(row -> after == null || row.cursor().compareTo(after) > 0).limit(limit).toList();
        });
        var out = new TestOutbox(1); source.init(out, new TestProcessorContext());
        assertThat(source.complete()).isFalse();
        assertThat(source.complete()).isFalse();
        assertThat(reads).containsExactly(64);
        var delivered = new ArrayList<Object>();
        out.drainQueueAndReset(0, delivered, false);
        boolean complete = false;
        for (int turn = 0; turn < 100 && !complete; turn++) {
            complete = source.complete(); out.drainQueueAndReset(0, delivered, false);
        }
        assertThat(complete).isTrue();
        assertThat(delivered).extracting(item -> ((NestHandoffRecovery.Key)item).value())
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, 70).mapToObj(i -> List.of("key-" + i)).toList());
        assertThat(reads).containsExactly(64, 64, 64);
    }

    @Test void malformedStoredKeysAndNonAdvancingCursorsRefuseWithACode() throws Exception {
        for (var pages : List.<NestHandoffRecovery.Pages>of(
                (after, limit) -> List.of(new NestStateStore.Pending("a", new byte[]{1})),
                (after, limit) -> List.of(new NestStateStore.Pending("a", NestHandoffRecovery.encodeKey("not-a-tuple"))),
                (after, limit) -> List.of(new NestStateStore.Pending("a", NestHandoffRecovery.encodeKey(List.of("k"))),
                        new NestStateStore.Pending("a", NestHandoffRecovery.encodeKey(List.of("k")))))) {
            var source = new NestHandoffRecovery(pages); source.init(new TestOutbox(16), new TestProcessorContext());
            assertThatThrownBy(source::complete).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test void payloadAndPendingIdentityTravelThroughTheSameMapStoreWrite() {
        var cold = new Cold(); var callbacks = callbacks(cold); var key = List.<Object>of("P1");
        var state = new ResolverState();
        var delivery = new KeyedElement(List.of("C1"), new NestElement(
                element(List.of("policies", "claims"), List.of("P1"), "K1", null),
                row("value", 1), at(1), Map.of()), 2);
        state.retainRelease(at(2), Map.of("policy", at(50)), List.of(delivery), 1);
        callbacks.store(key, state);
        assertThat(cold.pending("state", null, 64)).hasSize(1);
        assertThat(((ResolverState)callbacks.load(key)).releases()).hasSize(1);
        // An old release can retire and a new release arrive in one drain; index the final payload.
        state.forgetRelease(state.releases().getFirst());
        state.retainRelease(at(3), Map.of("policy", at(60)), List.of(delivery), 1);
        callbacks.store(key, state);
        assertThat(cold.pending("state", null, 64)).hasSize(1);
        assertThat(((ResolverState)callbacks.load(key)).releases().getFirst().order()).isEqualTo(at(3));
        state.forgetRelease(state.releases().getFirst()); callbacks.store(key, state);
        assertThat(cold.pending("state", null, 64)).isEmpty();
        assertThat(callbacks.loadAllKeys()).isEmpty();
    }

    @Test void corruptPayloadsAndOldSerialIdentitiesAreRefusedRatherThanLoadedAsEmpty() {
        var cold = new Cold(); var callbacks = callbacks(cold); var key = List.of("P1");
        callbacks.store(key, new ResolverState());
        var row = cold.rows.values().iterator().next();
        byte[] old = row.state().clone();
        byte[] name = ResolverState.class.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int offset = indexOf(old, name) + name.length;
        assertThat(offset).isGreaterThan(name.length);
        java.nio.ByteBuffer.wrap(old, offset, Long.BYTES).putLong(1L);
        for (byte[] bytes : List.of(old, new byte[]{1, 2, 3})) {
            cold.save("state", NestStateKeys.nameOf(key), bytes);
            assertThatThrownBy(() -> callbacks.load(key)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    private static int indexOf(byte[] bytes, byte[] text) {
        for (int i = 0; i + text.length <= bytes.length; i++) {
            if (Arrays.equals(bytes, i, i + text.length, text, 0, text.length)) return i;
        }
        return -1;
    }

    private static NestStateMapStore callbacks(Cold cold) {
        var context = new ConcurrentHashMap<String, Object>();
        var member = (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
                new Class<?>[]{HazelcastInstance.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getUserContext")) return context;
                    throw new UnsupportedOperationException(method.getName());
                });
        NestStateMapStoreFactory.bindTo(member, cold);
        var callbacks = new NestStateMapStore("state"); callbacks.init(member, new Properties(), "state");
        return callbacks;
    }

    private record Row(byte[] state, byte[] pendingKey) { }
    private static final class Cold implements NestStateStore {
        private final Map<String, Row> rows = new LinkedHashMap<>();
        @Override public Optional<byte[]> load(String ns, String key) { return Optional.ofNullable(rows.get(key)).map(Row::state); }
        @Override public void save(String ns, String key, byte[] value) { saveNest(ns, key, value, null); }
        @Override public void saveNest(String ns, String key, byte[] value, byte[] pending) { rows.put(key, new Row(value, pending)); }
        @Override public List<Pending> pending(String ns, String after, int limit) {
            return rows.entrySet().stream().filter(entry -> entry.getValue().pendingKey() != null)
                    .filter(entry -> after == null || entry.getKey().compareTo(after) > 0).sorted(Map.Entry.comparingByKey())
                    .limit(limit).map(entry -> new Pending(entry.getKey(), entry.getValue().pendingKey())).toList();
        }
        @Override public Optional<byte[]> saveIfAbsent(String ns, String key, byte[] value) { throw new UnsupportedOperationException(); }
        @Override public void delete(String ns, String key) { rows.remove(key); }
        @Override public void dropNamespace(String ns) { rows.clear(); }
        @Override public long count(String ns) { return rows.size(); }
    }
}
