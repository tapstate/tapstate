package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RegistrationGrowthBatchingTest {
    @Test void serializedUnionCarriersFindTheirCompositeBucketKeys() {
        Fixture f = new Fixture();
        f.serialize = true;
        Object first = NestLookup.bucketKey(List.of("C1", 1), 7);
        Object second = NestLookup.bucketKey(List.of("C2", 2), 9);
        f.store.addAll(Map.of(first, Set.of(List.of(11L)), second, Set.of(List.of(12L))));
        assertThat(f.states).containsEntry(first, Set.of(List.of(11L))).containsEntry(second, Set.of(List.of(12L)));
    }

    @Test void aUnionBatchKeepsExistingMembersWithoutMutatingResidentSetAliases() {
        Fixture f = new Fixture();
        Set<Object> before = new LinkedHashSet<>(Set.of("old"));
        f.states.put("a", before);
        f.store.addAll(Map.of("a", Set.of("new"), "b", Set.of("second")));
        assertThat(f.calls).isEqualTo(1);
        assertThat(before).containsExactly("old");
        assertThat(f.states.get("a")).containsExactlyInAnyOrder("old", "new");
        assertThat(f.states.get("b")).containsExactly("second");
    }

    @Test void aProtectedRetryConvergesAfterAnAlreadyAppliedPartitionPrefix() {
        Fixture f = new Fixture();
        f.protectionFailure = true;
        f.store.addAll(Map.of("a", Set.of("first"), "b", Set.of("second")));
        assertThat(f.calls).isEqualTo(2);
        assertThat(f.states).containsEntry("a", Set.of("first")).containsEntry("b", Set.of("second"));
    }

    @Test void anOrdinaryPartialFailureEscapesWithoutALocalRetry() {
        Fixture f = new Fixture();
        f.ordinaryFailure = true;
        Map<Object, Set<Object>> additions = Map.of("a", Set.of("first"), "b", Set.of("second"));
        assertThatThrownBy(() -> f.store.addAll(additions))
                .isInstanceOf(IllegalStateException.class).hasMessage("injected store failure");
        assertThat(f.calls).isEqualTo(1);
        assertThat(f.states).hasSize(1);
    }

    @SuppressWarnings("unchecked")
    private static final class Fixture {
        final Map<Object, Set<Object>> states = new LinkedHashMap<>();
        final IMap<Object, Set<Object>> map;
        final MapNestStore<Set<Object>> store;
        boolean protectionFailure;
        boolean ordinaryFailure;
        boolean serialize;
        int calls;

        Fixture() {
            map = (IMap<Object, Set<Object>>) Proxy.newProxyInstance(IMap.class.getClassLoader(),
                    new Class<?>[] {IMap.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "getName" -> "refs";
                        case "executeOnKeys" -> apply((Set<Object>) args[0],
                                (EntryProcessor<Object, Set<Object>, ?>) args[1]);
                        default -> throw new AssertionError("unexpected map call: " + method.getName());
                    });
            store = new MapNestStore<>(map);
        }

        private Map<Object, Object> apply(Set<Object> keys, EntryProcessor<Object, Set<Object>, ?> processor) throws Exception {
            calls++;
            if (serialize) {
                var bytes = new ByteArrayOutputStream();
                try (var output = new ObjectOutputStream(bytes)) { output.writeObject(processor); }
                try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                    processor = (EntryProcessor<Object, Set<Object>, ?>) input.readObject();
                }
            }
            for (Object key : keys) {
                var entry = new AbstractMap.SimpleEntry<>(key, states.get(key));
                processor.process(entry);
                states.put(key, entry.getValue());
                if (calls == 1 && protectionFailure) throw new SplitBrainProtectionException("injected refusal");
                if (ordinaryFailure) throw new IllegalStateException("injected store failure");
            }
            return Map.of();
        }
    }
}
