package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the batch read means for a store that has not implemented one. Every implementation inherits
 * this, so what it answers here is what six of the seven answer everywhere.
 *
 * <p>The batch is by named keys, which is what keeps it on the permitted side of the ban on
 * enumeration: the caller already holds the keys and is asking for their states, never for a listing
 * of what a namespace contains. A store may answer them in one round trip; the default answers them
 * one at a time, which is what the callers had been doing themselves.
 */
class KeyedStateStoreTest {

    @Test
    void theKeysAStoreHasComeBackUnderTheNamesTheCallerAskedFor() {
        RecordingStore store = new RecordingStore();
        store.save("ns", "a", bytes("state of a"));
        store.save("ns", "b", bytes("state of b"));

        assertThat(store.loadAll("ns", List.of("a", "b")))
                .containsOnlyKeys("a", "b")
                .containsEntry("a", bytes("state of a"))
                .containsEntry("b", bytes("state of b"));
    }

    @Test
    void aKeyWithNoStateIsAbsentFromTheAnswerRatherThanNull() {
        RecordingStore store = new RecordingStore();
        store.save("ns", "a", bytes("state of a"));

        Map<String, byte[]> loaded = store.loadAll("ns", List.of("a", "never-saved"));

        // A null value would read as "this key has no state" at every call site that iterates the
        // answer, and as a state at every one that asks the map whether it holds the key. Absence is
        // the one answer both readings agree on.
        assertThat(loaded).containsOnlyKeys("a");
    }

    @Test
    void aNamespaceNeverAnswersForAnothersKeyOfTheSameName() {
        RecordingStore store = new RecordingStore();
        store.save("mine", "shared", bytes("mine"));
        store.save("theirs", "shared", bytes("theirs"));

        assertThat(store.loadAll("mine", List.of("shared"))).containsEntry("shared", bytes("mine"));
    }

    @Test
    void askingForNothingReachesTheStoreNotAtAll() {
        RecordingStore store = new RecordingStore();

        assertThat(store.loadAll("ns", List.of())).isEmpty();
        // An empty batch is a legitimate thing for a caller to arrive with - a change touching only
        // keys already in memory - and a store that turned it into a round trip would pay for it on
        // the event path, where it is the common case rather than the odd one.
        assertThat(store.loadsAsked).isEmpty();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** A store with only the single-key read implemented: what six of the seven implementations are. */
    private static final class RecordingStore implements KeyedStateStore {

        private final Map<String, byte[]> entries = new HashMap<>();
        private final List<String> loadsAsked = new ArrayList<>();

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            loadsAsked.add(key);
            return Optional.ofNullable(entries.get(namespace + " " + key));
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            entries.put(namespace + " " + key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            entries.remove(namespace + " " + key);
        }

        @Override
        public void dropNamespace(String namespace) {
            entries.keySet().removeIf(id -> id.startsWith(namespace + " "));
        }

        @Override
        public long count(String namespace) {
            return entries.keySet().stream().filter(id -> id.startsWith(namespace + " ")).count();
        }
    }
}
