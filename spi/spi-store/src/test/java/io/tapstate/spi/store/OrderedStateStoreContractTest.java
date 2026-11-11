package io.tapstate.spi.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.spi.store.OrderedStateStore.End;
import io.tapstate.spi.store.OrderedStateStore.Entry;
import io.tapstate.spi.store.OrderedStateStore.Limit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * What every {@link OrderedStateStore} owes its caller, stated against the plainest store that can meet
 * it. A store with something remote behind it runs the same statements against itself.
 */
class OrderedStateStoreContractTest {

    /** A sorted map under the contract: the reference a real store is compared with. */
    static final class InMemory implements OrderedStateStore {
        private final TreeMap<byte[], byte[]> entries = new TreeMap<>(Arrays::compareUnsigned);
        private final Limit limit;

        InMemory(Limit limit) {
            this.limit = limit;
        }

        @Override
        public List<byte[]> loadAll(List<byte[]> keys) {
            List<byte[]> states = new ArrayList<>();
            keys.forEach(key -> {
                byte[] state = entries.get(key);
                states.add(state == null ? null : state.clone());
            });
            return states;
        }

        @Override
        public void commit(List<Entry> saves, List<byte[]> deletes) {
            long bytes = 0;
            TreeMap<byte[], Boolean> seen = new TreeMap<>(Arrays::compareUnsigned);
            for (Entry save : saves) {
                bytes += save.key().length + save.state().length;
                if (seen.put(save.key(), true) != null) {
                    throw new IllegalArgumentException("a key appears twice in one commit");
                }
            }
            for (byte[] delete : deletes) {
                bytes += delete.length;
                if (seen.put(delete, true) != null) {
                    throw new IllegalArgumentException("a key appears twice in one commit");
                }
            }
            if (saves.size() + deletes.size() > limit.operations() || bytes > limit.bytes()) {
                throw new IllegalArgumentException("a commit beyond the limit");
            }
            saves.forEach(save -> entries.put(save.key().clone(), save.state().clone()));
            deletes.forEach(entries::remove);
        }

        @Override
        public Limit commitLimit() {
            return limit;
        }

        @Override
        public Optional<Entry> first(byte[] from, byte[] to, End end) {
            var range = entries.subMap(from, true, to, false);
            var found = end == End.LOWEST ? range.firstEntry() : range.lastEntry();
            return Optional.ofNullable(found).map(entry -> new Entry(entry.getKey().clone(), entry.getValue().clone()));
        }

        @Override
        public List<Entry> scan(byte[] from, byte[] to, byte[] after, int limit) {
            var range = after == null ? entries.subMap(from, true, to, false) : entries.subMap(after, false, to, false);
            List<Entry> page = new ArrayList<>();
            for (var entry : range.entrySet()) {
                if (page.size() == limit) {
                    break;
                }
                page.add(new Entry(entry.getKey().clone(), entry.getValue().clone()));
            }
            return page;
        }
    }

    private final OrderedStateStore store = new InMemory(new Limit(4, 64));

    private static byte[] key(int... bytes) {
        byte[] key = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            key[i] = (byte) bytes[i];
        }
        return key;
    }

    private static Entry entry(byte[] key, int state) {
        return new Entry(key, new byte[] {(byte) state});
    }

    @Test
    void aCommitSavesAndDeletesTogether() {
        store.commit(List.of(entry(key(1), 10), entry(key(2), 20)), List.of());
        store.commit(List.of(entry(key(1), 11), entry(key(3), 30)), List.of(key(2)));

        assertThat(store.loadAll(List.of(key(3), key(2), key(1))))
                .containsExactly(new byte[] {30}, null, new byte[] {11});
    }

    @Test
    void aCommitBeyondTheLimitWritesNothing() {
        store.commit(List.of(entry(key(1), 10)), List.of());
        List<Entry> tooMany = List.of(entry(key(1), 99), entry(key(2), 2), entry(key(3), 3), entry(key(4), 4));

        assertThatThrownBy(() -> store.commit(tooMany, List.of(key(5)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.commit(List.of(new Entry(key(6), new byte[64])), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.loadAll(List.of(key(1), key(2), key(6)))).containsExactly(new byte[] {10}, null, null);
    }

    @Test
    void aKeyNamedTwiceInOneCommitIsRefused() {
        assertThatThrownBy(() -> store.commit(List.of(entry(key(1), 1)), List.of(key(1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.loadAll(List.of(key(1)))).containsExactly((byte[]) null);
    }

    @Test
    void keysSortUnsignedAndAShorterKeyComesBeforeTheKeysItBegins() {
        store.commit(List.of(entry(key(1, 0xFF), 3), entry(key(1), 1), entry(key(1, 0x00), 2), entry(key(0x80), 4)),
                List.of());

        assertThat(store.scan(key(0), key(0xFF), null, 10)).extracting(Entry::state)
                .containsExactly(new byte[] {1}, new byte[] {2}, new byte[] {3}, new byte[] {4});
    }

    @Test
    void firstReturnsEitherEndOfARangeThatExcludesItsUpperBound() {
        store.commit(List.of(entry(key(1, 5), 15), entry(key(1, 7), 17), entry(key(2), 20)), List.of());

        assertThat(store.first(key(1), key(2), End.LOWEST)).map(Entry::state).contains(new byte[] {15});
        assertThat(store.first(key(1), key(2), End.HIGHEST)).map(Entry::state).contains(new byte[] {17});
        assertThat(store.first(key(1, 5), key(1, 7), End.HIGHEST)).map(Entry::key).contains(key(1, 5));
        assertThat(store.first(key(1, 6), key(1, 7), End.LOWEST)).isEmpty();
    }

    @Test
    void aScanPagesInOrderAndAShortPageIsTheLast() {
        store.commit(List.of(entry(key(1), 1), entry(key(2), 2), entry(key(3), 3)), List.of());

        List<Entry> firstPage = store.scan(key(0), key(9), null, 2);
        assertThat(firstPage).extracting(Entry::state).containsExactly(new byte[] {1}, new byte[] {2});
        List<Entry> lastPage = store.scan(key(0), key(9), firstPage.get(1).key(), 2);
        assertThat(lastPage).extracting(Entry::state).containsExactly(new byte[] {3});
    }

    @Test
    void whatIsHandedOutAndTakenInIsACopy() {
        byte[] state = {1};
        store.commit(List.of(new Entry(key(1), state)), List.of());
        state[0] = 9;
        store.loadAll(List.of(key(1))).get(0)[0] = 8;

        assertThat(store.loadAll(List.of(key(1)))).containsExactly(new byte[] {1});
    }
}
