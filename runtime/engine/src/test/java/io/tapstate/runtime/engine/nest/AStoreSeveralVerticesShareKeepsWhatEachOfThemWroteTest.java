package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.RepeatedTest;

/**
 * That the heap stores standing in for a member's state can be written from several threads at once, which
 * is the only way anything ever uses them.
 *
 * <p><b>These are doubles, and that is exactly why this is worth a case.</b> A vertex runs a processor per
 * unit of parallelism, each on a thread of its own, and one store stands behind the namespace all of them
 * reach; what they stand in for is a distributed map, which cannot lose a write. So a double that can lose
 * one produces failures no deployment can produce - and it produces them in whichever case happened to be
 * running, never in one that names the store.
 *
 * <p><b>Nothing here writes a key twice.</b> Partitioning makes every key one processor's alone, so the
 * question is not whether two writers agree on an entry - they never meet on one. It is whether a write
 * survives another thread growing the same table at that moment, which an unsynchronised map does not
 * promise and which no amount of care at the call site can recover.
 *
 * <p>What one loss cost, before this case existed: a lookup filed a row while the registration naming it
 * had gone missing, so the row read as one nobody points at, no word of its arrival was sent, and the
 * document waiting for that row waited for the life of the job - running, throwing nothing, with every
 * other document correct.
 */
class AStoreSeveralVerticesShareKeepsWhatEachOfThemWroteTest {

    /** As many writers as a member gives one vertex by default, which is what shares one of these. */
    private static final int WRITERS = 4;

    /** Enough keys each that the table is grown several times while the others are writing into it. */
    private static final int KEYS_EACH = 400;

    @RepeatedTest(20)
    void aNestStoreKeepsEveryWriteWhenSeveralProcessorsFileKeysOfTheirOwn() throws Exception {
        HeapNestStore<Set<Object>> store = new HeapNestStore<>();

        writeFromEveryThread((writer, key) -> store.add(key, "referrer-" + key));

        List<String> lost = new ArrayList<>();
        forEachKey(key -> {
            Set<Object> held = store.load(key);
            if (held == null || !held.contains("referrer-" + key)) {
                lost.add(key);
            }
        });
        assertThat(lost)
                .describedAs("every one of these went to a key no other thread touched, so a store several "
                        + "processors share may not lose one. What came back missing: %s of %s",
                        lost.size(), WRITERS * KEYS_EACH)
                .isEmpty();
        assertThat(store.count())
                .describedAs("and the count is of what is held, not of what was written")
                .isEqualTo((long) WRITERS * KEYS_EACH);
    }

    @RepeatedTest(20)
    void aKeyedStateStoreKeepsEveryWriteWhenSeveralPartitionThreadsSpillAtOnce() throws Exception {
        HeapKeyedStateStore store = new HeapKeyedStateStore();

        writeFromEveryThread((writer, key) -> store.save("ns", key, new byte[] {(byte) writer}));

        List<String> lost = new ArrayList<>();
        forEachKey(key -> {
            if (store.load("ns", key).isEmpty()) {
                lost.add(key);
            }
        });
        assertThat(lost)
                .describedAs("what a member spills is spilled by several partition threads at once, and the "
                        + "store behind the map loses none of it. What came back missing: %s of %s",
                        lost.size(), WRITERS * KEYS_EACH)
                .isEmpty();
    }

    /** One writer's write of one key. */
    @FunctionalInterface
    private interface Writes {
        void write(int writer, String key);
    }

    /** Starts every writer on keys of its own at the same moment, and waits for all of them to finish. */
    private static void writeFromEveryThread(Writes writes) throws InterruptedException {
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int writer = 0; writer < WRITERS; writer++) {
            int mine = writer;
            Thread thread = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < KEYS_EACH; i++) {
                    writes.write(mine, keyOf(mine, i));
                }
            });
            threads.add(thread);
            thread.start();
        }
        go.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(thread.isAlive())
                    .describedAs("a writer that never finished would leave every reading below saying what "
                            + "an unstarted run says")
                    .isFalse();
        }
    }

    private static void forEachKey(Consumer<String> read) {
        for (int writer = 0; writer < WRITERS; writer++) {
            for (int i = 0; i < KEYS_EACH; i++) {
                read.accept(keyOf(writer, i));
            }
        }
    }

    private static String keyOf(int writer, int index) {
        return "w" + writer + "-k" + index;
    }
}
