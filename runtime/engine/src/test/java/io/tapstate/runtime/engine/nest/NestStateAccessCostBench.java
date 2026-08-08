package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Measures what one nest state access costs on the hit path, along the axis the carrier can still be
 * changed on: whether the state is read out, changed, and written back, or changed where it already
 * lies. Every event that reaches a vertex pays this once per hop, so it is the innermost cost in the
 * whole design and the one a deeper tree multiplies.
 *
 * <p><b>The question is answered by a count, not by a clock.</b> Whether a path copies the whole
 * document is a property of the path; how long that takes is a property of the machine. So the state
 * counts its own serializations and the clock only says what one of them costs here. A wall-clock
 * comparison alone could be read either way - a path that copies can look cheap on a small document.
 *
 * <p>No store stands behind these maps. Writing the document out to the cold layer is a cost both
 * paths pay identically and is measured against a real endpoint elsewhere; mixing it in here would
 * bury the difference this bench exists to show under an I/O number an order of magnitude larger.
 *
 * <p>This is an instrument, not a regression test: wall-clock numbers do not belong in a build gate.
 * Its name keeps it out of the default surefire selection, so it costs a build nothing and is run
 * deliberately:
 *
 * <pre>{@code mvn -o test -pl runtime/engine -am -Dtest=NestStateAccessCostBench -DfailIfNoTests=false}</pre>
 */
class NestStateAccessCostBench {

    /** Document sizes to walk, in bytes: a small assembly, a normal one, and a large root. */
    private static final List<Integer> SIZES = List.of(10 * 1024, 100 * 1024, 1024 * 1024);

    private static final int WARMUP = 200;
    private static final int RUNS = 2_000;

    private static final String READ_OUT_MAP = "nest.bench.readout";
    private static final String IN_PLACE_MAP = "nest.bench.inplace";
    private static final String IN_PLACE_STORED_MAP = "nest.bench.inplace.stored";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        // What the carrier is today: the state travels as bytes, and a get hands back a copy of them.
        config.addMapConfig(new MapConfig(READ_OUT_MAP).setBackupCount(0).setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.BINARY));
        // The candidate: the state lies on the partition as an object, and no index is defined on the
        // map - which is what decides whether an entry processor is handed the object or a clone of it.
        config.addMapConfig(new MapConfig(IN_PLACE_MAP).setBackupCount(0).setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.OBJECT));
        // The candidate again, with a cold layer under it: whether changing the state where it lies still
        // reaches that layer is what decides whether the candidate is usable at all.
        config.addMapConfig(new MapConfig(IN_PLACE_STORED_MAP).setBackupCount(0).setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setMapStoreConfig(new MapStoreConfig().setEnabled(true).setWriteDelaySeconds(0)
                        .setFactoryImplementation(new CountingStoreFactory())));
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void changingTheStateWhereItLiesStillReachesTheColdLayer() {
        IMap<String, CountedState> map = member.getMap(IN_PLACE_STORED_MAP);
        String key = "root";
        map.set(key, new CountedState(1024));
        long afterSet = CountingStore.STORED.get();

        map.executeOnKey(key, new ChangeInPlace());

        System.out.printf("cold-layer writes: set=%d, executeOnKey=+%d%n",
                afterSet, CountingStore.STORED.get() - afterSet);
    }

    @Test
    void readingTheStateOutAgainstChangingItWhereItLies() {
        System.out.println("size,path,writes/op,reads/op,ns/op");
        for (int size : SIZES) {
            report(size, "read-out-write-back", readOutWriteBack(size));
            report(size, "changed-in-place", changedInPlace(size));
            report(size, "carried-in-place", carriedInPlace(size));
            report(size, "read-out-of-object", readOutOfObject(size));
        }
    }

    private Sample readOutWriteBack(int size) {
        IMap<String, CountedState> map = member.getMap(READ_OUT_MAP);
        map.clear();
        String key = "root";
        map.set(key, new CountedState(size));

        for (int i = 0; i < WARMUP; i++) {
            CountedState state = map.get(key);
            state.change();
            map.set(key, state);
        }

        CountedState.WRITES.set(0);
        CountedState.READS.set(0);
        long start = System.nanoTime();
        for (int i = 0; i < RUNS; i++) {
            CountedState state = map.get(key);
            state.change();
            map.set(key, state);
        }
        return new Sample(System.nanoTime() - start, CountedState.WRITES.get(), CountedState.READS.get());
    }

    private Sample changedInPlace(int size) {
        IMap<String, CountedState> map = member.getMap(IN_PLACE_MAP);
        map.clear();
        String key = "root";
        map.set(key, new CountedState(size));

        for (int i = 0; i < WARMUP; i++) {
            map.executeOnKey(key, new ChangeInPlace());
        }

        CountedState.WRITES.set(0);
        CountedState.READS.set(0);
        long start = System.nanoTime();
        for (int i = 0; i < RUNS; i++) {
            map.executeOnKey(key, new ChangeInPlace());
        }
        return new Sample(System.nanoTime() - start, CountedState.WRITES.get(), CountedState.READS.get());
    }

    /**
     * The whole state carried to the key by the processor that changed it, rather than the change being
     * described and replayed there. Whether this costs a serialization decides how much of the operator
     * has to move: if carrying is free the change can stay where it is written, and if it is not, the
     * change itself has to become something that can travel.
     */
    private Sample carriedInPlace(int size) {
        IMap<String, CountedState> map = member.getMap(IN_PLACE_MAP);
        map.clear();
        String key = "root";
        map.set(key, new CountedState(size));

        CountedState carried = new CountedState(size);
        for (int i = 0; i < WARMUP; i++) {
            map.executeOnKey(key, new CarryInPlace(carried));
        }

        CountedState.WRITES.set(0);
        CountedState.READS.set(0);
        long start = System.nanoTime();
        for (int i = 0; i < RUNS; i++) {
            map.executeOnKey(key, new CarryInPlace(carried));
        }
        return new Sample(System.nanoTime() - start, CountedState.WRITES.get(), CountedState.READS.get());
    }

    /**
     * Reading the state out of a map that holds it as an object. The read is the half a carrying write
     * cannot remove, so what it costs on this format decides whether changing the format is worth
     * anything at all: a format that halves the write and doubles the read has moved the cost, not
     * removed it.
     */
    private Sample readOutOfObject(int size) {
        IMap<String, CountedState> map = member.getMap(IN_PLACE_MAP);
        map.clear();
        String key = "root";
        map.set(key, new CountedState(size));

        for (int i = 0; i < WARMUP; i++) {
            map.get(key);
        }

        CountedState.WRITES.set(0);
        CountedState.READS.set(0);
        long start = System.nanoTime();
        for (int i = 0; i < RUNS; i++) {
            map.get(key);
        }
        return new Sample(System.nanoTime() - start, CountedState.WRITES.get(), CountedState.READS.get());
    }

    private static void report(int size, String path, Sample sample) {
        System.out.printf(
                "%d,%s,%.2f,%.2f,%d%n",
                size,
                path,
                sample.writes() / (double) RUNS,
                sample.reads() / (double) RUNS,
                sample.elapsedNanos() / RUNS);
    }

    private record Sample(long elapsedNanos, long writes, long reads) { }

    /**
     * Stands in for an assembled document, and counts every time it is written out or read back. Its
     * payload is one block of bytes rather than the nested maps a real assembly holds: what is being
     * compared is how often the whole of it crosses the serializer, which is a property of the access
     * path and not of what is inside.
     */
    static final class CountedState implements Serializable {

        static final AtomicLong WRITES = new AtomicLong();
        static final AtomicLong READS = new AtomicLong();

        private final byte[] payload;
        private long version;

        CountedState(int size) {
            payload = new byte[size];
        }

        void change() {
            version++;
            // Touch the payload so that a copy of it cannot be shared away by the runtime.
            payload[(int) (version % payload.length)] = (byte) version;
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            WRITES.incrementAndGet();
            out.defaultWriteObject();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            READS.incrementAndGet();
            in.defaultReadObject();
        }
    }

    /** Counts what reaches the cold layer, and keeps it, so a read-through has something to answer with. */
    static final class CountingStore implements MapStore<String, CountedState> {

        static final AtomicLong STORED = new AtomicLong();

        private final Map<String, CountedState> held = new ConcurrentHashMap<>();

        @Override
        public void store(String key, CountedState value) {
            STORED.incrementAndGet();
            held.put(key, value);
        }

        @Override
        public void storeAll(Map<String, CountedState> map) {
            map.forEach(this::store);
        }

        @Override
        public void delete(String key) {
            held.remove(key);
        }

        @Override
        public void deleteAll(java.util.Collection<String> keys) {
            keys.forEach(this::delete);
        }

        @Override
        public CountedState load(String key) {
            return held.get(key);
        }

        @Override
        public Map<String, CountedState> loadAll(java.util.Collection<String> keys) {
            Map<String, CountedState> loaded = new java.util.LinkedHashMap<>();
            keys.forEach(key -> {
                CountedState state = load(key);
                if (state != null) {
                    loaded.put(key, state);
                }
            });
            return loaded;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            return List.of();
        }
    }

    static final class CountingStoreFactory implements MapStoreFactory<String, CountedState> {

        @Override
        public MapLoader<String, CountedState> newMapStore(String mapName, Properties properties) {
            return new CountingStore();
        }
    }

    /** Carries a whole state to the key and puts it there. Answers null, as whatever it answers travels. */
    static final class CarryInPlace implements EntryProcessor<String, CountedState, Void>, Serializable {

        private final CountedState carried;

        CarryInPlace(CountedState carried) {
            this.carried = carried;
        }

        @Override
        public Void process(Map.Entry<String, CountedState> entry) {
            entry.setValue(carried);
            return null;
        }

        @Override
        public EntryProcessor<String, CountedState, Void> getBackupProcessor() {
            return null;
        }
    }

    /** Changes the state where it lies. Answers null, because whatever it answers is serialized. */
    static final class ChangeInPlace implements EntryProcessor<String, CountedState, Void>, Serializable {

        @Override
        public Void process(Map.Entry<String, CountedState> entry) {
            CountedState state = entry.getValue();
            state.change();
            entry.setValue(state);
            return null;
        }

        @Override
        public EntryProcessor<String, CountedState, Void> getBackupProcessor() {
            return null;
        }
    }
}
