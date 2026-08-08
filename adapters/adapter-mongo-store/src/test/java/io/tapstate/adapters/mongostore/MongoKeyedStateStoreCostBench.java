package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import io.tapstate.testsupport.RequiresDocker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/**
 * Measures what the cold layer under a stateful operator costs against a real endpoint, on the two
 * paths an event can take through it: the write every handled key makes on its way through, and the
 * read a key not held in memory makes before it can be handled at all.
 *
 * <p>Both are paid synchronously by the caller. The write is through rather than behind, so the event
 * waits for it; the read happens before the state exists to change, so the event waits for that too.
 * A deeper tree pays the read once per hop, which is why the number that matters downstream is not
 * the median but the tail multiplied by the depth.
 *
 * <p>Reads are taken over keys written far enough back that they are unlikely to still be the
 * endpoint's most recent pages: what an event pays is the cost of fetching state that has been sitting
 * there, and a read straight after its own write measures a cache rather than the layer.
 *
 * <p>This is an instrument, not a regression test: wall-clock numbers against a container do not
 * belong in a build gate. Its name keeps it out of the default surefire selection and out of the
 * failsafe one, so it costs a build nothing and is run deliberately:
 *
 * <pre>{@code mvn -o test -pl adapters/adapter-mongo-store -am -Dtest=MongoKeyedStateStoreCostBench \
 *   -DfailIfNoTests=false -Dapi.version=1.44}</pre>
 */
@RequiresDocker
class MongoKeyedStateStoreCostBench {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    private static final String NAMESPACE = "nest.bench.assemble.orders";

    /**
     * Document sizes to walk, in bytes. The first is small enough to carry no cost of its own, so what
     * it measures is what any call costs before the document is considered - and the rest are read
     * against it rather than on their own, since a cost that does not move with size is not the one a
     * bigger document would make worse.
     */
    private static final List<Integer> SIZES = List.of(64, 10 * 1024, 100 * 1024, 1024 * 1024);

    private static final int WARMUP = 20;
    private static final int RUNS = 200;

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void whatAWriteThroughAndAColdReadCost() {
        withStore((store, unacked, database) -> {
            System.out.println("size,path,p50_us,p99_us,max_us,ops_per_second");
            report(0, "ping", ping(database));
            report(0, "absent-read", absentRead(store));
            for (int size : SIZES) {
                report(size, "write-through", writeThrough(store, size));
                report(size, "write-unacked", writeThrough(unacked, size));
                report(size, "cold-read", coldRead(store, size));
            }
        });
    }

    /**
     * A command that touches no storage at all. What is left is the trip out and back, so the gap
     * between this and everything else is what the endpoint actually does, and this number on its own
     * says how much of every other number belongs to the machine the bench ran on rather than to the
     * layer being measured.
     */
    private static long[] ping(MongoDatabase database) {
        Document command = new Document("ping", 1);
        for (int i = 0; i < WARMUP; i++) {
            database.runCommand(command);
        }

        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            database.runCommand(command);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /**
     * A read of a key nothing ever wrote. It carries no document back, so what is left is the round
     * trip and the lookup - the floor every other number here sits on top of. Reading a real number as
     * the layer's own cost, without this to take off it, would credit the layer with the endpoint's
     * latency.
     */
    private static long[] absentRead(MongoKeyedStateStore store) {
        for (int i = 0; i < WARMUP; i++) {
            store.load(NAMESPACE, "absent-" + i);
        }

        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            store.load(NAMESPACE, "absent-run-" + i);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /** One synchronous write of a whole document, which is what a handled key pays on its way through. */
    private static long[] writeThrough(MongoKeyedStateStore store, int size) {
        byte[] state = new byte[size];
        for (int i = 0; i < WARMUP; i++) {
            store.save(NAMESPACE, "warmup-" + size + "-" + i, state);
        }

        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            store.save(NAMESPACE, "write-" + size + "-" + i, state);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /**
     * One read of a key that is not held in memory, which is what an event pays before the state it
     * needs exists to be changed. The keys were all written before any of them is read, so no read is
     * served by what its own write just left behind.
     */
    private static long[] coldRead(MongoKeyedStateStore store, int size) {
        byte[] state = new byte[size];
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < RUNS + WARMUP; i++) {
            String key = "read-" + size + "-" + i;
            store.save(NAMESPACE, key, state);
            keys.add(key);
        }

        for (int i = 0; i < WARMUP; i++) {
            store.load(NAMESPACE, keys.get(i));
        }

        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            String key = keys.get(WARMUP + i);
            long start = System.nanoTime();
            Optional<byte[]> found = store.load(NAMESPACE, key);
            taken[i] = System.nanoTime() - start;
            if (found.isEmpty()) {
                throw new IllegalStateException("the key just written was not there: " + key);
            }
        }
        return taken;
    }

    private static void report(int size, String path, long[] taken) {
        long[] sorted = taken.clone();
        Arrays.sort(sorted);
        long p50 = sorted[(int) (sorted.length * 0.50)];
        long p99 = sorted[(int) (sorted.length * 0.99)];
        long max = sorted[sorted.length - 1];
        System.out.printf(
                "%d,%s,%d,%d,%d,%d%n",
                size, path, p50 / 1_000, p99 / 1_000, max / 1_000, 1_000_000_000L / Math.max(p99, 1));
    }

    /**
     * The same writes with the endpoint asked not to wait for them to be durable. What separates this
     * from the write above is what durability costs; what separates it from the ping is what the work
     * itself costs. Without the split, a slow write on a laptop and a slow write on a fsync are the
     * same number and lead to opposite decisions.
     */
    private interface Bench {
        void run(MongoKeyedStateStore store, MongoKeyedStateStore unacked, MongoDatabase database);
    }

    private static void withStore(Bench bench) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("tapstate");
            MongoCollection<Document> collection = database.getCollection("operator_state");
            MongoCollection<Document> unacked = collection.withWriteConcern(WriteConcern.W1.withJournal(false));
            bench.run(new MongoKeyedStateStore(collection), new MongoKeyedStateStore(unacked), database);
        }
    }
}
