package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the four transport properties a cross-level lower bound relies on, because all four belong to the
 * engine rather than to us and an upgrade could take any of them away without a compile error.
 *
 * <p>A bound travelling between operator levels has to satisfy two demands that pull against each other on
 * a partitioned edge: it must reach every downstream instance, including the ones no key routed data to,
 * and it must never arrive ahead of the data it claims to cover. A separate broadcast edge buys the first
 * and loses the second. These tests establish that a watermark offered from an ordinary processor buys
 * both, that a silent sender holds the coalesced value back entirely rather than being skipped, and that
 * the key byte carries independent axes so one chain's bound cannot be mistaken for another's.
 *
 * <p>The senders never finish: a finished queue stops constraining the coalesced value, which would quietly
 * turn the third test green for the wrong reason.
 */
class JetWatermarksCarryBoundsToEveryInstanceTest {

    private static final int SENDERS = 2;
    private static final int RECEIVERS = 4;

    /** What each receiver instance saw, in arrival order, keyed by its global processor index. */
    private static final Map<Integer, List<String>> SEEN = new ConcurrentHashMap<>();

    /** Held closed until a test wants the second sender to speak. */
    private static final AtomicBoolean SECOND_SENDER_SPEAKS = new AtomicBoolean(true);

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        SEEN.clear();
        SECOND_SENDER_SPEAKS.set(true);
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (job != null) {
            job.cancel();
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aBoundReachesEveryDownstreamInstanceIncludingTheOnesNoKeyRoutedDataTo() {
        // Two keys cannot occupy four instances, so some receiver is guaranteed to have been sent nothing.
        run(new Plan(List.of(0, 1), 100L, 100L));

        await(() -> receiversThatSaw("w:0:100").size() == RECEIVERS);

        assertThat(receiversThatSaw("w:0:100"))
                .describedAs("a bound that only some instances hear leaves the others free to over-claim")
                .hasSize(RECEIVERS);
        assertThat(receiversThatSawAnyData())
                .describedAs("the two keys have to land somewhere, or the comparison below is vacuous")
                .isNotEmpty()
                .describedAs("the point of the test is that data reached fewer instances than the bound did")
                .hasSizeLessThan(RECEIVERS);
    }

    @Test
    void aBoundNeverArrivesAheadOfTheDataItsSenderPutOnTheEdgeBeforeIt() {
        // Keys 0-3 go out before the bound and keys 4-7 after it, so a log that happens to end with the
        // bound is not enough to pass: the tail has to be data the bound was never meant to cover.
        run(new Plan(List.of(0, 1, 2, 3), 100L, 100L, null, null, List.of(4, 5, 6, 7)));

        await(() -> receiversThatSaw("w:0:100").size() == RECEIVERS);
        await(() -> deliveredAfterTheBound() > 0);

        for (Map.Entry<Integer, List<String>> receiver : snapshot().entrySet()) {
            List<String> log = receiver.getValue();
            int bound = log.indexOf("w:0:100");
            assertThat(bound).describedAs("receiver %s never saw the bound", receiver.getKey()).isNotNegative();
            assertThat(log.subList(bound + 1, log.size()))
                    .describedAs("receiver %s was told the bound covered data it had not been handed yet",
                            receiver.getKey())
                    .noneMatch(JetWatermarksCarryBoundsToEveryInstanceTest::isDataSentBeforeTheBound);
        }
    }

    @Test
    void oneSilentSenderHoldsTheCoalescedBoundBackRatherThanBeingSkipped() {
        SECOND_SENDER_SPEAKS.set(false);
        // The first sender puts a marker on the edge behind its bound. A receiver holding the marker has
        // therefore already been offered that bound, so an empty result below is a decision, not a race.
        run(new Plan(List.of(0, 1), 500L, 90L, null, null, List.of(0)));

        await(() -> deliveredAfterTheBound() > 0);
        assertThat(watermarksSeen())
                .describedAs("a silent sender was skipped, so its unspoken constraint went missing")
                .isEmpty();

        SECOND_SENDER_SPEAKS.set(true);
        await(() -> receiversThatSaw("w:0:90").size() == RECEIVERS);

        assertThat(watermarksSeen())
                .describedAs("the delivered bound is the lowest of the senders, never the highest")
                .containsExactly("w:0:90");
    }

    @Test
    void twoKeysCarryTheirBoundsWithoutOneStandingInForTheOther() {
        run(new Plan(List.of(0, 1), 100L, 100L, 7L, 7L));

        await(() -> receiversThatSaw("w:0:100").size() == RECEIVERS
                && receiversThatSaw("w:1:7").size() == RECEIVERS);

        assertThat(watermarksSeen())
                .describedAs("each key keeps its own value rather than the axes being merged")
                .containsExactlyInAnyOrder("w:0:100", "w:1:7");
    }

    // ---- the job under test ------------------------------------------------------------

    /** What each sender emits: keys ahead of the bound, a bound per watermark key, then keys behind it. */
    private record Plan(List<Integer> keys, long firstOnKey0, long secondOnKey0,
            Long firstOnKey1, Long secondOnKey1, List<Integer> keysAfterTheBound) implements Serializable {

        Plan(List<Integer> keys, long firstOnKey0, long secondOnKey0) {
            this(keys, firstOnKey0, secondOnKey0, null, null, List.of());
        }

        Plan(List<Integer> keys, long firstOnKey0, long secondOnKey0, Long firstOnKey1, Long secondOnKey1) {
            this(keys, firstOnKey0, secondOnKey0, firstOnKey1, secondOnKey1, List.of());
        }
    }

    private void run(Plan plan) {
        DAG dag = new DAG();
        Vertex senders = dag.newVertex("senders",
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(plan)))
                .localParallelism(SENDERS);
        Vertex receivers = dag.newVertex("receivers",
                ProcessorSupplier.of((SupplierEx<Processor>) Receiver::new))
                .localParallelism(RECEIVERS);
        dag.edge(Edge.between(senders, receivers)
                .partitioned((Item item) -> item.key())
                .distributed());
        job = member.getJet().newJob(dag);
    }

    private record Item(int key, int seq, int sender, boolean beforeTheBound) implements Serializable {
    }

    /**
     * Emits its share of the data, then the bound. The second instance can be held mute, which is what a
     * partitioned key space does for real whenever no key of an instance's share shows up.
     */
    private static final class Sender extends AbstractProcessor {

        private final Plan plan;
        private int index;
        private int nextItem;
        private int nextItemAfterTheBound;
        private boolean boundsSent;

        Sender(Plan plan) {
            this.plan = plan;
        }

        @Override
        protected void init(Context context) {
            index = context.globalProcessorIndex();
        }

        @Override
        public boolean complete() {
            while (nextItem < plan.keys().size()) {
                if (!tryEmit(new Item(plan.keys().get(nextItem), nextItem, index, true))) {
                    return false;
                }
                nextItem++;
            }
            if (!boundsSent) {
                boolean second = index % SENDERS == 1;
                if (second && !SECOND_SENDER_SPEAKS.get()) {
                    return false;
                }
                long onKeyZero = second ? plan.secondOnKey0() : plan.firstOnKey0();
                if (!tryEmit(new Watermark(onKeyZero, (byte) 0))) {
                    return false;
                }
                Long onKeyOne = second ? plan.secondOnKey1() : plan.firstOnKey1();
                if (onKeyOne != null && !tryEmit(new Watermark(onKeyOne, (byte) 1))) {
                    return false;
                }
                boundsSent = true;
            }
            while (nextItemAfterTheBound < plan.keysAfterTheBound().size()) {
                int key = plan.keysAfterTheBound().get(nextItemAfterTheBound);
                if (!tryEmit(new Item(key, nextItemAfterTheBound, index, false))) {
                    return false;
                }
                nextItemAfterTheBound++;
            }
            // Never finishes: a completed queue stops constraining the coalesced value.
            return false;
        }
    }

    /** Records what it was handed, in the order it was handed it. */
    private static final class Receiver extends AbstractProcessor {

        private List<String> log;

        @Override
        protected void init(Context context) {
            log = Collections.synchronizedList(new ArrayList<>());
            SEEN.put(context.globalProcessorIndex(), log);
        }

        @Override
        public void process(int ordinal, Inbox inbox) {
            for (Object item; (item = inbox.poll()) != null; ) {
                Item data = (Item) item;
                log.add("d:" + data.sender() + ":" + data.seq() + (data.beforeTheBound() ? ":pre" : ":post"));
            }
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            log.add("w:" + watermark.key() + ":" + watermark.timestamp());
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            return true;
        }
    }

    // ---- reading what the receivers saw -------------------------------------------------

    private static Map<Integer, List<String>> snapshot() {
        Map<Integer, List<String>> copy = new LinkedHashMap<>();
        SEEN.forEach((instance, log) -> {
            synchronized (log) {
                copy.put(instance, List.copyOf(log));
            }
        });
        return copy;
    }

    private static List<Integer> receiversThatSaw(String entry) {
        return snapshot().entrySet().stream()
                .filter(receiver -> receiver.getValue().contains(entry))
                .map(Map.Entry::getKey)
                .toList();
    }

    private static List<Integer> receiversThatSawAnyData() {
        return snapshot().entrySet().stream()
                .filter(receiver -> receiver.getValue().stream().anyMatch(seen -> seen.startsWith("d:")))
                .map(Map.Entry::getKey)
                .toList();
    }

    private static boolean isDataSentBeforeTheBound(String entry) {
        return entry.startsWith("d:") && entry.endsWith(":pre");
    }

    private static long deliveredAfterTheBound() {
        return snapshot().values().stream()
                .flatMap(List::stream)
                .filter(seen -> seen.endsWith(":post"))
                .count();
    }

    private static List<String> watermarksSeen() {
        return snapshot().values().stream()
                .flatMap(List::stream)
                .filter(seen -> seen.startsWith("w:"))
                .distinct()
                .sorted()
                .toList();
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
