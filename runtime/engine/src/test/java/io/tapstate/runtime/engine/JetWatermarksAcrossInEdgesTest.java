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
 * Pins what a lower bound means at a vertex fed by more than one edge, where the two callbacks a processor
 * may implement stop being interchangeable. A vertex that merges streams is handed a bound twice over: once
 * per edge, carrying that edge's lowest promise across the instances feeding it, and once globally, carrying
 * the lowest across every edge.
 *
 * <p>Which one is safe depends entirely on the role. Anything that writes the frontier down must use the
 * global one, because a single edge's promise says nothing about data still in flight on another edge. But
 * only the per-edge one can be seen at all for a chain that not every edge carries: the global bound for
 * such a chain is never delivered, and the omission is silent — no exception, no log, just a callback that
 * is never invoked. A level that keeps its books per edge therefore sees every chain; one that keeps them
 * globally silently loses the chains that arrive on a subset of its edges.
 *
 * <p>Both halves are engine behaviour rather than ours, and an upgrade could take either away without a
 * compile error, so both are pinned here. This is the level above the single-edge properties: there, the
 * question is whether a bound reaches every instance without overtaking its data; here, it is what happens
 * when the bounds of two edges have to be combined.
 *
 * <p>The senders never finish: a finished queue stops constraining the coalesced value and jumps it to the
 * highest value any queue ever reported, which would turn these tests green for the wrong reason.
 */
class JetWatermarksAcrossInEdgesTest {

    private static final int SENDERS_PER_EDGE = 2;
    private static final int RECEIVERS = 3;
    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    /** What each receiver instance was handed, in arrival order, keyed by its global processor index. */
    private static final Map<Integer, List<String>> SEEN = new ConcurrentHashMap<>();

    /** Held closed until a test wants the right-hand edge to speak. */
    private static final AtomicBoolean RIGHT_EDGE_SPEAKS = new AtomicBoolean(true);

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        SEEN.clear();
        RIGHT_EDGE_SPEAKS.set(true);
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
    void theGlobalBoundWaitsForEveryInEdgeAndIsThenTheLowestOfThem() {
        RIGHT_EDGE_SPEAKS.set(false);
        run(new Plan(List.of(0, 1), List.of(new Bound((byte) 0, 100L)), false),
                new Plan(List.of(0, 1), List.of(new Bound((byte) 0, 60L)), true));

        // Every receiver has been handed the left edge's bound, so an empty global result is a decision
        // rather than a race: the one thing still missing is the other edge.
        await(() -> receiversThatSaw("e:" + LEFT + ":0:100").size() == RECEIVERS);
        assertThat(receiversThatSaw("e:" + LEFT + ":0:100"))
                .describedAs("the speaking edge's bound never got as far as every receiver, which would leave "
                        + "the emptiness below saying nothing at all")
                .hasSize(RECEIVERS);
        assertThat(globalBoundsSeen())
                .describedAs("an edge that has not spoken was skipped, so its unspoken constraint went missing")
                .isEmpty();

        RIGHT_EDGE_SPEAKS.set(true);
        await(() -> receiversThatSaw("g:0:60").size() == RECEIVERS);

        assertThat(globalBoundsSeen())
                .describedAs("the global bound is the lowest of the edges, never the highest of them")
                .containsExactly("g:0:60");
    }

    @Test
    void aChainOnlyOneInEdgeCarriesReachesThatEdgesCallbackAndNeverTheGlobalOne() {
        // The left edge carries two chains, the right only the first — the shape of a merging vertex whose
        // in-edges draw on different sets of tables.
        run(new Plan(List.of(0, 1), List.of(new Bound((byte) 0, 100L), new Bound((byte) 1, 7L)), false),
                new Plan(List.of(0, 1), List.of(new Bound((byte) 0, 60L)), false));

        await(() -> receiversThatSaw("e:" + LEFT + ":1:7").size() == RECEIVERS
                && receiversThatSaw("g:0:60").size() == RECEIVERS);

        assertThat(edgeBoundsSeen())
                .describedAs("a chain only one edge carries still reaches that edge's own callback")
                .contains("e:" + LEFT + ":1:7");
        assertThat(globalBoundsSeen())
                .describedAs("the chain both edges carry reached the global callback, so the absence below is "
                        + "about the chain and not about the job being idle")
                .contains("g:0:60");
        assertThat(globalBoundsSeen())
                .describedAs("a chain that not every edge carries never reaches the global callback at all, "
                        + "and nothing reports that it did not")
                .noneMatch(bound -> bound.startsWith("g:1:"));
    }

    // ---- the job under test ------------------------------------------------------------

    /** One bound a sender puts on its edge: a value on one chain's axis. */
    private record Bound(byte key, long timestamp) implements Serializable {
    }

    /** What each sender emits: its share of the data, then its bounds. Gated senders wait to be let in. */
    private record Plan(List<Integer> dataKeys, List<Bound> bounds, boolean gated) implements Serializable {
    }

    private void run(Plan left, Plan right) {
        DAG dag = new DAG();
        Vertex leftSenders = dag.newVertex("left",
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(left)))
                .localParallelism(SENDERS_PER_EDGE);
        Vertex rightSenders = dag.newVertex("right",
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(right)))
                .localParallelism(SENDERS_PER_EDGE);
        Vertex receivers = dag.newVertex("receivers",
                ProcessorSupplier.of((SupplierEx<Processor>) Receiver::new))
                .localParallelism(RECEIVERS);
        dag.edge(Edge.from(leftSenders).to(receivers, LEFT)
                .partitioned((Item item) -> item.key())
                .distributed());
        dag.edge(Edge.from(rightSenders).to(receivers, RIGHT)
                .partitioned((Item item) -> item.key())
                .distributed());
        job = member.getJet().newJob(dag);
    }

    private record Item(int key) implements Serializable {
    }

    /** Emits its share of the data, then its bounds, and then stays alive without ever finishing. */
    private static final class Sender extends AbstractProcessor {

        private final Plan plan;
        private int nextItem;
        private int nextBound;

        Sender(Plan plan) {
            this.plan = plan;
        }

        @Override
        public boolean complete() {
            while (nextItem < plan.dataKeys().size()) {
                if (!tryEmit(new Item(plan.dataKeys().get(nextItem)))) {
                    return false;
                }
                nextItem++;
            }
            if (plan.gated() && !RIGHT_EDGE_SPEAKS.get()) {
                return false;
            }
            while (nextBound < plan.bounds().size()) {
                Bound bound = plan.bounds().get(nextBound);
                if (!tryEmit(new Watermark(bound.timestamp(), bound.key()))) {
                    return false;
                }
                nextBound++;
            }
            // Never finishes: a completed queue stops constraining the coalesced value.
            return false;
        }
    }

    /** Records what it was handed, through which callback, in the order it was handed it. */
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
                log.add("d:" + ordinal + ":" + ((Item) item).key());
            }
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            log.add("e:" + ordinal + ":" + watermark.key() + ":" + watermark.timestamp());
            return true;
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            log.add("g:" + watermark.key() + ":" + watermark.timestamp());
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

    private static List<String> globalBoundsSeen() {
        return boundsSeen("g:");
    }

    private static List<String> edgeBoundsSeen() {
        return boundsSeen("e:");
    }

    private static List<String> boundsSeen(String throughCallback) {
        return snapshot().values().stream()
                .flatMap(List::stream)
                .filter(seen -> seen.startsWith(throughCallback))
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
