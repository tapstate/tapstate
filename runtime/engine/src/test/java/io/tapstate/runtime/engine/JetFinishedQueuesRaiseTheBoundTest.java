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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the one place the engine raises a bound above what an instance promised, and how far that raise
 * travels. Everywhere else a bound is combined by taking the lowest of what the instances feeding it said.
 * Once every queue behind one edge has finished there is nothing left to take a lowest of, and the engine
 * offers the highest value any of those queues ever reported instead - a maximum where every other step
 * took a minimum.
 *
 * <p>That reversal is what a durable frontier cannot survive being handed. An instance that finishes while
 * still holding a change it never sent has been promising a value beneath that change all along; the moment
 * it finishes, its promise stops counting, a sibling's far higher promise becomes the bound, and a frontier
 * trusting the bound carries straight over the change nobody sent. Nothing reports it - every count is
 * right and the change is neither delivered nor replayable.
 *
 * <p><b>How far the raise travels is the whole point, and it is not far.</b> The raise is produced when the
 * edge's last queue finishes, which is also the moment the edge itself is finished and retired. A processor
 * whose only inbound edge that was is put straight into completing, and the raised bound is dropped
 * unhandled: neither watermark callback is ever invoked with it. Only a processor that still has another
 * inbound edge to be fed from is handed it. So the hazard belongs to vertices that merge edges, and a sink
 * fed by a single edge cannot be handed the raise at all.
 *
 * <p>All three cases are pinned together because each alone is green for the wrong reason. That the raise
 * never arrives says nothing unless the raise is shown to exist; that it exists says nothing about the sink
 * unless the sink's shape is shown to be exempt.
 *
 * <p>The senders here emit bounds and no data at all. A bound worked out as an edge finishes is held back
 * one drain when data was drained alongside it, and that drain never comes for an edge that has finished -
 * so data in the mix would decide these tests by timing rather than by the property.
 *
 * <p><b>Two different raises can carry the same value, and what tells them apart here is the order the
 * senders finish in.</b> One queue of an edge finishing raises the bound to the lowest of the queues still
 * open, so a low instance finishing while the high one is still open would also put the high promise in
 * front of a receiver. The senders below rule that out: the low instance yields until the high one's DONE is
 * on its queue, so the high promise is never the lowest of what stays open, and the only rule left that can
 * produce its value on that edge is the parting raise. These cases therefore ask <i>whether that value ever
 * arrived</i>, which the finishing order cannot answer differently.
 *
 * <p><b>Where it sits in a receiver's log does not tell them apart, though it reads as though it would.</b>
 * The parting raise is worked out as the edge is retired, which sounds like it can only land after that edge
 * completed, and measured across twenty receiver instances it landed after in nineteen of them. It is not a
 * property: one instance in twenty had it land first. A case asking what arrived <i>after the edge
 * completed</i> is therefore decided by the scheduler rather than by the behaviour, and asking it of every
 * instance at once only lowers the odds of noticing. Reading the engine's sources concludes that the
 * reversal cannot happen at all - the second time here that such a reading has been contradicted by printing
 * what actually arrived.
 *
 * <p>This is engine behaviour rather than ours, and an upgrade could change it without a compile error.
 */
class JetFinishedQueuesRaiseTheBoundTest {

    private static final int SENDERS_PER_EDGE = 2;
    private static final int RECEIVERS = 2;

    /** The instance of each edge that promises the high bound; the other promises the low one. */
    private static final int HIGH_INSTANCE = 0;
    private static final long HIGH = 100L;
    private static final long LOW = 10L;

    /** The one chain's axis these bounds travel on. */
    private static final byte AXIS = 0;

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    /** What each receiver instance was handed, in arrival order, keyed by its global processor index. */
    private static final Map<Integer, List<String>> SEEN = new ConcurrentHashMap<>();

    /** Per sender vertex, whether its high instance has put its DONE on the queue. */
    private static final Map<String, AtomicBoolean> HIGH_IS_DONE = new ConcurrentHashMap<>();

    /** Per sender vertex, the instances that finished, in the order they did. */
    private static final Map<String, List<Integer>> FINISHED = new ConcurrentHashMap<>();

    /** The thread the flag above was set on, which is what decides whether waiting for it is bounded. */
    private static final AtomicReference<String> DONE_WAS_FLAGGED_ON = new AtomicReference<>();

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        SEEN.clear();
        // A flag left set by an earlier case would let a low instance finish first after all.
        HIGH_IS_DONE.clear();
        FINISHED.clear();
        DONE_WAS_FLAGGED_ON.set(null);
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
    void aQueueFinishingLeavesTheBoundWhereTheQueueStillOpenPutIt() {
        runOneEdge(false);

        await(() -> boundsSeen().contains(global(LOW)));

        assertThat(boundsSeen())
                .describedAs("the open queue's promise never got through, so the absence below would be "
                        + "about nothing having happened yet")
                .contains(global(LOW));
        assertThat(boundsSeen())
                .describedAs("one queue finishing raised the bound above what the queue still open ever "
                        + "promised, while that queue is the one that would be holding the unsent change")
                .doesNotContain(global(HIGH), edge(LEFT, HIGH));
    }

    @Test
    void everyQueueOfAnEdgeFinishingRaisesTheBoundToTheHighestAnyOfThemPromised() {
        runTwoEdges();

        await(() -> boundsSeen().contains(edge(LEFT, HIGH)));

        assertThat(DONE_WAS_FLAGGED_ON.get())
                .describedAs("the low instance spins on this flag from a cooperative thread, so how long it "
                        + "spins is bounded only while the flag is set on one too. A processor that does not "
                        + "declare its close cooperative has that close handed to a shared pool instead, and "
                        + "the wait then lasts as long as that pool is busy - a question about the machine "
                        + "rather than about this property")
                .contains("cooperative");
        assertThat(highFinishedFirstOn("left"))
                .describedAs("the gate is what makes the value below mean one thing: the low instance is "
                        + "held until the high one's DONE is queued, so the high promise is never the lowest "
                        + "of what stays open and the bound one queue finishing leaves behind can only ever "
                        + "be the low one. Without the gate the same value reaches a receiver by that other "
                        + "route and the case below stops being about the parting raise")
                .isTrue();
        assertThat(boundsSeen())
                .describedAs("the low promise is what the bound stood at while both queues were open, so "
                        + "the raise below is a rise rather than the only value ever sent; and the edge did "
                        + "complete, without which the raise below would be missing for never having been "
                        + "made rather than for not being handed over")
                .contains(edge(LEFT, LOW), edgeCompleted(LEFT));
        assertThat(boundsSeen())
                .describedAs("with every queue of the edge finished the bound became the highest any of "
                        + "them ever promised - a maximum across instances, which is what would carry a "
                        + "frontier over a change an instance finished while still holding. That the value "
                        + "arrived at all is the question, the gate above having left the parting raise as "
                        + "the only rule on this edge that produces it")
                .contains(edge(LEFT, HIGH));
    }

    @Test
    void aRaiseOnTheLastEdgeAProcessorHasIsNeverHandedToIt() {
        runOneEdge(true);

        // The same shape as the case above but for the second edge, and the raise is worked out the same
        // way. Waiting for the job to have finished is what makes the absence below a decision.
        await(() -> job.getFuture().isDone());

        assertThat(highFinishedFirstOn("senders"))
                .describedAs("the same gate as the case above, and it is what leaves the absence below with "
                        + "one meaning: had the low instance finished first, the high promise would reach a "
                        + "receiver as the bound the still-open queue leaves behind, and the absence would "
                        + "be failing for a raise this case is not about")
                .isTrue();
        assertThat(boundsSeen())
                .describedAs("the low promise never arrived either, so the absence below would be about "
                        + "the job never having run; and the edge did complete, without which there is no "
                        + "raise for the absence below to be about")
                .contains(global(LOW), edge(LEFT, LOW), edgeCompleted(LEFT));
        assertThat(boundsSeen())
                .describedAs("the raise worked out as the last edge finished was handed to the processor, "
                        + "where a sink is put straight into completing instead and never sees it. Asked as "
                        + "whether the value reached a receiver at all - asking instead what came after the "
                        + "edge completed passes on any run where a receiver was handed it first, which is "
                        + "the same run the case is meant to catch")
                .doesNotContain(edge(LEFT, HIGH));
    }

    // ---- the job under test ------------------------------------------------------------

    /** One edge into the receivers; its low instance finishes only when {@code lowFinishes}. */
    private void runOneEdge(boolean lowFinishes) {
        DAG dag = new DAG();
        Vertex senders = senders(dag, "senders", lowFinishes);
        Vertex receivers = receivers(dag);
        dag.edge(Edge.from(senders).to(receivers, LEFT));
        job = member.getJet().newJob(dag);
    }

    /**
     * Two edges into the receivers: the left finishes entirely, the right stays open, which is what keeps
     * the receiver being fed and so able to be handed the left's parting bound.
     */
    private void runTwoEdges() {
        DAG dag = new DAG();
        Vertex left = senders(dag, "left", true);
        Vertex right = senders(dag, "right", false);
        Vertex receivers = receivers(dag);
        dag.edge(Edge.from(left).to(receivers, LEFT));
        dag.edge(Edge.from(right).to(receivers, RIGHT));
        job = member.getJet().newJob(dag);
    }

    private static Vertex senders(DAG dag, String name, boolean lowFinishes) {
        return dag.newVertex(name, ProcessorSupplier.of((SupplierEx<Processor>) () -> new Sender(lowFinishes)))
                .localParallelism(SENDERS_PER_EDGE);
    }

    private static Vertex receivers(DAG dag) {
        return dag.newVertex("receivers", ProcessorSupplier.of((SupplierEx<Processor>) Receiver::new))
                .localParallelism(RECEIVERS);
    }

    /**
     * Promises one bound - which one is decided by which instance it is - and then either finishes or stays
     * alive without ever finishing.
     */
    private static final class Sender extends AbstractProcessor {

        private final boolean lowFinishes;
        private boolean high;
        private int instance;
        private String vertex;
        private boolean promised;
        private AtomicBoolean highIsDone;

        Sender(boolean lowFinishes) {
            this.lowFinishes = lowFinishes;
        }

        @Override
        protected void init(Context context) {
            instance = context.localProcessorIndex();
            vertex = context.vertexName();
            high = instance == HIGH_INSTANCE;
            highIsDone = HIGH_IS_DONE.computeIfAbsent(vertex, name -> new AtomicBoolean());
        }

        @Override
        public boolean complete() {
            if (!promised) {
                if (!tryEmit(new Watermark(high ? HIGH : LOW, AXIS))) {
                    return false;
                }
                promised = true;
            }
            if (high) {
                return true;
            }
            // Yields until the high instance's own DONE has been put on its queue - which is what close()
            // being called means - so the queue holding the top promise is never the last one open. Were it
            // last, the bound would already have been raised to that promise as its sibling finished, and
            // the edge would part with nothing left to raise.
            return lowFinishes && highIsDone.get();
        }

        /**
         * Setting a flag blocks on nothing, and saying so is what keeps {@link #close()} on the cooperative
         * thread that just queued this instance's DONE. Left undeclared, a cooperative processor's close is
         * handed to a shared pool, and the low instance's wait for the flag lasts however long that pool is
         * busy - no part of which is the property these cases are about.
         */
        @Override
        public boolean closeIsCooperative() {
            return true;
        }

        @Override
        public void close() {
            // Recorded before the flag is raised, so the instance the low one is waiting on is already in
            // the list by the time the low one can get here.
            FINISHED.computeIfAbsent(vertex, name -> Collections.synchronizedList(new ArrayList<>()))
                    .add(instance);
            if (high) {
                DONE_WAS_FLAGGED_ON.set(Thread.currentThread().getName());
                highIsDone.set(true);
            }
        }
    }

    /** Records every bound it was handed, through which callback, in the order it was handed it. */
    private static final class Receiver extends AbstractProcessor {

        private List<String> log;

        @Override
        protected void init(Context context) {
            log = Collections.synchronizedList(new ArrayList<>());
            SEEN.put(context.globalProcessorIndex(), log);
        }

        @Override
        public void process(int ordinal, Inbox inbox) {
            while (inbox.poll() != null) {
                // No data is sent; draining anything that appears keeps the queues moving.
            }
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            log.add(edge(ordinal, watermark.timestamp()));
            return true;
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            log.add(global(watermark.timestamp()));
            return true;
        }

        @Override
        public boolean completeEdge(int ordinal) {
            log.add(edgeCompleted(ordinal));
            return true;
        }
    }

    // ---- reading what the receivers saw -------------------------------------------------

    private static String edge(int ordinal, long timestamp) {
        return "e:" + ordinal + ":" + AXIS + ":" + timestamp;
    }

    private static String global(long timestamp) {
        return "g:" + AXIS + ":" + timestamp;
    }

    private static String edgeCompleted(int ordinal) {
        return "edge-completed:" + ordinal;
    }

    /**
     * Whether the instance holding the high promise was the first of that vertex to finish. The low instance
     * yields until the high one's DONE is queued, so this holds by construction - asserting it is what keeps
     * a case reading the high promise's value from silently becoming a case about the bound a still-open
     * queue leaves behind, which carries that same value.
     */
    private static boolean highFinishedFirstOn(String vertex) {
        List<Integer> finished = FINISHED.get(vertex);
        if (finished == null) {
            return false;
        }
        synchronized (finished) {
            return !finished.isEmpty() && finished.get(0) == HIGH_INSTANCE;
        }
    }

    private static List<String> boundsSeen() {
        Map<Integer, List<String>> copy = new LinkedHashMap<>();
        SEEN.forEach((instance, log) -> {
            synchronized (log) {
                copy.put(instance, List.copyOf(log));
            }
        });
        return copy.values().stream().flatMap(List::stream).distinct().sorted().toList();
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
