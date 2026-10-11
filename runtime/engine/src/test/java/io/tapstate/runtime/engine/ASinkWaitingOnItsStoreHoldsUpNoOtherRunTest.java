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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A sink that reports what it has landed to a durable store waits on that store, and nothing else on its
 * member waits with it.
 *
 * <p>The report is a round trip made from the sink's own processing, and a round trip takes as long as the
 * store takes to answer. A store that is holding a document for a writer that stopped halfway through
 * writing it keeps every other writer of that document waiting, for as long as it goes on holding it. A
 * cooperative processor shares its thread with every other cooperative vertex on the member, of every run,
 * so a sink waiting there stops all of them - runs that have nothing to do with that store go quiet
 * together, and nothing fails or says why.
 *
 * <p>The member runs one cooperative thread, so whether another run shares a thread with the waiting sink
 * is not left to how the engine happened to spread the vertices out: every cooperative vertex shares it.
 */
class ASinkWaitingOnItsStoreHoldsUpNoOtherRunTest {

    private static final String ACK_KEY = "test.waiting.ack";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("waiting-sink-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(1);
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
        if (member != null) {
            member.getLifecycleService().terminate();
        }
    }

    @Test
    @DisplayName("another run on the member finishes while a sink is waiting on its store")
    void anotherRunFinishesWhileASinkWaitsOnItsStore() throws Exception {
        CountDownLatch reporting = new CountDownLatch(1);
        CountDownLatch answered = new CountDownLatch(1);
        member.getUserContext().put(ACK_KEY, new WaitingAck(reporting, answered));
        try {
            Job waiting = member.getJet().newJob(rowsIntoAnAckedSink());
            assertThat(reporting.await(30, TimeUnit.SECONDS))
                    .as("the sink reached its report and is waiting there for the store to answer, which is "
                            + "the state every assertion below is about")
                    .isTrue();

            Job other = member.getJet().newJob(aRunOfItsOwn());
            try {
                other.getFuture().get(20, TimeUnit.SECONDS);
            } catch (TimeoutException stillRunning) {
                throw new AssertionError("a run with no sink and no store in it did not finish while another "
                        + "run's sink was waiting on its store: the two were sharing a thread, and it was "
                        + "the sink's; its status was " + other.getStatus(), stillRunning);
            }
            assertThat(reporting.getCount())
                    .as("and the sink was still waiting when it finished, so this is not a run that went "
                            + "through once the store answered")
                    .isZero();
            assertThat(answered.getCount()).isOne();
            waiting.cancel();
        } finally {
            answered.countDown();
        }
    }

    /** Two rows on one chain into a sink that reports each position it lands through the member's ack. */
    private static DAG rowsIntoAnAckedSink() {
        DAG dag = new DAG();
        Vertex rows = dag.newVertex("rows", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) TwoRowsThenIdle::new)));
        Vertex sink = dag.newVertex("sink", SinkProcessor.metaSupplier("sink", LandingWriter::new,
                member -> (SinkAck) member.getUserContext().get(ACK_KEY), EveryPositionLands::new));
        dag.edge(Edge.between(rows, sink).distributed().allToOne("sink"));
        return dag;
    }

    /** One vertex of the default kind, which is cooperative, and which has nothing to do but finish. */
    private static DAG aRunOfItsOwn() {
        DAG dag = new DAG();
        dag.newVertex("done", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) Finishes::new)));
        return dag;
    }

    /** Reports that it has been asked, then waits until it is answered - a store holding a document. */
    private static final class WaitingAck implements SinkAck {

        private final transient CountDownLatch reporting;
        private final transient CountDownLatch answered;

        WaitingAck(CountDownLatch reporting, CountDownLatch answered) {
            this.reporting = reporting;
            this.answered = answered;
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            reporting.countDown();
            try {
                answered.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Hands every settled position to the ack as it settles, so the first write settling is a report. */
    private static final class EveryPositionLands implements SinkFrontier {

        @Override
        public List<ChainEntry> positions(List<Envelope> batch) {
            List<ChainEntry> entries = new ArrayList<>(batch.size());
            batch.forEach(row -> entries.add(new ChainEntry(row.src(), row.position())));
            return entries;
        }

        @Override
        public void settled(List<ChainEntry> positions, SinkAck ack) {
            positions.forEach(entry -> ack.advance(entry.chain(), entry.position()));
        }

        @Override
        public void bound(Watermark bound, SinkAck ack) {
        }

        @Override
        public Map<String, Long> gaps() {
            return Map.of();
        }

        @Override
        public Map<String, Long> stalls() {
            return Map.of();
        }
    }

    /** Lands every batch at once. */
    private static final class LandingWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** Emits two rows of chain {@code orders}, then stays open without emitting anything more. */
    private static final class TwoRowsThenIdle extends AbstractProcessor {

        private int emitted;

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            while (emitted < 2) {
                long seq = emitted + 1L;
                Envelope row = Envelope.insert(1L, "orders", Map.of("id", seq), null)
                        .withSrcPos("p" + seq)
                        .withOrder(new SourceOrder(1, seq));
                if (!tryEmit(row)) {
                    return false;
                }
                emitted++;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    /** Finishes on its first call. */
    private static final class Finishes extends AbstractProcessor {

        @Override
        public boolean complete() {
            return true;
        }
    }
}
