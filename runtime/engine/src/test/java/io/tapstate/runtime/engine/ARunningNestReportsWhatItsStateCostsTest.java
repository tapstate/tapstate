package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a running nest says how much state it is holding, how much of the reading it serves from memory,
 * and what the rest of it costs - and that those answers come back out of the run to whoever is watching.
 *
 * <p>The whole path is under test rather than any piece of it, because every piece of it is separately
 * plausible and silent when wrong. The counters are written on two different threads by two different
 * classes; the readings are left among the job's own statistics and can only be left from a job thread;
 * and they are picked back out by name, from a contract with an end at either side of the run. A break
 * anywhere along it shows up as a pipeline that reports no state at all - not as anything failing.
 *
 * <p>The job is a streaming one, left running while the assertions are made, because that is what the
 * readings are about: a run in flight. A bounded job that had already finished would report nothing, and
 * that absence is the intended answer rather than a gap.
 */
class ARunningNestReportsWhatItsStateCostsTest {

    private static final String PIPELINE = "p";
    private static final String STEP = "order_doc";

    /** The assembler's namespace, as the compiled tree names it. */
    private static final String ROOT_NAMESPACE = "nest." + PIPELINE + "." + STEP + ".$root";

    /** The cold layer, shared by every store the member builds. Static because they are built there. */
    private static final Map<String, byte[]> COLD = new ConcurrentHashMap<>();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        COLD.clear();
        Config config = new Config();
        config.setClusterName("nest-state-readings-test-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        // Collected often, because the assertions wait for a collection to happen. The default cadence is
        // several seconds, which would be spent waiting rather than testing.
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        member = Hazelcast.newHazelcastInstance(config);
        NestStateMapStoreFactory.bindTo(member, new MapBackedStore());
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("a live nest reports how full its state is, how often it was read and what the misses cost")
    void theReadingsOfARunningNestReachTheOutsideOfTheRun() {
        Engine engine = new Engine(member);
        engine.submit(PIPELINE, ordersWithItems());
        try {
            NestStateReading reading = awaitReading();

            assertThat(reading.entries())
                    .describedAs("every root is being held, including the last one written - a reading left "
                            + "only on reads reports what was there before the final write and then goes "
                            + "quiet, which is exactly when an alarm on how full the state is needed to fire")
                    .isEqualTo(3);
            assertThat(reading.accesses())
                    .describedAs("every row that arrived reached for the state of its root")
                    .isGreaterThanOrEqualTo(3);
            assertThat(reading.backfills())
                    .describedAs("a key read for the first time is not in memory, so it goes to the layer "
                            + "behind - which is the miss the whole reading exists to make visible")
                    .isPositive();
            assertThat(reading.backfills())
                    .describedAs("a trip behind the map is made by a reach for the state, so there cannot "
                            + "be more trips than reaches - which only holds because a write counts as one")
                    .isLessThanOrEqualTo(reading.accesses());
            assertThat(reading.backfillMillis())
                    .describedAs("time spent behind the map is measured, not assumed")
                    .isNotNegative();
        } finally {
            engine.cancel(PIPELINE);
        }
    }

    @Test
    void aPipelineWithNoLiveJobReportsNothingRatherThanAnEmptyState() {
        // Absence and zero call for opposite responses: a state layer that has stopped being reported and
        // one that has emptied look the same to a reader given zeroes for both.
        assertThat(new Engine(member).nestStateReadings("never-ran")).isEmpty();
    }

    // ---- fixtures ---------------------------------------------------------------------

    /** Waits for a collection carrying the assembler's readings; fails the test rather than returning none. */
    private NestStateReading awaitReading() {
        Engine engine = new Engine(member);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            NestStateReading reading = engine.nestStateReadings(PIPELINE).get(ROOT_NAMESPACE);
            if (reading != null && reading.accesses() >= 3) {
                return reading;
            }
            sleep();
        }
        throw new AssertionError("no reading for " + ROOT_NAMESPACE + " arrived within budget");
    }

    private static void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** orders as the root with order_items embedded beneath it, over map-backed state that reads through. */
    private static DAG ordersWithItems() {
        Embed item = new Embed("item", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "items",
                List.of("item_id"), null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(item)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("item", FromRef.literal("order_items"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("orders", "order_items"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        // Roots only, and no child row at all: it makes the last thing that ever touches the state a write.
        // Fed children too, every write would be followed by some later read that happened to carry its
        // effect out with it, and a reading left on reads alone would look exactly like one left on both.
        sources.put("orders", rowsSource("orders",
                List.of(row("order_id", 1, "code", "A"), row("order_id", 2, "code", "B"),
                        row("order_id", 3, "code", "C"))));
        sources.put("order_items", rowsSource("order_items", List.of()));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("item", new NestTable("order_items", List.of("item_id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) DiscardingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), element -> { }));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    /** Emits the rows once and then stays alive, so the job keeps running while the readings are read. */
    private static ProcessorMetaSupplier rowsSource(String stream, List<Map<String, Object>> rows) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsThenIdle(stream, rows)));
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(a, av);
        row.put(b, bv);
        return row;
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv, String c, Object cv) {
        Map<String, Object> row = row(a, av, b, bv);
        row.put(c, cv);
        return row;
    }

    /** A source that emits its rows and then never completes, keeping the job in flight. */
    private static final class RowsThenIdle extends AbstractProcessor {

        private final String stream;
        private final List<Map<String, Object>> rows;
        private int next;

        private RowsThenIdle(String stream, List<Map<String, Object>> rows) {
            this.stream = stream;
            this.rows = rows;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            if (next < rows.size()) {
                Map<String, Object> row = rows.get(next);
                Envelope event = Envelope.insert(next + 1L, stream, row, null)
                        .withOrder(new SourceOrder(1, next));
                if (!tryEmit(event)) {
                    return false;
                }
                next++;
                return false;
            }
            sleep();
            return false;
        }
    }

    /** A sink that keeps nothing: what is being watched is the state behind the assembler, not its output. */
    private static final class DiscardingSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> batch) {
            return CompletableFuture.completedFuture(new WriteResult(batch.size()));
        }

        @Override
        public void close() {
        }
    }

    /** A cold layer in a map, so a read through it is a real trip with a real duration. */
    private static final class MapBackedStore implements KeyedStateStore, java.io.Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(COLD.get(namespace + " " + key));
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            COLD.put(namespace + " " + key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            COLD.remove(namespace + " " + key);
        }

        @Override
        public long count(String namespace) {
            return COLD.keySet().stream().filter(entry -> entry.startsWith(namespace + " ")).count();
        }

        @Override
        public void dropNamespace(String namespace) {
            COLD.keySet().removeIf(entry -> entry.startsWith(namespace + " "));
        }
    }
}
