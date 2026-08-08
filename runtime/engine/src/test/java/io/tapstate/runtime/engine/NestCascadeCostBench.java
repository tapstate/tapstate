package io.tapstate.runtime.engine;

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
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
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
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Measures what a nest tree costs as it grows, along the two axes it grows on: deeper, which buys
 * shuffles, and wider, which buys vertices and the threads behind them. Both are decided while
 * compiling and so cannot be tuned afterwards, which is why the numbers are worth having on record
 * rather than reasoned about.
 *
 * <p>This is an instrument, not a regression test: the shapes it builds are already asserted
 * elsewhere, and wall-clock numbers do not belong in a build gate. Its name keeps it out of the
 * default surefire selection, so it costs a build nothing and is run deliberately:
 *
 * <pre>{@code mvn -o test -pl runtime/engine -am -Dtest=NestCascadeCostBench -DfailIfNoTests=false}</pre>
 *
 * <p>Latency is read off the row itself rather than off the job: a stamp travels in the deepest leaf
 * row and is read again when the document carrying it reaches the sink, so what is measured is one
 * row's trip across the cascade and not how long a batch job takes to start and stop. The leaf source
 * paces itself so the number is the cost of the hops rather than the depth of a queue.
 */
class NestCascadeCostBench {

    /** How many roots each run drives, each contributing one stamped leaf row. */
    private static final int ROOTS = 300;

    /** The gap the leaf source leaves between rows, so hops are measured rather than queueing. */
    private static final long PACE_MILLIS = 2;

    /** How long ancestors are given to land before the first leaf row is sent after them. */
    private static final long ANCESTOR_HEAD_START_MILLIS = 400;

    /** Latency of every document that reached the sink carrying a stamp, in nanoseconds. */
    private static final List<Long> LATENCIES = Collections.synchronizedList(new ArrayList<>());

    /**
     * The stamps already counted. A document is re-emitted every drain that touches it, and it still
     * carries the stamp of the row that arrived first, so without this a wide tree measures its own
     * later branches as if they were the same row arriving late.
     */
    private static final Set<Long> COUNTED = ConcurrentHashMap.newKeySet();

    /** The field a stamped leaf row carries its send time in. */
    private static final String STAMP = "t0_nanos";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.setProperty("hazelcast.logging.type", "none");
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
            member.shutdown();
        }
    }

    /**
     * The two sweeps take a member each rather than sharing one. Jet's blocking threads come from a
     * cached pool, so one sweep leaves idle threads behind that the next would count as its own - and
     * did, reading a flat 51 for every width until they were separated.
     */
    @Test
    void reportsWhatDepthCosts() {
        environment();
        System.out.println("-- depth: a chain of D levels, the deepest one a leaf --");
        header("D");
        for (int depth = 1; depth <= 5; depth++) {
            run(chain(depth), depth, depth - 1);
        }
        System.out.println();
    }

    @Test
    void reportsWhatWidthCosts() {
        environment();
        System.out.println("-- width: a root with W embeds side by side, each with one leaf of its own --");
        header("W");
        for (int width = 1; width <= 6; width++) {
            run(fan(width), width, 1);
        }
        System.out.println();
    }

    private void environment() {
        System.out.println();
        System.out.println("cores=" + Runtime.getRuntime().availableProcessors()
                + "  jet.cooperativeThreadCount=" + member.getConfig().getJetConfig().getCooperativeThreadCount());
        System.out.println();
    }

    private static void header(String axis) {
        System.out.printf("%3s %10s %6s %6s %11s %11s %9s %9s %9s%n",
                axis, "resolvers", "edges", "hops", "p50 ms", "p95 ms", "procs", "threads", "docs");
    }

    // ---- running one shape -------------------------------------------------------------

    /** Builds, runs and reports one shape, printing the row for it. */
    private void run(Shape shape, int axis, int hops) {
        LATENCIES.clear();
        COUNTED.clear();
        DAG dag = shape.dag();
        int vertices = 0;
        int edges = 0;
        for (Vertex vertex : dag) {
            if (vertex.getName().startsWith("nest:")) {
                vertices++;
                edges += dag.getInboundEdges(vertex.getName()).size();
            }
        }

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger peakThreads = new AtomicInteger();
        Thread sampler = sampleBlockingThreads(running, peakThreads);
        try {
            member.getJet().newJob(dag).join();
        } finally {
            running.set(false);
        }
        try {
            sampler.join(1_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }

        List<Long> latencies = new ArrayList<>(LATENCIES);
        Collections.sort(latencies);
        int parallelism = member.getConfig().getJetConfig().getCooperativeThreadCount();
        System.out.printf("%3d %10d %6d %6d %11.2f %11.2f %9d %9d %9d%n", axis, vertices - 1, edges, hops,
                millis(latencies, 50), millis(latencies, 95), vertices * parallelism,
                peakThreads.get(), latencies.size());
    }

    /**
     * Watches how many blocking threads the member is running while the job is. Every nest vertex is
     * non-cooperative, so its processors do not share the cooperative pool - each one holds a thread
     * for as long as the job does, and that is the resource width buys.
     */
    private static Thread sampleBlockingThreads(AtomicBoolean running, AtomicInteger peak) {
        Thread sampler = new Thread(() -> {
            while (running.get()) {
                int blocking = 0;
                for (Thread thread : Thread.getAllStackTraces().keySet()) {
                    if (thread.getName().contains(".jet.blocking.")) {
                        blocking++;
                    }
                }
                peak.accumulateAndGet(blocking, Math::max);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        sampler.setDaemon(true);
        sampler.start();
        return sampler;
    }

    private static double millis(List<Long> sorted, int percentile) {
        if (sorted.isEmpty()) {
            return Double.NaN;
        }
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1);
        return sorted.get(Math.max(0, index)) / 1_000_000.0;
    }

    // ---- the shapes under measurement ---------------------------------------------------

    /** One compiled shape: the DAG to run, kept with nothing else the caller needs. */
    private record Shape(DAG dag) {
    }

    /**
     * A root with a single chain of {@code depth} embeds hanging off it, the deepest one a leaf. It
     * compiles to {@code depth - 1} resolver vertices, and a leaf row travels that many hops.
     */
    private static Shape chain(int depth) {
        Embed embed = null;
        for (int level = depth; level >= 1; level--) {
            embed = new Embed("a" + level, Map.of("p" + level, level == 1 ? "k0" : "k" + (level - 1)),
                    EmbedAs.ARRAY, "l" + level, List.of("k" + level), null, null,
                    embed == null ? null : List.of(embed));
        }
        NestRoot root = new NestRoot("a0", List.of("k0"), null, null, List.of(embed));

        List<String> aliases = new ArrayList<>();
        for (int level = 0; level <= depth; level++) {
            aliases.add("a" + level);
        }
        Map<String, List<Map<String, Object>>> rows = new LinkedHashMap<>();
        for (int level = 0; level <= depth; level++) {
            rows.put("a" + level, level == 0 ? rootRows() : levelRows(level, level == depth));
        }
        return new Shape(build(root, aliases, rows, Set.of("a" + depth), depth));
    }

    /**
     * A root with {@code width} embeds side by side, each carrying one leaf of its own so each is a
     * resolver vertex rather than a leaf. Depth is held at two so only width moves.
     */
    private static Shape fan(int width) {
        List<Embed> embeds = new ArrayList<>();
        for (int branch = 1; branch <= width; branch++) {
            Embed leaf = new Embed("b" + branch, Map.of("pb" + branch, "k" + branch), EmbedAs.ARRAY,
                    "lb" + branch, List.of("kb" + branch), null, null, null);
            embeds.add(new Embed("a" + branch, Map.of("p" + branch, "k0"), EmbedAs.ARRAY,
                    "l" + branch, List.of("k" + branch), null, null, List.of(leaf)));
        }
        NestRoot root = new NestRoot("a0", List.of("k0"), null, null, embeds);

        List<String> aliases = new ArrayList<>();
        aliases.add("a0");
        Map<String, List<Map<String, Object>>> rows = new LinkedHashMap<>();
        Set<String> paced = new LinkedHashSet<>();
        rows.put("a0", rootRows());
        for (int branch = 1; branch <= width; branch++) {
            aliases.add("a" + branch);
            aliases.add("b" + branch);
            rows.put("a" + branch, branchRows(branch));
            // Every branch is kept feeding, not just the stamped one: a branch that finishes early
            // hands its threads back, and then what is counted is the pool rather than the shape.
            rows.put("b" + branch, leafRows(branch, branch == width));
            paced.add("b" + branch);
        }
        return new Shape(build(root, aliases, rows, paced, 2));
    }

    // ---- the rows each shape is driven with ---------------------------------------------

    private static List<Map<String, Object>> rootRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int root = 1; root <= ROOTS; root++) {
            rows.add(row("k0", root));
        }
        return rows;
    }

    /** One row per root at this level of a chain, stamped when it is the level that gets measured. */
    private static List<Map<String, Object>> levelRows(int level, boolean stamped) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int root = 1; root <= ROOTS; root++) {
            Map<String, Object> row = row("k" + level, root * 1000 + level,
                    "p" + level, level == 1 ? root : root * 1000 + (level - 1));
            if (stamped) {
                row.put(STAMP, 0L);
            }
            rows.add(row);
        }
        return rows;
    }

    private static List<Map<String, Object>> branchRows(int branch) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int root = 1; root <= ROOTS; root++) {
            rows.add(row("k" + branch, root * 1000 + branch, "p" + branch, root));
        }
        return rows;
    }

    private static List<Map<String, Object>> leafRows(int branch, boolean stamped) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int root = 1; root <= ROOTS; root++) {
            Map<String, Object> row = row("kb" + branch, root * 1000 + branch,
                    "pb" + branch, root * 1000 + branch);
            if (stamped) {
                row.put(STAMP, 0L);
            }
            rows.add(row);
        }
        return rows;
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    // ---- wiring -------------------------------------------------------------------------

    /** The pipeline every shape runs as: one source per alias, the nest step, one collecting sink. */
    private static DAG build(NestRoot root, List<String> aliases,
            Map<String, List<Map<String, Object>>> rows, Set<String> paced, int depth) {
        Map<String, FromRef> aliasRefs = new LinkedHashMap<>();
        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        Map<String, NestTable> tables = new LinkedHashMap<>();
        List<String> sourceIds = new ArrayList<>();
        for (String alias : aliases) {
            String table = "t_" + alias;
            aliasRefs.put(alias, FromRef.literal(table));
            sourceIds.add(table);
            List<Map<String, Object>> ownRows = rows.get(alias);
            sources.put(table, rowsSource(table, ownRows, paced.contains(alias)));
            tables.put(alias, new NestTable(table, List.of(ownRows.get(0).keySet().iterator().next())));
        }

        Step step = Step.inline("doc", FromClause.aliases(aliasRefs),
                new TransformBody.Nest(null, null, root), null, null);
        PipelineResource pipeline = new PipelineResource("bench" + depth, null, sourceIds,
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) TimingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onHeap(), element -> { }));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    private static ProcessorMetaSupplier rowsSource(String src, List<Map<String, Object>> rows, boolean paced) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsSource(src, rows, paced)));
    }

    /**
     * Emits rows as inserts, stamping the send time into the ones that carry the field for it. The
     * stamped source waits for the rows it hangs from to land and then paces itself, so what the sink
     * measures is a row crossing an idle cascade rather than one queued behind a burst.
     */
    private static final class RowsSource extends AbstractProcessor {

        private final String src;
        private final List<Map<String, Object>> rows;
        private final boolean paced;
        private int next;
        private long startAt;

        RowsSource(String src, List<Map<String, Object>> rows, boolean paced) {
            this.src = src;
            this.rows = rows;
            this.paced = paced;
        }

        @Override
        public boolean isCooperative() {
            return !paced;
        }

        @Override
        public boolean complete() {
            if (paced && startAt == 0) {
                startAt = System.currentTimeMillis() + ANCESTOR_HEAD_START_MILLIS;
            }
            while (next < rows.size()) {
                if (paced) {
                    long due = startAt + next * PACE_MILLIS;
                    long wait = due - System.currentTimeMillis();
                    if (wait > 0) {
                        try {
                            Thread.sleep(wait);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return true;
                        }
                    }
                }
                Map<String, Object> row = rows.get(next);
                if (row.containsKey(STAMP)) {
                    row = new LinkedHashMap<>(row);
                    row.put(STAMP, System.nanoTime());
                }
                Envelope event = Envelope.insert(next + 1L, src, row, null)
                        .withOrder(new SourceOrder(1, next));
                if (!tryEmit(event)) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }

    /** Reads the stamp back out of whatever document carries it and records how long the trip took. */
    private static final class TimingSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            long arrived = System.nanoTime();
            for (Envelope record : records) {
                Long sent = findStamp(record.after());
                if (sent != null && COUNTED.add(sent)) {
                    LATENCIES.add(arrived - sent);
                }
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }

        /** The stamp wherever it ended up in the document, since how deep that is is the point. */
        private static Long findStamp(Object node) {
            if (node instanceof Map<?, ?> map) {
                Object stamped = map.get(STAMP);
                if (stamped instanceof Long value) {
                    return value;
                }
                for (Object child : map.values()) {
                    Long found = findStamp(child);
                    if (found != null) {
                        return found;
                    }
                }
            } else if (node instanceof List<?> list) {
                for (Object child : list) {
                    Long found = findStamp(child);
                    if (found != null) {
                        return found;
                    }
                }
            }
            return null;
        }
    }
}
