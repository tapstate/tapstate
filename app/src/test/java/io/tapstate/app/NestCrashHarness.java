package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoKeyedStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.runtime.engine.nest.NestVertex;
import io.tapstate.spi.store.KeyedStateStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * The process the crash witness kills, and the same process started again over the state the killed one
 * left behind. It is a {@code main} rather than a test because the witness needs a real process to send a
 * real signal to: a member shut down from inside the test JVM gets to run its own code on the way out,
 * which is the one thing a crash never does.
 *
 * <p>It runs the smallest tree with a resolver in it - customers, policies beneath them, claims beneath
 * those - and drives it in one of two modes:
 *
 * <ul>
 *   <li><b>hold</b>: only claims arrive, and the policies they hang from never do. Every claim is
 *       therefore kept at the policies resolver, and the bound climbs past it: what the frontier promises
 *       is that everything at or below it is either at a sink or held somewhere durable. The bound the job
 *       lets through is written to the report file as it advances, each line forced to disk before the
 *       next is written - a report that was only in a page cache would be gone along with what it is
 *       supposed to be evidence about.</li>
 *   <li><b>recover</b>: nothing is fed to the resolver at all. The customer and the policies arrive, and
 *       every claim in the document therefore came back out of the store the killed process wrote it to.
 *       The claim ids that surfaced are written to the report file.</li>
 * </ul>
 *
 * <p><b>The store is slowed on purpose in hold mode, and that is what makes the witness decide anything.</b>
 * The order under test - the write comes back before this level says how far the frontier may go - is a
 * few statements apart, so a process killed at an arbitrary instant would find them in the right order
 * whether or not anything holds them there. Delaying each save widens the gap between "the bound was let
 * through" and "the write landed" from microseconds to seconds, so a kill taken the moment the bound moves
 * lands inside it. Nothing about the operator changes: this is the cold layer being slow, which is a thing
 * cold layers are.
 *
 * <p>The claims source stops feeding rows after its burst and only goes on raising its bound. A level that
 * settled its drain somewhere other than in the drain would have nothing left to trigger it, so the gap
 * stays open rather than being closed by the next batch happening along.
 */
public final class NestCrashHarness {

    /** Rows in the burst. Small: every one of them pays the store delay twice over, once here and once back. */
    static final int CLAIMS = 8;

    static final String CUSTOMERS = "customers";
    static final String POLICIES = "policies";
    static final String CLAIMS_STREAM = "claims";

    /**
     * Where each embed lands in the document. Deliberately not the stream names: a reader that went by the
     * stream it came from instead of the path it was placed at would still find something, and this is the
     * one place that would not notice.
     */
    static final String POLICIES_AT = "policy_list";

    static final String CLAIMS_AT = "claim_list";

    static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES, CLAIMS_STREAM));

    /** Prefix of the report line naming a state namespace this run uses. */
    static final String NAMESPACE_LINE = "namespace=";

    /** Prefix of the report line naming a bound that got past the nest. */
    static final String BOUND_LINE = "bound=";

    /** Prefix of the report line naming a claim id that came back out of the store. */
    static final String RECOVERED_LINE = "recovered=";

    /** Far above anything that arrives, so the run ends on a bound that is unmistakably past every claim. */
    static final long FAR_ABOVE = FrontierOrders.pack(CLAIMS_STREAM, new SourceOrder(1, 9000));

    /** Where the report goes. Static because the job runs on this member, in this process. */
    private static Report report;

    /** What the recover run has seen assembled, highest reading kept. */
    private static final List<Integer> RECOVERED = Collections.synchronizedList(new ArrayList<>());

    private NestCrashHarness() {
    }

    /** Where claim {@code index} came from, and so where the bound has to reach to have passed it. */
    static SourceOrder claimAt(int index) {
        return new SourceOrder(1, index + 1L);
    }

    /** The bound value that has just passed claim {@code index}. */
    static long boundPast(int index) {
        return FrontierOrders.pack(CLAIMS_STREAM, claimAt(index));
    }

    /**
     * Runs one mode to completion, or until killed.
     *
     * @param args mongo connection string, report file, mode ({@code hold} or {@code recover}), and the
     *             millis each save is delayed by (hold only; ignored otherwise)
     */
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Path reportFile = Path.of(args[1]);
        String mode = args[2];
        long saveDelayMillis = Long.parseLong(args[3]);
        boolean holding = "hold".equals(mode);

        report = new Report(reportFile);
        KeyedStateStore cold = new MongoKeyedStateStore(MongoClients.create(uri)
                .getDatabase(MongoStorePort.NEST_STATE_DATABASE)
                .getCollection(MongoStorePort.OPERATOR_STATE));

        HazelcastInstance member = member();
        NestStateMapStoreFactory.bindTo(member, holding ? new SlowToStore(cold, saveDelayMillis) : cold);
        member.getJet().newJob(job(member, holding));

        if (holding) {
            // Killed from outside, at a moment the report file decides. Nothing here ends the run on its
            // own account: an exit of its own would be a shutdown, and a shutdown is the one thing this
            // witness must not be measuring. The deadline is only so that a run whose killer died first
            // does not sit here holding a member for good - it is far beyond how long the witness waits,
            // so reaching it means something went wrong rather than something finished.
            long abandoned = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
            while (System.nanoTime() < abandoned) {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
            return;
        }
        awaitRecovered();
        member.shutdown();
        System.exit(0);
    }

    /**
     * Waits until every claim the killed process left behind has surfaced inside a document, and reports
     * the ones that did.
     *
     * <p>What it waits for is that count, not a lull in it. An earlier version stopped at the first 200ms
     * sample that added nothing, which says nothing about the run being over: the claims come back one
     * policy at a time, and any gap between two of them wider than the sample - a read from the cold layer,
     * a coalescing window, a thread not scheduled - reads exactly like the end of them. The run was then
     * shut down with the rest still in flight and reported the few that had arrived, so what the witness
     * compared against was settled by how fast the machine was rather than by what survived the kill: the
     * same state passed here and failed on a slower machine.
     *
     * <p>The deadline stays, as the failure path, and it is what keeps this able to fail at all: a run that
     * genuinely lost claims never reaches the count, reports the few it has, and is judged on those.
     *
     * <p><b>Its length is set by how long a healthy run can take on the slowest machine this is run on, not
     * by how long one usually takes.</b> Measured: the whole case takes about 7 seconds on an idle developer
     * machine and about 29 on the same machine loaded past its cores, and on a shared two-core runner the same
     * tree has finished in 7 seconds and been unfinished at 60 - the claims arriving in the same order in both,
     * simply fewer of them. So a minute is a bound on the machine rather than on the state, and the two are
     * told apart by which of them the number is chosen from.
     *
     * <p>Lengthening it costs nothing in what this can catch. A run that lost claims never reaches the count
     * however long it waits, so the deadline only decides how late that verdict arrives; what a short one buys
     * is a red build on a busy machine, which says nothing about the state at all.
     */
    private static void awaitRecovered() {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
        Set<Integer> surfaced = new TreeSet<>(List.copyOf(RECOVERED));
        while (System.nanoTime() < deadline && surfaced.size() < CLAIMS) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
            // Copied rather than iterated in place: the collector is still adding to it from the job's
            // threads, and a synchronized list only synchronizes the call, not a traversal of it.
            surfaced = new TreeSet<>(List.copyOf(RECOVERED));
        }
        for (Integer claim : surfaced) {
            report.line(RECOVERED_LINE + claim);
        }
        report.line("done");
    }

    // ---- the job ------------------------------------------------------------------------

    /**
     * Customers with policies beneath them and claims beneath those, wired onto map-backed state so that
     * what a level keeps outlives the process keeping it.
     */
    private static DAG job(HazelcastInstance member, boolean holding) {
        Embed claims = new Embed("cl", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, CLAIMS_AT,
                List.of("claim_id"), null, null, null);
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, POLICIES_AT,
                List.of("policy_id"), null, null, List.of(claims));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(policies)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));
        tables.put("p", new NestTable(POLICIES, List.of("policy_id")));
        tables.put("cl", new NestTable(CLAIMS_STREAM, List.of("claim_id")));

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, holding ? List.of() : customerRows())));
        byAlias.put("p", dag.newVertex(POLICIES, source(POLICIES, holding ? List.of() : policyRows())));
        byAlias.put("cl", dag.newVertex(CLAIMS_STREAM,
                source(CLAIMS_STREAM, holding ? claimRows() : List.of())));

        NestTopology topology = NestTopology.compile("p", "doc", body, tables::get);
        // Reported before the job starts: the namespaces are what the witness reads afterwards, and after
        // the kill there is nobody left to name them.
        for (NestVertex vertex : topology.vertices()) {
            report.line(NAMESPACE_LINE + vertex.mapName());
        }

        Map<String, String> chainOfAlias = Map.of("c", CUSTOMERS, "p", POLICIES, "cl", CLAIMS_STREAM);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag, topology, "doc", "c", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, NestBinding.onMap().bind(member),
                        (from, released) -> report.line("unassemblable=" + from + ":" + released)),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))));

        Vertex collector = dag.newVertex("collector", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) Collector::new)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1)
                .to(collector, 0).distributed());
        return dag;
    }

    private static List<Row> claimRows() {
        List<Row> rows = new ArrayList<>();
        for (int index = 0; index < CLAIMS; index++) {
            rows.add(new Row(row("claim_id", index, "policy_id", "P" + index), claimAt(index)));
        }
        return rows;
    }

    private static List<Row> policyRows() {
        List<Row> rows = new ArrayList<>();
        for (int index = 0; index < CLAIMS; index++) {
            rows.add(new Row(row("policy_id", "P" + index, "customer_id", "C1"),
                    new SourceOrder(1, index + 1L)));
        }
        return rows;
    }

    private static List<Row> customerRows() {
        return List.of(new Row(row("customer_id", "C1"), new SourceOrder(1, 1)));
    }

    /** One row a source emits, with the order the engine would have stamped on it. */
    private record Row(Map<String, Object> fields, SourceOrder order) implements java.io.Serializable {
    }

    private static ProcessorMetaSupplier source(String stream, List<Row> rows) {
        List<Row> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsThenBounds(stream, plan)));
    }

    /**
     * Emits its rows, then raises its bound one claim at a time and stops feeding rows for good. The
     * pauses between bounds are what let the witness catch the bound crossing a particular claim rather
     * than finding it already far past every one of them.
     */
    private static final class RowsThenBounds extends AbstractProcessor {

        private final String stream;
        private final List<Row> rows;
        private final List<Long> bounds = new ArrayList<>();
        private int nextRow;
        private int nextBound;

        RowsThenBounds(String stream, List<Row> rows) {
            this.stream = stream;
            this.rows = rows;
            // Each source raises the bound on its own chain. The claims source climbs one claim at a
            // time, which is what lets the witness read off how far the frontier has actually gone; the
            // other two go straight up, since nothing is waiting on them to be read off.
            if (CLAIMS_STREAM.equals(stream)) {
                for (int index = 0; index < CLAIMS; index++) {
                    bounds.add(boundPast(index));
                }
                bounds.add(FAR_ABOVE);
            } else {
                bounds.add(FrontierOrders.pack(stream, new SourceOrder(1, 9000)));
            }
        }

        /** Not cooperative so it can pace itself: raising the bound as fast as it can spins the job. */
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            while (nextRow < rows.size()) {
                Row row = rows.get(nextRow);
                if (!tryEmit(Envelope.insert(nextRow + 1L, stream, row.fields(), null)
                        .withOrder(row.order()))) {
                    return false;
                }
                nextRow++;
            }
            // Past the staged list it goes on climbing: a level only reconsiders a chain when a bound on
            // it arrives, so a source that stopped would leave nothing to carry the next answer.
            long next = nextBound < bounds.size() ? bounds.get(nextBound) : bounds.getLast() + nextBound;
            if (!tryEmit(new Watermark(next, AXES.axisOf(stream)))) {
                return false;
            }
            nextBound++;
            // Never finishes: a completed queue stops constraining the coalesced bound, and this run is
            // about what the bound is allowed to be.
            return false;
        }
    }

    /** Reports every bound that got past the nest, and every claim that came back inside a document. */
    private static final class Collector extends AbstractProcessor {

        /** Not cooperative: reporting forces the file to disk, which is not something to do on a shared thread. */
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess(int ordinal, Object item) {
            if (item instanceof Envelope document) {
                report.line("document=" + document.after());
                Object embedded = document.after().get(POLICIES_AT);
                if (embedded instanceof List<?> policies) {
                    for (Object policy : policies) {
                        Object nested = ((Map<String, Object>) policy).get(CLAIMS_AT);
                        if (nested instanceof List<?> claims) {
                            for (Object claim : claims) {
                                RECOVERED.add((Integer) ((Map<String, Object>) claim).get("claim_id"));
                            }
                        }
                    }
                }
            }
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            if (CLAIMS_STREAM.equals(AXES.chainOn(watermark.key()))) {
                report.line(BOUND_LINE + watermark.timestamp());
            }
            return true;
        }
    }

    // ---- the two things the witness needs from outside the process ------------------------

    /**
     * The cold layer, made slow. A save that takes its time is the only difference from the real one, and
     * it is the difference the witness is built around: it turns the instant between "the write went out"
     * and "the write came back" into a window a signal can be delivered in.
     */
    private static final class SlowToStore implements KeyedStateStore {

        private final KeyedStateStore delegate;
        private final long delayMillis;

        SlowToStore(KeyedStateStore delegate, long delayMillis) {
            this.delegate = delegate;
            this.delayMillis = delayMillis;
        }

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            return delegate.load(namespace, key);
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMillis));
            delegate.save(namespace, key, state);
        }

        @Override
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMillis));
            return delegate.saveIfAbsent(namespace, key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            delegate.delete(namespace, key);
        }

        @Override
        public void dropNamespace(String namespace) {
            delegate.dropNamespace(namespace);
        }

        @Override
        public long count(String namespace) {
            return delegate.count(namespace);
        }
    }

    /**
     * The report, forced to disk a line at a time. A buffered report would be a claim about what happened
     * before the kill that was itself lost to the kill.
     */
    private static final class Report {

        private final FileChannel channel;

        Report(Path file) throws IOException {
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
        }

        synchronized void line(String text) {
            try {
                channel.write(StandardCharsets.UTF_8.encode(text + System.lineSeparator()));
                channel.force(false);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < fields.length; index += 2) {
            row.put((String) fields[index], fields[index + 1]);
        }
        return row;
    }

    private static HazelcastInstance member() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        return Hazelcast.newHazelcastInstance(config);
    }
}
