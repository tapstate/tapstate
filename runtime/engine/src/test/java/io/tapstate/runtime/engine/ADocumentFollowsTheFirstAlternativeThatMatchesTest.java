package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
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
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That an embed with {@code orOn} alternatives shows the row named by the first alternative that has a live
 * match, and keeps showing the right one as rows on either side change.
 *
 * <p>The shape is a wallet transaction pointing at a location either by the location's code or by its hub
 * id: the same location table, matched on one of two columns. Each change below moves at least one document
 * from one answer to another, and each move is the one a hand-written "first lookup, else second lookup"
 * would make:
 *
 * <ul>
 *   <li>a document whose first alternative names no row shows the row its second one names;
 *   <li>a row arriving under the first alternative takes over from the second, and deleting it hands back;
 *   <li>a location whose hub id is edited stops answering under the value it left, and starts answering
 *       under the new one for a document that is re-pointed at it;
 *   <li>a null in an alternative's column matches nothing, however many rows share that null.
 * </ul>
 *
 * <p>It runs a whole job on one member, so the lookups, the record of who points where and the assembler
 * are all the ones a pipeline builds, wired the way a pipeline wires them.
 */
class ADocumentFollowsTheFirstAlternativeThatMatchesTest {

    private static final String WALLETS = "wallets";
    private static final String LOCATIONS = "locations";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(WALLETS, LOCATIONS));

    /** Every document the job sent, in the order it sent them. Static: the job runs on the member. */
    private static final List<Map<String, Object>> DOCUMENTS = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void start() {
        DOCUMENTS.clear();
        Config config = new Config();
        config.setClusterName("nest-or-on-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stop() {
        if (job != null) {
            job.cancel();
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("each document shows the row its first matching alternative names, through every change")
    void eachDocumentFollowsItsFirstMatchingAlternative() {
        Map<String, Object> alpha = row("loc_id", 1, "locn_code", "A", "hub_locn_id", null, "name", "Alpha");
        Map<String, Object> hub = row("loc_id", 2, "locn_code", null, "hub_locn_id", "H2", "name", "Hub");
        Map<String, Object> beta = row("loc_id", 3, "locn_code", "B", "hub_locn_id", null, "name", "Beta");
        Map<String, Object> hubMoved = row("loc_id", 2, "locn_code", null, "hub_locn_id", "H9", "name", "Hub");
        Map<String, Object> hubRenamed = row("loc_id", 2, "locn_code", null, "hub_locn_id", "H9", "name", "Hub9");
        Map<String, Object> idle = row("wallet_id", 4, "loc_code", "Z", "hub_id", "Z9");

        run(List.of(
                Envelope.insert(1, LOCATIONS, alpha, null),
                Envelope.insert(2, LOCATIONS, hub, null),
                Envelope.insert(3, LOCATIONS,
                        row("loc_id", 5, "locn_code", null, "hub_locn_id", null, "name", "Nowhere"), null)),
                List.of(
                Envelope.insert(1, WALLETS, row("wallet_id", 1, "loc_code", "A", "hub_id", null), null),
                Envelope.insert(2, WALLETS, row("wallet_id", 2, "loc_code", null, "hub_id", "H2"), null),
                Envelope.insert(3, WALLETS, row("wallet_id", 3, "loc_code", "B", "hub_id", "H2"), null),
                Envelope.insert(4, WALLETS, idle, null),
                Envelope.insert(5, WALLETS, row("wallet_id", 5, "loc_code", null, "hub_id", null), null)));

        await(() -> locationOf(1).equals("Alpha") && locationOf(2).equals("Hub") && locationOf(3).equals("Hub")
                && seen(4) && seen(5));
        assertThat(locationOf(3))
                .describedAs("wallet 3 names a code no location has yet, so its hub id supplies the location")
                .isEqualTo("Hub");
        assertThat(locationOf(5))
                .describedAs("a wallet with both columns null matches nothing - not the location whose columns "
                        + "are null too")
                .isEqualTo("-");

        feed(LOCATIONS, Envelope.insert(10, LOCATIONS, beta, null));
        await(() -> locationOf(3).equals("Beta"));

        feed(LOCATIONS, Envelope.delete(11, LOCATIONS, beta, null));
        await(() -> locationOf(3).equals("Hub"));

        feed(LOCATIONS, Envelope.update(12, LOCATIONS, hub, hubMoved, null));
        await(() -> locationOf(2).equals("-") && locationOf(3).equals("-"));

        feed(WALLETS, Envelope.update(13, WALLETS, idle, row("wallet_id", 4, "loc_code", "Z", "hub_id", "H9"), null));
        await(() -> locationOf(4).equals("Hub"));

        feed(LOCATIONS, Envelope.update(14, LOCATIONS, hubMoved, hubRenamed, null));
        await(() -> locationOf(4).equals("Hub9"));

        assertThat(latestByWallet())
                .describedAs("the documents settle where a lookup by code, else by hub id, would put them")
                .containsEntry(1, "Alpha")
                .containsEntry(2, "-")
                .containsEntry(3, "-")
                .containsEntry(4, "Hub9")
                .containsEntry(5, "-");
    }

    // ---- the job under test ------------------------------------------------------------

    /** Rows each source is due to send next, fed while the job runs. Static: the sources run on the member. */
    private static final Map<String, List<Envelope>> QUEUED = new java.util.concurrent.ConcurrentHashMap<>();

    private static void feed(String stream, Envelope event) {
        QUEUED.computeIfAbsent(stream, ignored -> Collections.synchronizedList(new ArrayList<>())).add(event);
    }

    private void run(List<Envelope> locations, List<Envelope> wallets) {
        QUEUED.clear();
        locations.forEach(event -> feed(LOCATIONS, event));
        wallets.forEach(event -> feed(WALLETS, event));

        Embed location = new Embed("l", Map.of("locn_code", "loc_code"), EmbedAs.OBJECT, "location", null,
                null, null, null, null, List.of(Map.of("hub_locn_id", "hub_id")));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("w", List.of("wallet_id"), null, null, List.of(location)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("w", new NestTable(WALLETS, List.of("wallet_id")));
        tables.put("l", new NestTable(LOCATIONS, List.of("loc_id")));

        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("w", dag.newVertex(WALLETS, source(WALLETS)));
        byAlias.put("l", dag.newVertex(LOCATIONS, source(LOCATIONS)));

        Map<String, String> chainOfAlias = Map.of("w", WALLETS, "l", LOCATIONS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag,
                NestTopology.compile("p", "doc", body, tables::get),
                "doc", "w", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))),
                new NodeWidth("doc", 2, 1, null));

        Vertex collector = dag.newVertex("collect", ProcessorSupplier.of((SupplierEx<Processor>) Collecting::new));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1).to(collector, 0));
        job = JetJobs.submit(member, dag, "nest-or-on");
    }

    /** Keeps every document that reaches it. */
    private static final class Collecting extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (item instanceof Envelope envelope && envelope.after() != null) {
                DOCUMENTS.add(envelope.after());
            }
            return true;
        }
    }

    private static ProcessorMetaSupplier source(String stream) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new QueuedThenBounds(stream)));
    }

    /**
     * Sends whatever has been queued for its stream, in order, and keeps raising its bound for as long as the
     * job runs. Rows are spaced far apart on the chain so the bound can rise on every call without ever
     * passing a row that has not been sent yet: a level only reconsiders a chain when a bound on it arrives.
     */
    private static final class QueuedThenBounds extends AbstractProcessor {

        private static final long GAP = 1_000_000L;

        private final String stream;
        private int next;
        private long promised;

        QueuedThenBounds(String stream) {
            this.stream = stream;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            List<Envelope> queued = QUEUED.getOrDefault(stream, List.of());
            synchronized (queued) {
                while (next < queued.size()) {
                    Envelope event = queued.get(next).withOrder(new SourceOrder(1, (next + 1) * GAP));
                    if (!tryEmit(event)) {
                        return false;
                    }
                    next++;
                }
            }
            long ceiling = (next + 1) * GAP - 1;
            if (promised < ceiling) {
                promised = Math.max(promised + 1, next * GAP);
                if (!tryEmit(new Watermark(FrontierOrders.pack(stream, new SourceOrder(1, promised)),
                        AXES.axisOf(stream)))) {
                    promised--;
                    return false;
                }
            }
            return false;
        }
    }

    // ---- reading what got sent ----------------------------------------------------------

    /** The location name on the last document sent for each wallet, {@code -} where it showed none. */
    private static Map<Object, String> latestByWallet() {
        Map<Object, String> latest = new LinkedHashMap<>();
        synchronized (DOCUMENTS) {
            for (Map<String, Object> document : DOCUMENTS) {
                latest.put(document.get("wallet_id"), document.get("location") instanceof Map<?, ?> location
                        ? String.valueOf(location.get("name")) : "-");
            }
        }
        return latest;
    }

    private static String locationOf(Object wallet) {
        return Objects.requireNonNullElse(latestByWallet().get(wallet), "?");
    }

    private static boolean seen(Object wallet) {
        return latestByWallet().containsKey(wallet);
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    private static void await(BooleanSupplier reached) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            if (reached.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        throw new AssertionError("what was waited for never happened; latest: " + latestByWallet());
    }
}
