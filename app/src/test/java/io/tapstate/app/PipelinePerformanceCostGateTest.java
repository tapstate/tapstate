package io.tapstate.app;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import io.tapstate.runtime.engine.SettledPositions;
import io.tapstate.runtime.engine.join.ImapJoinStores;
import io.tapstate.runtime.engine.join.JoinBinding;
import io.tapstate.runtime.engine.join.JoinStores;
import io.tapstate.runtime.engine.nest.CostGateNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.net.ServerSocket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fixed small copy, stateless and Join/Nest sequences through the real compiler and supplied processors.
 * The source and external target are counting SPI fixtures. The cold Nest uses the production state
 * bridge and codecs. Join API calls, cold IO and cold codec completions remain separate quantities.
 * Queues are actual bounded TestOutbox queues driven synchronously; their peak is fixture occupancy,
 * not sampled native scheduler occupancy. Clocks are never performance verdicts in this gate.
 */
class PipelinePerformanceCostGateTest {
    private static HazelcastInstance member;
    private static final AtomicInteger NEXT = new AtomicInteger();
    private static final Map<String, Ledger> LEDGERS = new ConcurrentHashMap<>();

    enum Fault { NONE, EXTERNAL, STATE, CODEC, EMISSION, QUEUE }

    @BeforeAll
    static void initializeSupplierResolution() throws Exception {
        Config config = new Config();
        config.setClusterName("pipeline-cost-gate-" + System.nanoTime());
        try (ServerSocket socket = new ServerSocket(0)) {
            config.getNetworkConfig().setPort(socket.getLocalPort()).setPortAutoIncrement(false).setReuseAddress(false);
        }
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll static void closeSupplierResolution() { if (member != null) { member.shutdown(); } }

    @Test void copyLedgerHasTheExactCostOfFourDeliveredRows() throws Exception { copy(requestedFault()); }
    @Test void statelessLedgerHasTheExactFilterProjectionAndExpansionCost() throws Exception { stateless(requestedFault()); }
    @Test void statefulLedgerSeparatesJoinCallsFromNestColdCodecs() throws Exception { stateful(requestedFault()); }

    @Test void aRedundantExternalWriteCannotHideBehindAnIdempotentTarget() {
        assertThatThrownBy(() -> copy(Fault.EXTERNAL)).isInstanceOf(AssertionError.class).hasMessageContaining("target writes");
    }
    @Test void aRedundantStateReadCannotHideBehindTheSameLoadedValue() {
        assertThatThrownBy(() -> stateful(Fault.STATE)).isInstanceOf(AssertionError.class).hasMessageContaining("Nest cold loads");
    }
    @Test void aSecondRealEncodingCannotHideBehindDiscardedBytes() {
        assertThatThrownBy(() -> stateful(Fault.CODEC)).isInstanceOf(AssertionError.class).hasMessageContaining("Nest cold encodes");
    }
    @Test void anExtraDataOfferCannotHideBehindIdempotentOutput() {
        assertThatThrownBy(() -> copy(Fault.EMISSION)).isInstanceOf(AssertionError.class).hasMessageContaining("data edge offers");
    }
    @Test void holdingOneMoreRealQueuedItemChangesTheMeasuredPeak() {
        assertThatThrownBy(() -> copy(Fault.QUEUE)).isInstanceOf(AssertionError.class).hasMessageContaining("fixture queue high-water");
    }

    private static Fault requestedFault() {
        return Fault.valueOf(System.getProperty("tapstate.cost-gate.mutation", "none").toUpperCase(Locale.ROOT));
    }

    private static void copy(Fault fault) throws Exception {
        try (Ledger ledger = new Ledger(fault)) {
            Target target = new Target(ledger);
            PipelineResource pipeline = parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: cost_copy
                    source: rows
                    serve:
                      from: rows
                      sync: [{ id: target, source: target }]
                    """);
            try (Pump pump = new Pump(PipelineDagBuilder.build(pipeline, bindings(ledger, target, null, null)), ledger)) {
                pump.feed("rows", rows(), true);
                assertThat(target.documents).hasSize(4);
                assertThat(target.documents.keySet()).containsExactly("1", "2", "3", "4");
                ledger.expect(4, 0, 0, 0, new CostGateNestStores.Costs(0, 0, 0, 0, 0, 0, 0), 4);
            }
        }
    }

    private static void stateless(Fault fault) throws Exception {
        try (Ledger ledger = new Ledger(fault)) {
            Target target = new Target(ledger);
            PipelineResource pipeline = parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: cost_stateless
                    source: rows
                    transforms:
                      - { id: kept, from: rows, type: filter, expr: "int(after.id) % 2 == 0" }
                      - id: projected
                        from: kept
                        type: map
                        fields: { region: false, projected_qty: "=int(after.qty) * 2" }
                      - { id: expanded, from: projected, type: unwind, path: items, include_array_index: item_index }
                    serve:
                      from: expanded
                      sync: [{ id: target, source: target }]
                    """);
            try (Pump pump = new Pump(PipelineDagBuilder.build(pipeline, bindings(ledger, target, null, null)), ledger)) {
                pump.feed("rows", rows(), true);
                assertThat(target.documents.keySet()).containsExactly("2/0", "2/1", "4/0", "4/1");
                assertThat(target.documents.values()).allSatisfy(row -> assertThat(row)
                        .containsKey("projected_qty").doesNotContainKey("region"));
                // Four filters, two projections and two expansions are actual calls on the shared port.
                ledger.expect(4, 8, 0, 0, new CostGateNestStores.Costs(0, 0, 0, 0, 0, 0, 0), 12);
            }
        }
    }

    private static void stateful(Fault fault) throws Exception {
        try (Ledger ledger = new Ledger(fault)) {
            Target joined = new Target(ledger);
            Target nested = new Target(ledger);
            String joinId = "cost_join_" + NEXT.incrementAndGet();
            PipelineResource join = parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: [join_orders, join_customers]
                    transforms:
                      - id: joined
                        from: { o: join_orders, c: join_customers }
                        type: join
                        engine: builtin
                        sql: "SELECT o.id AS order_id, o.qty AS qty, c.name AS customer_name FROM o JOIN c ON o.customer_id = c.id"
                    view:
                      id: joined_output
                      from: joined
                      primary_key: order_id
                      storage: { warm: { collection: joined_output } }
                    """.formatted(joinId));
            var columns = List.of(new SourceTable("o", List.of(
                            new SourceColumn("id", TapstateType.INT64, false),
                            new SourceColumn("qty", TapstateType.INT64, false),
                            new SourceColumn("customer_id", TapstateType.INT64, false))),
                    new SourceTable("c", List.of(new SourceColumn("id", TapstateType.INT64, false),
                            new SourceColumn("name", TapstateType.STRING, false))));
            var plan = SqlFrontEnd.derive(((io.tapstate.core.model.TransformBody.Join)
                    ((io.tapstate.core.model.Step.Inline) join.transforms().getFirst()).body()).sql(), columns);
            String ledgerScope = ledger.scope;
            JoinBinding joinBinding = new JoinBinding(step -> plan, step -> List.of("id"), step -> Map.of("c", List.of("id")),
                    (owner, pipeline, step) -> new CountedJoin(new ImapJoinStores(owner, pipeline, step), requiredLedger(ledgerScope)),
                    io.tapstate.runtime.engine.join.DimensionRowDisplacedAlert.NONE);
            try (Pump pump = new Pump(PipelineDagBuilder.build(join, bindings(ledger, joined, null, joinBinding)), ledger)) {
                pump.feed("join_customers", List.of(insert("join_customers", Map.of("id", 7L, "name", "Ada"))), false);
                pump.feed("join_orders", List.of(insert("join_orders", Map.of("id", 11L, "qty", 3L, "customer_id", 7L))), false);
                assertThat(joined.documents).containsExactly(Map.entry("11",
                        Map.of("order_id", 11L, "qty", 3L, "customer_name", "Ada")));
            }

            PipelineResource nest = parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: cost_nest
                    source: [orders, items]
                    transforms:
                      - id: nested
                        from: { o: orders, i: items }
                        type: nest
                        root:
                          from: o
                          key: [id]
                          embed:
                            - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [id] }
                    serve:
                      from: nested
                      sync: [{ id: target, source: target }]
                    """);
            try (CostGateNestStores cold = new CostGateNestStores(fault == Fault.STATE, fault == Fault.CODEC)) {
                Map<String, NestTable> tables = Map.of("o", new NestTable("orders", List.of("id")),
                        "i", new NestTable("items", List.of("id")));
                var nestStep = (io.tapstate.core.model.Step.Inline) nest.transforms().getFirst();
                var topology = NestTopology.compile(nest.id(), nestStep.id(),
                        (io.tapstate.core.model.TransformBody.Nest) nestStep.body(), tables::get);
                // This count fixture deliberately sends each drain: its fixed clock must not leave
                // updates folded inside the deployment's default 50 ms window. Macro settings stay
                // separate; this exercises the existing no-window control without a clock verdict.
                NestSettings settings = NestSettings.defaults().withSendWindow(topology.assembler().mapName(), 0);
                NestBinding nestBinding = new NestBinding(tables::get, cold, (from, rejected) -> {
                    throw new AssertionError("the fixed complete-parent sequence must not dead-letter");
                }, ReplayFloorFactory.NONE, NestStateLedger.NONE, settings, () -> 0L);
                try (Pump pump = new Pump(PipelineDagBuilder.build(nest, bindings(ledger, nested, nestBinding, null)), ledger)) {
                    String edges = pump.edgeDescription();
                    pump.feed("orders", List.of(insert("orders", Map.of("id", 1L, "name", "one"))), false);
                    Map<String, Object> before = Map.of("id", 10L, "order_id", 1L, "qty", 1L);
                    Map<String, Object> after = Map.of("id", 10L, "order_id", 1L, "qty", 2L);
                    pump.feed("items", List.of(new Envelope(Op.INSERT, 2L, "items", null, before, null)
                                    .withOrder(new SourceOrder(1, 1)),
                            new Envelope(Op.UPDATE, 3L, "items", before, after, null)
                                    .withOrder(new SourceOrder(1, 2))), true);
                    if (Boolean.getBoolean("tapstate.cost-gate.trace")) {
                        System.out.println("COST_GATE_NEST_EDGES " + edges);
                        System.out.println("COST_GATE_NEST_IMAGES " + nested.images);
                        System.out.println("COST_GATE_NEST_COLD " + cold.snapshot());
                    }
                    assertThat(nested.documents).hasSize(1);
                    assertThat(nested.images).as("compiled root/child routing: " + edges + "; images=" + nested.images)
                            .allSatisfy(image -> assertThat(image).containsEntry("id", 1L));
                    assertThat((List<Map<String, Object>>) nested.documents.get("1").get("items"))
                            .as("final nested image; edges=" + edges + "; images=" + nested.images + "; cold=" + cold.snapshot())
                            .singleElement().satisfies(item -> assertThat(item).containsEntry("qty", 2L));
                    // A leaf embed goes directly to the assembler: three drains mean three load/save pairs.
                    // Only the two later loads find state, hence three encodes and two decodes. Join has
                    // three driver reads (page count, fact, dimension), two nonempty final-projection
                    // reads (fact batch, dimension), and two empty batch requests for the ordered
                    // position-only words. All seven API calls count; the two empty requests incur
                    // no map IO. Its three writes are dimension, fact and reverse-index operations.
                    ledger.expect(4, 0, 7, 3, cold.snapshot(), 10);
                }
            }
        }
    }

    private static PipelineResource parse(String yaml) { return (PipelineResource) new DslParser().parse(yaml); }
    private static Envelope insert(String stream, Map<String, Object> row) {
        return Envelope.insert(1L, stream, row, null).withOrder(new SourceOrder(1, 1));
    }
    private static List<Envelope> rows() {
        List<Envelope> rows = new ArrayList<>();
        for (long id = 1; id <= 4; id++) {
            rows.add(insert("rows", Map.of("id", id, "qty", id, "region", "north",
                    "items", List.of(Map.of("sku", "a"), Map.of("sku", "b"))))
                    .withOrder(new SourceOrder(1, id)));
        }
        return rows;
    }

    private static DagBindings bindings(Ledger ledger, Target target, NestBinding nest, JoinBinding join) {
        String scope = ledger.scope;
        String targetId = target.id;
        return new DagBindings(
                source -> ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(() -> new InputRows())),
                step -> {
                    // Only the production serializable factory and fixture id travel with the supplier.
                    var factory = StoreBackedDagSource.transformPort(step, List.of("id"));
                    return () -> {
                        TransformPort port = factory.get();
                        Ledger current = requiredLedger(scope);
                        return event -> { current.transforms++; return port.transform(event); };
                    };
                },
                sink -> () -> requiredLedger(scope).targets.get(targetId).writer(),
                ref -> List.of(((FromRef.Literal) ref).ref()), source -> List.of(source),
                view -> () -> requiredLedger(scope).targets.get(targetId).writer(), nest, join);
    }

    private static Ledger requiredLedger(String scope) {
        Ledger ledger = LEDGERS.get(scope);
        if (ledger == null) { throw new IllegalStateException("cost fixture was accessed after its scope ended"); }
        return ledger;
    }

    private static final class Ledger implements AutoCloseable {
        private final String scope = UUID.randomUUID().toString();
        private final Map<String, Target> targets = new LinkedHashMap<>();
        private final Fault fault;
        private boolean externalInjected;
        private boolean emissionInjected;
        private boolean queueInjected;
        private long writes;
        private long transforms;
        private long joinReads;
        private long joinEmptyBatchReads;
        private long joinWrites;
        private long emissions;
        private int queuePeak;
        private Ledger(Fault fault) { this.fault = fault; LEDGERS.put(scope, this); }
        @Override public void close() { LEDGERS.remove(scope, this); }
        private void expect(long writes, long transforms, long joinReads, long joinWrites,
                CostGateNestStores.Costs cold, long emissions) {
            assertThat(this.emissions).as("data edge offers").isEqualTo(emissions);
            assertThat(queuePeak).as("fixture queue high-water").isEqualTo(2);
            assertThat(this.writes).as("target writes").isEqualTo(writes);
            assertThat(this.transforms).as("stateless port calls").isEqualTo(transforms);
            assertThat(this.joinReads).as("Join store API reads").isEqualTo(joinReads);
            assertThat(joinEmptyBatchReads).as("Join empty batch API requests").isEqualTo(joinReads > 0 ? 2 : 0);
            assertThat(this.joinWrites).as("Join store API writes").isEqualTo(joinWrites);
            boolean stateful = joinReads > 0;
            assertThat(cold.loads()).as("Nest cold loads").isEqualTo(stateful ? 3 : 0);
            assertThat(cold.saves()).as("Nest cold saves").isEqualTo(stateful ? 3 : 0);
            assertThat(cold.batchLoads()).as("Nest cold batch loads").isZero();
            assertThat(cold.deletes()).as("Nest cold deletes").isZero();
            assertThat(cold.conditionalSaves()).as("Nest cold conditional saves").isZero();
            assertThat(cold.encodes()).as("Nest cold encodes").isEqualTo(stateful ? 3 : 0);
            assertThat(cold.decodes()).as("Nest cold decodes").isEqualTo(stateful ? 2 : 0);
        }
    }

    private static final class Target {
        private final String id = UUID.randomUUID().toString();
        private final Ledger ledger;
        private final Map<String, Map<String, Object>> documents = new TreeMap<>();
        private final List<Map<String, Object>> images = new ArrayList<>();
        private Target(Ledger ledger) { this.ledger = ledger; ledger.targets.put(id, this); }
        private SinkWriter writer() {
            SinkWriter counted = new SinkWriter() {
                @Override public CompletionStage<WriteResult> write(List<Envelope> records) {
                    ledger.writes++;
                    for (Envelope record : records) {
                        Map<String, Object> row = record.after();
                        String key = String.valueOf(row.getOrDefault("order_id", row.get("id")));
                        if (row.containsKey("item_index")) { key += "/" + row.get("item_index"); }
                        documents.put(key, new LinkedHashMap<>(row));
                        images.add(new LinkedHashMap<>(row));
                    }
                    return CompletableFuture.completedFuture(new WriteResult(records.size()));
                }
                @Override public void close() { }
            };
            return new SinkWriter() {
                @Override public CompletionStage<WriteResult> write(List<Envelope> records) {
                    CompletionStage<WriteResult> first = counted.write(records);
                    if (ledger.fault == Fault.EXTERNAL && !ledger.externalInjected) {
                        ledger.externalInjected = true;
                        counted.write(records);
                    }
                    return first;
                }
                @Override public void close() { counted.close(); }
            };
        }
    }

    private static final class InputRows extends AbstractProcessor {
        private final Deque<Envelope> pending = new ArrayDeque<>();
        @Override public boolean complete() {
            if (!pending.isEmpty() && tryEmit(pending.peekFirst())) { pending.removeFirst(); }
            return false;
        }
    }

    private static final class QueueOutbox implements Outbox {
        private final TestOutbox delegate;
        private final Ledger ledger;
        private final boolean source;
        private QueueOutbox(int ordinals, Ledger ledger, boolean source) {
            int[] capacities = new int[ordinals];
            java.util.Arrays.fill(capacities, 3);
            delegate = new TestOutbox(capacities, 16);
            this.ledger = ledger;
            this.source = source;
        }
        private boolean measured(java.util.function.BooleanSupplier offer, Object item) {
            int[] before = sizes();
            boolean accepted = offer.getAsBoolean();
            capture(before);
            if (accepted && source && ledger.fault == Fault.EMISSION && !ledger.emissionInjected) {
                ledger.emissionInjected = true;
                before = sizes();
                assertThat(delegate.offer(item)).as("the redundant actual data offer fits its bounded queue").isTrue();
                capture(before);
            }
            return accepted;
        }
        private int[] sizes() {
            int[] sizes = new int[bucketCount()];
            for (int i = 0; i < sizes.length; i++) { sizes[i] = delegate.queue(i).size(); }
            return sizes;
        }
        private void capture(int[] before) {
            for (int i = 0; i < before.length; i++) {
                int added = delegate.queue(i).size() - before[i];
                int index = 0;
                for (Object item : delegate.queue(i)) {
                    if (index++ >= before[i] && !(item instanceof Watermark) && !(item instanceof SettledPositions)) {
                        ledger.emissions++;
                    }
                }
                if (added > 0) { ledger.queuePeak = Math.max(ledger.queuePeak, delegate.queue(i).size()); }
            }
        }
        @Override public int bucketCount() { return delegate.bucketCount(); }
        @Override public boolean offer(int ordinal, Object item) { return measured(() -> delegate.offer(ordinal, item), item); }
        @Override public boolean offer(int[] ordinals, Object item) { return measured(() -> delegate.offer(ordinals, item), item); }
        @Override public boolean offer(Object item) { return measured(() -> delegate.offer(item), item); }
        @Override public boolean offerToSnapshot(Object key, Object value) { return delegate.offerToSnapshot(key, value); }
        @Override public boolean hasUnfinishedItem() { return delegate.hasUnfinishedItem(); }
    }

    private record Node(Processor processor, QueueOutbox output, InputRows source) { }

    private static final class Pump implements AutoCloseable {
        private final DAG dag;
        private final Ledger ledger;
        private final Map<String, Node> nodes = new LinkedHashMap<>();
        private Pump(DAG dag, Ledger ledger) throws Exception {
            this.dag = dag;
            this.ledger = ledger;
            Address address = member.getCluster().getLocalMember().getAddress();
            for (Vertex vertex : dag) {
                // The metric-registration setup has no transport edge. Every source of these fixed
                // fixtures is used; a terminal sink has an inbound edge and is retained here.
                if (dag.getOutboundEdges(vertex.getName()).isEmpty()
                        && dag.getInboundEdges(vertex.getName()).isEmpty()) { continue; }
                ProcessorMetaSupplier meta = vertex.getMetaSupplier();
                meta.init(new TestProcessorMetaSupplierContext().setHazelcastInstance(member)
                        .setVertexName(vertex.getName()).setTotalParallelism(1).setLocalParallelism(1));
                ProcessorSupplier supplier = meta.get(List.of(address)).apply(address);
                supplier.init(new TestProcessorSupplierContext().setHazelcastInstance(member)
                        .setVertexName(vertex.getName()).setTotalParallelism(1).setLocalParallelism(1));
                Processor processor = supplier.get(1).iterator().next();
                List<Edge> outgoing = dag.getOutboundEdges(vertex.getName());
                int ordinals = outgoing.stream().mapToInt(Edge::getSourceOrdinal).max().orElse(-1) + 1;
                boolean source = processor instanceof InputRows;
                QueueOutbox output = new QueueOutbox(ordinals, ledger, source);
                processor.init(output, new TestProcessorContext().setVertexName(vertex.getName()));
                nodes.put(vertex.getName(), new Node(processor, output, source ? (InputRows) processor : null));
            }
        }
        private void feed(String sourceName, List<Envelope> input, boolean prefill) throws Exception {
            Node node = nodes.get(sourceName);
            node.source().pending.addAll(input);
            int turns = 0;
            while (!node.source().pending.isEmpty()) {
                node.output().delegate.reset();
                node.processor().complete();
                turns++;
                int beforeDrain = prefill ? 2 : 1;
                if (prefill && ledger.fault == Fault.QUEUE && !ledger.queueInjected) { beforeDrain = 3; }
                if (turns >= beforeDrain || node.source().pending.isEmpty()) {
                    if (beforeDrain == 3) { ledger.queueInjected = true; }
                    drain();
                    turns = 0;
                }
            }
            drain();
        }
        private String edgeDescription() {
            List<String> edges = new ArrayList<>();
            for (String name : nodes.keySet()) {
                for (Edge edge : dag.getOutboundEdges(name)) {
                    edges.add(edge.getSourceName() + "#" + edge.getSourceOrdinal() + "->"
                            + edge.getDestName() + "#" + edge.getDestOrdinal());
                }
            }
            return edges.toString();
        }
        private void drain() throws Exception {
            for (int turn = 0; turn < 256; turn++) {
                boolean progress = false;
                for (Map.Entry<String, Node> entry : nodes.entrySet()) {
                    Node node = entry.getValue();
                    if (node.source() == null) {
                        long before = ledger.emissions;
                        node.output().delegate.reset();
                        node.processor().tryProcess();
                        progress |= before != ledger.emissions;
                    }
                    List<Edge> edges = new ArrayList<>(dag.getOutboundEdges(entry.getKey()));
                    edges.sort(Comparator.comparingInt(Edge::getSourceOrdinal));
                    for (Edge edge : edges) {
                        java.util.Queue<Object> queue = node.output().delegate.queue(edge.getSourceOrdinal());
                        if (queue.isEmpty()) { continue; }
                        TestInbox inbox = new TestInbox();
                        inbox.add(queue.peek());
                        Node downstream = nodes.get(edge.getDestName());
                        downstream.output().delegate.reset();
                        downstream.processor().process(edge.getDestOrdinal(), inbox);
                        if (inbox.isEmpty()) { queue.remove(); progress = true; }
                    }
                }
                if (!progress) {
                    assertThat(nodes.values()).allSatisfy(node -> {
                        for (int ordinal = 0; ordinal < node.output().bucketCount(); ordinal++) {
                            assertThat(node.output().delegate.queue(ordinal)).as("all compiled data edges have drained").isEmpty();
                        }
                    });
                    return;
                }
            }
            throw new AssertionError("compiled processors did not drain within the fixed callback budget");
        }
        @Override public void close() throws Exception {
            List<Node> closing = new ArrayList<>(nodes.values());
            java.util.Collections.reverse(closing);
            for (Node node : closing) { node.processor().close(); }
        }
    }

    private static final class CountedJoin implements JoinStores {
        private final JoinStores delegate;
        private final Ledger ledger;
        private CountedJoin(JoinStores delegate, Ledger ledger) { this.delegate = delegate; this.ledger = ledger; }
        @Override public Map<String, Object> fact(String key) { ledger.joinReads++; return delegate.fact(key); }
        @Override public Map<String, Map<String, Object>> factsUnder(Collection<String> keys) {
            ledger.joinReads++;
            if (keys.isEmpty()) { ledger.joinEmptyBatchReads++; }
            return delegate.factsUnder(keys);
        }
        @Override public void putFact(String key, Map<String, Object> row) { ledger.joinWrites++; delegate.putFact(key, row); }
        @Override public void removeFact(String key) { ledger.joinWrites++; delegate.removeFact(key); }
        @Override public Map<String, Object> dimensionRow(String source, String key) {
            ledger.joinReads++; return delegate.dimensionRow(source, key);
        }
        @Override public Map<String, Object> putDimensionRow(String source, String key, Map<String, Object> row) {
            ledger.joinWrites++; return delegate.putDimensionRow(source, key, row);
        }
        @Override public void removeDimensionRow(String source, String key) { ledger.joinWrites++; delegate.removeDimensionRow(source, key); }
        @Override public int indexPageCount(String source, String key) { ledger.joinReads++; return delegate.indexPageCount(source, key); }
        @Override public List<String> indexPage(String source, String key, int page) { ledger.joinReads++; return delegate.indexPage(source, key, page); }
        @Override public void indexAdd(String source, String key, String fact) { ledger.joinWrites++; delegate.indexAdd(source, key, fact); }
        @Override public void indexRemove(String source, String key, String fact) { ledger.joinWrites++; delegate.indexRemove(source, key, fact); }
    }
}
