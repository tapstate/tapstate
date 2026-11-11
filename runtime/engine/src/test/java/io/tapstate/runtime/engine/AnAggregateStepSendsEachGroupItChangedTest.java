package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.AggregateStepRules;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.aggregate.AggregateProcessor;
import io.tapstate.runtime.engine.aggregate.AggregateRequests;
import io.tapstate.runtime.engine.aggregate.AggregateStores;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.OrderedStateStore;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * An aggregate step drawn into a pipeline and run: the rows of its input go in, and what reaches the sink
 * is one row per group as it stands, a delete for a group its last row left, and the same rows again when
 * the same events are delivered a second time.
 *
 * <p>The second delivery is the case the step exists for. A run that stops after storing and before
 * sending is told the same events again, and a step that added them a second time would send totals that
 * are wrong by exactly what was replayed, with every row well formed.
 */
class AnAggregateStepSendsEachGroupItChangedTest {

    private static final String YAML = """
            version: tapstate/v1
            kind: pipeline
            id: sales
            source: src_orders
            transforms:
              - id: by_region
                from: [ orders ]
                type: aggregate
                where: "=after.status != 'cancelled'"
                groupBy:
                  region: $region
                fields:
                  orders: { count: true }
                  revenue: { sum: $amount }
                  largest: { max: $amount }
            serve:
              from: by_region
              sync: [ { id: dw, source: tgt, write_mode: upsert, on_full_load: clear } ]
            """;

    private static final Map<String, Stored> STORES = new ConcurrentHashMap<>();
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    /** What the source of the next run emits; a job carries no events of its own. */
    private static volatile List<Envelope> SCRIPT = List.of();
    private static volatile boolean HELD;

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        STORES.clear();
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("aggregate-step-test-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
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

    @Test
    @DisplayName("each group reaches the sink as it stands after the rows that changed it")
    void theSinkEndsWithEveryGroupAsItsRowsMakeIt() {
        run(List.of(
                insert(1, 1, "EU", "open", "100.00"),
                insert(2, 2, "EU", "open", "40.50"),
                insert(3, 3, "US", "open", "7.00"),
                insert(4, 4, "EU", "cancelled", "999.00"),
                // Order 2 moves to the US, order 3 is deleted, and order 1 grows.
                update(2, 5, row(2, "EU", "open", "40.50"), row(2, "US", "open", "40.50")),
                delete(3, 6, row(3, "US", "open", "7.00")),
                update(1, 7, row(1, "EU", "open", "100.00"), row(1, "EU", "open", "120.00"))));

        Map<Object, Envelope> last = lastPerGroup();
        assertThat(last.keySet()).containsExactlyInAnyOrder("EU", "US");
        assertThat(last.get("EU").after()).containsEntry("orders", 1L).containsEntry("revenue", new BigDecimal("120.00"))
                .containsEntry("largest", new BigDecimal("120.00")).containsOnlyKeys("region", "orders", "revenue", "largest");
        assertThat(last.get("US").after()).containsEntry("orders", 1L).containsEntry("revenue", new BigDecimal("40.50"))
                .containsEntry("largest", new BigDecimal("40.50"));
        assertThat(WRITTEN).allSatisfy(written -> assertThat(written.src()).isEqualTo("by_region"));
    }

    @Test
    @DisplayName("a group its last row left is sent as a delete of its key")
    void anEmptiedGroupIsDeleted() {
        // Two runs over one store, so the row's arrival and its leaving cannot be taken in one go: a row
        // that comes and goes within what the step takes at once leaves no group to send or to delete.
        run(List.of(insert(1, 1, "EU", "open", "100.00")));
        assertThat(lastPerGroup().get("EU").op()).as("the group was sent as a row first").isEqualTo(Op.INSERT);
        WRITTEN.clear();

        run(List.of(update(1, 2, row(1, "EU", "open", "100.00"), row(1, "EU", "cancelled", "100.00"))));

        Envelope last = lastPerGroup().get("EU");
        assertThat(last.op()).isEqualTo(Op.DELETE);
        assertThat(last.before()).containsOnlyKeys("region").containsEntry("region", "EU");
    }

    @Test
    @DisplayName("the same events delivered again send the same groups and add nothing")
    void aSecondDeliveryOfTheSameEventsChangesNoTotal() {
        List<Envelope> events = List.of(
                insert(1, 1, "EU", "open", "100.00"),
                insert(2, 2, "EU", "open", "40.50"),
                update(2, 3, row(2, "EU", "open", "40.50"), row(2, "US", "open", "40.50")));
        run(events);
        Map<Object, Envelope> first = lastPerGroup();
        WRITTEN.clear();

        run(events);

        Map<Object, Envelope> again = lastPerGroup();
        assertThat(again.keySet()).as("every group the events touched is sent again").containsExactlyInAnyOrder("EU", "US");
        assertThat(again.get("EU").after()).isEqualTo(first.get("EU").after())
                .containsEntry("orders", 1L).containsEntry("revenue", new BigDecimal("100.00"));
        assertThat(again.get("US").after()).isEqualTo(first.get("US").after())
                .containsEntry("orders", 1L).containsEntry("revenue", new BigDecimal("40.50"));
    }

    @Test
    @DisplayName("a group that cannot be given exactly is sent at its last good values with a marker, and recovers")
    @SuppressWarnings("unchecked")
    void aGroupThatCannotBeGivenKeepsItsLastGoodValuesAndSaysSo() {
        run(List.of(insert(1, 1, "EU", "open", "100.00"), insert(2, 2, "US", "open", "5.00")));
        WRITTEN.clear();

        // A row whose amount is text where the column is a decimal: it is not coerced and not left out.
        Map<String, Object> wrong = row(3, "EU", "open", "1.00");
        wrong.put("amount", "a lot");
        run(List.of(Envelope.insert(3, "orders", wrong, null).withOrder(new SourceOrder(1, 3)),
                insert(4, 4, "US", "open", "2.00")));

        Map<Object, Envelope> held = lastPerGroup();
        assertThat(held.get("EU").after()).as("the values the group last had, not ones that count the row or skip it")
                .containsEntry("orders", 1L).containsEntry("revenue", new BigDecimal("100.00"));
        Map<String, Object> marker = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>)
                held.get("EU").after().get("__tap")).get("aggregate")).get("by_region");
        assertThat(marker).containsEntry("status", "failed").containsEntry("code", "aggregate.value-type-mismatch")
                .containsKeys("failed_since", "last_good_at");
        assertThat(held.get("US").after()).as("every other group carries on, and a current row carries no marker")
                .containsEntry("orders", 2L).doesNotContainKey("__tap");
        WRITTEN.clear();

        run(List.of(delete(3, 5, wrong)));

        assertThat(lastPerGroup().get("EU").after()).as("once the row is gone the group is current again")
                .containsEntry("orders", 1L).containsEntry("revenue", new BigDecimal("100.00")).doesNotContainKey("__tap");
    }

    @Test
    @DisplayName("state built under one grouping does not start under another, and says which part changed")
    void stateBuiltByAnotherDefinitionRefusesTheRun() {
        run(List.of(insert(1, 1, "EU", "open", "100.00")));
        WRITTEN.clear();

        assertThatThrownBy(() -> run(List.of(insert(2, 2, "US", "open", "5.00")),
                YAML.replace("region: $region", "region: $status")))
                .hasStackTraceContaining("aggregate.definition-changed")
                .hasStackTraceContaining("groupBy");

        assertThat(WRITTEN).isEmpty();
        // The definition that built the state still runs on it.
        run(List.of(insert(2, 2, "US", "open", "5.00")));
        assertThat(lastPerGroup()).containsKey("US");
    }

    @Test
    @DisplayName("a row that is gone is forgotten once the event that removed it cannot be delivered again")
    void whatASecondDeliveryCannotNeedIsForgotten() {
        run(List.of(insert(1, 1, "EU", "open", "100.00"), insert(2, 2, "US", "open", "5.00")));
        run(List.of(delete(1, 3, row(1, "EU", "open", "100.00"))));
        assertThat(STORES.get("by_region/MEMBERS").entries).as("the deleted row is kept to answer a replay").hasSize(2);

        // Deliveries resume above everything sent so far, and the step sweeps whenever it is idle.
        run(List.of(insert(3, 4, "US", "open", "1.00")), YAML, member -> chain -> Optional.of(new SourceOrder(1, 4)));

        assertThat(STORES.get("by_region/MEMBERS").entries).as("rows 2 and 3 remain").hasSize(2);
        // The definition, and for US its totals, two rows and two entries behind the greatest amount.
        assertThat(STORES.get("by_region/GROUPS").entries).as("nothing of the emptied group remains").hasSize(6);
        assertThat(lastPerGroup().get("US").after()).containsEntry("orders", 2L);
    }

    private void run(List<Envelope> events) {
        run(events, YAML);
    }

    private void run(List<Envelope> events, String yaml) {
        run(events, yaml, ReplayFloorFactory.NONE);
    }

    private void run(List<Envelope> events, String yaml, ReplayFloorFactory floor) {
        start(events, yaml, floor).join();
    }

    private com.hazelcast.jet.Job start(List<Envelope> events, String yaml, ReplayFloorFactory floor) {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse(yaml);
        Step.Inline step = (Step.Inline) pipeline.transforms().getFirst();
        Map<String, ValueType> columns = new LinkedHashMap<>();
        columns.put("id", ValueType.INT64);
        columns.put("region", ValueType.of(TapstateType.STRING));
        columns.put("status", ValueType.of(TapstateType.STRING));
        columns.put("amount", ValueType.decimal(2));
        AggregateStepSpec spec = AggregateStepRules.compile(step.id(), (TransformBody.Aggregate) step.body(),
                columns, true, List.of("id"), "orders");
        SCRIPT = List.copyOf(events);
        SupplierEx<SinkWriter> sink = Collecting::new;
        AggregateStores stores = (ignored, id, kind) -> STORES.computeIfAbsent(id + "/" + kind, key -> new Stored());
        DagBindings bindings = new DagBindings(
                key -> ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of((SupplierEx<Processor>) Scripted::new), key),
                ignored -> {
                    throw new AssertionError("an aggregate step is not a transform port");
                },
                element -> sink,
                ref -> ref instanceof FromRef.Literal literal ? List.of(literal.ref()) : List.of(),
                sourceId -> List.of("orders"))
                .withAggregate(new AggregateBinding(ignored -> spec, stores, floor,
                        new AggregateProcessor.Sweep(200, 0, 0)));
        return member.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings));
    }

    private AggregateRequests.Report ask(AggregateRequests.Request request) throws Exception {
        long giveUp = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (true) {
            var answer = AggregateRequests.offer(member, "sales", "by_region", request);
            if (answer.isPresent()) {
                return answer.get().get(30, java.util.concurrent.TimeUnit.SECONDS);
            }
            assertThat(System.nanoTime()).as("the step registers once it runs").isLessThan(giveUp);
            Thread.sleep(20);
        }
    }

    @Test
    @DisplayName("a running step checks its groups against their rows when asked, and a repair sends the group again")
    void aRunningStepAnswersACheckAndARepair() throws Exception {
        run(List.of(insert(1, 1, "EU", "open", "100.00"), insert(3, 2, "US", "open", "5.00")));
        Stored groups = STORES.get("by_region/GROUPS");
        byte[] eu = totalsKey("EU");
        byte[] stale = groups.entries.get(eu);
        run(List.of(insert(2, 3, "EU", "open", "40.50")));
        // The totals of when EU held one order, beside the rows of two.
        groups.entries.put(eu, stale);
        WRITTEN.clear();

        HELD = true;
        try {
            com.hazelcast.jet.Job job = start(List.of(), YAML, ReplayFloorFactory.NONE);

            AggregateRequests.Report found = ask(AggregateRequests.Request.page(AggregateRequests.Action.CHECK, null, 10));
            assertThat(found.groups()).isEqualTo(2);
            assertThat(found.next()).isNull();
            assertThat(found.findings()).singleElement().satisfies(finding -> {
                assertThat(finding.group()).containsExactly(Map.entry("region", "EU"));
                assertThat(finding.verdict()).isEqualTo("mismatch");
                assertThat(finding.fields()).containsExactly("(rows)", "orders", "revenue");
            });
            assertThat(WRITTEN).as("a check sends nothing").isEmpty();

            AggregateRequests.Report repaired = ask(AggregateRequests.Request.one(AggregateRequests.Action.REPAIR, Map.of("region", "EU")));
            assertThat(repaired.findings()).singleElement()
                    .satisfies(finding -> assertThat(finding.fields()).containsExactly("(rows)", "orders", "revenue"));

            AggregateRequests.Report refused = ask(AggregateRequests.Request.one(AggregateRequests.Action.CHECK, Map.of("country", "EU")));
            assertThat(refused.refusal()).isEqualTo("aggregate.group-invalid");
            assertThat(refused.refusalArgs()).containsEntry("step", "by_region");

            AggregateRequests.Report after = ask(AggregateRequests.Request.page(AggregateRequests.Action.CHECK, null, 10));
            assertThat(after.findings()).isEmpty();
            HELD = false;
            job.join();
        } finally {
            HELD = false;
        }
        assertThat(lastPerGroup().get("EU").after())
                .containsEntry("orders", 2L).containsEntry("revenue", new BigDecimal("140.50"));
        assertThat(AggregateRequests.runsOn(member, "sales", "by_region")).as("a stopped step takes no request").isFalse();
    }

    private static byte[] totalsKey(String region) {
        byte[] group = io.tapstate.core.dsl.aggregate.OrderedBytes.ofKey(List.of(region));
        byte[] key = new byte[group.length + 1];
        key[0] = 0x01;
        System.arraycopy(group, 0, key, 1, group.length);
        return key;
    }

    private static Map<Object, Envelope> lastPerGroup() {
        Map<Object, Envelope> last = new LinkedHashMap<>();
        synchronized (WRITTEN) {
            for (Envelope written : WRITTEN) {
                Map<String, Object> row = written.after() != null ? written.after() : written.before();
                last.put(row.get("region"), written);
            }
        }
        return last;
    }

    private static Map<String, Object> row(long id, String region, String status, String amount) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("region", region);
        row.put("status", status);
        row.put("amount", new BigDecimal(amount));
        return row;
    }

    private static Envelope insert(long id, long seq, String region, String status, String amount) {
        return Envelope.insert(seq, "orders", row(id, region, status, amount), null).withOrder(new SourceOrder(1, seq));
    }

    private static Envelope update(long id, long seq, Map<String, Object> before, Map<String, Object> after) {
        return Envelope.update(seq, "orders", before, after, null).withOrder(new SourceOrder(1, seq));
    }

    private static Envelope delete(long id, long seq, Map<String, Object> before) {
        return Envelope.delete(seq, "orders", before, null).withOrder(new SourceOrder(1, seq));
    }

    private static final class Scripted extends AbstractProcessor {
        private final List<Envelope> events = SCRIPT;
        private int next;

        @Override
        public boolean complete() {
            while (next < events.size()) {
                if (!tryEmit(events.get(next))) {
                    return false;
                }
                next++;
            }
            // A held source keeps the job running, the way a live pipeline is, until the test lets go.
            return !HELD;
        }
    }

    private static final class Collecting implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            WRITTEN.addAll(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** The store as a sorted map in memory, shared by every run of one case as a cache would be. */
    private static final class Stored implements OrderedStateStore {
        private final TreeMap<byte[], byte[]> entries = new TreeMap<>(Arrays::compareUnsigned);

        @Override
        public synchronized List<byte[]> loadAll(List<byte[]> keys) {
            List<byte[]> states = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                states.add(entries.get(key));
            }
            return states;
        }

        @Override
        public synchronized void commit(List<Entry> saves, List<byte[]> deletes) {
            saves.forEach(save -> entries.put(save.key(), save.state()));
            deletes.forEach(entries::remove);
        }

        @Override
        public Limit commitLimit() {
            return new Limit(256, 1 << 20);
        }

        @Override
        public synchronized Optional<Entry> first(byte[] from, byte[] to, End end) {
            var range = entries.subMap(from, true, to, false);
            if (range.isEmpty()) {
                return Optional.empty();
            }
            var found = end == End.LOWEST ? range.firstEntry() : range.lastEntry();
            return Optional.of(new Entry(found.getKey(), found.getValue()));
        }

        @Override
        public synchronized List<Entry> scan(byte[] from, byte[] to, byte[] after, int limit) {
            List<Entry> page = new ArrayList<>();
            var range = after == null ? entries.subMap(from, true, to, false) : entries.subMap(after, false, to, false);
            for (var entry : range.entrySet()) {
                if (page.size() == limit) {
                    break;
                }
                page.add(new Entry(entry.getKey(), entry.getValue()));
            }
            return page;
        }
    }
}
