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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.adapters.mongostore.BenchCacheStore;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * What a snapshot of an order document costs end to end: an order root holding an array of its lines
 * and the customer it points at, fed from three snapshot streams at once (so lines routinely arrive
 * before their order) into the nest operator, whose state is kept in the transactional cache store on a
 * real Mongo replica set. The measure is wall time from the first row to the last order document being
 * complete - every line gathered and the customer resolved - plus the commands the store sent.
 *
 * <p>Knobs, as system properties: {@code bench.customers}, {@code bench.orders}, {@code bench.lines}
 * (defaults 1000 / 10000 / 23000, the 34k fixture), {@code bench.latencyMs} (one-way latency added in
 * front of Mongo by a proxy, default 0), {@code bench.label} and {@code bench.out} (a file the result
 * line is appended to).
 */
class NestStateBoundaryBench {

    private static final String STEP = "order_doc";
    private static final Map<String, Queue<Map<String, Object>>> FEED = new ConcurrentHashMap<>();
    private static final Map<Object, Map<String, Object>> LATEST = new ConcurrentHashMap<>();
    private static final AtomicLong EMITTED = new AtomicLong();

    @Test
    void aSnapshotOfOrderDocuments() throws Exception {
        int customers = Integer.getInteger("bench.customers", 1_000);
        int orders = Integer.getInteger("bench.orders", 10_000);
        int lines = Integer.getInteger("bench.lines", 23_000);
        int latency = Integer.getInteger("bench.latencyMs", 0);
        Duration budget = Duration.ofMinutes(Long.getLong("bench.budgetMinutes", 30));
        String label = System.getProperty("bench.label", System.getProperty("tapstate.nest.store", "map"));

        try (Network network = Network.newNetwork();
             MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                     .withNetwork(network).withNetworkAliases("mongo");
             ToxiproxyContainer toxiproxy = latency > 0
                     ? new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0")).withNetwork(network)
                     : null) {
            mongo.start();
            String direct = "mongodb://" + mongo.getHost() + ":" + mongo.getMappedPort(27017) + "/?directConnection=true";
            String work = direct;
            if (toxiproxy != null) {
                toxiproxy.start();
                var client = new eu.rekawek.toxiproxy.ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
                var proxy = client.createProxy("mongo", "0.0.0.0:8666", "mongo:27017");
                proxy.toxics().latency("down", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, latency);
                proxy.toxics().latency("up", eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM, latency);
                work = "mongodb://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666) + "/?directConnection=true";
            }
            try (BenchCacheStore cache = BenchCacheStore.open(direct, work)) {
                HazelcastInstance member = Hazelcast.newHazelcastInstance(memberConfig());
                try {
                    NestStateMapStoreFactory.bindTo(member, cache.stores());
                    run(member, cache, customers, orders, lines, latency, budget, label);
                } finally {
                    member.shutdown();
                }
            }
        }
    }

    private void run(HazelcastInstance member, BenchCacheStore cache, int customers, int orders, int lines,
            int latency, Duration budget, String label) throws IOException {
        FEED.clear();
        LATEST.clear();
        EMITTED.set(0);
        Map<Integer, Integer> linesPerOrder = new ConcurrentHashMap<>();
        List<Map<String, Object>> customerRows = new ArrayList<>();
        for (int c = 0; c < customers; c++) customerRows.add(row("customer_id", c, "name", "customer-" + c, "tier", c % 5));
        List<Map<String, Object>> orderRows = new ArrayList<>();
        for (int o = 0; o < orders; o++) orderRows.add(row("order_id", o, "cust_ref", o % customers, "code", "order-" + o, "amount", o * 3L));
        List<Map<String, Object>> lineRows = new ArrayList<>();
        for (int l = 0; l < lines; l++) {
            int order = (int) ((long) l * orders / lines);
            linesPerOrder.merge(order, 1, Integer::sum);
            lineRows.add(row("line_id", l, "order_id", order, "sku", "sku-" + (l % 97), "qty", l % 7 + 1));
        }

        Job job = member.getJet().newJob(dag());
        long began = System.nanoTime();
        feed("customers", customerRows);
        feed("orders", orderRows);
        feed("order_lines", lineRows);
        long deadline = began + budget.toNanos();
        long completeAt = -1;
        while (System.nanoTime() < deadline) {
            if (complete(orders, linesPerOrder)) {
                completeAt = System.nanoTime();
                break;
            }
            sleep(50);
        }
        job.cancel();
        assertThat(completeAt).describedAs("the snapshot did not complete within " + budget
                + "; " + LATEST.size() + " of " + orders + " documents seen").isPositive();
        double seconds = (completeAt - began) / 1e9;
        int input = customers + orders + lines;
        Map<String, Long> commands = cache.commands();
        long total = commands.values().stream().mapToLong(Long::longValue).sum();
        String result = String.format(java.util.Locale.ROOT,
                "{\"label\":\"%s\",\"latencyMs\":%d,\"input\":%d,\"orders\":%d,\"seconds\":%.3f,"
                        + "\"docsPerSecond\":%.1f,\"inputRowsPerSecond\":%.1f,\"emitted\":%d,\"storeCommands\":%d,"
                        + "\"commandsPerInputRow\":%.2f,\"storedRows\":%d,\"byCommand\":%s}",
                label, latency, input, orders, seconds, orders / seconds, input / seconds, EMITTED.get(), total,
                (double) total / input, cache.storedRows(), json(commands));
        System.out.println("NEST-BENCH " + result);
        String out = System.getProperty("bench.out");
        if (out != null) Files.writeString(Path.of(out), result + System.lineSeparator(),
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
    }

    private static boolean complete(int orders, Map<Integer, Integer> linesPerOrder) {
        if (LATEST.size() < orders) return false;
        for (int o = 0; o < orders; o++) {
            Map<String, Object> document = LATEST.get(o);
            if (document == null || !(document.get("customer") instanceof Map)) return false;
            Object held = document.get("lines");
            int expected = linesPerOrder.getOrDefault(o, 0);
            int size = held instanceof List<?> list ? list.size() : 0;
            if (size != expected) return false;
        }
        return true;
    }

    private static Config memberConfig() {
        Config config = new Config();
        config.setClusterName("nest-state-bench-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        return config;
    }

    private static DAG dag() {
        Embed lines = new Embed("line", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "lines",
                List.of("line_id"), null, null, null);
        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(lines, customer)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("line", FromRef.literal("order_lines"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null);
        PipelineResource resource = new PipelineResource("bench", null,
                List.of(SourceRef.bare("orders"), SourceRef.bare("order_lines"), SourceRef.bare("customers")),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null)), null, null),
                null, null);
        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        for (String table : List.of("orders", "order_lines", "customers")) sources.put(table, fedSource(table));
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("line", new NestTable("order_lines", List.of("line_id")));
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) LatestDocumentSink::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }));
        return PipelineDagBuilder.build(resource, bindings);
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int field = 0; field < fields.length; field += 2) row.put((String) fields[field], fields[field + 1]);
        return row;
    }

    private static void feed(String stream, List<Map<String, Object>> rows) {
        FEED.computeIfAbsent(stream, name -> new ConcurrentLinkedQueue<>()).addAll(rows);
    }

    private static ProcessorMetaSupplier fedSource(String stream) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new FedSource(stream)));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static String json(Map<String, Long> counts) {
        StringBuilder out = new StringBuilder("{");
        counts.forEach((name, count) -> out.append(out.length() > 1 ? "," : "").append('"').append(name).append("\":").append(count));
        return out.append('}').toString();
    }

    private static final class FedSource extends AbstractProcessor {
        private final String stream;
        private long emitted;

        private FedSource(String stream) {
            this.stream = stream;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            Queue<Map<String, Object>> queue = FEED.computeIfAbsent(stream, name -> new ConcurrentLinkedQueue<>());
            for (int i = 0; i < 256; i++) {
                Map<String, Object> row = queue.peek();
                if (row == null) {
                    if (i == 0) sleep(5);
                    return false;
                }
                Envelope event = Envelope.read(emitted + 1L, stream, row, null).withOrder(new SourceOrder(1, emitted));
                if (!tryEmit(event)) return false;
                queue.poll();
                emitted++;
            }
            return false;
        }
    }

    private static final class LatestDocumentSink implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> batch) {
            for (Envelope written : batch) {
                Map<String, Object> document = written.after();
                if (document != null) {
                    EMITTED.incrementAndGet();
                    LATEST.put(document.get("order_id"), document);
                }
            }
            return CompletableFuture.completedFuture(new WriteResult(batch.size()));
        }

        @Override
        public void close() {
        }
    }
}
