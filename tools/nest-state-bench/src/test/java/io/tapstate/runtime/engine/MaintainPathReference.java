package io.tapstate.runtime.engine;

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
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.IntFunction;

/**
 * The documents the maintain path produces for the same rows, used as the reference a build-phase run is
 * compared against: the unchanged nest operator, state held in process memory, the three snapshot streams
 * fed at once. Whatever the build phase writes for a root has to equal what this emits last for it.
 */
public final class MaintainPathReference {

    private static final Map<String, Queue<Map<String, Object>>> FEED = new ConcurrentHashMap<>();
    private static final Map<Object, Map<String, Object>> LATEST = new ConcurrentHashMap<>();

    private MaintainPathReference() {
    }

    /** The latest document per root key, once every root carries {@code linesOf(root)} lines and a customer. */
    public static Map<Object, Map<String, Object>> documents(TransformBody.Nest body, String step,
            int customers, int orders, int lines, IntFunction<Map<String, Object>> customer,
            IntFunction<Map<String, Object>> order, IntFunction<Map<String, Object>> line,
            IntFunction<Integer> linesOf, long budgetMillis) {
        FEED.clear();
        LATEST.clear();
        HazelcastInstance member = Hazelcast.newHazelcastInstance(memberConfig());
        try {
            NestStateMapStoreFactory.bindTo(member, new MemoryState());
            Job job = member.getJet().newJob(dag(body, step));
            Queue<Map<String, Object>> c = FEED.computeIfAbsent("customers", n -> new ConcurrentLinkedQueue<>());
            for (int i = 0; i < customers; i++) c.add(customer.apply(i));
            Queue<Map<String, Object>> o = FEED.computeIfAbsent("orders", n -> new ConcurrentLinkedQueue<>());
            for (int i = 0; i < orders; i++) o.add(order.apply(i));
            Queue<Map<String, Object>> l = FEED.computeIfAbsent("order_lines", n -> new ConcurrentLinkedQueue<>());
            for (int i = 0; i < lines; i++) l.add(line.apply(i));
            long deadline = System.currentTimeMillis() + budgetMillis;
            while (System.currentTimeMillis() < deadline && !complete(orders, linesOf)) sleep(50);
            sleep(2_000);
            job.cancel();
            if (!complete(orders, linesOf)) throw new IllegalStateException("the maintain-path reference did not complete");
            return new LinkedHashMap<>(LATEST);
        } finally {
            member.shutdown();
        }
    }

    private static boolean complete(int orders, IntFunction<Integer> linesOf) {
        if (LATEST.size() < orders) return false;
        for (int o = 0; o < orders; o++) {
            Map<String, Object> document = LATEST.get(o);
            if (document == null || !(document.get("customer") instanceof Map)) return false;
            int size = document.get("lines") instanceof List<?> held ? held.size() : 0;
            if (size != linesOf.apply(o)) return false;
        }
        return true;
    }

    private static Config memberConfig() {
        Config config = new Config();
        config.setClusterName("nest-maintain-reference-" + System.nanoTime());
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

    private static DAG dag(TransformBody.Nest body, String stepName) {
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("line", FromRef.literal("order_lines"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline(stepName, FromClause.aliases(aliases), body, null);
        PipelineResource resource = new PipelineResource("bench", null,
                List.of(SourceRef.bare("orders"), SourceRef.bare("order_lines"), SourceRef.bare("customers")),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(stepName),
                        List.of(new SyncElement("sync_1", "dest", null, null, null)), null, null),
                null, null);
        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        for (String table : List.of("orders", "order_lines", "customers")) {
            sources.put(table, ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of((SupplierEx<Processor>) () -> new FedSource(table))));
        }
        Map<String, NestTable> tables = tables();
        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) LatestDocumentSink::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }));
        return PipelineDagBuilder.build(resource, bindings);
    }

    /** The table behind each alias of the bench tree. */
    public static Map<String, NestTable> tables() {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("line", new NestTable("order_lines", List.of("line_id")));
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        return tables;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
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

    private static final class MemoryState implements io.tapstate.spi.store.KeyedStateStore {
        private final Map<String, byte[]> held = new ConcurrentHashMap<>();
        @Override public Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(held.get(namespace + "\0" + key));
        }
        @Override public void save(String namespace, String key, byte[] state) { held.put(namespace + "\0" + key, state); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return Optional.ofNullable(held.putIfAbsent(namespace + "\0" + key, state));
        }
        @Override public void delete(String namespace, String key) { held.remove(namespace + "\0" + key); }
        @Override public void dropNamespace(String namespace) { held.keySet().removeIf(k -> k.startsWith(namespace + "\0")); }
        @Override public long count(String namespace) { return held.keySet().stream().filter(k -> k.startsWith(namespace + "\0")).count(); }
    }

    private static final class LatestDocumentSink implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> batch) {
            for (Envelope written : batch) {
                Map<String, Object> document = written.after();
                if (document != null) LATEST.put(document.get("order_id"), document);
            }
            return CompletableFuture.completedFuture(new WriteResult(batch.size()));
        }

        @Override
        public void close() {
        }
    }
}
