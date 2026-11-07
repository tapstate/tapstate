package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.adapters.mongostore.BenchCacheStore;
import io.tapstate.adapters.mongostore.SessionCommitStore;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.MaintainPathReference;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Whether a build phase can load a NEST view within twice the time of a plain copy of the same rows.
 *
 * <p>The tree is the order document of the state-boundary bench: an order root with an array of its lines
 * and the customer it points at. Four steps run one after the other against one Mongo replica set, every
 * write in batches of the same size:
 *
 * <ol>
 *   <li><b>copy</b>: the three tables upserted as they are into target collections. The denominator.</li>
 *   <li><b>stage</b>: lines and customers upserted into one private staging collection in the
 *       operator-state database, indexed on the join column each embed is read by, then a staged-complete
 *       marker per table.</li>
 *   <li><b>render</b>: orders streamed in batches; per batch one {@code $in} read of staging per level,
 *       one {@link RootAssembly} per root built and rendered by the code the maintain path uses, the
 *       assemblies committed as one group through the direct store, then the documents bulk upserted.</li>
 *   <li><b>index</b>: the reverse reference index written once, in bulk, through the direct store.</li>
 * </ol>
 *
 * <p>A fifth step, <b>handoff</b>, writes the referenced rows into the lookup namespace the maintain phase
 * reads them from. It is reported apart, because whether it is needed at all depends on whether staging
 * can serve as that namespace.
 *
 * <p>Knobs, as system properties: {@code bench.customers}, {@code bench.orders}, {@code bench.lines}
 * (default 1000 / 10000 / 23000), {@code bench.batch} (256), {@code bench.latencyMs} (one-way latency in
 * front of Mongo, 0), {@code bench.commit} ({@code production}, or {@code session} for the lever priced by
 * {@code SessionCommitStore}), {@code bench.pipelineEmit} (emit each batch while the next one is built,
 * off), {@code bench.warmup} (a tenth-size round first, on), {@code bench.compare} (compare every document with the maintain path's, default on
 * up to 20,000 orders), {@code bench.label}, {@code bench.out} (a file the result line is appended to).
 */
class NestBuildPhaseBench {

    private static final String STEP = "order_doc";
    private static final String STAGE = "_nest_build_stage";
    private static final String MARKS = "_nest_build_marks";

    private int customers;
    private int orders;
    private int lines;
    private int batch;
    private SessionCommitStore session;
    private static java.util.function.Supplier<Map<String, Long>> storeCommands;

    @Test
    void aBuildPhaseAgainstAPlainCopy() throws Exception {
        customers = Integer.getInteger("bench.customers", 1_000);
        orders = Integer.getInteger("bench.orders", 10_000);
        lines = Integer.getInteger("bench.lines", 23_000);
        batch = Integer.getInteger("bench.batch", 256);
        int latency = Integer.getInteger("bench.latencyMs", 0);
        boolean compare = Boolean.parseBoolean(System.getProperty("bench.compare", String.valueOf(orders <= 20_000)));
        String label = System.getProperty("bench.label", "build");

        try (Network network = Network.newNetwork();
             MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                     .withNetwork(network).withNetworkAliases("mongo");
             ToxiproxyContainer toxiproxy = latency > 0
                     ? new ToxiproxyContainer(DockerImageName.parse("shopify/toxiproxy:2.1.4")).withNetwork(network)
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
            Map<String, AtomicLong> commands = new ConcurrentHashMap<>();
            CommandListener counting = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    commands.computeIfAbsent(event.getDatabaseName() + ":" + event.getCommandName(),
                            ignored -> new AtomicLong()).incrementAndGet();
                }
            };
            try (BenchCacheStore cache = BenchCacheStore.open(direct, work);
                 MongoClient client = MongoClients.create(MongoClientSettings.builder()
                         .applyConnectionString(new ConnectionString(work)).addCommandListener(counting).build())) {
                HazelcastInstance member = Hazelcast.newHazelcastInstance(memberConfig());
                try {
                    String mode = System.getProperty("bench.commit", "production");
                    session = "session".equals(mode) ? new SessionCommitStore(work, cache.stores().defaultDatabase()) : null;
                    storeCommands = session == null ? cache::commands : session::commands;
                    NestStateMapStoreFactory.bindTo(member, session == null ? cache.stores() : session.stores());
                    if (Boolean.parseBoolean(System.getProperty("bench.warmup", "true"))) {
                        // A tenth-size round first, in its own namespaces and collections, so neither arm
                        // of the measured round pays for class loading and JIT warm-up alone.
                        int[] measured = {customers, orders, lines};
                        customers = Math.max(1, customers / 10);
                        orders = Math.max(1, orders / 10);
                        lines = Math.max(1, lines / 10);
                        run(member, cache, client, commands, latency, false, label, "warm", false);
                        customers = measured[0];
                        orders = measured[1];
                        lines = measured[2];
                    }
                    run(member, cache, client, commands, latency, compare, label, "bench", true);
                } finally {
                    member.shutdown();
                    if (session != null) session.close();
                }
            }
        }
    }

    private void run(HazelcastInstance member, BenchCacheStore cache, MongoClient client,
            Map<String, AtomicLong> commands, int latency, boolean compare, String label, String scope,
            boolean measured) throws IOException {
        TransformBody.Nest body = body();
        NestTopology topology = NestTopology.compile(scope, STEP, body, MaintainPathReference.tables()::get);
        NestVertex assembler = topology.assembler();
        NestInbound lineEdge = assembler.inbound().stream()
                .filter(edge -> edge.pathId().equals(List.of("lines"))).findFirst().orElseThrow();
        NestLookup customerLookup = topology.lookups().get(0);
        List<EmbedSlot> slots = topology.slots();

        MongoDatabase target = client.getDatabase("target_" + scope);
        MongoDatabase state = client.getDatabase(cache.stores().defaultDatabase());
        Map<String, StepResult> steps = new LinkedHashMap<>();

        // 1. copy: the denominator.
        Mark copy = mark(commands);
        long copyWrites = upsertAll(target.getCollection("customers"), customers, this::customer, "customer_id")
                + upsertAll(target.getCollection("orders"), orders, this::order, "order_id")
                + upsertAll(target.getCollection("order_lines"), lines, this::line, "line_id");
        steps.put("copy", copy.end(commands, customers + orders + lines, copyWrites));

        // 2. stage: dependency tables by the column each is read by, then a staged-complete marker each.
        Mark staging = mark(commands);
        MongoCollection<Document> stage = state.getCollection(scope + STAGE);
        stage.createIndex(Indexes.ascending("t", "on"));
        long stagedAt = System.nanoTime();
        long stagedWrites = stageAll(stage, "line", lines, this::line, "line_id", "order_id");
        long linesStaged = System.nanoTime();
        stagedWrites += stageAll(stage, "customer", customers, this::customer, "customer_id", "customer_id");
        Map<String, Long> split = new LinkedHashMap<>();
        split.put("stage.lines", linesStaged - stagedAt);
        split.put("stage.customers", System.nanoTime() - linesStaged);
        state.getCollection(scope + MARKS).insertMany(List.of(
                new Document("_id", "line").append("stagedComplete", true).append("rows", lines),
                new Document("_id", "customer").append("stagedComplete", true).append("rows", customers)));
        steps.put("stage", staging.end(commands, lines + customers, stagedWrites));
        Document footprint = state.runCommand(new Document("collStats", scope + STAGE));

        // 3. render: orders in batches, one read per level, render once, commit the group, then emit.
        NestStore<RootAssembly> assemblies = DirectNestStore.on(member, assembler.mapName());
        MongoCollection<Document> documents = target.getCollection(STEP);
        Map<Object, Integer> writesPerRoot = new HashMap<>();
        Map<Object, Set<Object>> references = new LinkedHashMap<>();
        long dangling = 0;
        long stagingReads = 0;
        java.util.concurrent.ExecutorService emitter = Boolean.getBoolean("bench.pipelineEmit")
                ? java.util.concurrent.Executors.newSingleThreadExecutor() : null;
        java.util.concurrent.CompletableFuture<Void> inFlight = null;
        Mark rendering = mark(commands);
        for (int from = 0; from < orders; from += batch) {
            int to = Math.min(orders, from + batch);
            List<Map<String, Object>> roots = new ArrayList<>();
            List<Object> rootIds = new ArrayList<>();
            Set<Object> customerIds = new LinkedHashSet<>();
            for (int o = from; o < to; o++) {
                Map<String, Object> root = order(o);
                roots.add(root);
                rootIds.add(root.get("order_id"));
                customerIds.add(root.get("cust_ref"));
            }
            long t0 = System.nanoTime();
            Map<Object, List<Map<String, Object>>> linesByRoot = new HashMap<>();
            for (Document staged : stage.find(Filters.and(Filters.eq("t", "line"), Filters.in("on", rootIds)))) {
                linesByRoot.computeIfAbsent(staged.get("on"), ignored -> new ArrayList<>()).add(plain(staged.get("r", Document.class)));
            }
            Map<Object, Map<String, Object>> customersByKey = new HashMap<>();
            for (Document staged : stage.find(Filters.and(Filters.eq("t", "customer"), Filters.in("on", customerIds)))) {
                Map<String, Object> row = plain(staged.get("r", Document.class));
                customersByKey.put(NestKeys.valuesOf(row, customerLookup.partitionKey(), customerLookup.mapName()), row);
            }
            stagingReads += 2;
            long t1 = System.nanoTime();
            Map<String, Map<Object, Map<String, Object>>> resolved = Map.of(customerLookup.mapName(), customersByKey);
            Map<Object, RootAssembly> group = new LinkedHashMap<>();
            List<WriteModel<Document>> emitted = new ArrayList<>();
            for (Map<String, Object> root : roots) {
                List<Object> key = NestKeys.valuesOf(root, assembler.partitionKey(), assembler.mapName());
                SourceOrder order = new SourceOrder(1, ((Number) root.get("order_id")).longValue());
                RootAssembly assembly = new RootAssembly();
                assembly.applyRoot(root, order);
                List<Map<String, Object>> children = linesByRoot.getOrDefault(root.get("order_id"), List.of());
                children.sort(Comparator.comparingLong(row -> ((Number) row.get("line_id")).longValue()));
                for (Map<String, Object> child : children) {
                    ElementRef ref = new ElementRef(lineEdge.pathId(), null,
                            NestKeys.valuesOf(child, lineEdge.elementKey(), assembler.mapName()), null);
                    assembly.applyElement(ref, child, new SourceOrder(1, ((Number) child.get("line_id")).longValue()), Map.of());
                }
                if (assembly.waitsForARowItPointsAt(slots, resolved)) dangling++;
                Map<String, Object> rendered = assembly.render(slots, resolved).orElseThrow();
                assembly.documentSent();
                group.put(key, assembly);
                List<Object> referrer = NestKeys.valuesOf(root, customerLookup.referrerIdentity(), customerLookup.mapName());
                List<Object> referenced = NestKeys.valuesOf(root, customerLookup.referenceFields(), customerLookup.mapName());
                references.computeIfAbsent(NestLookup.bucketKey(referenced, NestLookup.bucketOf(referrer)),
                        ignored -> new LinkedHashSet<>()).add(referrer);
                Document written = new Document("_id", root.get("order_id"));
                written.putAll(rendered);
                emitted.add(new ReplaceOneModel<>(Filters.eq("_id", root.get("order_id")), written, new ReplaceOptions().upsert(true)));
                writesPerRoot.merge(root.get("order_id"), 1, Integer::sum);
            }
            long t2 = System.nanoTime();
            // Durable before passed: the group is committed before any of its documents leaves.
            assemblies.saveAll(group, group.keySet());
            assemblies.commit();
            long t3 = System.nanoTime();
            if (emitter == null) {
                documents.bulkWrite(emitted, new BulkWriteOptions().ordered(false));
            } else {
                // Pipelined lever: this batch's documents go out while the next batch is read, built and
                // committed. Still durable before passed: the write starts only after its own commit.
                if (inFlight != null) inFlight.join();
                inFlight = java.util.concurrent.CompletableFuture.runAsync(
                        () -> documents.bulkWrite(emitted, new BulkWriteOptions().ordered(false)), emitter);
            }
            long t4 = System.nanoTime();
            split.merge("render.read", t1 - t0, Long::sum);
            split.merge("render.assemble", t2 - t1, Long::sum);
            split.merge("render.commit", t3 - t2, Long::sum);
            split.merge("render.emit", t4 - t3, Long::sum);
        }
        if (inFlight != null) inFlight.join();
        if (emitter != null) emitter.shutdown();
        steps.put("render", rendering.end(commands, orders, orders));

        // 4. index: the reverse reference index, once, in bulk.
        Mark indexing = mark(commands);
        NestStore<Set<Object>> referenceIndex = DirectNestStore.on(member, customerLookup.referencesMapName());
        referenceIndex.saveAll(references, references.keySet());
        referenceIndex.commit();
        steps.put("index", indexing.end(commands, references.size(), references.size()));

        // 5. handoff: referenced rows into the lookup namespace the maintain phase reads.
        Mark handoff = mark(commands);
        NestStore<Map<String, Object>> lookup = DirectNestStore.on(member, customerLookup.mapName());
        Map<Object, Map<String, Object>> pending = new LinkedHashMap<>();
        long handedOff = 0;
        for (Document staged : stage.find(Filters.eq("t", "customer")).batchSize(4096)) {
            Map<String, Object> row = plain(staged.get("r", Document.class));
            pending.put(NestKeys.valuesOf(row, customerLookup.partitionKey(), customerLookup.mapName()), row);
            if (pending.size() == 4096) {
                lookup.saveAll(pending, pending.keySet());
                lookup.commit();
                handedOff += pending.size();
                pending.clear();
            }
        }
        if (!pending.isEmpty()) {
            lookup.saveAll(pending, pending.keySet());
            lookup.commit();
            handedOff += pending.size();
        }
        steps.put("handoff", handoff.end(commands, customers, handedOff));

        // Staging is dropped once the build is done; untimed, it is a single command.
        stage.drop();

        // Checks, outside the clock.
        long targetDocuments = documents.countDocuments();
        int mostWrites = writesPerRoot.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        long incomplete = 0;
        Map<Object, Map<String, Object>> built = new HashMap<>();
        for (Document document : documents.find()) {
            Object id = document.remove("_id");
            int expected = linesOf((Integer) id);
            int held = document.get("lines") instanceof List<?> list ? list.size() : -1;
            if (held != expected || !(document.get("customer") instanceof Map)) incomplete++;
            if (compare) built.put(id, document);
        }
        long differing = -1;
        if (compare) {
            Map<Object, Map<String, Object>> reference = MaintainPathReference.documents(body, STEP, customers, orders, lines,
                    this::customer, this::order, this::line, this::linesOf, 600_000L);
            differing = 0;
            for (int o = 0; o < orders; o++) {
                String mine = new Document(built.get(o)).toJson();
                String theirs = new Document(reference.get(o)).toJson();
                if (!mine.equals(theirs)) {
                    if (differing == 0) System.out.println("NEST-BUILD first difference at " + o + ":\n  build    " + mine + "\n  maintain " + theirs);
                    differing++;
                }
            }
        }

        if (!measured) return;
        double copySeconds = steps.get("copy").seconds;
        double buildSeconds = steps.get("stage").seconds + steps.get("render").seconds + steps.get("index").seconds;
        double withHandoff = buildSeconds + steps.get("handoff").seconds;
        StringBuilder out = new StringBuilder();
        out.append(String.format(Locale.ROOT,
                "{\"label\":\"%s\",\"commit\":\"%s\",\"pipelineEmit\":%b,\"latencyMs\":%d,\"customers\":%d,\"orders\":%d,\"lines\":%d,\"batch\":%d,"
                        + "\"copySeconds\":%.3f,\"buildSeconds\":%.3f,\"ratio\":%.2f,\"buildWithHandoffSeconds\":%.3f,"
                        + "\"ratioWithHandoff\":%.2f,\"targetDocuments\":%d,\"mostTargetWritesPerRoot\":%d,"
                        + "\"incompleteDocuments\":%d,\"danglingReferences\":%d,\"differingFromMaintain\":%d,"
                        + "\"stagingReadsPerBatch\":%.1f,\"stagingBytes\":%d,\"stagingStorageBytes\":%d,"
                        + "\"stagingIndexBytes\":%d,\"referenceBuckets\":%d,\"steps\":{",
                label, session == null ? "production" : "session", Boolean.getBoolean("bench.pipelineEmit"), latency, customers, orders, lines, batch, copySeconds, buildSeconds, buildSeconds / copySeconds,
                withHandoff, withHandoff / copySeconds, targetDocuments, mostWrites, incomplete, dangling, differing,
                (double) stagingReads / Math.ceil((double) orders / batch), number(footprint, "size"),
                number(footprint, "storageSize"), number(footprint, "totalIndexSize"), references.size()));
        boolean first = true;
        for (Map.Entry<String, StepResult> step : steps.entrySet()) {
            if (!first) out.append(',');
            first = false;
            out.append('"').append(step.getKey()).append("\":").append(step.getValue().json());
        }
        out.append("},\"splitSeconds\":{");
        first = true;
        for (Map.Entry<String, Long> part : split.entrySet()) {
            if (!first) out.append(',');
            first = false;
            out.append(String.format(Locale.ROOT, "\"%s\":%.3f", part.getKey(), part.getValue() / 1e9));
        }
        out.append("}}");
        System.out.println("NEST-BUILD " + out);
        String file = System.getProperty("bench.out");
        if (file != null) Files.writeString(Path.of(file), out + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        assertThat(targetDocuments).isEqualTo(orders);
        assertThat(mostWrites).isEqualTo(1);
        assertThat(incomplete).isZero();
        if (compare) assertThat(differing).isZero();
    }

    private long upsertAll(MongoCollection<Document> into, int count, IntFunction<Map<String, Object>> rows, String key) {
        List<WriteModel<Document>> pending = new ArrayList<>(batch);
        for (int i = 0; i < count; i++) {
            Map<String, Object> row = rows.apply(i);
            Document written = new Document("_id", row.get(key));
            written.putAll(row);
            pending.add(new ReplaceOneModel<>(Filters.eq("_id", row.get(key)), written, new ReplaceOptions().upsert(true)));
            if (pending.size() == batch) {
                into.bulkWrite(pending, new BulkWriteOptions().ordered(false));
                pending = new ArrayList<>(batch);
            }
        }
        if (!pending.isEmpty()) into.bulkWrite(pending, new BulkWriteOptions().ordered(false));
        return count;
    }

    private long stageAll(MongoCollection<Document> stage, String table, int count, IntFunction<Map<String, Object>> rows,
            String key, String on) {
        List<WriteModel<Document>> pending = new ArrayList<>(batch);
        for (int i = 0; i < count; i++) {
            Map<String, Object> row = rows.apply(i);
            String id = table + ":" + row.get(key);
            Document staged = new Document("_id", id).append("t", table).append("on", row.get(on)).append("r", new Document(row));
            pending.add(new ReplaceOneModel<>(Filters.eq("_id", id), staged, new ReplaceOptions().upsert(true)));
            if (pending.size() == batch) {
                stage.bulkWrite(pending, new BulkWriteOptions().ordered(false));
                pending = new ArrayList<>(batch);
            }
        }
        if (!pending.isEmpty()) stage.bulkWrite(pending, new BulkWriteOptions().ordered(false));
        return count;
    }

    private Map<String, Object> customer(int c) {
        return row("customer_id", c, "name", "customer-" + c, "tier", c % 5);
    }

    private Map<String, Object> order(int o) {
        return row("order_id", o, "cust_ref", o % customers, "code", "order-" + o, "amount", o * 3L);
    }

    private Map<String, Object> line(int l) {
        return row("line_id", l, "order_id", (int) ((long) l * orders / lines), "sku", "sku-" + (l % 97), "qty", l % 7 + 1);
    }

    /** How many lines {@link #line} gives order {@code o}: the l with floor(l * orders / lines) == o. */
    private int linesOf(int o) {
        long first = ceilDiv((long) o * lines, orders);
        long next = ceilDiv((long) (o + 1) * lines, orders);
        return (int) (Math.min(next, lines) - Math.min(first, lines));
    }

    private static long ceilDiv(long a, long b) {
        return (a + b - 1) / b;
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int field = 0; field < fields.length; field += 2) row.put((String) fields[field], fields[field + 1]);
        return row;
    }

    private static Map<String, Object> plain(Document document) {
        return new LinkedHashMap<>(document);
    }

    private static long number(Document document, String field) {
        Object value = document.get(field);
        return value instanceof Number n ? n.longValue() : -1;
    }

    private static TransformBody.Nest body() {
        Embed lines = new Embed("line", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "lines",
                List.of("line_id"), null, null, null);
        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        return new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(lines, customer)));
    }

    private static Config memberConfig() {
        Config config = new Config();
        config.setClusterName("nest-build-bench-" + System.nanoTime());
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

    private static Mark mark(Map<String, AtomicLong> commands) {
        return new Mark(System.nanoTime(), storeCommands.get(), snapshot(commands));
    }

    private static long total(Map<String, Long> counts) {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }

    private static Map<String, Long> snapshot(Map<String, AtomicLong> commands) {
        Map<String, Long> out = new TreeMap<>();
        commands.forEach((name, count) -> out.put(name, count.get()));
        return out;
    }

    private record Mark(long began, Map<String, Long> storeBefore, Map<String, Long> clientCommands) {
        StepResult end(Map<String, AtomicLong> commands, long rows, long writes) {
            double seconds = (System.nanoTime() - began) / 1e9;
            Map<String, Long> now = snapshot(commands);
            Map<String, Long> delta = new TreeMap<>();
            now.forEach((name, count) -> {
                long moved = count - clientCommands.getOrDefault(name, 0L);
                if (moved > 0 && !name.startsWith("admin:") && !name.endsWith(":hello") && !name.endsWith(":isMaster")) {
                    delta.put(name.substring(0, name.indexOf(':')).startsWith("bench_") ? "state:" + name.substring(name.indexOf(':') + 1) : name, moved);
                }
            });
            Map<String, Long> store = new TreeMap<>();
            storeCommands.get().forEach((name, count) -> {
                long moved = count - storeBefore.getOrDefault(name, 0L);
                if (moved > 0) store.put(name, moved);
            });
            return new StepResult(seconds, rows, writes, total(store), store, delta);
        }
    }

    private record StepResult(double seconds, long rows, long writes, long storeCommands, Map<String, Long> storeByCommand,
            Map<String, Long> clientCommands) {
        String json() {
            StringBuilder store = new StringBuilder("{");
            storeByCommand.forEach((name, count) -> store.append(store.length() > 1 ? "," : "")
                    .append('"').append(name).append("\":").append(count));
            store.append('}');
            StringBuilder client = new StringBuilder("{");
            clientCommands.forEach((name, count) -> client.append(client.length() > 1 ? "," : "")
                    .append('"').append(name).append("\":").append(count));
            client.append('}');
            return String.format(Locale.ROOT,
                    "{\"seconds\":%.3f,\"rows\":%d,\"rowsPerSecond\":%.1f,\"writes\":%d,\"storeCommands\":%d,"
                            + "\"storeCommandsPerRow\":%.3f,\"storeByCommand\":%s,\"clientCommands\":%s}",
                    seconds, rows, rows / seconds, writes, storeCommands, rows == 0 ? 0.0 : (double) storeCommands / rows, store, client);
        }
    }
}
