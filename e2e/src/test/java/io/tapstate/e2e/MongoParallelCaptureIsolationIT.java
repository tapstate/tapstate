package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.adapters.pdk.ConnectorNoteScopes;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SharedNotes;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Formal PDK instances share source/global identity while retaining separate cursors and primary notes. */
@RequiresDocker
class MongoParallelCaptureIsolationIT {
    private static final Duration BOUND = Duration.ofSeconds(30);
    private static final String SENTINEL = "parallel-capture-preserved";
    private static final Map<String, String> HASHES = Map.of(
            "mongodb", "7fbdbf1ef2965053c9c5e6d28c0c21c3aa2818938ab758b965cc278d61e0c7b4",
            "mongodb-atlas", "2f70bfe42baafcadb0c2d88042e173156c714f508d5089ce7eb8eab7edaa8e18");

    @BeforeAll static void requireFormalArtifacts() { RealConnectorGate.require("mongodb", "mongodb-atlas"); }

    @ParameterizedTest
    @ValueSource(strings = {"mongodb", "mongodb-atlas"})
    void closingAndReopeningOneReaderLeavesTheOtherCursorAndNotesIntact(String connector) throws Exception {
        var jar = ConnectorJars.pathFor(connector);
        var digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(jar)) {
            var buffer = new byte[64 * 1024];
            for (int read; (read = input.read(buffer)) != -1;) digest.update(buffer, 0, read);
        }
        String hash = HexFormat.of().formatHex(digest.digest());
        assertThat(hash).isEqualTo(HASHES.get(connector));
        var info = new ConnectorIntrospector().introspect(List.of(jar));
        var ref = new ConnectorRef(List.of(jar), info.className(), info.pdkApiVersion(), null, info.spec(), hash, false);
        String database = "parallel_capture_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        try (var storage = PreparedNativeStore.create();
                var configuration = MongoConfigurationDomain.openExisting(new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)));
                MongoClient observer = MongoClients.create(uri);
                AutoCloseable cleanup = () -> observer.getDatabase(database).drop()) {
            var rows = observer.getDatabase(database).getCollection("orders");
            rows.insertMany(List.of(row("seed", 1), row("remove", 20)));
            Map<String, Object> settings = Map.of("isUri", true, "uri", uri);
            long binding = configuration.bindings().load().orElseThrow().bindingVersion();
            var notes = configuration.requiredNotes();
            var global = notes.initialize(new RequiredNoteOwner(binding, "pdk.global-state",
                    RequiredNoteOwner.Kind.GLOBAL, storage.clusterId(), 0));
            var primaryA = primary(notes, binding, "parallel-a");
            var primaryB = primary(notes, binding, "parallel-b");
            var origin = ConnectorNoteOrigin.capture(connector, hash, settings);
            notes.prepareOrigin(primaryA, origin); notes.prepareOrigin(primaryB, origin);
            for (var scope : List.of(global, primaryA, primaryB))
                assertThat(notes.compareAndSet(scope, SENTINEL, 0, scope.owner().namespace().getBytes(StandardCharsets.UTF_8), false)).isTrue();
            var node = new PipelineNode("same_pipeline", "same_source");
            var configA = new CaptureConfig(connector, settings, List.of("orders"), node)
                    .sharing(new SharedNotes(primaryA.owner().ownerId(), List.of()));
            var configB = new CaptureConfig(connector, settings, List.of("orders"), node)
                    .sharing(new SharedNotes(primaryB.owner().ownerId(), List.of()));
            var portA = port(ref, notes, primaryA, global);
            var portB = port(ref, notes, primaryB, global);
            SourcePosition seamA = snapshot(portA, configA), seamB = snapshot(portB, configB);
            var a = new Recording(); var b = new Recording();
            try (var firstA = portA.cdc(configA, CaptureStart.resume(seamA), a)) {
                Await.until("the first real source cursor", BOUND, () -> cursors(observer, database).size() == 1,
                        () -> cursors(observer, database).toString());
                long cursorA = cursors(observer, database).iterator().next();
                try (var liveB = portB.cdc(configB, CaptureStart.resume(seamB), b)) {
                    Await.until("two independent cursors for identical source settings", BOUND,
                            () -> cursors(observer, database).size() == 2, () -> cursors(observer, database).toString());
                    long cursorB = cursors(observer, database).stream().filter(id -> id != cursorA).findFirst().orElseThrow();
                    rows.insertOne(row("live", 2));
                    rows.updateOne(new Document("_id", "seed"), new Document("$set", new Document("value", 3)));
                    rows.deleteOne(new Document("_id", "remove"));
                    for (var recording : List.of(a, b)) {
                        recording.await("live", Op.INSERT, 2); recording.await("seed", Op.UPDATE, 3);
                        recording.await("remove", Op.DELETE, null);
                    }
                    closeAndAwaitOnly(firstA, observer, database, cursorB);
                    SourcePosition retainedA = a.position("remove", Op.DELETE, null).orElseThrow();
                    int stoppedCount = a.events.size();
                    rows.insertOne(row("while-a-stopped", 4));
                    b.await("while-a-stopped", Op.INSERT, 4);
                    assertThat(a.events).hasSize(stoppedCount);
                    assertThat(cursors(observer, database)).containsExactly(cursorB);
                    assertNotes(notes, global, primaryB, origin);
                    var restarted = new Recording();
                    try (var reopenedA = portA.cdc(configA, CaptureStart.resume(retainedA), restarted)) {
                        restarted.await("while-a-stopped", Op.INSERT, 4);
                        rows.insertOne(row("after-reopen", 5));
                        restarted.await("after-reopen", Op.INSERT, 5); b.await("after-reopen", Op.INSERT, 5);
                        assertThat(restarted.events).noneMatch(event -> event.envelope().op() == Op.READ);
                        assertThat(restarted.events).noneMatch(received -> {
                            var event = received.envelope();
                            var data = event.op() == Op.DELETE ? event.before() : event.after();
                            return data != null && Set.of("live", "seed", "remove").contains(String.valueOf(data.get("_id")));
                        });
                        assertThat(restarted.position("after-reopen", Op.INSERT, 5)).get().isNotEqualTo(retainedA);
                        assertThat(cursors(observer, database)).contains(cursorB).hasSize(2);
                        assertNotes(notes, global, primaryB, origin);
                        assertNotes(notes, global, primaryA, origin);
                    }
                }
            }
            Await.until("all owned readers release their cursors", Duration.ofSeconds(5),
                    () -> cursors(observer, database).isEmpty(), () -> cursors(observer, database).toString());
            assertNotes(notes, global, primaryB, origin);
            assertNotes(notes, global, primaryA, origin);
            // No flush-offset function is registered: these real resume tokens do not prove a native consumer ACK.
        }
    }

    private static RequiredNoteScope primary(RequiredNoteStore notes, long binding, String chain) {
        return notes.initialize(new RequiredNoteOwner(binding, "pdk.chain." + chain, RequiredNoteOwner.Kind.CHAIN, chain, 0));
    }
    private static PdkCapturePort port(ConnectorRef ref, RequiredNoteStore notes, RequiredNoteScope primary, RequiredNoteScope global) {
        return PdkCapturePort.withRequiredNotes(id -> ref, null, ConnectorNoteScopes.of(notes, namespace -> {
            if (namespace.equals(primary.owner().namespace())) return primary;
            if (namespace.equals(global.owner().namespace())) return global;
            throw new AssertionError("an unexpected note namespace was requested: " + namespace);
        }, () -> {}));
    }
    private static SourcePosition snapshot(PdkCapturePort port, CaptureConfig config) {
        var values = new java.util.TreeMap<String, Long>();
        try (var batch = port.snapshot(config)) {
            while (batch.hasNext()) {
                var event = batch.next(); assertThat(event.op()).isEqualTo(Op.READ);
                values.put(String.valueOf(event.after().get("_id")), ((Number) event.after().get("value")).longValue());
            }
            assertThat(values).containsExactlyInAnyOrderEntriesOf(Map.of("seed", 1L, "remove", 20L));
            return batch.seam().orElseThrow();
        }
    }
    private static void assertNotes(RequiredNoteStore notes, RequiredNoteScope global, RequiredNoteScope primary, Object origin) {
        for (var scope : List.of(global, primary)) {
            var retained = notes.read(scope, SENTINEL);
            assertThat(retained.revision()).isEqualTo(1);
            assertThat(retained.bytes()).containsExactly(scope.owner().namespace().getBytes(StandardCharsets.UTF_8));
            assertThat(notes.open(scope.owner())).isEqualTo(scope);
        }
        assertThat(notes.origin(primary)).isEqualTo(origin);
    }
    private static Document row(String id, int value) { return new Document("_id", id).append("value", value); }
    private static void closeAndAwaitOnly(Subscription original, MongoClient observer, String database, long retained) throws Exception {
        try (var worker = Executors.newSingleThreadExecutor()) {
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            var closing = worker.submit(original::close);
            try {
                Await.until("closing A kills only its original cursor", Duration.ofNanos(Math.max(1, deadline - System.nanoTime())),
                        () -> cursors(observer, database).equals(Set.of(retained)), () -> cursors(observer, database).toString());
                assertThat(deadline - System.nanoTime()).as("the original cursor-release deadline").isNotNegative();
                // Cursor release is bounded above; local plugin-thread shutdown is observed separately.
                closing.get(10, TimeUnit.SECONDS);
            } finally { worker.shutdownNow(); }
        }
    }
    private static Set<Long> cursors(MongoClient observer, String database) {
        var result = new TreeSet<Long>();
        for (var operation : observer.getDatabase("admin").aggregate(List.of(
                new Document("$currentOp", new Document("allUsers", true).append("idleCursors", true)),
                new Document("$match", new Document("cursor.originatingCommand.$db", database)
                        .append("cursor.originatingCommand.pipeline.0.$changeStream", new Document("$exists", true)))))
                .into(new ArrayList<>()))
            result.add(((Number) operation.get("cursor", Document.class).get("cursorId")).longValue());
        return result;
    }
    private record Received(Envelope envelope, SourcePosition position) { }
    private static final class Recording implements CaptureListener {
        final List<Received> events = new CopyOnWriteArrayList<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        @Override public void onBatch(List<Envelope> batch, Optional<SourcePosition> position) {
            batch.forEach(event -> events.add(new Received(event, position.orElse(null))));
        }
        @Override public void onError(Throwable error) { failure.compareAndSet(null, error); }
        Optional<SourcePosition> position(String id, Op op, Integer value) {
            return events.stream().filter(received -> {
                var event = received.envelope(); var data = op == Op.DELETE ? event.before() : event.after();
                return event.op() == op && data != null && id.equals(String.valueOf(data.get("_id")))
                        && (value == null || data.get("value") instanceof Number actual && actual.intValue() == value);
            }).map(Received::position).filter(java.util.Objects::nonNull).findFirst();
        }
        void await(String id, Op op, Integer value) {
            Await.until("the real " + op + " for " + id + " with its source token", BOUND, () -> {
                if (failure.get() != null) throw new AssertionError("the real stream failed", failure.get());
                return position(id, op, value).isPresent();
            }, () -> events.toString());
        }
    }
}
