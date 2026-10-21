package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** A gated Atlas witness using separate source and target databases and the production PDK ports. */
class ManagedAtlasDataFlowIT {

    private static final String CONNECTOR_ID = "mongodb-atlas";
    private static final String COLLECTION = "probe";

    @Test
    void snapshotAndChangesTravelThroughTheRealAtlasSourceAndTargetFunctions() throws Throwable {
        String jarPath = System.getProperty("tapstate.pdk.it.atlasJar");
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(jarPath != null && !jarPath.isBlank() && baseUri != null && !baseUri.isBlank(),
                "real Atlas jar and controlled URI are required for this live witness");

        String suffix = UUID.randomUUID().toString().substring(0, 12);
        String sourceDatabase = "ts_plan_source_" + suffix;
        String targetDatabase = "ts_plan_target_" + suffix;
        String sourceUri = inDatabase(baseUri, sourceDatabase);
        String targetUri = inDatabase(baseUri, targetDatabase);
        Path jar = Path.of(jarPath);
        IntrospectedConnector introspected = new ConnectorIntrospector().introspect(List.of(jar));
        ConnectorRef ref = new ConnectorRef(List.of(jar), introspected.className(),
                introspected.pdkApiVersion(), null, introspected.spec());
        Map<String, Object> sourceSettings = Map.of("isUri", true, "uri", sourceUri);
        Map<String, Object> targetSettings = Map.of("isUri", true, "uri", targetUri);

        try (MongoClient source = MongoClients.create(sourceUri);
             MongoClient target = MongoClients.create(targetUri)) {
            try {
                MongoCollection<Document> sourceRows = source.getDatabase(sourceDatabase).getCollection(COLLECTION);
                sourceRows.insertMany(List.of(
                        new Document("_id", "first").append("value", 11),
                        new Document("_id", "second").append("value", 22)));
                assertThat(sourceRows.countDocuments()).isEqualTo(2);
                Document listed = source.getDatabase(sourceDatabase).runCommand(
                        new Document("listCollections", 1)
                                .append("authorizedCollections", true)
                                .append("nameOnly", true));
                Document cursor = listed.get("cursor", Document.class);
                assertThat(cursor.getList("firstBatch", Document.class))
                        .extracting(collection -> collection.getString("name"))
                        .contains(COLLECTION);

                PdkCapturePort capture = new PdkCapturePort(id -> ref);
                CaptureConfig sourceConfig = new CaptureConfig(
                        CONNECTOR_ID, sourceSettings, List.of(COLLECTION));
                assertThat(capture.discoverSchema(new CaptureConfig(CONNECTOR_ID, sourceSettings, List.of()))
                        .tables())
                        .extracting(table -> table.name())
                        .contains(COLLECTION);
                List<Envelope> snapshot = new ArrayList<>();
                SourcePosition seam;
                try (CaptureBatch batch = capture.snapshot(sourceConfig)) {
                    seam = batch.seam().orElseThrow(
                            () -> new AssertionError("Atlas snapshot did not report a CDC seam"));
                    while (batch.hasNext()) {
                        snapshot.add(batch.next());
                    }
                }
                assertThat(snapshot).hasSize(2);
                assertThat(snapshot).extracting(Envelope::op).containsOnly(Op.READ);
                assertThat(snapshot).extracting(row -> row.after().get("_id"))
                        .containsExactlyInAnyOrder("first", "second");

                PdkSinkPort sink = new PdkSinkPort(id -> ref);
                TargetTable targetTable = new TargetTable(COLLECTION,
                        List.of(new TargetField("_id", null, true),
                                new TargetField("value", null, false)));
                try (SinkWriter writer = sink.open(new SinkConfig(
                        CONNECTOR_ID, targetSettings, WriteMode.UPSERT, DdlPolicy.FAIL,
                        targetTable))) {
                    assertThat(writer.write(snapshot).toCompletableFuture().get(30, TimeUnit.SECONDS).written())
                            .isEqualTo(2);
                }
                MongoCollection<Document> targetRows = target.getDatabase(targetDatabase).getCollection(COLLECTION);
                assertThat(targetRows.countDocuments()).isEqualTo(2);
                assertThat(((Number) targetRows.find(eq("_id", "first")).first().get("value"))
                        .longValue()).isEqualTo(11L);
                assertThat(((Number) targetRows.find(eq("_id", "second")).first().get("value"))
                        .longValue()).isEqualTo(22L);

                List<Envelope> changes = new CopyOnWriteArrayList<>();
                AtomicReference<SourcePosition> lastPosition = new AtomicReference<>();
                AtomicReference<Throwable> streamError = new AtomicReference<>();
                CountDownLatch delivered = new CountDownLatch(3);
                CaptureListener listener = new CaptureListener() {
                    @Override
                    public void onBatch(List<Envelope> batch, java.util.Optional<SourcePosition> position) {
                        if (!batch.isEmpty()) {
                            position.ifPresent(lastPosition::set);
                        }
                        for (Envelope change : batch) {
                            if (COLLECTION.equals(change.src()) && change.op() != Op.DDL) {
                                changes.add(change);
                                delivered.countDown();
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        streamError.set(error);
                        while (delivered.getCount() > 0) {
                            delivered.countDown();
                        }
                    }
                };
                try (Subscription ignored = capture.cdc(sourceConfig, CaptureStart.resume(seam), listener)) {
                    sourceRows.insertOne(new Document("_id", "third").append("value", 33));
                    sourceRows.updateOne(eq("_id", "first"), set("value", 111));
                    sourceRows.deleteOne(eq("_id", "second"));
                    assertThat(delivered.await(45, TimeUnit.SECONDS)).isTrue();
                    assertThat(streamError.get() == null).as("Atlas CDC stream failed: %s",
                            streamError.get() == null ? "none" : streamError.get().getClass().getSimpleName())
                            .isTrue();
                }
                assertThat(changes).extracting(Envelope::op)
                        .contains(Op.INSERT, Op.UPDATE, Op.DELETE);
                assertThat(changes).hasSize(3);
                assertThat(lastPosition.get()).isNotNull();

                try (SinkWriter writer = sink.open(new SinkConfig(
                        CONNECTOR_ID, targetSettings, WriteMode.UPSERT, DdlPolicy.FAIL,
                        targetTable))) {
                    for (Envelope change : changes) {
                        assertThat(writer.write(List.of(change)).toCompletableFuture()
                                .get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
                    }
                }
                assertThat(targetRows.countDocuments()).isEqualTo(2);
                assertThat(((Number) targetRows.find(eq("_id", "first")).first().get("value"))
                        .longValue()).isEqualTo(111L);
                assertThat(targetRows.find(eq("_id", "second")).first()).isNull();
                assertThat(((Number) targetRows.find(eq("_id", "third")).first().get("value"))
                        .longValue()).isEqualTo(33L);

                List<Envelope> resumed = new CopyOnWriteArrayList<>();
                AtomicReference<Throwable> resumeError = new AtomicReference<>();
                CountDownLatch resumedDelivery = new CountDownLatch(1);
                CaptureListener resumeListener = new CaptureListener() {
                    @Override
                    public void onBatch(List<Envelope> batch, java.util.Optional<SourcePosition> position) {
                        for (Envelope change : batch) {
                            if (COLLECTION.equals(change.src()) && change.op() != Op.DDL) {
                                resumed.add(change);
                                resumedDelivery.countDown();
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        resumeError.set(error);
                        resumedDelivery.countDown();
                    }
                };
                try (Subscription ignored = capture.cdc(sourceConfig,
                        CaptureStart.resume(lastPosition.get()), resumeListener)) {
                    sourceRows.insertOne(new Document("_id", "fourth").append("value", 44));
                    assertThat(resumedDelivery.await(45, TimeUnit.SECONDS)).isTrue();
                    assertThat(resumeError.get() == null).as("Atlas CDC resume failed: %s",
                            resumeError.get() == null ? "none" : resumeError.get().getClass().getSimpleName())
                            .isTrue();
                }
                assertThat(resumed).hasSize(1);
                assertThat(resumed.get(0).op()).isEqualTo(Op.INSERT);
                assertThat(resumed.get(0).after()).containsEntry("_id", "fourth");
                try (SinkWriter writer = sink.open(new SinkConfig(
                        CONNECTOR_ID, targetSettings, WriteMode.UPSERT, DdlPolicy.FAIL,
                        targetTable))) {
                    assertThat(writer.write(resumed).toCompletableFuture()
                            .get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
                }
                assertThat(targetRows.countDocuments()).isEqualTo(3);
                assertThat(((Number) targetRows.find(eq("_id", "fourth")).first().get("value"))
                        .longValue()).isEqualTo(44L);
            } finally {
                source.getDatabase(sourceDatabase).drop();
                target.getDatabase(targetDatabase).drop();
            }
        }
    }

    private static String inDatabase(String uri, String database) {
        int schemeEnd = uri.indexOf("://");
        int slash = schemeEnd < 0 ? -1 : uri.indexOf('/', schemeEnd + 3);
        if (slash < 0) {
            throw new IllegalArgumentException("Atlas test URI must include a database path");
        }
        int options = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database
                + (options < 0 ? "" : uri.substring(options));
    }
}
