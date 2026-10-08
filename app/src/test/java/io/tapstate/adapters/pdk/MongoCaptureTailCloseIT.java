package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Observes the real source cursor across the production tail's graceful close. */
class MongoCaptureTailCloseIT {
    private static final String DATABASE = "tail_close_probe";
    private static final String COLLECTION = "orders";

    @Test
    void closingATailKillsItsSourceChangeStreamCursor() throws Exception {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar");
        assumeTrue(configured != null, "a real MongoDB connector jar is required");
        List<Path> classpath = List.of(Path.of(configured));
        IntrospectedConnector artifact = new ConnectorIntrospector().introspect(classpath);
        ConnectorRef ref = new ConnectorRef(classpath, artifact.className(),
                artifact.pdkApiVersion(), null, artifact.spec());
        PdkCapturePort port = new PdkCapturePort(id -> ref);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch delivered = new CountDownLatch(1);
        CaptureListener listener = new CaptureListener() {
            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                if (!events.isEmpty()) {
                    delivered.countDown();
                }
            }

            @Override
            public void onError(Throwable error) {
                failure.set(error);
            }
        };

        try (MongoDBContainer source = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            source.start();
            String uri = source.getReplicaSetUrl(DATABASE);
            try (MongoClient observer = MongoClients.create(uri)) {
                var collection = observer.getDatabase(DATABASE).getCollection(COLLECTION);
                collection.insertOne(new Document("_id", "seed").append("value", 1));
                CaptureConfig config = new CaptureConfig("mongodb",
                        Map.of("uri", uri, "database", DATABASE, "isUri", true), List.of(COLLECTION));
                long cursorId;
                try (var tail = port.cdc(config, CaptureStart.present(), listener)) {
                    List<Document> cursors = awaitCursor(observer, failure);
                    assertThat(cursors).as("the tail opened one source change-stream cursor").hasSize(1);
                    cursorId = ((Number) cursors.getFirst().get("cursor", Document.class)
                            .get("cursorId")).longValue();
                    collection.insertOne(new Document("_id", "after_start").append("value", 2));
                    assertThat(delivered.await(20, TimeUnit.SECONDS))
                            .as("the real tail delivered a change before close; reader failure: %s", failure.get())
                            .isTrue();
                    assertThat(failure.get()).as("the reader was healthy before close").isNull();
                }

                // Give server-side cleanup a bounded window, then inspect the exact cursor opened above.
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                List<Document> remaining;
                do {
                    remaining = changeStreamCursors(observer).stream()
                            .filter(op -> ((Number) op.get("cursor", Document.class).get("cursorId"))
                                    .longValue() == cursorId)
                            .toList();
                    if (remaining.isEmpty()) {
                        break;
                    }
                    Thread.sleep(50);
                } while (System.nanoTime() < deadline);
                assertThat(remaining)
                        .as("tail.close() must kill source change-stream cursor %s before releasing its client; "
                                + "$currentOp still reports: %s", cursorId, remaining)
                        .isEmpty();
            }
        }
    }

    private static List<Document> awaitCursor(MongoClient observer, AtomicReference<Throwable> failure)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        List<Document> cursors;
        do {
            assertThat(failure.get()).as("the real connector started successfully").isNull();
            cursors = changeStreamCursors(observer);
            if (!cursors.isEmpty()) {
                return cursors;
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return cursors;
    }

    private static List<Document> changeStreamCursors(MongoClient observer) {
        return observer.getDatabase("admin").aggregate(List.of(
                new Document("$currentOp", new Document("allUsers", true).append("idleCursors", true)),
                new Document("$match", new Document("cursor.originatingCommand.$db", DATABASE)
                        .append("cursor.originatingCommand.pipeline.0.$changeStream",
                                new Document("$exists", true)))))
                .into(new ArrayList<>());
    }
}
