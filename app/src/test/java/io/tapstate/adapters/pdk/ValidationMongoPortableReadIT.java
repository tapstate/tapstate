package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Full independent query plus recursive codecs; not model conformance, strict confirmation or lifecycle acceptance. */
@RequiresDocker
class ValidationMongoPortableReadIT {
    private static final String DATABASE = "validation_portable_read";
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(16384, 250, 24);

    @Test @Timeout(60)
    void nestedDriverValuesBecomeOwnedFactsAndLossyNestedNumbersFailInsteadOfPassing() throws Exception {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real MongoDB connector jar is required");
        var paths = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(paths);
        var ref = new ConnectorRef(paths, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            String uri = mongo.getReplicaSetUrl(DATABASE);
            Instant instant = Instant.parse("2026-10-21T01:02:03.123Z");
            ObjectId identifier = new ObjectId("0123456789abcdef01234567");
            try (var observer = MongoClients.create(uri)) {
                var database = observer.getDatabase(DATABASE);
                database.getCollection("portable").insertOne(new Document("id", 9007199254740993L)
                        .append("body", new Document("identifier", identifier).append("binary", new Binary((byte)128, new byte[] { 0, 1, -1 }))
                                .append("time", Date.from(instant)).append("text", " 001 ").append("nullable", null)
                                .append("items", List.of(new Document("n", 1L), new Document("n", 2L)))));
                database.getCollection("lossy").insertOne(new Document("id", 1L)
                        .append("body", new Document("items", List.of(new Document("amount", Decimal128.parse("9007199254740993"))))));
                try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor()) {
                    ValidationValues.Row owned;
                    try (var cursor = scan(ref, uri, "portable", queryIo, decodeIo)) {
                        try (var row = cursor.next().orElseThrow()) { owned = row.values(); }
                        assertThat(cursor.next()).isEmpty();
                    }
                    assertThat(value(owned.fields().get("id"))).isEqualTo(9007199254740993L);
                    assertThat(owned.fields()).containsKey("_id");
                    var body = (Map<?, ?>) value(owned.fields().get("body"));
                    assertThat(value(body.get("identifier"))).isEqualTo(identifier.toHexString());
                    assertThat(((ValidationValues.Typed)body.get("identifier")).conversionOrigin()).isEqualTo("custom");
                    assertThat(((ValidationValues.Typed)body.get("identifier")).logicalType()).isEqualTo("object-id");
                    var binary = (ValidationValues.Binary) value(body.get("binary"));
                    assertThat(binary.subtype()).isEqualTo(128);
                    assertThat(binary.bytes()).containsExactly((byte)0, (byte)1, (byte)-1);
                    assertThat(value(body.get("time"))).isEqualTo(instant);
                    assertThat(value(body.get("text"))).isEqualTo(" 001 ");
                    assertThat(body.containsKey("nullable")).isTrue();
                    assertThat(body.get("nullable")).isNull();
                    assertThat(body.containsKey("absent")).isFalse();
                    var items = (List<?>) value(body.get("items"));
                    assertThat(value(((Map<?, ?>) value(items.get(0))).get("n"))).isEqualTo(1L);
                    assertThat(value(((Map<?, ?>) value(items.get(1))).get("n"))).isEqualTo(2L);
                    try (var cursor = scan(ref, uri, "lossy", queryIo, decodeIo)) {
                        assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE);
                            assertThat(failure.args()).containsEntry("field", "body/items/0/amount");
                        });
                    }
                }
            }
        }
    }

    @Test @Timeout(120)
    void aFullNativeReadPassesPreviewRowAndByteLimitsWithoutAccumulatingAllRows() throws Exception {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real MongoDB connector jar is required");
        var paths = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(paths);
        var ref = new ConnectorRef(paths, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
        int expectedRows = 20_001;
        String payload = "x".repeat(2048);
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            String uri = mongo.getReplicaSetUrl(DATABASE);
            try (var observer = MongoClients.create(uri)) {
                var collection = observer.getDatabase(DATABASE).getCollection("large");
                collection.createIndex(new Document("id", 1));
                var batch = new ArrayList<Document>();
                // Reverse insertion order discriminates a real native ascending scan from natural order.
                for (int id = expectedRows - 1; id >= 0; id--) {
                    batch.add(new Document("id", (long)id).append("payload", payload));
                    if (batch.size() == 128 || id == 0) { collection.insertMany(batch); batch.clear(); }
                }
                long rows = 0;
                long deliveredPayloadBytes = 0;
                try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor();
                     var cursor = scan(ref, uri, "large", queryIo, decodeIo)) {
                    while (true) {
                        var next = cursor.next();
                        if (next.isEmpty()) break;
                        try (var row = next.orElseThrow()) {
                            assertThat(value(row.values().fields().get("id"))).isEqualTo(rows);
                            String actual = (String)value(row.values().fields().get("payload"));
                            assertThat(actual).isEqualTo(payload);
                            deliveredPayloadBytes += actual.length();
                            rows++;
                        }
                    }
                }
                assertThat(rows).isEqualTo(expectedRows);
                assertThat(deliveredPayloadBytes).isEqualTo((long)expectedRows * payload.length()).isGreaterThan(32L * 1024 * 1024);
            }
        }
    }

    private static ValidationRowCursor scan(ConnectorRef ref, String uri, String table,
            ExecutorService queryIo, ExecutorService decodeIo) {
        return PdkValidationSession.scan("mongodb", ref, Map.of("uri", uri, "database", DATABASE, "isUri", true),
                "retained-mongo-artifact", new PdkValidationSession.Scan(table, List.of("id"), table.equals("large") ? 64 : 2),
                (model, codecs) -> new PdkValidationRows(model, codecs, path -> {
                    // This fixture explicitly knows the stored BSON Date field is epoch milliseconds;
                    // the PDK DateTime payload expresses that instant as seconds plus nanoseconds.
                    if (path.equals(List.of("body", "time"))) return new PdkValidationRows.Rule(
                            PdkValidationRows.Shape.INSTANT, PdkValidationTemporal.LegacyEncoding.EPOCH_SECONDS);
                    return PdkValidationRows.Rule.AUTO;
                }, LIMITS), () -> { }, queryIo, decodeIo, 1, LIMITS);
    }
    private static Object value(Object value) { return ((ValidationValues.Typed)value).value(); }
}
