package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.runtime.engine.validation.ValidationOrderedGroups;
import io.tapstate.runtime.engine.validation.ValidationOrderedMerge;
import io.tapstate.spi.store.ValidationRowCursor;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real independent native ordered reads and grouping; no stable-window or validation lifecycle claim. */
@RequiresDocker
class ValidationMongoOrderedReadIT {
    private static final String DATABASE = "validation_ordered_read";
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(4096, 50, 8);

    @Test @Timeout(60)
    void projectedPhysicalKeysStreamThroughSeparateReadersAndCompleteDuplicateGroups() throws Exception {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real MongoDB connector jar is required");
        var classpath = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(classpath);
        var ref = new ConnectorRef(classpath, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            String uri = mongo.getReplicaSetUrl(DATABASE);
            try (var observer = MongoClients.create(uri)) {
                var database = observer.getDatabase(DATABASE);
                database.getCollection("source_records").insertMany(rows("source_id", List.of(7L, 3L, 1L, 3L, 6L, 5L)));
                database.getCollection("target_records").insertMany(rows("target_id", List.of(8L, 6L, 2L, 5L, 3L)));
                database.runCommand(new Document("profile", 2));
                var decoded = new AtomicInteger();
                var facts = new ArrayList<String>();
                try (var queryIo = Executors.newFixedThreadPool(2); var decodeIo = Executors.newFixedThreadPool(2);
                     var source = scan(ref, uri, "source_records", "source_id", queryIo, decodeIo, decoded);
                     var target = scan(ref, uri, "target_records", "target_id", queryIo, decodeIo, decoded);
                     var merge = new ValidationOrderedMerge(
                             new ValidationOrderedGroups("source", source,
                                     row -> key(row, "source_records", "source_id"), LIMITS.bytes()),
                             new ValidationOrderedGroups("target", target,
                                     row -> key(row, "target_records", "target_id"), LIMITS.bytes()))) {
                    while (true) {
                        var next = merge.next();
                        if (next.isEmpty()) break;
                        try (var pair = next.orElseThrow()) {
                            facts.add(pair.state() + ":" + pair.source().map(group -> group.occurrences()).orElse(0L)
                                    + ":" + pair.target().map(group -> group.occurrences()).orElse(0L));
                            pair.source().ifPresent(group ->
                                    assertThat(group.key().physical().keyFields()).containsExactly(List.of("source_id")));
                            pair.target().ifPresent(group ->
                                    assertThat(group.key().physical().keyFields()).containsExactly(List.of("target_id")));
                        }
                    }
                }
                assertThat(decoded).hasValue(11);
                assertThat(facts).containsExactly("SOURCE_ONLY:1:0", "TARGET_ONLY:0:1", "MATCHED:2:1",
                        "MATCHED:1:1", "MATCHED:1:1", "SOURCE_ONLY:1:0", "TARGET_ONLY:0:1");
                var commands = database.getCollection("system.profile").find().into(new ArrayList<>()).stream()
                        .map(profile -> profile.get("command", Document.class))
                        .filter(command -> command != null && command.get("projection") instanceof Document
                                && ("source_records".equals(command.get("find")) || "target_records".equals(command.get("find"))))
                        .toList();
                assertThat(commands).hasSize(2);
                for (Document command : commands) {
                    String field = "source_records".equals(command.get("find")) ? "source_id" : "target_id";
                    assertThat(command.get("projection", Document.class)).containsEntry(field, 1).doesNotContainKey("unrelated");
                    assertThat(command.get("sort", Document.class)).containsEntry(field, 1);
                    assertThat(command.getOrDefault("limit", 0)).isEqualTo(0);
                    assertThat(command.getOrDefault("skip", 0)).isEqualTo(0);
                }
            }
        }
    }

    private static List<Document> rows(String key, List<Long> keys) {
        return keys.stream().map(value -> new Document(key, value)
                // An unrelated lossy codec lane must not become a prerequisite for comparing keys.
                .append("unrelated", Decimal128.parse("9007199254740993"))).toList();
    }

    private static ValidationRowCursor scan(ConnectorRef ref, String uri, String table, String key,
            ExecutorService queryIo, ExecutorService decodeIo, AtomicInteger decoded) {
        return PdkValidationSession.scan("mongodb", ref, Map.of("uri", uri, "database", DATABASE, "isUri", true),
                "fixture-artifact", new PdkValidationSession.Scan(table, List.of(key), List.of(key), 2),
                (model, codecs) -> {
                    assertThat(model.getNameFieldMap()).containsOnlyKeys(key);
                    return row -> {
                        decoded.incrementAndGet();
                        assertThat(row).containsOnlyKeys(key);
                        Number actual = codecs.exactNumber(key, codecs.select(key, row.get(key),
                                model.getNameFieldMap().get(key).getTapType()));
                        return ValidationValues.ownRow(Map.of(key, actual), LIMITS);
                    };
                }, () -> { }, queryIo, decodeIo, 1, LIMITS);
    }

    private static ValidationOrderedGroups.Key key(ValidationValues.Row row, String table, String field) {
        var actual = (Number) row.fields().get(field);
        assertThat(actual).isInstanceOf(Long.class);
        var key = new ValidationKey(List.of(ValidationKey.Part.number((Long)actual)));
        return new ValidationOrderedGroups.Key(key, new ValidationPhysicalIdentity(table + "-connection",
                DATABASE, null, table, List.of(List.of(field)), key));
    }
}
