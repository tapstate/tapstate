package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoSrsLogStore;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.SrsLogRecord;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** A retained command must survive an actual client/store restart, without connector objects. */
class ReportedSrsDdlPersistenceIT {
    @Test
    void reopenedDurableLogRetainsDdlAndStillReadsLegacyRecords() {
        String template = System.getProperty("tapstate.e2e.report.mongo-uri-template");
        org.junit.jupiter.api.Assumptions.assumeTrue(template != null && !template.isBlank());
        String database = "ddl_log_" + UUID.randomUUID().toString().replace("-", "");
        String uri = template.replace("{database}", database);
        var schema = Map.<String,Object>of("origin", "ALTER TABLE T ADD C INTEGER DEFAULT 99",
                "tapstateDdl", Map.of("format", 1, "action", "add", "fields",
                        List.of(Map.of("name", "C", "kind", "INT64", "bit", 32, "default", "99"))));
        try {
            try (var client = MongoClients.create(uri)) {
                var collection = client.getDatabase(database).getCollection("events");
                new MongoSrsLogStore(collection).store("test-ring", 0,
                        new SrsLogRecord("ddl-position", Op.DDL, 42, null, null, 3, null, 7, schema));
                collection.insertOne(new Document("_id", new Document("ring", "test-ring").append("seq", 1L))
                        .append("op", "ddl").append("ts", 43L).append("schemaVer", 3L));
            }
            try (var client = MongoClients.create(uri)) {
                var store = new MongoSrsLogStore(client.getDatabase(database).getCollection("events"));
                var restored = store.load("test-ring", 0).orElseThrow();
                assertThat(restored.schema()).isEqualTo(schema);
                assertThat(restored.epoch()).isEqualTo(7);
                assertThat(restored.srcToken()).isEqualTo("ddl-position");
                assertThat(store.load("test-ring", 1).orElseThrow().schema()).isNull();
            }
        } finally {
            try (var client = MongoClients.create(uri)) { client.getDatabase(database).drop(); }
        }
    }
}
