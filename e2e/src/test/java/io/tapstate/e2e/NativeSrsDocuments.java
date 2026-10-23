package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.Document;

/** Read-only observations of one prepared installation's original native shared-log identity. */
final class NativeSrsDocuments implements AutoCloseable {
    private final MongoClient client;
    private final MongoDatabase database;
    private final String incarnation;

    NativeSrsDocuments(PreparedNativeStore storage) {
        try (var configuration = MongoConfigurationDomain.openExisting(
                new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)))) {
            incarnation = configuration.bindings().cachePreparation().orElseThrow().businessScope().cacheIncarnation();
        }
        client = MongoClients.create(storage.cacheUri());
        database = client.getDatabase(storage.cacheDatabase());
    }

    Set<String> miningChainIds() {
        return SystemCollections.SRS_CHAINS.on(database).find(new Document("cacheIncarnation", incarnation))
                .map(row -> row.getString("_id")).into(new ArrayList<>()).stream().collect(Collectors.toSet());
    }

    Document chain(String chain) {
        return SystemCollections.SRS_CHAINS.on(database)
                .find(new Document("_id", chain).append("cacheIncarnation", incarnation)).first();
    }

    Set<String> consumersOf(String chain) {
        return SystemCollections.SRS_CONSUMERS.on(database).find(chainFilter(chain).append("detached", false))
                .map(row -> row.get("_id", Document.class).getString("consumer"))
                .into(new ArrayList<>()).stream().collect(Collectors.toSet());
    }

    Document membership(String chain, String consumer) {
        return SystemCollections.SRS_CONSUMERS.on(database)
                .find(chainFilter(chain).append("_id.consumer", consumer)).first();
    }

    Document consumerOffset(String chain, String consumer) {
        return SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database)
                .find(chainFilter(chain).append("_id.consumer", consumer)).first();
    }

    long consumerReadSeq(String chain, String consumer, String table) {
        var row = consumerOffset(chain, consumer);
        if (row == null) return -1;
        return row.get("progress", Document.class).getList("perTableSeq", Document.class).stream()
                .filter(value -> table.equals(value.getString("table")))
                .mapToLong(value -> value.get("sequence", Number.class).longValue()).findFirst().orElse(-1L);
    }

    private Document chainFilter(String chain) {
        return new Document("_id.cacheIncarnation", incarnation).append("_id.chain", chain);
    }

    @Override public void close() { client.close(); }
}
