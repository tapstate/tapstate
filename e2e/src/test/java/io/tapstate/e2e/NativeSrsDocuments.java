package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.SrsConsumerId;
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
    private final CacheBusinessScope scope;

    NativeSrsDocuments(PreparedNativeStore storage) {
        try (var configuration = MongoConfigurationDomain.openExisting(
                new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)))) {
            scope = configuration.bindings().cachePreparation().orElseThrow().businessScope();
            incarnation = scope.cacheIncarnation();
        }
        client = MongoClients.create(storage.cacheUri());
        database = client.getDatabase(storage.cacheDatabase());
    }

    CacheBusinessScope scope() { return scope; }

    Document pipelineMembership(String pipeline, String source) {
        var members = SystemCollections.SRS_CONSUMERS.on(database).find(
                new Document("_id.cacheIncarnation", incarnation).append("pipelineId", pipeline)
                        .append("sourceId", source).append("detached", false)).into(new ArrayList<>());
        if (members.isEmpty()) return null;
        if (members.size() != 1) throw new AssertionError("expected one original active source consumer: " + members);
        var member = members.getFirst();
        if (!SrsConsumerId.of(pipeline, source).value().equals(member.get("_id", Document.class).getString("consumer")))
            throw new AssertionError("the membership names another source consumer");
        if (chain(member.get("_id", Document.class).getString("chain")) == null)
            throw new AssertionError("the consumer's original chain is unavailable");
        return member;
    }

    Document directChannel(String pipeline, String source) {
        var channels = SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(
                new Document("_id.scope.clusterId", scope.clusterId()).append("_id.scope.bindingVersion", scope.bindingVersion())
                        .append("_id.scope.cacheIncarnation", incarnation).append("_id.scope.pipelineId", pipeline)
                        .append("_id.sourceId", source)).into(new ArrayList<>());
        if (channels.isEmpty()) return null;
        if (channels.size() != 1) throw new AssertionError("expected one original direct channel: " + channels);
        return channels.getFirst();
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

    long largestSequence(String chain, String table) {
        if (chain(chain) == null) return -1;
        String ring = io.tapstate.runtime.srs.SrsRingbuffer.ringName(chain, table);
        var bounds = SystemCollections.SRS_LOG_BOUNDS.on(database)
                .find(new Document("_id", "srs-log-bounds:" + ring)).first();
        return bounds == null ? -1 : bounds.getLong("largestSequence");
    }

    private Document chainFilter(String chain) {
        return new Document("_id.cacheIncarnation", incarnation).append("_id.chain", chain);
    }

    @Override public void close() { client.close(); }
}
