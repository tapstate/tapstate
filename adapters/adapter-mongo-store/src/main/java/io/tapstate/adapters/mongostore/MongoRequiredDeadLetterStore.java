package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.RequiredDeadLetterContext;
import io.tapstate.spi.store.RequiredDeadLetterFact;
import io.tapstate.spi.store.RequiredDeadLetterSnapshot;
import io.tapstate.spi.store.RequiredDeadLetterStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** One necessary diagnostic per element and captured physical generation, without a configuration-side run lease. */
final class MongoRequiredDeadLetterStore implements RequiredDeadLetterStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final MongoCollection<Document> rows;
    private final Runnable requireActive;

    MongoRequiredDeadLetterStore(MongoClient client, MongoDatabase database, String clusterId) { this(client, database, clusterId, () -> {}); }
    private MongoRequiredDeadLetterStore(MongoClient client, MongoDatabase database, String clusterId, Runnable guard) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId"); this.requireActive = guard;
        this.rows = SystemCollections.CONFIG_NEST_DEAD_LETTERS.on(database).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }
    @Override public RequiredDeadLetterStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoRequiredDeadLetterStore(client, database, clusterId, () -> { requireActive.run(); additional.run(); });
    }
    static void prepareIndexes(MongoDatabase database) {
        StoreIo.run(() -> {
            for (var index : SystemCollections.CONFIG_NEST_DEAD_LETTERS.indexes()) IndexEnsure.createIfAbsent(SystemCollections.CONFIG_NEST_DEAD_LETTERS.on(database), index);
        });
    }
    void verify() {
        StoreIo.run(() -> {
            var indexes = rows.listIndexes().into(new ArrayList<>());
            if (indexes.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw RequiredDeadLetterCodec.unreadable();
            for (var declared : SystemCollections.CONFIG_NEST_DEAD_LETTERS.indexes()) {
                var found = indexes.stream().filter(index -> declared.indexName().equals(index.get("name"))).findFirst().orElse(null);
                Document keys = new Document(); declared.keys().forEach(key -> keys.append(key, 1));
                if (found == null || !keys.equals(found.get("key")) || Boolean.TRUE.equals(found.get("unique"))
                        || Boolean.TRUE.equals(found.get("sparse")) || Boolean.TRUE.equals(found.get("hidden"))
                        || found.containsKey("partialFilterExpression")) throw RequiredDeadLetterCodec.unreadable();
            }
        });
    }
    @Override public Optional<RequiredDeadLetterSnapshot> find(RequiredDeadLetterContext context, String element) {
        Objects.requireNonNull(element, "element");
        return transaction(session -> {
            requireScope(session, context);
            Document row = rows.find(session, new Document("_id", id(context, element))).first();
            return row == null ? Optional.empty() : Optional.of(decode(context, row));
        });
    }
    @Override public List<RequiredDeadLetterSnapshot> read(RequiredDeadLetterContext context, int limit) {
        return transaction(session -> {
            requireScope(session, context); if (limit <= 0) return List.of();
            return rows.find(session, filter(context)).sort(new Document("fact.discardedAt", -1)).limit(limit)
                    .map(row -> decode(context, row)).into(new ArrayList<>());
        });
    }
    @Override public boolean record(RequiredDeadLetterContext context, long expected, RequiredDeadLetterFact fact) {
        Objects.requireNonNull(context, "context"); Objects.requireNonNull(fact, "fact");
        if (expected < 0 || expected >= Long.MAX_VALUE - 1 || !context.namespace().equals(fact.record().namespace())) throw new IllegalArgumentException("invalid dead-letter command");
        Document captured = RequiredDeadLetterCodec.capture(fact);
        Document key = id(context, fact.record().element());
        return transaction(session -> {
            Document mapping = requireScope(session, context);
            var state = MongoPipelineStateMappings.decode(context.pipelineId(), mapping);
            if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() != context.stateGeneration()) throw fenced();
            Document current = rows.find(session, new Document("_id", key)).first();
            long revision = current == null ? 0 : decode(context, current).revision();
            if (revision != expected) return false;
            if (current != null) {
                Document previous = current.get("fact", Document.class);
                if (RequiredDeadLetterCodec.eventIdentity(previous).equals(RequiredDeadLetterCodec.eventIdentity(captured))
                        && !RequiredDeadLetterCodec.content(previous).equals(RequiredDeadLetterCodec.content(captured))) {
                    throw new TapstateException(IoError.REQUIRED_DEAD_LETTER_CONFLICT, Map.of(), null);
                }
            }
            touch(session, context, mapping);
            Document replacement = new Document("_id", key).append("formatVersion", 1).append("revision", expected + 1).append("fact", captured);
            if (current == null) rows.insertOne(session, replacement);
            else if (rows.replaceOne(session, new Document("_id", key).append("revision", expected), replacement).getMatchedCount() != 1) {
                throw new IllegalStateException("dead-letter revision changed inside one transaction snapshot");
            }
            return true;
        });
    }
    @Override public void dropNamespace(RequiredDeadLetterContext context, String operationId) {
        if (operationId == null || operationId.isBlank()) throw new IllegalArgumentException("retirement operation must not be blank");
        transaction(session -> {
            Document mapping = requireScope(session, context); var state = MongoPipelineStateMappings.decode(context.pipelineId(), mapping);
            if (state.stateGeneration() != context.stateGeneration() || !operationId.equals(state.operationId())
                    || (state.phase() != PipelineStateMapping.Phase.RETIRING && state.phase() != PipelineStateMapping.Phase.RETIRED)) throw fenced();
            touch(session, context, mapping); rows.deleteMany(session, filter(context)); return null;
        });
    }
    private Document requireScope(ClientSession session, RequiredDeadLetterContext context) {
        Objects.requireNonNull(context, "context");
        Document identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
        if (identity == null || !(identity.get("bindingVersion") instanceof Long binding)
                || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))) throw RequiredDeadLetterCodec.unreadable();
        if (!clusterId.equals(context.clusterId()) || !clusterId.equals(identity.get("clusterId")) || binding != context.bindingVersion()) throw fenced();
        Document mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", context.pipelineId())).first();
        var state = MongoPipelineStateMappings.decode(context.pipelineId(), mapping);
        Document pipeline = SystemCollections.PIPELINES.on(database).find(session, new Document("_id", context.pipelineId())).first();
        if ((state.phase() == PipelineStateMapping.Phase.RETIRED) != (pipeline == null)) throw RequiredDeadLetterCodec.unreadable();
        if (context.stateGeneration() > state.stateGeneration()) throw fenced();
        return mapping;
    }
    private void touch(ClientSession session, RequiredDeadLetterContext context, Document mapping) {
        if (mapping.getLong("noteWriteFence") >= Long.MAX_VALUE - 1) throw RequiredDeadLetterCodec.unreadable();
        if (SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,
                new Document("_id", context.pipelineId()).append("stateGeneration", mapping.get("stateGeneration"))
                        .append("phase", mapping.get("phase")).append("operationId", mapping.get("operationId")).append("noteWriteFence", mapping.get("noteWriteFence")),
                new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) {
            throw new IllegalStateException("state mapping changed inside one transaction snapshot");
        }
    }
    private RequiredDeadLetterSnapshot decode(RequiredDeadLetterContext context, Document row) {
        try {
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "revision", "fact")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !(row.get("revision") instanceof Long revision) || !(row.get("_id") instanceof Document key)) throw RequiredDeadLetterCodec.unreadable();
            var fact = RequiredDeadLetterCodec.decode(row.get("fact", Document.class));
            if (!context.namespace().equals(fact.record().namespace()) || !id(context, fact.record().element()).equals(key)) throw RequiredDeadLetterCodec.unreadable();
            return new RequiredDeadLetterSnapshot(revision, fact);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw RequiredDeadLetterCodec.unreadable(); }
    }
    private static Document id(RequiredDeadLetterContext context, String element) {
        return new Document("clusterId", context.clusterId()).append("bindingVersion", context.bindingVersion()).append("pipelineId", context.pipelineId())
                .append("stepId", context.stepId()).append("pathId", context.pathId()).append("stateGeneration", context.stateGeneration())
                .append("namespace", context.namespace()).append("element", element);
    }
    private static Document filter(RequiredDeadLetterContext context) {
        Document key = id(context, ""); key.remove("element"); Document filter = new Document();
        key.forEach((field, value) -> filter.append("_id." + field, value)); return filter;
    }
    private <T> T transaction(Function<ClientSession, T> operation) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    requireActive.run(); verify(); T result = operation.apply(session); requireActive.run(); return result;
                }, TRANSACTION);
            }
        });
    }
    private static TapstateException fenced() { return new TapstateException(IoError.REQUIRED_DEAD_LETTER_FENCED, Map.of(), null); }
}
