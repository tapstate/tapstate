package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.SourceIdentityPreparation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

/** Explicit same-version maintenance. The caller must stop old writers before the first invocation. */
final class MongoSourceIdentityPreparation {
    static final int MAX_SOURCES = 20;
    static final long MAX_READ_BYTES = 32L * 1024 * 1024;
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final SourceConfigCipher cipher;
    private final Runnable requireActive;

    MongoSourceIdentityPreparation(MongoClient client, MongoDatabase database, String clusterId,
            SourceConfigCipher cipher, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId"); this.cipher = Objects.requireNonNull(cipher, "cipher");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
    }

    Optional<SourceIdentityPreparation> load(MongoReadBudget budget) {
        return Optional.ofNullable(budget.transaction(client, session -> current(session))).map(MongoSourceIdentityPreparation::progress);
    }

    SourceIdentityPreparation step(String operationId, MongoReadBudget budget) {
        if (!MongoSourceIdentitySchema.text(operationId, 256)) {
            throw new IllegalArgumentException("maintenance operation identity must be bounded non-control text");
        }
        return StoreIo.call(() -> {
            MongoSourceIdentitySchema.State state = transaction(budget, session -> {
                var existing = current(session);
                if (existing != null) {
                    requireOperation(existing, operationId); return existing;
                }
                var sources = SystemCollections.SOURCES.on(database);
                if (sources.find(session, new Document(MongoSourceIdentitySchema.FIELD, new Document("$exists", true)))
                        .projection(new Document("_id", 0).append("present", new Document("$literal", true))).limit(1).first() != null) {
                    throw MongoSourceIdentitySchema.unreadable("unmarked-headers");
                }
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
                var initial = new MongoSourceIdentitySchema.State(MongoSourceIdentitySchema.Phase.PREPARING,
                        operationId, clusterId, cipher.activeKeyId(), null, 0);
                if (SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(session,
                        new Document("_id", "config").append(MongoSourceIdentitySchema.FIELD, new Document("$exists", false)),
                        new Document("$set", new Document(MongoSourceIdentitySchema.FIELD, MongoSourceIdentitySchema.encode(initial))))
                        .getMatchedCount() != 1) { throw MongoSourceIdentitySchema.unreadable("begin-fence"); }
                return initial;
            });
            var reads = new ReadAllowance();
            int handled = 0;
            try {
            while (state.phase() != MongoSourceIdentitySchema.Phase.READY && handled < MAX_SOURCES) {
                requireActive.run(); budget.remainingMillis();
                var next = budget.transaction(client, session -> {
                    var observed = current(session); requireOperation(observed, operationId);
                    return new Next(observed, observed.phase() == MongoSourceIdentitySchema.Phase.READY ? null
                            : reads.source(session, after(session, observed, reads), null));
                });
                state = next.state();
                if (state.phase() == MongoSourceIdentitySchema.Phase.READY) { break; }
                Document row = next.row();
                if (row == null) {
                    state = transaction(budget, session -> finish(session, operationId, reads));
                    continue;
                }
                if (!(row.get("_id") instanceof String id)) { throw MongoSourceIdentitySchema.unreadable("source-id"); }
                budget.remainingMillis();
                if (!(new EncryptedArtifactCodec(cipher).decode(row) instanceof SourceResource)) {
                    throw MongoSourceIdentitySchema.unreadable("source-kind");
                }
                String hash = row.getString("contentHash");
                var candidate = MongoSourceIdentity.create(id);
                state = transaction(budget, session -> enroll(session, operationId, id, hash, candidate, reads));
                handled++;
            }
            if (state.phase() != MongoSourceIdentitySchema.Phase.READY) {
                state = transaction(budget, session -> finish(session, operationId, reads));
            }
            } catch (Yield exhausted) {
                // The enclosing transaction aborted; only previously committed cursor progress survives.
                state = budget.transaction(client, session -> current(session));
                requireOperation(state, operationId);
            }
            requireActive.run(); budget.remainingMillis();
            return progress(state);
        });
    }

    private MongoSourceIdentitySchema.State enroll(ClientSession session, String operationId, String id,
            String expectedHash, MongoSourceIdentity.Header candidate, ReadAllowance reads) {
        var state = current(session); requireOperation(state, operationId);
        if (state.phase() == MongoSourceIdentitySchema.Phase.READY) { return state; }
        var sources = SystemCollections.SOURCES.on(database);
        var after = after(session, state, reads);
        // The database orders the cursor; Java UTF-16 ordering differs for supplementary code points.
        Document row = reads.source(session, after, new Document("kind", 1)
                .append("contentHash", 1).append(MongoSourceIdentitySchema.FIELD, 1));
        if (row == null || !id.equals(row.get("_id"))) { return state; }
        if (!"source".equals(row.get("kind")) || !expectedHash.equals(row.get("contentHash"))
                || row.containsKey(MongoSourceIdentitySchema.FIELD)) { throw MongoSourceIdentitySchema.unreadable("source-fence"); }
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
        if (sources.updateOne(session, new Document("_id", id).append("contentHash", expectedHash)
                        .append(MongoSourceIdentitySchema.FIELD, new Document("$exists", false)),
                new Document("$set", new Document(MongoSourceIdentitySchema.FIELD, MongoSourceIdentity.encode(candidate))))
                .getMatchedCount() != 1) { throw MongoSourceIdentitySchema.unreadable("source-fence"); }
        if (state.preparedCount() == Long.MAX_VALUE) { throw MongoSourceIdentitySchema.unreadable("count"); }
        var next = new MongoSourceIdentitySchema.State(state.phase(), state.operationId(), state.clusterId(),
                state.keyId(), candidate.binding().incarnation(), state.preparedCount() + 1);
        replace(session, state, next);
        return next;
    }

    private MongoSourceIdentitySchema.State finish(ClientSession session, String operationId, ReadAllowance reads) {
        var state = current(session); requireOperation(state, operationId);
        if (state.phase() == MongoSourceIdentitySchema.Phase.READY) { return state; }
        var filter = after(session, state, reads);
        if (reads.source(session, filter, new Document("_id", 0).append("present", new Document("$literal", true))) != null) {
            return state;
        }
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
        var next = new MongoSourceIdentitySchema.State(MongoSourceIdentitySchema.Phase.READY, state.operationId(),
                state.clusterId(), state.keyId(), state.lastSourceIncarnation(), state.preparedCount());
        replace(session, state, next);
        return next;
    }

    private Document after(ClientSession session, MongoSourceIdentitySchema.State state, ReadAllowance reads) {
        if (state.lastSourceIncarnation() == null) { return new Document(); }
        // A fixed UUID locates the exact original row without bounding or truncating user-defined ids.
        var filter = new Document("sourceIdentity.incarnation", state.lastSourceIncarnation());
        reads.fixed(128);
        if (SystemCollections.SOURCES.on(database).countDocuments(session, filter,
                new com.mongodb.client.model.CountOptions().limit(2)) != 1) {
            throw MongoSourceIdentitySchema.unreadable("cursor-source");
        }
        var row = reads.source(session, filter, new Document("kind", 1).append(MongoSourceIdentitySchema.FIELD, 1));
        if (row == null) { throw MongoSourceIdentitySchema.unreadable("cursor-source"); }
        var header = MongoSourceIdentity.read(row);
        if (header.binding().bindingRevision() != 1 || header.writeFence() != 0) {
            throw MongoSourceIdentitySchema.unreadable("cursor-source");
        }
        return new Document("_id", new Document("$gt", row.getString("_id")));
    }

    private record Next(MongoSourceIdentitySchema.State state, Document row) { }

    /** All returned Source BSON, including cursor/projection reads and retried callbacks, shares one allowance. */
    private final class ReadAllowance {
        private long used = 128;
        void fixed(long bytes) {
            if (used + bytes > MAX_READ_BYTES) { throw new Yield(); }
            used += bytes;
        }
        Document source(ClientSession session, Document filter, Document projection) {
            long remaining = MAX_READ_BYTES - used - 128;
            if (remaining <= 0) { throw new Yield(); }
            var pipeline = new java.util.ArrayList<Document>();
            pipeline.add(new Document("$match", filter));
            pipeline.add(new Document("$sort", new Document("_id", 1)));
            pipeline.add(new Document("$limit", 1));
            if (projection != null) { pipeline.add(new Document("$project", projection)); }
            // The server withholds an oversized row; the response itself cannot overrun the remaining allowance.
            pipeline.add(new Document("$project", new Document("_id", 0).append("row", new Document("$cond",
                    java.util.Arrays.asList(new Document("$lte", List.of(new Document("$bsonSize", "$$ROOT"), remaining)), "$$ROOT", null)))));
            Document response = SystemCollections.SOURCES.on(database).aggregate(session, pipeline).first();
            fixed(response == null ? 5 : size(response));
            if (response == null) { return null; }
            if (!(response.get("row") instanceof Document row)) { throw new Yield(); }
            return row;
        }
    }

    private static final class Yield extends RuntimeException {
        private Yield() { super(null, null, false, false); }
    }

    private void replace(ClientSession session, MongoSourceIdentitySchema.State old, MongoSourceIdentitySchema.State next) {
        Document value = MongoSourceIdentitySchema.encode(next);
        if (size(value) > MongoSourceIdentitySchema.MAX_CONTROL_BYTES) { throw MongoSourceIdentitySchema.unreadable("control-size"); }
        String field = MongoSourceIdentitySchema.FIELD + ".";
        Document filter = new Document("_id", "config").append(field + "operationId", old.operationId())
                .append(field + "phase", old.phase().name()).append(field + "lastSourceIncarnation", old.lastSourceIncarnation())
                .append(field + "preparedCount", old.preparedCount());
        if (SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(session, filter,
                new Document("$set", new Document(MongoSourceIdentitySchema.FIELD, value))).getMatchedCount() != 1) {
            throw MongoSourceIdentitySchema.unreadable("cursor-fence");
        }
    }

    private MongoSourceIdentitySchema.State current(ClientSession session) {
        requireActive.run();
        Document identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
        Document key = SystemCollections.CLUSTER_KEYS.on(database).find(session, new Document("_id", "cluster"))
                .projection(new Document("keyId", 1)).first();
        if (identity == null || !clusterId.equals(identity.get("clusterId")) || key == null
                || !cipher.activeKeyId().equals(key.get("keyId"))) { throw MongoSourceIdentitySchema.unreadable("original-identity"); }
        return MongoSourceIdentitySchema.read(session, database, cipher.activeKeyId());
    }

    private <T> T transaction(MongoReadBudget budget, java.util.function.Function<ClientSession, T> action) {
        // Only this enclosing operation may translate driver labels after transaction retries.
        try (var session = client.startSession()) {
            return session.withTransaction(() -> {
                requireActive.run(); budget.remainingMillis();
                T result = action.apply(session);
                requireActive.run(); budget.remainingMillis(); return result;
            }, TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                    .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                    .timeout(budget.remainingMillis(), TimeUnit.MILLISECONDS).build());
        }
    }
    private static void requireOperation(MongoSourceIdentitySchema.State state, String operationId) {
        if (state == null || state.phase() != MongoSourceIdentitySchema.Phase.READY && !operationId.equals(state.operationId())) {
            throw MongoSourceIdentitySchema.unreadable("operation");
        }
    }
    private static SourceIdentityPreparation progress(MongoSourceIdentitySchema.State state) {
        return new SourceIdentityPreparation(MongoSourceIdentitySchema.VERSION,
                state.phase() == MongoSourceIdentitySchema.Phase.READY ? SourceIdentityPreparation.Phase.READY
                        : SourceIdentityPreparation.Phase.PREPARING,
                state.operationId(), state.lastSourceIncarnation(), state.preparedCount());
    }
    static int size(Document value) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); return output.getSize();
        }
    }
}
