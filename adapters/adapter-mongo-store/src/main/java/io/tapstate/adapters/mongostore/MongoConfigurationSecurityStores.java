package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;
import com.mongodb.client.cursor.TimeoutMode;

/** Necessary configuration security facts; sessions belong to the independently prepared cache domain. */
final class MongoConfigurationSecurityStores {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final String clusterId;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> tokens;
    private final MongoCollection<Document> versions;
    private final AuditStore audit;
    private final TokenStore tokenStore = new Tokens(null);
    private final ClusterIdentityStore identityStore = new Identity();

    MongoConfigurationSecurityStores(MongoClient client, MongoDatabase database, String clusterId) {
        this.client = Objects.requireNonNull(client, "client");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.identity = durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database));
        this.tokens = durable(SystemCollections.CONFIG_TOKENS.on(database));
        this.versions = SystemCollections.CONFIGURATION_VERSION.on(database);
        this.audit = new MongoAuditStore(durable(SystemCollections.CONFIG_AUDIT.on(database)));
    }

    TokenStore tokens() { return tokenStore; }
    TokenStore tokens(MongoReadBudget budget) { return new Tokens(Objects.requireNonNull(budget)); }
    AuditStore audit() { return audit; }
    ClusterIdentityStore clusterIdentity() { return identityStore; }

    void verify() {
        StoreIo.run(() -> {
            readIdentity(null);
            try (var cursor = tokens.find().iterator()) {
                while (cursor.hasNext()) decodeToken(cursor.next());
            }
        });
    }

    private final class Tokens implements TokenStore {
        private final MongoReadBudget budget;
        private Tokens(MongoReadBudget budget) { this.budget = budget; }
        private MongoCollection<Document> reads() {
            return budget == null ? tokens : tokens.withTimeout(budget.operationMillis(), TimeUnit.MILLISECONDS);
        }
        private <T> T transaction(Function<ClientSession, T> body) {
            return budget == null ? MongoConfigurationSecurityStores.this.transaction(body) : budget.transaction(client, body);
        }
        @Override public void save(TokenRecord record) {
            Objects.requireNonNull(record, "record");
            StoreIo.run(record.tokenId(), () -> transaction(session -> {
                readIdentity(session);
                ConfigurationVersion.advance(session, versions, client);
                tokens.replaceOne(session, new Document("_id", record.tokenId()), MongoTokenStore.toDocument(record),
                        new ReplaceOptions().upsert(true));
                return null;
            }));
        }

        @Override public Optional<TokenRecord> find(String id) {
            Objects.requireNonNull(id, "id");
            return StoreIo.call(() -> {
                var found = Optional.ofNullable(reads().find(new Document("_id", id)).first())
                        .map(MongoConfigurationSecurityStores::decodeToken);
                if (budget != null) budget.requireRemaining();
                return found;
            });
        }

        @Override public void revoke(String id) {
            Objects.requireNonNull(id, "id");
            StoreIo.run(() -> transaction(session -> {
                readIdentity(session);
                Document row = tokens.find(session, new Document("_id", id)).first();
                if (row == null || decodeToken(row).revoked()) return null;
                ConfigurationVersion.advance(session, versions, client);
                tokens.updateOne(session, new Document("_id", id), new Document("$set", new Document("revoked", true)));
                return null;
            }));
        }

        @Override public List<TokenRecord> list() {
            return StoreIo.call(() -> {
                List<TokenRecord> result = new ArrayList<>();
                var query = reads().find();
                if (budget != null) query.timeoutMode(TimeoutMode.CURSOR_LIFETIME);
                try (var cursor = query.iterator()) {
                    while (cursor.hasNext()) result.add(decodeToken(cursor.next()));
                }
                if (budget != null) budget.requireRemaining();
                return List.copyOf(result);
            });
        }
    }

    private final class Identity implements ClusterIdentityStore {
        @Override public Optional<ClusterIdentity> find() {
            return StoreIo.call(() -> Optional.of(readIdentity(null)));
        }

        @Override public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
            Objects.requireNonNull(proposed, "proposed");
            // Bootstrap alone creates this identity. Existing callers may only load its stored winner.
            return StoreIo.call(() -> readIdentity(null));
        }
    }

    private ClusterIdentity readIdentity(ClientSession session) {
        var query = new Document("_id", "cluster");
        Document row = session == null ? identity.find(query).first() : identity.find(session, query).first();
        if (row == null || !clusterId.equals(row.get("clusterId"))
                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))
                || !(row.get("bindingVersion") instanceof Long binding) || binding < 0 || binding > 1) {
            throw unreadable("cluster", "identity");
        }
        return new ClusterIdentity(clusterId);
    }

    private static TokenRecord decodeToken(Document row) {
        String id = row.get("_id") instanceof String text ? text : "<invalid>";
        if (!row.keySet().equals(Set.of("_id", "scope", "secretHash", "revoked", "createdAt"))
                || !(row.get("_id") instanceof String) || !(row.get("scope") instanceof String scope)
                || !(row.get("secretHash") instanceof String hash) || !(row.get("revoked") instanceof Boolean revoked)
                || !(row.get("createdAt") instanceof Long createdAt)) throw unreadable(id, "token");
        try {
            return new TokenRecord(id, scope, hash, revoked, Instant.ofEpochMilli(createdAt));
        } catch (IllegalArgumentException invalid) {
            throw unreadable(id, "token");
        }
    }

    private <T> T transaction(Function<ClientSession, T> body) {
        try (var session = client.startSession()) {
            return session.withTransaction(() -> body.apply(session), TRANSACTION);
        }
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }
}
