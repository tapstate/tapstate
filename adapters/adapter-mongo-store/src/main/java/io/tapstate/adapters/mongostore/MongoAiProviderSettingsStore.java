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
import io.tapstate.spi.store.AiProviderSettingsStore;
import org.bson.Document;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Provider metadata and purpose-bound ciphertext, fenced against Cluster key retirement. */
public final class MongoAiProviderSettingsStore implements AiProviderSettingsStore {
    static final String KIND = "ai-provider-settings";
    private final MongoClient client;
    private final MongoCollection<Document> meta;
    private final SourceConfigKeyringStore keyrings;
    MongoAiProviderSettingsStore(MongoClient client, MongoDatabase database) {
        this.client = client;
        this.meta = SystemCollections.SYSTEM_META.on(database)
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.keyrings = new SourceConfigKeyringStore(client, database);
    }
    @Override public Optional<Settings> get(String owner) {
        Document record = StoreIo.call(() -> meta.find(new Document("_id", id(owner))).first());
        if (record == null) return Optional.empty();
        String provider = record.getString("providerId");
        String secret = keyrings.loadExistingCipher().providerSecrets().decrypt(owner, context(record), record.getString("secret"));
        return Optional.of(new Settings(provider, record.getString("baseUrl"), record.getString("model"), secret));
    }
    @Override public void put(String owner, Settings settings) {
        StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    SourceConfigCipher cipher = keyrings.loadExistingCipher();
                    if (!keyrings.fenceActiveWriter(session, cipher.activeKeyId())) {
                        throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
                    }
                    meta.replaceOne(session, new Document("_id", id(owner)),
                            new Document("_id", id(owner)).append("kind", KIND).append("owner", owner)
                                    .append("providerId", settings.providerId()).append("baseUrl", settings.baseUrl())
                                    .append("model", settings.model()).append("secret", cipher.providerSecrets()
                                            .encrypt(owner, context(settings), settings.apiKey())),
                            new ReplaceOptions().upsert(true));
                    return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build());
            }
            return null;
        });
    }
    @Override public void delete(String owner) {
        StoreIo.call(() -> meta.deleteOne(new Document("_id", id(owner))));
    }
    static Document recordsFilter() {
        return new Document("$or", java.util.List.of(new Document("kind", KIND),
                new Document("_id", new Document("$regex", "^ai-provider:")),
                new Document("secret", new Document("$regex", "^tsai:"))));
    }
    static String context(Document record) {
        for (String field : java.util.List.of("owner", "providerId", "baseUrl", "model", "secret")) {
            if (!(record.get(field) instanceof String value) || value.isBlank()) {
                throw new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", "ai-provider-settings", "field", field), null);
            }
        }
        return io.tapstate.core.common.JsonWriter.write(java.util.List.of(record.getString("providerId"), record.getString("baseUrl"), record.getString("model")));
    }
    private static String context(Settings settings) {
        return io.tapstate.core.common.JsonWriter.write(java.util.List.of(settings.providerId(), settings.baseUrl(), settings.model()));
    }
    static String id(String owner) {
        Objects.requireNonNull(owner, "owner");
        try {
            return "ai-provider:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(owner.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }
}
