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
import io.tapstate.spi.store.IoError;
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
    private static final String MODEL = "model";
    private static final String BASE_URL = "baseUrl";
    private static final String OWNER = "owner";
    private final SourceConfigCipherProvider cipherProvider;
    MongoAiProviderSettingsStore(MongoClient client, MongoDatabase database) {
        this(client, database, new SourceConfigKeyringHandle(new SourceConfigKeyringStore(client, database)));
    }
    MongoAiProviderSettingsStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider cipherProvider) {
        this.client = client;
        this.meta = SystemCollections.SYSTEM_META.on(database)
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.keyrings = new SourceConfigKeyringStore(client, database);
        this.cipherProvider = Objects.requireNonNull(cipherProvider);
    }
    @Override public Optional<Settings> get(String owner) {
        Document record = StoreIo.call(() -> meta.find(new Document("_id", id(owner))).first());
        if (record == null) return Optional.empty();
        String provider = record.getString("providerId");
        String secret = decrypt(owner, context(record), record.getString("secret"));
        return Optional.of(new Settings(provider, record.getString(BASE_URL), record.getString(MODEL), secret,
                Boolean.TRUE.equals(record.get(MongoConfigurationAiProviderSettingsStore.MANAGED))));
    }
    private String decrypt(String owner, String context, String envelope) {
        try {
            return cipherProvider.current().providerSecrets().decrypt(owner, context, envelope);
        } catch (TapstateException firstRead) {
            if (firstRead.code() != IoError.DOCUMENT_UNREADABLE) throw firstRead;
            return cipherProvider.refresh().providerSecrets().decrypt(owner, context, envelope);
        }
    }
    @Override public void put(String owner, Settings settings) {
        StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    SourceConfigCipher cipher = keyrings.loadExistingCipher();
                    if (!keyrings.fenceActiveWriter(session, cipher.activeKeyId())) {
                        throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
                    }
                    var row = new Document("_id", id(owner)).append("kind", KIND).append(OWNER, owner)
                                    .append("providerId", settings.providerId()).append(BASE_URL, settings.baseUrl())
                                    .append(MODEL, settings.model()).append("secret", cipher.providerSecrets()
                                            .encrypt(owner, context(settings), settings.apiKey()));
                    if (settings.managedSelected()) row.append(MongoConfigurationAiProviderSettingsStore.MANAGED, true);
                    meta.replaceOne(session, new Document("_id", id(owner)), row, new ReplaceOptions().upsert(true));
                    return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build());
            }
            return null;
        });
    }
    @Override public boolean selectManaged(String owner, boolean selected) {
        return StoreIo.call(() -> meta.updateOne(new Document("_id", id(owner)),
                MongoConfigurationAiProviderSettingsStore.selectionUpdate(selected)).getMatchedCount() == 1);
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
        for (String field : java.util.List.of(OWNER, "providerId", BASE_URL, MODEL, "secret")) {
            if (!(record.get(field) instanceof String value) || value.isBlank()) {
                throw new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", "ai-provider-settings", "field", field), null);
            }
        }
        return io.tapstate.core.common.JsonWriter.write(java.util.List.of(record.getString("providerId"), record.getString(BASE_URL), record.getString(MODEL)));
    }
    private static String context(Settings settings) {
        return io.tapstate.core.common.JsonWriter.write(java.util.List.of(settings.providerId(), settings.baseUrl(), settings.model()));
    }
    static String id(String owner) {
        Objects.requireNonNull(owner, OWNER);
        try {
            return "ai-provider:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(owner.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }
}
