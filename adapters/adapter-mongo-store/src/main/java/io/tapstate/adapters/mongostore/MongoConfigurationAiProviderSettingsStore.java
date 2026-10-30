package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** One current owner-bound provider setting over the original CONFIG client and persistent key. */
final class MongoConfigurationAiProviderSettingsStore implements AiProviderSettingsStore {
    private static final int MAX_BYTES = 256 * 1024;
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "owner", "providerId", "baseUrl", "model", "secret");
    private static final TransactionOptions TX = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String cluster;
    private final SourceConfigCipherProvider keys;
    private final Runnable active;

    MongoConfigurationAiProviderSettingsStore(MongoClient client, MongoDatabase database, String cluster,
            SourceConfigCipherProvider keys, Runnable active) {
        this.client = client; this.database = database; this.cluster = cluster; this.keys = keys; this.active = active;
    }

    @Override public Optional<Settings> get(String owner) {
        requireOwner(owner);
        return transaction(session -> {
            var cipher = keys.refresh();
            var row = original(session, owner, cipher);
            return row.map(value -> decode(value, owner, cipher));
        });
    }

    @Override public void put(String owner, Settings settings) {
        requireOwner(owner); Objects.requireNonNull(settings);
        fields(settings.providerId(), settings.baseUrl(), settings.model());
        if (settings.apiKey() == null || settings.apiKey().isBlank() || settings.apiKey().length() > 8192
                || settings.apiKey().chars().anyMatch(value -> value < 32 || value == 127)) throw invalid("apiKey");
        var cipher = keys.currentForWrite();
        var captured = new Document("_id", MongoAiProviderSettingsStore.id(owner)).append("formatVersion", 1)
                .append("owner", owner).append("providerId", settings.providerId()).append("baseUrl", settings.baseUrl())
                .append("model", settings.model()).append("secret", cipher.providerSecrets().encrypt(owner,
                        context(settings.providerId(), settings.baseUrl(), settings.model()), settings.apiKey()));
        size(captured);
        transaction(session -> {
            var previous = original(session, owner, cipher);
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            if (!keys.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) throw invalid("key");
            var rows = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(database);
            if (previous.isEmpty()) rows.insertOne(session, new Document(captured));
            else if (rows.replaceOne(session, new Document("_id", captured.get("_id")), new Document(captured)).getMatchedCount() != 1)
                throw new IllegalStateException("provider setting disappeared inside its CONFIG transaction");
            return null;
        });
    }

    @Override public void delete(String owner) {
        requireOwner(owner);
        transaction(session -> {
            var cipher = keys.currentForWrite();
            var previous = original(session, owner, cipher);
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            if (!keys.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) throw invalid("key");
            if (previous.isPresent()) SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(database)
                    .deleteOne(session, new Document("_id", MongoAiProviderSettingsStore.id(owner)));
            return null;
        });
    }

    private Optional<Document> original(ClientSession session, String owner, SourceConfigCipher cipher) {
        var identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
        if (identity == null || !cluster.equals(identity.get("clusterId")) || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId")))
            throw invalid("identity");
        var schema = SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first();
        if (schema == null || !Integer.valueOf(2).equals(schema.get("installedVersion")))
            throw new TapstateException(StoreError.AI_PROVIDER_SCHEMA_UNSUPPORTED,
                    Map.of("required", "2"), null);
        ConfigurationVersion.read(session, SystemCollections.CONFIGURATION_VERSION.on(database));
        structure();
        var found = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(database).find(session).limit(2).into(new ArrayList<>());
        structure();
        if (found.size() > 1) throw invalid("owner");
        if (found.isEmpty()) return Optional.empty();
        decode(found.getFirst(), owner, cipher);
        return Optional.of(found.getFirst());
    }

    private void structure() {
        if (!database.listCollectionNames().into(new ArrayList<>()).contains("ai_provider_settings")) throw invalid("collection");
        var indexes = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(database).listIndexes().into(new ArrayList<>());
        if (indexes.size() != 1 || !"_id_".equals(indexes.getFirst().get("name"))
                || !new Document("_id", 1).equals(indexes.getFirst().get("key"))
                || indexes.getFirst().containsKey("expireAfterSeconds")) throw invalid("indexes");
    }

    private Settings decode(Document row, String owner, SourceConfigCipher cipher) {
        try {
            if (!row.keySet().equals(FIELDS) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !owner.equals(row.get("owner")) || !MongoAiProviderSettingsStore.id(owner).equals(row.get("_id"))) throw invalid("owner");
            size(row);
            String provider = row.getString("providerId"), endpoint = row.getString("baseUrl"), model = row.getString("model");
            fields(provider, endpoint, model);
            String key = cipher.providerSecrets().decrypt(owner, context(provider, endpoint, model), row.getString("secret"));
            if (key.isBlank() || key.length() > 8192 || key.chars().anyMatch(value -> value < 32 || value == 127)) throw invalid("secret");
            return new Settings(provider, endpoint, model, key);
        } catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw invalid("record"); }
    }

    private void requireOwner(String owner) {
        try {
            if (owner == null || owner.getBytes(StandardCharsets.UTF_8).length > 4096
                    || !(JsonReader.parse(owner) instanceof List<?> values) || values.size() != 3
                    || !"cloud".equals(values.get(0)) || !(values.get(1) instanceof String org) || org.isBlank()
                    || !cluster.equals(values.get(2)) || !owner.equals(JsonWriter.write(values))) throw invalid("owner");
        } catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw invalid("owner"); }
    }
    private static void fields(String provider, String endpoint, String model) {
        if (!"openai-compatible".equals(provider) || endpoint == null || endpoint.isBlank()
                || endpoint.getBytes(StandardCharsets.UTF_8).length > 4096 || model == null || model.isBlank() || model.length() > 200)
            throw invalid("settings");
    }
    private static String context(String provider, String endpoint, String model) { return JsonWriter.write(List.of(provider, endpoint, model)); }
    private static void size(Document row) {
        if (new RawBsonDocument(row, new DocumentCodec()).getByteBuffer().remaining() > MAX_BYTES)
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", "ai-provider-settings"), null);
    }
    private <T> T transaction(Function<ClientSession,T> operation) {
        return StoreIo.call(() -> {
            active.run();
            try (var session = client.startSession()) {
                T result = session.withTransaction(() -> { active.run(); T value = operation.apply(session); active.run(); return value; }, TX);
                active.run(); return result;
            }
        });
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "ai-provider-settings", "field", field), null);
    }
}
