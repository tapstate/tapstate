package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SourceConfigCipher;
import io.tapstate.adapters.mongostore.SourceConfigKeyringStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Moves every stored Source config from structured plaintext to one authenticated envelope. */
public final class V12EncryptSourceConfigs implements ChangeSet {

    private static final DslParser PARSER = new DslParser();

    @Override
    public int version() {
        return 12;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        SourceConfigCipher cipher = new SourceConfigKeyringStore(database).loadOrCreateCipher();
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        List<Rewrite> rewrites = new ArrayList<>();

        try (MongoCursor<Document> cursor = artifacts.find(new Document("kind", "source")).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String id = String.valueOf(document.get("_id"));
                if (!(document.get("body") instanceof Document body)) throw unreadable(id);
                Map<String, Object> tree = new LinkedHashMap<>(body);
                String connector = tree.get("connector") instanceof String value ? value : null;
                if (connector == null) throw unreadable(id);
                Object storedConfig = tree.getOrDefault("config", Map.of());
                boolean hadConfig = tree.containsKey("config");
                if (storedConfig instanceof String envelope) {
                    verifyEncrypted(document, tree, id, connector, envelope, cipher);
                    continue;
                }
                Resource resource = parse(tree, id);
                verifyIdentity(document, resource, id);
                String encrypted = cipher.encrypt(id, connector, JsonWriter.write(storedConfig));
                rewrites.add(new Rewrite(id, document.getString("contentHash"), hadConfig, storedConfig, encrypted));
            }
        }

        for (Rewrite rewrite : rewrites) {
            fence.requireStillHeld();
            Document filter = new Document("_id", rewrite.id()).append("contentHash", rewrite.contentHash());
            filter.append("body.config", rewrite.hadConfig()
                    ? rewrite.oldConfig()
                    : new Document("$exists", false));
            if (artifacts.updateOne(filter,
                    new Document("$set", new Document("body.config", rewrite.envelope())))
                    .getMatchedCount() != 1) {
                throw new IllegalStateException("Source config changed while it was being encrypted: " + rewrite.id());
            }
        }
    }

    private static void verifyEncrypted(Document document, Map<String, Object> tree, String id,
            String connector, String envelope, SourceConfigCipher cipher) {
        String plaintext;
        try {
            plaintext = cipher.decrypt(id, connector, envelope);
        } catch (TapstateException unreadable) {
            throw unreadable(id);
        }
        Object config;
        try {
            config = JsonReader.parse(plaintext);
        } catch (IllegalArgumentException malformed) {
            throw unreadable(id);
        }
        if (!(config instanceof Map<?, ?>)) throw unreadable(id);
        tree.put("config", config);
        verifyIdentity(document, parse(tree, id), id);
    }

    private static Resource parse(Map<String, Object> tree, String id) {
        try {
            Resource resource = PARSER.fromTree(tree);
            if (!(resource instanceof SourceResource)) throw unreadable(id);
            return resource;
        } catch (DslException invalid) {
            throw unreadable(id);
        }
    }

    private static void verifyIdentity(Document document, Resource resource, String id) {
        if (!id.equals(resource.id()) || !CanonicalHash.of(resource).equals(document.getString("contentHash"))) {
            throw unreadable(id);
        }
    }

    private static IllegalStateException unreadable(String id) {
        return new IllegalStateException("Stored Source cannot be encrypted safely: " + id);
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        long plaintext = 0;
        try (MongoCursor<Document> cursor = SystemCollections.ARTIFACTS.on(database)
                .find(new Document("kind", "source")).projection(new Document("body.config", 1)).iterator()) {
            while (cursor.hasNext()) {
                Document body = cursor.next().get("body", Document.class);
                if (body == null || !(body.get("config") instanceof String)) plaintext++;
            }
        }
        return plaintext == 0
                ? "no Source config remains in structured plaintext"
                : "encrypts " + plaintext + " Source config(s) under the metadata-backed Cluster keyring";
    }

    private record Rewrite(String id, String contentHash, boolean hadConfig, Object oldConfig, String envelope) {
    }
}
