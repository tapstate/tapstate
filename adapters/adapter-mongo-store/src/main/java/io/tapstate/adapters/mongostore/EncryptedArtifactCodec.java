package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.bson.Document;
import com.mongodb.client.ClientSession;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Keeps physical Source ciphertext separate from the unchanged in-memory resource and logical hash. */
final class EncryptedArtifactCodec {
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();
    private final SourceConfigCipherProvider ciphers;

    EncryptedArtifactCodec(SourceConfigCipher cipher) {
        this(SourceConfigCipherProvider.fixed(Objects.requireNonNull(cipher, "cipher")));
    }

    EncryptedArtifactCodec(SourceConfigCipherProvider ciphers) {
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
    }

    Document encode(Resource resource) {
        return encode(resource, null);
    }

    Document encode(Resource resource, ClientSession session) {
        Map<String, Object> tree = new LinkedHashMap<>(WRITER.tree(resource));
        if (resource instanceof SourceResource source) {
            Object config = tree.getOrDefault("config", Map.of());
            SourceConfigCipher cipher = ciphers.currentForWrite();
            if (ciphers.requiresWriteFence()) {
                if (session == null) throw new IllegalStateException("metadata-backed Source writes require a transaction");
                if (!ciphers.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) {
                    throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
                }
            }
            tree.put("config", cipher.encrypt(source.id(), source.connector(), JsonWriter.write(config)));
        }
        return new Document("_id", resource.id()).append("kind", resource.kind())
                .append("body", new Document(tree)).append("contentHash", CanonicalHash.of(resource));
    }

    Resource decode(Document document) {
        String id = String.valueOf(document.get("_id"));
        if (!(document.get("body") instanceof Document stored)) throw unreadable(id, "body");
        boolean indexedSource = "source".equals(document.get("kind"));
        boolean bodySource = "source".equals(stored.get("kind"));
        if (indexedSource != bodySource) throw unreadable(id, "kind");
        Map<String, Object> tree = new LinkedHashMap<>(stored);
        if ("source".equals(tree.get("kind"))) {
            if (!(tree.get("config") instanceof String envelope)
                    || !(tree.get("connector") instanceof String connector)) {
                throw unreadable(id, "config");
            }
            String json;
            try {
                json = ciphers.current().decrypt(id, connector, envelope);
            } catch (TapstateException firstRead) {
                if (firstRead.code() != IoError.DOCUMENT_UNREADABLE) throw firstRead;
                json = ciphers.refresh().decrypt(id, connector, envelope);
            }
            Object config;
            try {
                config = JsonReader.parse(json);
            } catch (IllegalArgumentException invalidJson) {
                // JSON diagnostics include rejected input. No original message or cause may cross
                // the boundary, even when a valid key decrypted an unsupported/corrupt payload.
                throw unreadable(id, "config");
            }
            if (!(config instanceof Map<?, ?>)) throw unreadable(id, "config");
            tree.put("config", config);
        }
        Resource reconstructed;
        try {
            reconstructed = PARSER.fromTree(tree);
        } catch (DslException invalidBody) {
            throw unreadable(id, invalidBody.path().isEmpty() ? "body" : invalidBody.path());
        }
        if (reconstructed instanceof SourceResource
                && (!id.equals(reconstructed.id())
                        || !CanonicalHash.of(reconstructed).equals(document.get("contentHash")))) {
            throw unreadable(id, "config");
        }
        return reconstructed;
    }

    StoredArtifactRecord browse(Document document) {
        String id = String.valueOf(document.get("_id"));
        String kind = document.get("kind") instanceof String value ? value : "unknown";
        String hash = document.get("contentHash") instanceof String value ? value : null;
        try {
            return new StoredArtifactRecord(id, kind, WRITER.write(decode(document)), hash, true);
        } catch (TapstateException invalidBody) {
            if (invalidBody.code() != IoError.DOCUMENT_UNREADABLE) throw invalidBody;
            return new StoredArtifactRecord(id, kind, null, hash, false);
        }
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }
}
