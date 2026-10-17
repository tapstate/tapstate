package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.BSONException;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Current connection definitions, fully encrypted inside the configuration revision transaction. */
final class MongoConfigurationCatalogStore implements CatalogStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final DocumentCodec BSON = new DocumentCodec();
    private final MongoClient client;
    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> versions;
    private final SourceConfigCipherProvider ciphers;

    MongoConfigurationCatalogStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        this.collection = SystemCollections.CONFIG_CONNECTIONS.on(database)
                .withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.versions = SystemCollections.CONFIGURATION_VERSION.on(database);
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
    }

    @Override public void save(ConnectionConfig connection) {
        Objects.requireNonNull(connection, "connection");
        String id = connection.id();
        // Capture once before transaction retries so mutable caller values cannot change the command.
        byte[] body = encode(connection);
        try {
            StoreIo.run(id, () -> {
                try (var session = client.startSession()) {
                    session.withTransaction(() -> {
                        ConfigurationVersion.advance(session, versions);
                        SourceConfigCipher cipher = ciphers.currentForWrite();
                        if (ciphers.requiresWriteFence()
                                && !ciphers.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) {
                            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
                        }
                        String envelope = cipher.encryptBytes(context(id), "connection-catalog", body);
                        collection.replaceOne(session, new Document("_id", id), new Document("_id", id)
                                .append("formatVersion", 1).append("envelope", envelope), new ReplaceOptions().upsert(true));
                        return null;
                    }, TRANSACTION);
                }
            });
        } finally {
            Arrays.fill(body, (byte) 0);
        }
    }

    private static byte[] encode(ConnectionConfig connection) {
        try {
            RawBsonDocument raw = new RawBsonDocument(MongoCatalogStore.toDocument(connection), BSON);
            // Refuse values the public model cannot reconstruct before committing any ciphertext.
            MongoCatalogStore.toConfig(raw.decode(BSON));
            return Arrays.copyOfRange(raw.getBackingArray(), raw.getByteOffset(), raw.getByteOffset() + raw.getByteLength());
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ArithmeticException invalid) {
            throw new TapstateException(IoError.DOCUMENT_INVALID, Map.of("id", connection.id(), "field", "connection"), null);
        }
    }

    @Override public Optional<ConnectionConfig> get(String id) {
        Objects.requireNonNull(id, "id");
        return StoreIo.call(() -> Optional.ofNullable(collection.find(new Document("_id", id)).first()).map(this::decode));
    }

    @Override public List<ConnectionConfig> list() {
        return StoreIo.call(() -> {
            List<ConnectionConfig> result = new ArrayList<>();
            try (var cursor = collection.find().iterator()) {
                while (cursor.hasNext()) result.add(decode(cursor.next()));
            }
            return List.copyOf(result);
        });
    }

    void verifyEnvelopes() {
        StoreIo.run(() -> {
            try (var cursor = collection.find().iterator()) {
                while (cursor.hasNext()) decode(cursor.next());
            }
        });
    }

    private ConnectionConfig decode(Document row) {
        String id = row.get("_id") instanceof String text ? text : "<invalid>";
        if (!(row.get("_id") instanceof String) || !row.keySet().equals(Set.of("_id", "formatVersion", "envelope"))
                || !Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("envelope") instanceof String envelope)) {
            throw unreadable(id);
        }
        byte[] plaintext;
        try {
            plaintext = ciphers.current().decryptBytes(context(id), "connection-catalog", envelope);
        } catch (TapstateException first) {
            if (first.code() != IoError.DOCUMENT_UNREADABLE) throw first;
            plaintext = ciphers.refresh().decryptBytes(context(id), "connection-catalog", envelope);
        }
        try {
            if (plaintext.length < 5 || ByteBuffer.wrap(plaintext).order(ByteOrder.LITTLE_ENDIAN).getInt() != plaintext.length
                    || plaintext[plaintext.length - 1] != 0) throw unreadable(id);
            Document body = new RawBsonDocument(plaintext).decode(BSON);
            if (!body.keySet().equals(Set.of("_id", "connectorId", "settings")) || !id.equals(body.get("_id"))
                    || !(body.get("connectorId") instanceof String) || !(body.get("settings") instanceof Document)) {
                throw unreadable(id);
            }
            return MongoCatalogStore.toConfig(body);
        } catch (BSONException | IllegalArgumentException | ArithmeticException | IndexOutOfBoundsException | BufferUnderflowException invalid) {
            throw unreadable(id);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    private static String context(String id) { return JsonWriter.write(List.of("connections", id)); }

    private static TapstateException unreadable(String id) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", "connection"), null);
    }
}
