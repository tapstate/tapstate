package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.OperatorStateStore;
import io.tapstate.spi.store.OperatorStateStores;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.bson.types.Binary;

/**
 * A stand-in for the commit a state session would make, used to price one lever and nothing more: the
 * same rows, the same collection and the same transaction options as the production store, but the
 * placement, inventory and shape checks taken once when the session opens instead of inside every
 * transaction, one fence read per commit, and the whole commit group in a single transaction rather than
 * one per 256 keys. It is not a store anyone should run: the fence is read, not compared and advanced,
 * and nothing here survives a lease change.
 */
public final class SessionCommitStore implements AutoCloseable {

    private static final TransactionOptions OPTIONS = TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    private final MongoClient client;
    private final MongoDatabase database;
    private final Map<String, AtomicLong> commands = new ConcurrentHashMap<>();
    private final AtomicLong transactions = new AtomicLong();

    public SessionCommitStore(String workUrl, String databaseName) {
        CommandListener counting = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getDatabaseName().equals(databaseName) || "admin".equals(event.getDatabaseName())) {
                    commands.computeIfAbsent(event.getCommandName(), ignored -> new AtomicLong()).incrementAndGet();
                }
            }
        };
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(workUrl))
                .addCommandListener(counting).build());
        database = client.getDatabase(databaseName);
    }

    /** Commands sent so far, by command name. */
    public Map<String, Long> commands() {
        Map<String, Long> out = new TreeMap<>();
        commands.forEach((name, count) -> out.put(name, count.get()));
        return out;
    }

    /** Transactions committed so far. */
    public long transactions() {
        return transactions.get();
    }

    /** The stores a member is bound to. */
    public OperatorStateStores stores() {
        KeyedStateStore state = new Session();
        OperatorStateStore bound = new OperatorStateStore(state, new NestDeadLetterStore() {
            @Override public void record(NestDeadLetterRecord record) { }
            @Override public List<NestDeadLetterRecord> read(String namespace, int limit) { return List.of(); }
            @Override public void dropNamespace(String namespace) { }
        });
        return new OperatorStateStores() {
            @Override public String defaultDatabase() { return database.getName(); }
            @Override public OperatorStateStore inDatabase(String ignored) { return bound; }
        };
    }

    @Override
    public void close() {
        client.close();
    }

    private final class Session implements KeyedStateStore {
        private Document id(String namespace, String key) {
            return new Document("placement", new Document("namespace", OperatorStateText.encode(namespace)))
                    .append("key", OperatorStateText.encode(key));
        }

        @Override public Optional<byte[]> load(String namespace, String key) {
            Document row = SystemCollections.CACHE_NEST_STATE.on(database).find(new Document("_id", id(namespace, key))).first();
            return row == null ? Optional.empty() : Optional.of(row.get("state", Binary.class).getData());
        }

        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            List<Document> ids = new ArrayList<>();
            for (String key : keys) ids.add(id(namespace, key));
            Map<String, byte[]> found = new LinkedHashMap<>();
            for (Document row : SystemCollections.CACHE_NEST_STATE.on(database).find(new Document("_id", new Document("$in", ids)))) {
                found.put(OperatorStateText.decode(row.get("_id", Document.class).getString("key")), row.get("state", Binary.class).getData());
            }
            return found;
        }

        @Override public void save(String namespace, String key, byte[] state) {
            saveAll(namespace, Map.of(key, state));
        }

        @Override public void saveAll(String namespace, Map<String, byte[]> states) {
            if (states.isEmpty()) return;
            List<WriteModel<Document>> writes = new ArrayList<>(states.size());
            states.forEach((key, bytes) -> {
                Document id = id(namespace, key);
                writes.add(new ReplaceOneModel<>(new Document("_id", id),
                        new Document("_id", id).append("formatVersion", 2).append("state", new Binary(bytes)),
                        new ReplaceOptions().upsert(true)));
            });
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    // One fence read per commit; the placement and shape checks were taken when the session opened.
                    SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session, new Document("_id", "bench"))
                            .projection(new Document("runtimeWriteFence", 1)).first();
                    SystemCollections.CACHE_NEST_STATE.on(database).bulkWrite(session, writes, new BulkWriteOptions().ordered(true));
                    return null;
                }, OPTIONS);
            }
            transactions.incrementAndGet();
        }

        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            Optional<byte[]> held = load(namespace, key);
            if (held.isEmpty()) save(namespace, key, state);
            return held;
        }

        @Override public void delete(String namespace, String key) {
            SystemCollections.CACHE_NEST_STATE.on(database).deleteOne(new Document("_id", id(namespace, key)));
        }

        @Override public void dropNamespace(String namespace) { }

        @Override public long count(String namespace) { return 0; }
    }
}
