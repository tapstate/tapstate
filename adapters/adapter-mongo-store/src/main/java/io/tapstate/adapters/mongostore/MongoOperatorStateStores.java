package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.OperatorStateStore;
import io.tapstate.spi.store.OperatorStateStores;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Operator state databases opened from one verified MongoDB client. */
final class MongoOperatorStateStores implements OperatorStateStores {

    private final MongoClient client;
    private final String controlDatabase;
    private final String defaultDatabase;
    private final ConcurrentMap<String, OperatorStateStore> stores = new ConcurrentHashMap<>();

    MongoOperatorStateStores(MongoClient client, String controlDatabase, String defaultDatabase) {
        this.client = Objects.requireNonNull(client, "client");
        this.controlDatabase = Objects.requireNonNull(controlDatabase, "controlDatabase");
        this.defaultDatabase = MongoStorePort.requireOperatorStateDatabase(defaultDatabase, controlDatabase);
    }

    @Override
    public String defaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public OperatorStateStore inDatabase(String database) {
        String valid = MongoStorePort.requireOperatorStateDatabase(database, controlDatabase);
        return stores.computeIfAbsent(valid, this::open);
    }

    private OperatorStateStore open(String database) {
        MongoDatabase target = client.getDatabase(database)
                .withWriteConcern(MongoStorePort.NEST_STATE_WRITE_CONCERN);
        return new OperatorStateStore(
                new MongoKeyedStateStore(SystemCollections.OPERATOR_STATE.on(target)),
                new MongoNestDeadLetterStore(SystemCollections.NEST_DEAD_LETTERS.on(target)));
    }
}
