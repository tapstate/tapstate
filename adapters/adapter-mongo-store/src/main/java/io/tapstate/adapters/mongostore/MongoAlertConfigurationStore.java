package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import java.util.List;

/** Owns preparation of the four CONFIG definition resources; ordinary reads never create them. */
final class MongoAlertConfigurationStore {
    static final List<SystemCollections> COLLECTIONS = List.of(SystemCollections.ALERT_RULES,
            SystemCollections.ALERT_RECEIVERS, SystemCollections.ALERT_SUBSCRIPTIONS, SystemCollections.ALERT_SILENCES);

    private MongoAlertConfigurationStore() { }

    static void prepare(ClientSession session, MongoDatabase database) {
        MongoAlertStateStore.requireTransaction(session);
        for (SystemCollections collection : COLLECTIONS) {
            collection.prepareCollectionOn(database, session);
            for (SystemCollections.IndexSpec index : collection.indexes()) {
                IndexEnsure.createInTransaction(session, collection.on(database), index);
            }
        }
    }
}
