package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import java.util.List;

/** Owns preparation of scheduling, intent, limit and quota resources in the original CONFIG domain. */
final class MongoAlertNotificationStore {
    static final List<SystemCollections> COLLECTIONS = List.of(SystemCollections.ALERT_NOTIFICATION_SCHEDULES,
            SystemCollections.ALERT_DELIVERY_INTENTS, SystemCollections.ALERT_TARGET_LIMITS, SystemCollections.ALERT_TEST_QUOTAS);

    private MongoAlertNotificationStore() { }

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
