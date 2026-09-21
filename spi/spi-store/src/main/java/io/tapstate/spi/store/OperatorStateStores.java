package io.tapstate.spi.store;

import java.util.Objects;
import java.util.List;

/** Resolves durable operator stores by database on the deployment's already configured store connection. */
public interface OperatorStateStores {

    /** The database selected by deployment configuration when an operator declares no override. */
    String defaultDatabase();

    /** The state and dead-letter stores in {@code database}, validated before they are returned. */
    OperatorStateStore inDatabase(String database);

    /** A backend with one physical target, used by store doubles that do not model database routing. */
    static OperatorStateStores fixed(
            String defaultDatabase, KeyedStateStore state, NestDeadLetterStore deadLetters) {
        Objects.requireNonNull(defaultDatabase, "defaultDatabase");
        OperatorStateStore stores = new OperatorStateStore(state, deadLetters);
        return new OperatorStateStores() {
            @Override
            public String defaultDatabase() {
                return defaultDatabase;
            }

            @Override
            public OperatorStateStore inDatabase(String database) {
                Objects.requireNonNull(database, "database");
                return stores;
            }
        };
    }

    /** A fixed state store for callers that never reach the dead-letter half. */
    static OperatorStateStores stateOnly(String defaultDatabase, KeyedStateStore state) {
        return fixed(defaultDatabase, state, new NestDeadLetterStore() {
            @Override
            public void record(NestDeadLetterRecord record) {
            }

            @Override
            public List<NestDeadLetterRecord> read(String namespace, int limit) {
                return List.of();
            }

            @Override
            public void dropNamespace(String namespace) {
            }
        });
    }
}
