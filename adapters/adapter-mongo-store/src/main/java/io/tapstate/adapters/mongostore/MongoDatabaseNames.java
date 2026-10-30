package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;

/** Keeps driver-specific database name validation behind the Mongo adapter boundary. */
public final class MongoDatabaseNames {
    private MongoDatabaseNames() { }

    /** Returns an explicit URI database without letting driver diagnostics expose credentials. */
    public static String fromUri(String uri) {
        try {
            String database = new com.mongodb.ConnectionString(uri).getDatabase();
            if (database == null || database.isBlank() || !isValid(database)) throw new IllegalArgumentException();
            return database;
        } catch (IllegalArgumentException invalid) {
            throw new io.tapstate.core.common.TapstateException(StoreError.INVALID_URI, java.util.Map.of(), null);
        }
    }

    public static boolean isValid(String name) {
        try {
            MongoNamespace.checkDatabaseNameValidity(name);
            return true;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }
}
