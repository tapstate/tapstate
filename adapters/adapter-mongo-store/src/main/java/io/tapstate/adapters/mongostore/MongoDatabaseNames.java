package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;

/** Keeps driver-specific database name validation behind the Mongo adapter boundary. */
public final class MongoDatabaseNames {
    private MongoDatabaseNames() { }

    public static boolean isValid(String name) {
        try {
            MongoNamespace.checkDatabaseNameValidity(name);
            return true;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }
}
