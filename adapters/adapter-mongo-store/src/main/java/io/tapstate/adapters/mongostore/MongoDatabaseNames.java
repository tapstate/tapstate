package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;
import java.nio.charset.StandardCharsets;

/** Keeps driver-specific database name validation behind the Mongo adapter boundary. */
public final class MongoDatabaseNames {
    static final int MAX_DATABASE_NAME_BYTES = 63;

    private MongoDatabaseNames() { }

    /** Returns an explicit URI database without letting driver diagnostics expose credentials. */
    public static String fromUri(String uri) {
        try {
            String database = MongoStorageConnectionInput.syntaxOnly(uri).getDatabase();
            if (database == null || database.isBlank() || !isValid(database)) throw new IllegalArgumentException();
            return database;
        } catch (IllegalArgumentException invalid) {
            throw new io.tapstate.core.common.TapstateException(StoreError.INVALID_URI, java.util.Map.of(), null);
        }
    }

    public static boolean isValid(String name) {
        if (name == null || name.getBytes(StandardCharsets.UTF_8).length > MAX_DATABASE_NAME_BYTES) return false;
        try {
            MongoNamespace.checkDatabaseNameValidity(name);
            return true;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }
}
