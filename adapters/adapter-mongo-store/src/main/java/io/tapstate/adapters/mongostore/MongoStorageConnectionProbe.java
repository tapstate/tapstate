package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageConnectionProbe;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/** A short-lived, always-aborted permission probe; it never runs storage migrations. */
public final class MongoStorageConnectionProbe implements StorageConnectionProbe {
    private final Duration timeout;
    private final java.util.function.Function<String,com.mongodb.ConnectionString> parser;
    public MongoStorageConnectionProbe(Duration timeout) { this(timeout, com.mongodb.ConnectionString::new); }
    MongoStorageConnectionProbe(Duration timeout, java.util.function.Function<String,com.mongodb.ConnectionString> parser) {
        this.timeout = Objects.requireNonNull(timeout, "timeout"); this.parser = Objects.requireNonNull(parser, "parser");
    }
    @Override public StorageBinding.Connection normalize(StorageBinding.Connection candidate) {
        return MongoStorageConnectionInput.normalizeDatabase(candidate, parser);
    }
    @Override public StorageBinding.Connection validate(StorageBinding.Connection candidate) {
        return MongoStorageConnectionInput.validate(candidate);
    }
    @Override public void test(StorageBinding.Connection normalized) {
        try (var connection = MongoConnection.forCache(normalized, timeout, null)) {
            connection.verifyConnectivity();
            connection.verifyStoragePermissions();
        }
    }

    @Override public CacheTestResult testCache(StorageBinding.Connection normalized) {
        try (var connection = MongoConnection.forCache(normalized, timeout, null)) {
            connection.verifyConnectivity();
            String database = normalized.database();
            boolean occupied;
            try {
                occupied = hasCollections(connection.database());
            } catch (MongoException unavailable) {
                throw databaseAccessFailure(database);
            }
            if (occupied) {
                Optional<String> suggestion;
                try {
                    suggestion = CacheDatabaseSuggestions.find(database,
                            () -> ThreadLocalRandom.current().nextInt(100_000, 1_000_000),
                            candidate -> hasCollections(connection.client().getDatabase(candidate)));
                } catch (MongoException unavailable) {
                    suggestion = Optional.empty();
                }
                return CacheTestResult.occupied(suggestion.orElse(null));
            }
            connection.verifyStoragePermissions();
            return CacheTestResult.emptyDatabase();
        }
    }

    private static boolean hasCollections(MongoDatabase database) {
        return database.listCollectionNames().first() != null;
    }

    private static TapstateException databaseAccessFailure(String database) {
        return new TapstateException(StoreError.DATABASE_ACCESS_FAILED, Map.of("database", database), null);
    }
}
