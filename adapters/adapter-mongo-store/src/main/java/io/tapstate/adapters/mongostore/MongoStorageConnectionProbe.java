package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageConnectionProbe;
import java.time.Duration;
import java.util.Objects;

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
}
