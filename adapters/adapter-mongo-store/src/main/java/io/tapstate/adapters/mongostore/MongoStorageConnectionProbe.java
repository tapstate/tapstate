package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageConnectionProbe;
import java.time.Duration;
import java.util.Objects;

/** A short-lived, always-aborted permission probe; it never runs storage migrations. */
public final class MongoStorageConnectionProbe implements StorageConnectionProbe {
    private final Duration timeout;
    public MongoStorageConnectionProbe(Duration timeout) { this.timeout = Objects.requireNonNull(timeout, "timeout"); }
    @Override public StorageBinding.Connection normalize(StorageBinding.Connection candidate) {
        var normalized = MongoStorageConnectionInput.normalize(candidate);
        return new StorageBinding.Connection(normalized.kind(), MongoStorageConnectionInput.databaseUri(normalized),
                normalized.database(), normalized.caPem());
    }
    @Override public void test(StorageBinding.Connection normalized) {
        try (var connection = MongoConnection.forCache(normalized, timeout, null)) {
            connection.verifyConnectivity();
            connection.verifyStoragePermissions();

        }
    }
}
