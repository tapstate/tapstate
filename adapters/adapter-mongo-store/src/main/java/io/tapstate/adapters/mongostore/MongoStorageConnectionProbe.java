package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageConnectionProbe;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.bson.Document;

/** A short-lived, always-aborted permission probe; it never runs storage migrations. */
public final class MongoStorageConnectionProbe implements StorageConnectionProbe {
    private final Duration timeout;
    public MongoStorageConnectionProbe(Duration timeout) { this.timeout = Objects.requireNonNull(timeout, "timeout"); }
    @Override public StorageBinding.Connection normalize(StorageBinding.Connection candidate) {
        var normalized = MongoStorageConnectionInput.normalize(candidate);
        return new StorageBinding.Connection(normalized.kind(), MongoStorageConnectionInput.databaseUri(normalized),
                normalized.database(), normalized.caPem());
    }
    @Override public java.util.Optional<String> test(StorageBinding.Connection normalized) {
        try (var connection = MongoConnection.forCache(normalized, timeout, null)) {
            connection.verifyConnectivity();
            connection.verifyStoragePermissions();
            return StoreIo.call(() -> {
                Document hello = connection.client().getDatabase("admin").runCommand(new Document("hello", 1));
                // Set names alone are not identities: unrelated deployments often use the same name.
                if (!(hello.get("setName") instanceof String set) || !(hello.get("hosts") instanceof List<?> hosts)
                        || hosts.isEmpty() || hosts.stream().anyMatch(host -> !(host instanceof String))) {
                    return java.util.Optional.empty();
                }
                var members = hosts.stream().map(String.class::cast).sorted().toList();
                return java.util.Optional.of(CanonicalHash.ofText(JsonWriter.write(List.of(set, members, normalized.database()))));
            });
        }
    }
}
