package io.tapstate.spi.store;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Internal configuration binding; connection secrets must never be included in a public status response. */
public record StorageBinding(StorageSetupScope setup, Mode mode, long bindingVersion,
        String defaultSourceId, Connection cache) {
    public enum Mode { CLOUD, OP }

    public StorageBinding {
        Objects.requireNonNull(setup, "setup");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(defaultSourceId, "defaultSourceId");
        Objects.requireNonNull(cache, "cache");
        if (bindingVersion < 1 || !defaultSourceId.equals(defaultSourceId(mode))) {
            throw new IllegalArgumentException("storage binding must have a positive version and its mode's default Source");
        }
    }

    public static String defaultSourceId(Mode mode) {
        return Objects.requireNonNull(mode, "mode") == Mode.CLOUD ? "atlas-store" : "views";
    }

    /** A normalized, already tested cache endpoint, retaining the CA contents rather than a temporary path. */
    public record Connection(Kind kind, String uri, String database, String caPem) {
        public enum Kind { MONGODB, ATLAS }

        public Connection {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(uri, "uri");
            Objects.requireNonNull(database, "database");
            if (uri.isBlank() || database.isBlank()
                    || (caPem != null && (caPem.isBlank() || caPem.getBytes(StandardCharsets.UTF_8).length > 65_536))) {
                throw new IllegalArgumentException("storage endpoint fields or CA size are invalid");
            }
        }

        @Override public String toString() { return "StorageConnection[redacted]"; }
    }

    @Override public String toString() { return "StorageBinding[redacted]"; }
}
