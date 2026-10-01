package io.tapstate.adapters.mongostore;

import java.time.Duration;
import java.util.Objects;

/**
 * The externalized settings for the store connection — the driver-free shape the assembly root
 * binds from configuration and hands to {@link MongoConnection}. Keeping it free of any driver
 * type is what lets the assembly root wire the store without importing the Mongo driver (rule R3).
 *
 * <p>TLS to the store is opt-in: the connection is plaintext by default and uses TLS only when the
 * URI asks for it ({@code ssl=true} / {@code tls=true}). When TLS is on, the URI's own settings are
 * honored as-is; {@code tlsCaFile} additionally trusts a self-signed chain.
 *
 * @param uri                    a {@code mongodb://} connection string; it carries the host(s), the
 *                               default database, the {@code replicaSet} parameter, and — to use TLS
 *                               — {@code ssl=true}
 * @param tlsCaFile              an optional path to a PEM CA certificate to trust for the TLS
 *                               handshake — a self-signed chain. Consulted only when TLS is on;
 *                               {@code null} falls back to the JVM default trust store
 * @param serverSelectionTimeout how long connection verification waits for a reachable server
 *                               before reporting the store unreachable (bounds startup fail-fast)
 * @param processId              this start of the process, which the client is named after so that
 *                               another process can find and end what this one leaves open at the
 *                               store; {@code null} for a process nobody will need to do that for
 */
public record MongoConnectionSettings(
        String uri, String tlsCaFile, Duration serverSelectionTimeout, String processId) {

    public MongoConnectionSettings {
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(serverSelectionTimeout, "serverSelectionTimeout");
    }

    /** Settings for a connection no other process will ever have to clean up after. */
    public MongoConnectionSettings(String uri, String tlsCaFile, Duration serverSelectionTimeout) {
        this(uri, tlsCaFile, serverSelectionTimeout, null);
    }
}
