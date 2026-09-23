package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * The externalized store connection settings, bound from {@code tapstate.store.mongo.*}. Defaults
 * target a local single-node replica-set; the distribution's {@code conf/} and environment
 * variables override them (build once, run anywhere).
 */
@ConfigurationProperties("tapstate.store.mongo")
public class MongoProperties {

    /** The compatible default for durable operator state when no database name is configured. */
    static final String DEFAULT_OPERATOR_STATE_DATABASE = "tapstate_nest";

    /** Whether to connect to the store at startup. Off only where the store is intentionally absent. */
    private boolean enabled = true;

    /**
     * A {@code mongodb://} connection string carrying the host(s), default database and replica-set.
     * TLS is opt-in: the connection is plaintext unless the URI asks for TLS with {@code ssl=true}.
     */
    private String uri = "mongodb://localhost:27017/tapstate?replicaSet=rs0";

    /**
     * The separate database holding durable operator state and nest dead letters. It is independent
     * of the control database named by {@link #uri}; changing it selects a different state database
     * and does not copy records from the previous one.
     */
    private String operatorStateDatabase = DEFAULT_OPERATOR_STATE_DATABASE;

    /**
     * An optional path to a PEM CA certificate to trust for the store's TLS handshake — a self-signed
     * chain. Consulted only when TLS is enabled via the URI; unset falls back to the JVM default trust
     * store.
     */
    private String tlsCaFile;

    /** How long connection verification waits for a reachable server before reporting the store unreachable. */
    private Duration serverSelectionTimeout = Duration.ofSeconds(5);

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getOperatorStateDatabase() {
        return operatorStateDatabase;
    }

    public void setOperatorStateDatabase(String operatorStateDatabase) {
        this.operatorStateDatabase = operatorStateDatabase;
    }

    public String getTlsCaFile() {
        return tlsCaFile;
    }

    public void setTlsCaFile(String tlsCaFile) {
        this.tlsCaFile = tlsCaFile;
    }

    public Duration getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }

    public void setServerSelectionTimeout(Duration serverSelectionTimeout) {
        this.serverSelectionTimeout = serverSelectionTimeout;
    }
}
