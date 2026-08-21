package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.UserStore;

import java.util.Objects;

/**
 * The MongoDB implementation of the three standalone authentication ports — the user store, the machine
 * token store, and the append-only audit log — each bound to its own collection on the verified
 * connection's database. These are reached on their own rather than through the aggregate
 * {@link io.tapstate.spi.store.StorePort} (they mirror one another, not the artifact / state / catalog
 * surface), so this is their counterpart binding: the assembly root builds one of these over the store
 * connection and exposes only the driver-free ports, so no driver type escapes this module (rule R3).
 */
public final class MongoAuthStores {

    /** The collection holding one document per authenticatable user. */
    public static final String USERS = "users";
    /** The collection holding one document per issued machine token (only the secret's hash). */
    public static final String TOKENS = "tokens";
    /** The append-only collection holding one document per audited operation. */
    public static final String AUDIT = "audit";
    /** The singleton document holding the stable server issuer input. */
    public static final String CLUSTER_IDENTITY = "cluster_identity";

    private final UserStore users;
    private final TokenStore tokens;
    private final AuditStore audit;
    private final ClusterIdentityStore clusterIdentity;

    /**
     * Binds the four auth stores to their own collections on the verified connection's database. The
     * connection must have been verified first (its client opened); the stores share that one client and
     * are closed with it when the connection closes.
     */
    public MongoAuthStores(MongoConnection connection) {
        Objects.requireNonNull(connection, "connection");
        MongoDatabase database = connection.database();
        this.users = new MongoUserStore(database.getCollection(USERS));
        this.tokens = new MongoTokenStore(database.getCollection(TOKENS));
        this.audit = new MongoAuditStore(database.getCollection(AUDIT));
        this.clusterIdentity = new MongoClusterIdentityStore(database.getCollection(CLUSTER_IDENTITY));
    }

    public UserStore users() {
        return users;
    }

    public TokenStore tokens() {
        return tokens;
    }

    public AuditStore audit() {
        return audit;
    }

    public ClusterIdentityStore clusterIdentity() {
        return clusterIdentity;
    }
}
