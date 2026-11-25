package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.StoreErrorDefinitions;

import java.util.Set;

/**
 * The {@code store} domain's error codes: reaching the backing store at startup. These are
 * user-facing, diagnosable failures — the operator supplied an invalid store setting, pointed the
 * server at a store that is not reachable, used a standalone server where transactions require a
 * replica set or sharded cluster, or opened metadata whose Source config keyring cannot safely be used.
 *
 * <p>Driver exceptions are translated into these coded diagnostics inside this module, so no
 * driver type escapes it (rule R3). {@code placeholders()} is the named-argument contract: every
 * throw site supplies a value for each name, and the build-time placeholder gate checks the
 * catalog templates against it.
 */
public enum StoreError implements TapstateErrorCode {

    /** The store could not be reached: {@code target} is the connection target that failed. */
    UNREACHABLE(StoreErrorDefinitions.UNREACHABLE),

    /** A deployment-owned database could not prove read/write access using the configured identity. */
    DATABASE_ACCESS_FAILED(StoreErrorDefinitions.DATABASE_ACCESS_FAILED),

    /** The cache identity cannot enumerate collections to verify a fresh database. */
    CACHE_LIST_COLLECTIONS_DENIED(StoreErrorDefinitions.CACHE_LIST_COLLECTIONS_DENIED),

    /** The store was reached but lacks a transactional topology: {@code target} is the connection target. */
    NOT_REPLICA_SET(StoreErrorDefinitions.NOT_REPLICA_SET),

    /** The configured durable operator-state database name is unsafe or not valid for MongoDB. */
    INVALID_OPERATOR_STATE_DATABASE(StoreErrorDefinitions.INVALID_OPERATOR_STATE_DATABASE),

    /**
     * The configured connection string is not a valid Mongo URI. Carries no placeholder on purpose:
     * echoing the raw URI back could leak an embedded credential.
     */
    INVALID_URI(StoreErrorDefinitions.INVALID_URI),

    /** This initialized CONFIG schema does not support provider settings. */
    AI_PROVIDER_SCHEMA_UNSUPPORTED(StoreErrorDefinitions.AI_PROVIDER_SCHEMA_UNSUPPORTED),

    /** The metadata-backed Source config keyring is absent, malformed, or internally inconsistent. */
    SOURCE_CONFIG_KEYRING_INVALID(StoreErrorDefinitions.SOURCE_CONFIG_KEYRING_INVALID),

    /** This node could not bind its live node-session generation to the current keyring epoch. */
    SOURCE_CONFIG_KEYRING_NOT_READY(StoreErrorDefinitions.SOURCE_CONFIG_KEYRING_NOT_READY),

    /** A prepared key cannot become active until every live node session has acknowledged its epoch. */
    SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED(StoreErrorDefinitions.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED),

    /** Existing configuration facts do not belong to this exact new-installation operation. */
    INITIALIZATION_REFUSED(StoreErrorDefinitions.INITIALIZATION_REFUSED),

    /** A normal authoring write cannot replace or remove the storage binding's system-owned Source. */
    DEFAULT_SOURCE_PROTECTED(StoreErrorDefinitions.DEFAULT_SOURCE_PROTECTED),

    /** A Pipeline's retained state identity cannot be removed or reused before its explicit retirement. */
    PIPELINE_RETIREMENT_REQUIRED(StoreErrorDefinitions.PIPELINE_RETIREMENT_REQUIRED),

    /**
     * The configured TLS CA certificate file could not be read or parsed. {@code path} is the CA
     * file path (a filesystem path, not a credential, so it is safe to echo back). Reached only when
     * TLS is enabled (opt-in via the URI) and a CA file is configured.
     */
    TLS_CA_UNREADABLE(StoreErrorDefinitions.TLS_CA_UNREADABLE),

    /** A normalized storage endpoint violates its database, server TLS or authentication boundary. */
    STORAGE_CONNECTION_UNSUPPORTED(StoreErrorDefinitions.STORAGE_CONNECTION_UNSUPPORTED),

    /** A cache history retention cannot be represented as an exact positive MongoDB TTL in seconds. */
    INVALID_HISTORY_RETENTION(StoreErrorDefinitions.INVALID_HISTORY_RETENTION),

    /** Existing cache history indexes require explicit offline maintenance before using a new retention. */
    HISTORY_RETENTION_MISMATCH(StoreErrorDefinitions.HISTORY_RETENTION_MISMATCH),

    /** Stored trust material is invalid; no URI, certificate or private-key contents are exposed. */
    TLS_TRUST_INVALID(StoreErrorDefinitions.TLS_TRUST_INVALID);

    private final ProductErrorDefinition definition;

    StoreError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
