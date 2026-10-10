package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/**
 * The connector distribution store: the single truth for which connector artifacts are registered and
 * the bytes to load them from. Registration is content-hash idempotent — the same artifact bytes
 * register once and re-registering them is a no-op — so a startup seed sweep and an explicit runtime
 * register share one path with no duplication. A pure interface (rule R2); it carries no
 * connector-framework or store-driver types.
 */
public interface ConnectorRegistry {

    /**
     * Registers the artifact if its content hash is not already registered (register-if-absent),
     * storing the bytes and a {@link ConnectorRegistration} keyed by that hash. Re-registering bytes
     * whose hash is already stored is a no-op that returns the existing registration with
     * {@code newlyRegistered} false. The content hash is computed from the bytes, so identity is the
     * store's to decide, not the caller's.
     */
    RegistrationOutcome register(String connectorId, String pdkApiVersion, RegistrationSource source, byte[] artifact);

    /** Every registered connector. */
    List<ConnectorRegistration> list();

    /**
     * Every registration filed under a connector id — normally one, empty when none is.
     *
     * <p>Scoped to the id on purpose: asking about one connector must not depend on every other one
     * being readable, and a caller answering a single-id question through {@link #list()} fails whenever
     * any one stored registration cannot be reconstructed, turning one corrupt entry into an outage
     * across every connector. The default scans {@code list()} because a registry that cannot look up by
     * id has no better answer; a store that can query by id overrides this.
     *
     * <p>A list rather than one entry, because more than one is a state callers must be able to see.
     * One artifact per id is what a register enforces, but two concurrent registers can both pass that
     * check before either stores, and an id carrying two artifacts is one a connector load refuses
     * outright — so a caller handed a single arbitrary entry could neither refuse the duplicate nor
     * report it.
     */
    default List<ConnectorRegistration> findAll(String connectorId) {
        return list().stream()
                .filter(registration -> registration.connectorId().equals(connectorId))
                .toList();
    }

    /** The artifact bytes stored under a content hash, or empty if none is stored. */
    Optional<byte[]> artifact(String contentHash);

    /**
     * The version of the certification that one instance of the artifact stored under {@code contentHash} may
     * serve every writer of a sink on one member, or empty where there is none. A certification is for exact
     * bytes: a changed artifact is another one, and runs an instance per writer until it is certified in turn.
     * A registry that records no certifications has none for any artifact, which is the default.
     */
    default Optional<String> shareSafeCertification(String connectorId, String contentHash) {
        return Optional.empty();
    }

    /**
     * Whether bytes are stored under a content hash, without fetching them. A read face asking "can this
     * connector actually run here?" needs the answer, not the artifact; {@link #artifact(String)} would
     * pull tens of megabytes to compute a boolean.
     */
    boolean hasArtifact(String contentHash);
}
