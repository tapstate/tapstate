package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code io} domain's error codes: the user-facing, diagnosable failures a store data operation
 * raises at runtime — reaching the store to save / get / list / read / swap, or reading a stored
 * document back into its model. These are distinct from the {@code store} domain, which polices
 * reaching the store at startup (unreachable, not-a-replica-set, TLS policy); {@code io} is the
 * data-plane counterpart, raised while operating on an already-verified store.
 *
 * <p>Declared beside the store ports rather than inside one implementation of them: these are what a
 * store raises whatever it is built on, so a caller can both expect them and tell them apart. A read
 * face deciding whether a failure is about one damaged document or about reaching the store at all
 * needs to name that difference, and it cannot name a classification that lives in an adapter it must
 * not depend on. Each implementation translates its own driver's exceptions into these, so no driver
 * type escapes the adapter (rule R3). Programmer errors / invariant violations (a compare-and-swap on an
 * unseeded pipeline, say) stay bare and are allowed to crash — they are not laundered into an
 * {@code io.*} code that would hide the defect. {@code placeholders()} is the named-argument
 * contract: every throw site supplies a value for each name, and the build-time placeholder gate
 * checks the catalog templates against it.
 */
public enum IoError implements TapstateErrorCode {

    /**
     * A store data operation could not complete against the store — a timeout, a dropped connection,
     * a failed TLS handshake, or a command error. {@code detail} is the failure the driver reported
     * (host and cluster state, never a credential).
     */
    STORE_UNAVAILABLE("io.store-unavailable", Set.of("detail")),

    /**
     * The store rejected the credentials for the operation. Carries no placeholder on purpose: echoing
     * anything back risks leaking a credential.
     */
    STORE_UNAUTHORIZED("io.store-unauthorized", Set.of()),

    /**
     * A stored document could not be read back into its model — a corrupt body, or one written by a
     * newer grammar, or a document missing a field this version requires. {@code id} is the stored
     * document's id.
     */
    DOCUMENT_UNREADABLE("io.document-unreadable", Set.of("id", "field"));

    private final String code;
    private final Set<String> placeholders;

    IoError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
