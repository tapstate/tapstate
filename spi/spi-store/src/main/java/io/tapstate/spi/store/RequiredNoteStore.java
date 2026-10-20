package io.tapstate.spi.store;

import java.util.List;

/** Necessary PDK notes, namespace generations and carry barriers in one configuration transaction domain. */
public interface RequiredNoteStore {
    /** Binds an existing local execution/lifecycle check, rechecked inside every transaction attempt. */
    RequiredNoteStore guardedBy(Runnable requireActive);

    /** Explicit new-scope preparation; ordinary opens must never use it to repair missing facts. */
    RequiredNoteScope initialize(RequiredNoteOwner owner);

    /** Opens an existing scope for the captured persistent owner; missing or inconsistent facts refuse. */
    RequiredNoteScope open(RequiredNoteOwner owner);

    RequiredNoteValue read(RequiredNoteScope scope, String key);

    /** Explicit preparation before plugin code; a retained or missing published origin is never overwritten. */
    RequiredNoteOrigin prepareOrigin(RequiredNoteScope scope, RequiredNoteOrigin origin);

    /** Load the exact prepared context without repairing missing facts or following a newer scope. */
    RequiredNoteOrigin origin(RequiredNoteScope scope);

    /** Explicit authorized preparation, before a shared drive can use earlier private notes. */
    RequiredNoteCarry prepareCarry(RequiredNoteCarry carry);

    /** Load all prepared dependencies for this exact shared generation, without repairing missing facts. */
    List<RequiredNoteCarry> carries(RequiredNoteScope shared);

    /** Read only through the saved relationship and the shared drive's guard, not the private owner's guard. */
    RequiredNoteValue readCarried(RequiredNoteCarry carry, String key);

    /** Whether this shared namespace has deliberately blocked all earlier private values. */
    boolean allCarryBlocked(RequiredNoteScope shared);

    /**
     * The caller must already have verified the original shared capture and external resources retired.
     * Atomically fences this shared token, preserves blockers and releases its retained dependencies.
     * An old token refuses; it never discovers a newer token and repeats the retirement.
     */
    RequiredNoteScope retireShared(RequiredNoteScope shared);

    /** Loads original private tokens only under the already-persisted retirement operation, including its results. */
    default List<RequiredNoteRetirement> retirements(PipelineIntentScope original, String operationId) {
        throw new io.tapstate.core.common.TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, java.util.Map.of(), null);
    }

    /** The fixed original private token may be used only by its persisted retirement operation. */
    default RequiredNoteRetirement retirement(RequiredNoteScope original, String operationId) {
        throw new io.tapstate.core.common.TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, java.util.Map.of(), null);
    }

    /** Null bytes delete logically; a shared removal may atomically block carry from earlier private notes. */
    boolean compareAndSet(RequiredNoteScope scope, String key, long expectedRevision, byte[] bytes,
            boolean blockEarlierValue);

    /** Whether the shared all-keys or per-key carry barrier has been published. */
    boolean carryBlocked(RequiredNoteScope scope, String key);

    /** Advances exactly this token; only the successful caller may adopt the returned token. */
    RequiredNoteScope clear(RequiredNoteScope scope, boolean blockEarlierValues);

    /** Physical reclamation by this exact scope and a captured upper generation, independently retryable. */
    long reclaimBefore(RequiredNoteScope scope);
}
