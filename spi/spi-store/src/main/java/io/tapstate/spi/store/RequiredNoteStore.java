package io.tapstate.spi.store;

/** Necessary PDK notes, namespace generations and carry barriers in one configuration transaction domain. */
public interface RequiredNoteStore {
    /** Binds an existing local execution/lifecycle check, rechecked inside every transaction attempt. */
    RequiredNoteStore guardedBy(Runnable requireActive);

    /** Explicit new-scope preparation; ordinary opens must never use it to repair missing facts. */
    RequiredNoteScope initialize(RequiredNoteOwner owner);

    /** Opens an existing scope for the captured persistent owner; missing or inconsistent facts refuse. */
    RequiredNoteScope open(RequiredNoteOwner owner);

    RequiredNoteValue read(RequiredNoteScope scope, String key);

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
