package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/** Necessary per-element diagnostics live with their fixed configuration state identity. */
public interface RequiredDeadLetterStore {
    RequiredDeadLetterStore guardedBy(Runnable requireActive);
    Optional<RequiredDeadLetterSnapshot> find(RequiredDeadLetterContext context, String element);
    List<RequiredDeadLetterSnapshot> read(RequiredDeadLetterContext context, int limit);

    /** Zero means never written. A failed CAS must return to the original execution, never retarget a retry. */
    boolean record(RequiredDeadLetterContext context, long expectedRevision, RequiredDeadLetterFact fact);

    /** Only the original retiring generation and operation may erase this exact physical namespace. */
    void dropNamespace(RequiredDeadLetterContext context, String retirementOperationId);
}
