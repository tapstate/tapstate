package io.tapstate.spi.store;

/** Access to one retained private origin and notes under the original persisted retirement operation. */
public interface RequiredNoteRetirement {
    RequiredNoteScope scope();
    String operationId();

    /** Only the fixed private token and the same binding's global notes are available to release code. */
    RequiredNoteStore notes();

    /** Empty only for a verified unused private scope; published missing context always refuses. */
    java.util.Optional<RequiredNoteOrigin> origin();

    /**
     * The caller has verified the original external resource retired using its supported connector
     * protocol. Publish exact private cleanup and its original small result in one CONFIG transaction.
     */
    RequiredNoteScope completeAfterVerifiedRelease();
}
