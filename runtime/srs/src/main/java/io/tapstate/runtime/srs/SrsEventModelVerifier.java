package io.tapstate.runtime.srs;

/** A member-local check before a ring item reaches projection or advances the reader. */
@FunctionalInterface
public interface SrsEventModelVerifier {
    SrsEventModelVerifier NONE = (item, sequence) -> {};
    void verify(SrsItem item, long sequence);

    interface Batch extends SrsEventModelVerifier {
        void verifyBatch(java.util.List<SrsItem> items, long firstSequence);

        @Override default void verify(SrsItem item, long sequence) {
            verifyBatch(java.util.List.of(item), sequence);
        }
    }
}
