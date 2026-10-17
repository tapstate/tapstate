package io.tapstate.spi.store;

/** A detached value and its CAS revision. Null bytes retain a deletion revision; zero means never stored. */
public record RequiredNoteValue(long revision, byte[] bytes) {
    public RequiredNoteValue {
        if (revision < 0 || (revision == 0 && bytes != null)) {
            throw new IllegalArgumentException("invalid required-note revision");
        }
        bytes = bytes == null ? null : bytes.clone();
    }

    @Override
    public byte[] bytes() {
        return bytes == null ? null : bytes.clone();
    }
}
