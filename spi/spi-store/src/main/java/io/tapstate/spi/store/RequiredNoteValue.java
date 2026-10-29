package io.tapstate.spi.store;

import java.util.Arrays;

/** A detached value and its CAS revision. Null bytes retain a deletion revision; zero means never stored. */
public record RequiredNoteValue(long revision, byte[] bytes) {
    public RequiredNoteValue {
        if (revision < 0 || (revision == 0 && bytes != null)) {
            throw new IllegalArgumentException("invalid required-note revision");
        }
        bytes = bytes == null ? null : bytes.clone();
    }

    public boolean present() {
        return bytes != null;
    }

    @Override
    public byte[] bytes() {
        return bytes == null ? null : bytes.clone();
    }

    @Override public boolean equals(Object value) {
        return value instanceof RequiredNoteValue other && revision == other.revision && Arrays.equals(bytes, other.bytes);
    }
    @Override public int hashCode() { return 31 * Long.hashCode(revision) + Arrays.hashCode(bytes); }
    @Override public String toString() {
        return "RequiredNoteValue[revision=" + revision + ", bytes=" + (bytes == null ? "absent" : bytes.length + " bytes") + "]";
    }
}
