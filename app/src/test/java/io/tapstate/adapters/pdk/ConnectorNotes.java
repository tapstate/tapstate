package io.tapstate.adapters.pdk;

/**
 * Writes a connector note the way a connector's own drive would, for a case that seeds notes without opening a
 * connector. Lives beside the codec so the bytes come from it rather than from a copy of its format.
 */
public final class ConnectorNotes {

    private ConnectorNotes() {
    }

    /** The stored form of {@code value}, as a connector's notes keep it. */
    public static byte[] encode(Object value) {
        return ConnectorStateCodec.encode(value);
    }
}
