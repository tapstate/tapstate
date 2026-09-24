package io.tapstate.spi.capture;

import java.util.Objects;

/**
 * An opaque position in a source's change stream, carried as a connector-defined token. It marks the
 * seam a snapshot hands its change tail ({@link CaptureBatch#seam()}), and the point a cdc stream is
 * started from ({@link CaptureStart.Resume}). The token is opaque: tapstate never parses it, only stores
 * it and threads it back to the connector. The token string — never a connector object — is what crosses
 * a persistence or serialization boundary. An immutable value; two positions with the same token are
 * equal.
 *
 * <p>A token says where, and nothing about order: two of them cannot be compared, and ranking one
 * position against another is done with the coordinates the runtime assigns, not with this.
 */
public record SourcePosition(String token) {

    public SourcePosition {
        Objects.requireNonNull(token, "token");
    }
}
