package io.tapstate.control.core;

import java.util.Arrays;
import java.util.Objects;

/** The image bytes and media type projected for a registered connector's catalog icon. */
public record ConnectorIcon(byte[] bytes, String mediaType) {

    public ConnectorIcon {
        bytes = Objects.requireNonNull(bytes, "bytes").clone();
        mediaType = Objects.requireNonNull(mediaType, "mediaType");
    }

    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ConnectorIcon icon
                && Arrays.equals(bytes, icon.bytes)
                && mediaType.equals(icon.mediaType);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(bytes) + mediaType.hashCode();
    }

    @Override
    public String toString() {
        return "ConnectorIcon[bytes=" + bytes.length + " bytes, mediaType=" + mediaType + "]";
    }
}
