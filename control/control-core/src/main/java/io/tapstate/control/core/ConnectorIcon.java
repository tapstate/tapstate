package io.tapstate.control.core;

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
}
