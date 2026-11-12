package io.tapstate.spi.store;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/** Icon data published alongside an image-backed connector registration. */
public record BundledConnectorIcon(String mediaType, byte[] bytes) {
    public static final int MAX_BYTES = 1024 * 1024;

    public BundledConnectorIcon {
        Objects.requireNonNull(mediaType, "mediaType");
        bytes = bytes.clone();
    }

    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof BundledConnectorIcon icon
                && mediaType.equals(icon.mediaType) && Arrays.equals(bytes, icon.bytes);
    }

    @Override
    public int hashCode() {
        return 31 * mediaType.hashCode() + Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "BundledConnectorIcon[mediaType=" + mediaType + ", bytes=" + bytes.length + "]";
    }

    /** The icon media type for its declared JAR entry path. */
    public static String mediaTypeFor(String path) {
        int dot = path.lastIndexOf('.');
        String extension = dot < 0 ? "" : path.substring(dot + 1).toLowerCase(Locale.ROOT);
        return switch (extension) {
            case "png" -> "image/png";
            case "jpg", "jpeg" -> "image/jpeg";
            case "gif" -> "image/gif";
            case "webp" -> "image/webp";
            case "svg" -> "image/svg+xml";
            default -> "application/octet-stream";
        };
    }
}
