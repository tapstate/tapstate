package io.tapstate.core.lifecycle;

/** Identifies connector positions stored in the legacy Base64 Java stream format. */
public final class LegacyPositionEncoding {

    private LegacyPositionEncoding() {
    }

    public static boolean hasJavaStreamHeader(String position) {
        if (position == null || position.length() < 8 || !position.startsWith("rO0AB")) {
            return false;
        }
        // The sixth Base64 digit completes AC ED 00 05. Its low four bits belong to the next byte.
        char digit = position.charAt(5);
        return (digit >= 'Q' && digit <= 'Z') || (digit >= 'a' && digit <= 'f');
    }
}
