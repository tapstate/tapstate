package io.tapstate.core.logging;

/** Hides Mongo connection credentials even before any Source secrets have been registered. */
final class MongoLogSanitizer {

    private MongoLogSanitizer() {
    }

    static String redact(String text) {
        if (text == null) {
            return null;
        }
        String safe = hideBlock(text, "MongoCredential{", "mechanismProperties=<hidden>}");
        safe = hideBlock(safe, "ProxySettings{", "}");
        StringBuilder result = new StringBuilder();
        int cursor = 0;
        int start = nextUri(safe, cursor);
        while (start >= 0) {
            result.append(safe, cursor, start);
            int next = nextUri(safe, start + 1);
            int end = next < 0 ? safe.length() : next;
            // The driver accepts raw quotes and whitespace in userinfo. Only another scheme can
            // safely split this diagnostic span; the URI helper identifies its authority boundary.
            result.append(MongoUriUserInfo.redact(safe.substring(start, end)));
            cursor = end;
            start = next;
        }
        result.append(safe, cursor, safe.length());
        return result.toString();
    }

    private static int nextUri(String text, int from) {
        int plain = text.indexOf("mongodb://", from);
        int srv = text.indexOf("mongodb+srv://", from);
        return plain < 0 ? srv : srv < 0 ? plain : Math.min(plain, srv);
    }

    private static String hideBlock(String text, String startMarker, String endMarker) {
        int start = text.indexOf(startMarker);
        if (start < 0) {
            return text;
        }
        // Driver values can contain braces and quotes. Prefer the last terminator to interpreting
        // credential bytes as delimiters; an incomplete dump hides the rest rather than leaking it.
        int end = text.lastIndexOf(endMarker);
        String suffix = end >= start + startMarker.length() ? text.substring(end + endMarker.length()) : "";
        return text.substring(0, start) + startMarker.substring(0, startMarker.length() - 1)
                + MongoUriUserInfo.REDACTED + suffix;
    }
}
