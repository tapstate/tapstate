package io.tapstate.core.logging;

import java.util.regex.Pattern;

/** Hides Mongo connection credentials even before any Source secrets have been registered. */
final class MongoLogSanitizer {

    private static final Pattern REPORT_URI_USERINFO = Pattern.compile(
            "(mongodb(?:\\+srv)?://)[^\\s/?#]*@", Pattern.UNICODE_CHARACTER_CLASS);

    private MongoLogSanitizer() {
    }

    static String redact(String text) {
        if (text == null) {
            return null;
        }
        String safe = redactCredentialDump(text);
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
            result.append(MongoUriUserInfo.redactAuthority(safe.substring(start, end)));
            cursor = end;
            start = next;
        }
        result.append(safe, cursor, safe.length());
        return result.toString();
    }

    static String redactReport(String text) {
        if (text == null) {
            return null;
        }
        String safe = redactCredentialDump(text);
        int proxy = safe.indexOf("ProxySettings{");
        if (proxy >= 0 && (safe.indexOf("username=", proxy) >= 0 || safe.indexOf("password=", proxy) >= 0)) {
            safe = hideBlock(safe, "ProxySettings{", "}");
        }
        // Do not extend a host-only endpoint into prose containing an unrelated at-sign.
        return REPORT_URI_USERINFO.matcher(safe).replaceAll("$1" + MongoUriUserInfo.REDACTED + "@");
    }

    private static String redactCredentialDump(String text) {
        return text == null ? null : hideBlock(text, "MongoCredential{", "mechanismProperties=<hidden>}");
    }

    private static int nextUri(String text, int from) {
        int plain = text.indexOf("mongodb://", from);
        int srv = text.indexOf("mongodb+srv://", from);
        if (plain < 0) {
            return srv;
        }
        if (srv < 0) {
            return plain;
        }
        return Math.min(plain, srv);
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
