package io.tapstate.core.logging;

import java.util.regex.Pattern;

/** Hides Mongo connection credentials even before any Source secrets have been registered. */
final class MongoLogSanitizer {

    private static final String MONGO_CREDENTIAL = "MongoCredential{";
    private static final String MONGO_CREDENTIAL_END = "mechanismProperties=<hidden>}";
    private static final String PROXY_SETTINGS = "ProxySettings{";
    private static final Pattern REPORT_URI_USERINFO = Pattern.compile(
            "(mongodb(?:\\+srv)?://)(?:[^\\s/?#]*"
                    + "|[^\\s:/?#@\\[\\],]++:(?![0-9]+[\\s,;.!)])[^\\s/?#@]*+\\s[^/?#@]*+"
                    + "|[^\\s/?#@]*+\\s[^/?#@]*+(?=@[^\\s/?#@]++[/?#]))@",
            Pattern.UNICODE_CHARACTER_CLASS);

    private MongoLogSanitizer() {
    }

    static String redact(String text) {
        if (text == null) {
            return null;
        }
        String safe = hideBlock(text, MONGO_CREDENTIAL, MONGO_CREDENTIAL_END, false);
        safe = hideBlock(safe, PROXY_SETTINGS, "}", false);
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
        String safe = hideBlock(text, MONGO_CREDENTIAL, MONGO_CREDENTIAL_END, true);
        int proxy = safe.indexOf(PROXY_SETTINGS);
        if (proxy >= 0 && (safe.indexOf("username=", proxy) >= 0 || safe.indexOf("password=", proxy) >= 0)) {
            // ProxySettings prints the password without escaping. A trailing brace can be
            // part of that value, so a report cannot reliably restore any text after it.
            safe = safe.substring(0, proxy) + "ProxySettings" + MongoUriUserInfo.REDACTED;
        }
        // Extend past whitespace if the first token looks like userinfo, or an
        // explicit path, query or fragment confirms the following URI authority.
        return REPORT_URI_USERINFO.matcher(safe).replaceAll("$1" + MongoUriUserInfo.REDACTED + "@");
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

    private static String hideBlock(String text, String startMarker, String endMarker, boolean coverLastDump) {
        int start = text.indexOf(startMarker);
        if (start < 0) {
            return text;
        }
        // Driver values can contain braces and quotes. Prefer the last terminator to interpreting
        // credential bytes as delimiters; an incomplete dump hides the rest rather than leaking it.
        int end = text.lastIndexOf(endMarker);
        int lastStart = coverLastDump ? text.lastIndexOf(startMarker) : start;
        String suffix = end >= lastStart + startMarker.length() ? text.substring(end + endMarker.length()) : "";
        return text.substring(0, start) + startMarker.substring(0, startMarker.length() - 1)
                + MongoUriUserInfo.REDACTED + suffix;
    }
}
