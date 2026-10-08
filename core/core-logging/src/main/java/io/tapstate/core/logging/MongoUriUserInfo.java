package io.tapstate.core.logging;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Keeps Mongo connection locations visible while hiding credentials in URI userinfo. */
public final class MongoUriUserInfo {

    public static final String REDACTED = "<redacted>";

    private MongoUriUserInfo() {
    }

    /** Replaces only the URI authority's userinfo, or hides an unparseable value carrying an at-sign. */
    public static String redact(String uri) {
        Objects.requireNonNull(uri, "uri");
        UserInfo userInfo = find(uri);
        if (userInfo == null) {
            return uri.indexOf('@') >= 0 ? REDACTED : uri;
        }
        return uri.substring(0, userInfo.start()) + REDACTED + uri.substring(userInfo.end());
    }

    /** Whether a request is carrying the display marker in place of connection credentials. */
    public static boolean isRedactedDisplay(String uri) {
        Objects.requireNonNull(uri, "uri");
        if (REDACTED.equals(uri)) {
            return true;
        }
        UserInfo userInfo = find(uri);
        return userInfo != null
                && uri.substring(userInfo.start(), userInfo.end()).equals(REDACTED);
    }

    /** Raw userinfo plus password spellings that a connector or driver could put in a log line. */
    public static List<String> secretValues(String uri) {
        Objects.requireNonNull(uri, "uri");
        UserInfo userInfo = find(uri);
        if (userInfo == null) {
            return uri.indexOf('@') >= 0 ? List.of(uri) : List.of();
        }
        String raw = uri.substring(userInfo.start(), userInfo.end());
        if (raw.isBlank()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        values.add(raw);
        int separator = raw.indexOf(':');
        if (separator >= 0 && separator + 1 < raw.length()) {
            String password = raw.substring(separator + 1);
            values.add(password);
            try {
                String decoded = URLDecoder.decode(password.replace("+", "%2B"), StandardCharsets.UTF_8);
                if (!decoded.equals(password)) {
                    values.add(decoded);
                }
            } catch (IllegalArgumentException invalidEncoding) {
                // The raw spelling is still registered even if percent escapes are malformed.
            }
        }
        return List.copyOf(values);
    }

    private static UserInfo find(String uri) {
        int scheme = uri.indexOf("://");
        if (scheme < 1) {
            return null;
        }
        int start = scheme + 3;
        int end = uri.length();
        for (char terminator : new char[] {'/', '?', '#'}) {
            int found = uri.indexOf(terminator, start);
            if (found >= 0 && found < end) {
                end = found;
            }
        }
        int at = uri.lastIndexOf('@', end - 1);
        return at >= start ? new UserInfo(start, at) : null;
    }

    private record UserInfo(int start, int end) {
    }
}
