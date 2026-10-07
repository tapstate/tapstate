package io.tapstate.control.core;

import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Makes generic Source reads safe to display without modifying the authoritative resource. */
final class SourceReadProjection {

    static final String REDACTED = "<redacted>";

    private static final Pattern MONGO_URI =
            Pattern.compile("(?i)mongodb(?:\\+srv)?://[^\\s\\\"]+");

    private final CanonicalWriter writer = new CanonicalWriter();

    String canonicalForRead(SourceResource source) {
        return canonicalForRead(writer.write(Objects.requireNonNull(source, "source")));
    }

    String canonicalForRead(String canonical) {
        Objects.requireNonNull(canonical, "canonical");
        Matcher matcher = MONGO_URI.matcher(canonical);
        StringBuilder result = new StringBuilder(canonical.length());
        boolean changed = false;
        while (matcher.find()) {
            String uri = matcher.group();
            String safe = redactUserInfo(uri);
            changed |= !safe.equals(uri);
            matcher.appendReplacement(result, Matcher.quoteReplacement(safe));
        }
        matcher.appendTail(result);
        return changed ? result.toString() : canonical;
    }

    /**
     * Whether a Source contains a display-only value that must never return to the truth layer.
     * Config also has standalone secret markers; Mongo URI markers are sought in the complete
     * canonical form because {@link #canonicalForRead(String)} projects that same complete form.
     */
    static boolean containsDisplayMarker(SourceResource source) {
        Objects.requireNonNull(source, "source");
        return containsDisplayMarker(source.config())
                || containsRedactedMongoUri(new CanonicalWriter().write(source));
    }

    static boolean containsDisplayMarker(Object value) {
        if (value instanceof String text) {
            return REDACTED.equals(text) || isRedactedUri(text);
        }
        if (value instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(SourceReadProjection::containsDisplayMarker);
        }
        if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                if (containsDisplayMarker(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean containsRedactedMongoUri(String canonical) {
        Matcher matcher = MONGO_URI.matcher(canonical);
        while (matcher.find()) {
            if (isRedactedUri(matcher.group())) {
                return true;
            }
        }
        return false;
    }

    public static String redactUserInfo(String uri) {
        Objects.requireNonNull(uri, "uri");
        UserInfo userInfo = findUserInfo(uri);
        if (userInfo == null) {
            return uri;
        }
        return uri.substring(0, userInfo.start()) + REDACTED + uri.substring(userInfo.end());
    }

    static boolean isRedactedUri(String uri) {
        Objects.requireNonNull(uri, "uri");
        if (REDACTED.equals(uri)) {
            return true;
        }
        UserInfo userInfo = findUserInfo(uri);
        return userInfo != null
                && uri.substring(userInfo.start(), userInfo.end()).equals(REDACTED);
    }

    private static UserInfo findUserInfo(String uri) {
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
