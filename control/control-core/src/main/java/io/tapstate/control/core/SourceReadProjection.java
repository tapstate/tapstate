package io.tapstate.control.core;

import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Omits Source connector config and redacts URI credentials elsewhere in its public projection. */
final class SourceReadProjection {

    static final String WITHHELD = "<redacted-source>";
    static final String REDACTED = MongoUriUserInfo.REDACTED;

    private static final Pattern MONGO_URI =
            Pattern.compile("(?i)mongodb(?:\\+srv)?://[^\\s\\\"]+");

    private final CanonicalWriter writer = new CanonicalWriter();

    String canonicalForRead(SourceResource source) {
        Objects.requireNonNull(source, "source");
        try {
            SourceResource display = new SourceResource(
                    source.id(), source.metadata(), source.connector(), Map.of(),
                    source.mode(), source.tables(), source.srs(), source.execution(), source.experimental());
            return redactMongoUserInfo(writer.write(display));
        } catch (RuntimeException unsafeProjection) {
            // A broken stored Source is not permission to return its raw connection configuration.
            return WITHHELD;
        }
    }

    /** Whether a Source carries a display-only marker that must never reach the truth layer. */
    static boolean containsDisplayMarker(SourceResource source) {
        Objects.requireNonNull(source, "source");
        return containsDisplayMarker(source.config())
                || containsRedactedMongoUri(new CanonicalWriter().write(source));
    }

    static boolean containsDisplayMarker(Object value) {
        if (value instanceof String text) {
            return MongoUriUserInfo.REDACTED.equals(text)
                    || MongoUriUserInfo.isRedactedDisplay(text);
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

    private static String redactMongoUserInfo(String canonical) {
        Matcher matcher = MONGO_URI.matcher(canonical);
        StringBuilder result = new StringBuilder(canonical.length());
        boolean changed = false;
        while (matcher.find()) {
            String uri = matcher.group();
            String safe = MongoUriUserInfo.redact(uri);
            changed |= !safe.equals(uri);
            matcher.appendReplacement(result, Matcher.quoteReplacement(safe));
        }
        matcher.appendTail(result);
        return changed ? result.toString() : canonical;
    }

    private static boolean containsRedactedMongoUri(String canonical) {
        Matcher matcher = MONGO_URI.matcher(canonical);
        while (matcher.find()) {
            if (MongoUriUserInfo.isRedactedDisplay(matcher.group())) {
                return true;
            }
        }
        return false;
    }
    public static String redactUserInfo(String uri) {
        Objects.requireNonNull(uri, "uri");
        return MongoUriUserInfo.redact(uri);
    }

    static boolean isRedactedUri(String uri) {
        Objects.requireNonNull(uri, "uri");
        return MongoUriUserInfo.isRedactedDisplay(uri);
    }

}
