package io.tapstate.core.logging;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Redacts runtime secret values before log text reaches an observable sink. Registrations are
 * grouped by owner so replacing or deleting a Source cannot leave stale values behind.
 */
public final class SecretRedactor {

    private static final String REDACTED = "********";
    private static final List<String> EXISTING_MARKERS = List.of(REDACTED, MongoUriUserInfo.REDACTED);
    private static final int TOKEN_REDACTION_LIMIT = 8;
    private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{N}_]+");
    private static final Pattern WORD_BOUNDARY = Pattern.compile("(?<![\\p{L}\\p{N}_])%s(?![\\p{L}\\p{N}_])");

    private final Map<String, List<String>> valuesByOwner = new LinkedHashMap<>();
    private final AtomicReference<List<String>> snapshot = new AtomicReference<>(List.of());

    /** Replaces every secret associated with {@code owner}. Blank values are ignored. */
    public synchronized void replace(String owner, Collection<String> values) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(values, "values");
        List<String> normalized = values.stream()
                .filter(Objects::nonNull)
                .filter(value -> !value.isBlank())
                .distinct()
                .toList();
        if (normalized.isEmpty()) {
            valuesByOwner.remove(owner);
        } else {
            valuesByOwner.put(owner, normalized);
        }
        rebuildSnapshot();
    }

    /** Removes every secret associated with {@code owner}. */
    public synchronized void remove(String owner) {
        Objects.requireNonNull(owner, "owner");
        valuesByOwner.remove(owner);
        rebuildSnapshot();
    }

    /** Replaces every currently registered value in {@code text} with a fixed marker. */
    public String redact(String text) {
        return redactValues(text, snapshot.get(), REDACTED);
    }

    /** An independent secret set for an already admitted operation, unaffected by later Source edits. */
    public synchronized SecretRedactor fork(Collection<String> owners) {
        Objects.requireNonNull(owners, "owners");
        var retained = new SecretRedactor();
        for (String owner : owners) retained.replace(owner, valuesByOwner.getOrDefault(owner, List.of()));
        return retained;
    }

    /** Also hides connection credential structures at an operational log output boundary. */
    public String redactLog(String text) {
        return redact(MongoLogSanitizer.redact(text));
    }

    /** Redacts supplied values using the same matcher as the process-wide log registry. */
    public static String redactValues(String text, Collection<String> values, String replacement) {
        if (text == null) {
            return null;
        }
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(replacement, "replacement");
        List<String> ordered = values.stream()
                .filter(Objects::nonNull)
                .filter(value -> !value.isBlank())
                .distinct()
                .sorted((left, right) -> Integer.compare(right.length(), left.length()))
                .toList();
        String result = text;
        for (String secret : ordered) {
            result = replaceOutsideMarkers(result, secret, replacement);
        }
        return result;
    }

    private void rebuildSnapshot() {
        snapshot.set(valuesByOwner.values().stream()
                .flatMap(Collection::stream)
                .distinct()
                .sorted((left, right) -> Integer.compare(right.length(), left.length()))
                .toList());
    }

    private static String replaceOutsideMarkers(String text, String secret, String replacement) {
        StringBuilder result = new StringBuilder(text.length());
        int segmentStart = 0;
        int cursor = 0;
        while (cursor < text.length()) {
            String marker = nextMarker(text, cursor);
            if (marker == null) {
                break;
            }
            int markerStart = text.indexOf(marker, cursor);
            result.append(replaceSegment(text.substring(segmentStart, markerStart), secret, replacement));
            result.append(marker);
            cursor = markerStart + marker.length();
            segmentStart = cursor;
        }
        result.append(replaceSegment(text.substring(segmentStart), secret, replacement));
        return result.toString();
    }

    private static String nextMarker(String text, int fromIndex) {
        String next = null;
        int nextIndex = Integer.MAX_VALUE;
        for (String marker : EXISTING_MARKERS) {
            int found = text.indexOf(marker, fromIndex);
            if (found >= 0 && found < nextIndex) {
                next = marker;
                nextIndex = found;
            }
        }
        return next;
    }

    private static String replaceSegment(String text, String secret, String replacement) {
        if (text.isEmpty()) {
            return text;
        }
        int codePoints = secret.codePointCount(0, secret.length());
        if (codePoints <= TOKEN_REDACTION_LIMIT && WORD.matcher(secret).matches()) {
            Pattern token = Pattern.compile(WORD_BOUNDARY.pattern().formatted(Pattern.quote(secret)));
            return token.matcher(text).replaceAll(java.util.regex.Matcher.quoteReplacement(replacement));
        }
        return text.replace(secret, replacement);
    }
}
