package io.tapstate.control.core;

import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.Map;

/** Public Source reads omit the complete connector configuration without changing the stored resource. */
final class SourceReadProjection {

    static final String WITHHELD = "<redacted-source>";

    private final CanonicalWriter writer = new CanonicalWriter();

    String canonicalForRead(SourceResource source) {
        try {
            SourceResource display = new SourceResource(
                    source.id(), source.metadata(), source.connector(), Map.of(),
                    source.mode(), source.tables(), source.srs(), source.experimental());
            return writer.write(display);
        } catch (RuntimeException unsafeProjection) {
            // A broken stored Source is not permission to return its raw connection configuration.
            return WITHHELD;
        }
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
}
