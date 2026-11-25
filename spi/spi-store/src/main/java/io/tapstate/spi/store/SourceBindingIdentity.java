package io.tapstate.spi.store;

import java.util.Objects;
import java.util.UUID;

/** Captured original Source lifetime and execution binding; it is not an authorization credential. */
public record SourceBindingIdentity(String sourceId, String incarnation, long bindingRevision) {
    public SourceBindingIdentity {
        Objects.requireNonNull(sourceId, "sourceId"); Objects.requireNonNull(incarnation, "incarnation");
        if (sourceId.isBlank()
                || sourceId.codePoints().anyMatch(Character::isISOControl)
                || !java.nio.charset.StandardCharsets.UTF_8.newEncoder().canEncode(sourceId)) {
            throw new IllegalArgumentException("Source identity requires a valid original id");
        }
        UUID uuid = UUID.fromString(incarnation);
        if (incarnation.length() != 36 || !uuid.toString().equals(incarnation) || uuid.version() != 4 || uuid.variant() != 2
                || bindingRevision < 1) {
            throw new IllegalArgumentException("Source identity requires a canonical creation UUID and positive binding revision");
        }
    }
}
