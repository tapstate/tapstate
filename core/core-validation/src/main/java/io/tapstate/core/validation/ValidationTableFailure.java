package io.tapstate.core.validation;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** A safe, bounded diagnostic for one failed mapping in an otherwise usable validation run. */
public record ValidationTableFailure(String mappingId, Side side, String code, String message) {
    public enum Side { SOURCE, TARGET, PAIR }

    public ValidationTableFailure {
        ValidationError.identifier(mappingId, "tableFailure.mapping");
        Objects.requireNonNull(side, "side");
        if (code == null || code.length() > 96 || !code.matches("[a-z][a-z0-9.-]*"))
            throw ValidationError.invalid("tableFailure.code");
        if (message == null || !StandardCharsets.UTF_8.newEncoder().canEncode(message) || message.getBytes(StandardCharsets.UTF_8).length > 256)
            throw ValidationError.invalid("tableFailure.message");
    }
}
