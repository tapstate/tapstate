package io.tapstate.core.validation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** The chosen method and actual immutable comparison scope for one new run, never a latest-task lookup. */
public record ValidationRunSpec(ValidationTaskSpec task, ValidationMode mode, ValidationMethod method) {
    public ValidationRunSpec { Objects.requireNonNull(task); task.requireFor(method, mode); }
    public String fingerprint() { return fingerprint(task.fingerprint(), mode, method); }
    public static String fingerprint(String comparisonFingerprint, ValidationMode mode, ValidationMethod method) {
        Objects.requireNonNull(mode); Objects.requireNonNull(method);
        if (comparisonFingerprint == null || !comparisonFingerprint.matches("[a-f0-9]{64}") || !method.supports(mode))
            throw ValidationError.invalid("runSpec.fingerprint");
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update(("validation-run-spec-v1\n" + mode.name() + "\n" + method.name() + "\n" + comparisonFingerprint).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 is required by the Java platform", impossible); }
    }
}
