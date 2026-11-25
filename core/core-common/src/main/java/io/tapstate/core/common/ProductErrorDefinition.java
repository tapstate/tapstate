package io.tapstate.core.common;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** Immutable authority shared by typed producers and independent wire clients. */
public record ProductErrorDefinition(ProductErrorModule module, int number, Severity severity,
                                     Set<String> parameters) {
    public ProductErrorDefinition {
        Objects.requireNonNull(module, "module");
        Objects.requireNonNull(severity, "severity");
        if (number < 1 || number > 9999) throw new IllegalArgumentException("invalid diagnostic serial number");
        parameters = Set.copyOf(parameters);
        if (parameters.stream().anyMatch(name -> !name.matches("[a-z][A-Za-z0-9]*")))
            throw new IllegalArgumentException("invalid named diagnostic parameter");
    }

    public String code() { return "TS" + module.number() + String.format(Locale.ROOT, "%04d", number); }
}
