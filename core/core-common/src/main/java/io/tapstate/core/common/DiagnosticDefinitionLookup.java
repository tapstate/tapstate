package io.tapstate.core.common;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Resolves one diagnostic in the catalog belonging to its already established source. */
@FunctionalInterface
public interface DiagnosticDefinitionLookup {
    record Resolved(String code, String message, String solution) {}

    Optional<Resolved> resolve(String code, Map<String, Object> named, List<String> positional, Locale language);
}
