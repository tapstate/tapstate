package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One element of {@code source.tables} (ADR-0016 §4, X9/X12): bare name (literal, frozen
 * link), {@code /…/} regex (dynamic link), or an object carrying per-table configuration.
 */
@Doc("A table selected from the source: a bare name, a regex pattern, or an object with per-table configuration.")
public sealed interface TableRef {

    static Literal literal(String name) {
        return new Literal(name);
    }

    static Regex regex(String pattern) {
        return new Regex(pattern);
    }

    static Spec spec(String name, String filter, List<String> pk) {
        return new Spec(name, filter, pk);
    }

    /** Bare table name — literal match, frozen link. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Literal(String name) implements TableRef {
        public Literal {
            Objects.requireNonNull(name, "name");
        }
    }

    /** {@code /…/} regex (full match) — dynamic: new matching tables join the universe. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Regex(String pattern) implements TableRef {
        public Regex {
            Objects.requireNonNull(pattern, "pattern");
        }
    }

    /**
     * Object form: literal name plus per-table configuration. {@code filter} is a CEL row
     * expression (§12); {@code options} is the connector-owned per-table extension point (X12).
     */
    @Doc("A table selected by literal name with optional per-table configuration.")
    record Spec(
            @Doc(value = "Literal name of the table to select from the source.", required = true)
            String name,
            @Doc("CEL row expression that filters which rows of the table are included.")
            String filter,
            @Doc("Primary key column names used to identify rows when the source does not declare one.")
            List<String> pk)
            implements TableRef {
        public Spec {
            Objects.requireNonNull(name, "name");
            pk = pk == null ? null : List.copyOf(pk);
        }
    }
}
