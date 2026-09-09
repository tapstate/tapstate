package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Push egress payload shape (§8, X11). Absent = the canonical tapstate envelope
 * (§6); custom = whole-payload CEL projection or per-field object form.
 */
@Doc("Shape of the egress payload pushed to the target; absent means the default tapstate envelope.")
public sealed interface PushFormat {

    static Cel cel(String expr) {
        return new Cel(expr);
    }

    static Fields fields(Map<String, FieldRule> fields) {
        return new Fields(fields);
    }

    /** {@code format: "=<CEL>"} — whole-payload projection; stored without the marker. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Cel(String expr) implements PushFormat {
        public Cel {
            Objects.requireNonNull(expr, "expr");
        }
    }

    /** {@code format: { field: rule, … }} — per-field rules; declared order is semantic. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Fields(Map<String, FieldRule> fields) implements PushFormat {
        public Fields {
            Objects.requireNonNull(fields, "fields");
            fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        }
    }
}
