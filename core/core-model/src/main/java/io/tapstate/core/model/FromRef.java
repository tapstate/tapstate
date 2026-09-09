package io.tapstate.core.model;

import java.util.Objects;

/**
 * One upstream reference in the {@code from:} addressing system (§4/§5/§8, X9):
 * a literal token (step id / table name / {@code source_id.table} disambiguation — frozen
 * link) or a {@code /…/} regex (dynamic link). Resolution against the declared source
 * universe is a validate-layer concern; the model stores the token.
 */
@Doc("An upstream reference: either a literal token (step id, table name, or source_id.table) or a /…/ regex matching multiple upstreams.")
public sealed interface FromRef {

    static Literal literal(String ref) {
        return new Literal(ref);
    }

    static Regex regex(String pattern) {
        return new Regex(pattern);
    }

    @YamlForm(YamlForm.Form.UNWRAP)
    record Literal(String ref) implements FromRef {
        public Literal {
            Objects.requireNonNull(ref, "ref");
        }
    }

    @YamlForm(YamlForm.Form.UNWRAP)
    record Regex(String pattern) implements FromRef {
        public Regex {
            Objects.requireNonNull(pattern, "pattern");
        }
    }
}
