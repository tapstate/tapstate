package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code from:} wiring of a transform step (§5): list form for streaming
 * steps (multi-item = per-table group), alias-map form for nest / join named upstreams.
 */
@Doc("The upstream wiring of a transform step: either a list of references for streaming steps, or a map of named upstreams for nest and join.")
public sealed interface FromClause {

    static Flow list(FromRef... refs) {
        return new Flow(List.of(refs));
    }

    static Aliases aliases(Map<String, FromRef> aliases) {
        return new Aliases(aliases);
    }

    /** {@code from: [ref, …]} — a list of upstream references. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Flow(List<FromRef> refs) implements FromClause {
        public Flow {
            refs = List.copyOf(refs);
            if (refs.isEmpty()) {
                throw new IllegalArgumentException("from: must reference at least one upstream");
            }
        }
    }

    /** {@code from: { alias: ref, … }} — named upstreams for nest / join. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Aliases(Map<String, FromRef> aliases) implements FromClause {
        public Aliases {
            if (aliases.isEmpty()) {
                throw new IllegalArgumentException("from: alias map must not be empty");
            }
            aliases = Collections.unmodifiableMap(new LinkedHashMap<>(aliases));
        }
    }
}
