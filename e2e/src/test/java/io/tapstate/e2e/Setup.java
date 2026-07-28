package io.tapstate.e2e;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The provisioning facet, in dependency order: what the harness must bring up, then the three real
 * product verbs run against it.
 *
 * <p>{@code databases} is the harness's own and comes first - a resource cannot be applied before the
 * endpoint whose address it interpolates exists. The other three are the product's: a connector jar must
 * be registered before a source referencing it can be applied, and a source model must be discovered
 * before a target table can be derived from it.
 *
 * <p>Asking for a store here is also what makes the case selectable: the same declaration is what its
 * {@code connectors} and {@code cost} tags are derived from, so an author never writes them twice.
 *
 * @param databases stores the harness provisions before anything else, keyed by the name whose address
 *     the resources interpolate; nothing can be applied until the endpoint it names is up
 * @param connectors connector ids whose runtime jars are registered (content-hash idempotent)
 * @param apply product resource files applied before the pipeline; sources may not be inlined, so a
 *     pipeline can only reference them by id once they exist
 * @param discover resource ids whose source model is discovered, feeding target-table creation
 */
public record Setup(
        Map<String, DatabaseRequest> databases,
        List<String> connectors,
        List<String> apply,
        List<String> discover) {

    public static final Setup NONE = new Setup(Map.of(), List.of(), List.of(), List.of());

    public Setup {
        // Insertion order kept: a specification asking for two stores gets them in the order it wrote,
        // which is the order any failure will name them in.
        databases = Collections.unmodifiableMap(new LinkedHashMap<>(databases));
        connectors = List.copyOf(connectors);
        apply = List.copyOf(apply);
        discover = List.copyOf(discover);
    }
}
