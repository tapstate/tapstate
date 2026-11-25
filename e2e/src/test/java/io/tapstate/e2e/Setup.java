package io.tapstate.e2e;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The provisioning facet, in dependency order: what the harness must bring up, then the three real
 * product verbs run against it.
 *
 * <p>{@code databases} and {@code managedTarget} are installation inputs supplied by the harness before
 * product operations. An authored resource cannot be applied before its endpoint exists. The other
 * three are the product's: a connector jar must
 * be registered before a source referencing it can be applied, and a source model must be discovered
 * before a target table can be derived from it.
 *
 * <p>Asking for a store here is also what makes the case selectable: the same declaration is what its
 * {@code connectors} and {@code cost} tags are derived from, so an author never writes them twice.
 *
 * @param databases stores the harness provisions before anything else, keyed by the name whose address
 *     the resources interpolate; nothing can be applied until the endpoint it names is up
 * @param connectors connector ids whose runtime jars are registered (content-hash idempotent)
 * @param managedTarget optional Source file used as installation input, read for endpoint assertions but never artifact-applied
 * @param apply product resource files applied with the pipeline as one batch; sources may not be
 *     inlined, so a pipeline can only reference them by id, and ids resolve within the batch they are
 *     submitted in. The pipeline the envelope names is in that batch whether or not it is listed here
 * @param discover resource ids whose source model is discovered, feeding target-table creation
 */
public record Setup(
        Map<String, DatabaseRequest> databases,
        String managedTarget,
        List<String> connectors,
        List<String> apply,
        List<String> discover) {

    public static final Setup NONE = new Setup(Map.of(), null, List.of(), List.of(), List.of());

    public Setup {
        // Insertion order kept: a specification asking for two stores gets them in the order it wrote,
        // which is the order any failure will name them in.
        databases = Collections.unmodifiableMap(new LinkedHashMap<>(databases));
        connectors = List.copyOf(connectors);
        apply = List.copyOf(apply);
        discover = List.copyOf(discover);
        if (managedTarget != null && (managedTarget.isBlank() || apply.contains(managedTarget)))
            throw new EnvelopeException("setup.managed_target must name a separate installation input, not an apply resource");
    }
}
