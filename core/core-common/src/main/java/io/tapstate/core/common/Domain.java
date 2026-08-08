package io.tapstate.core.common;

import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * The authoritative registry of first-party error-code domains (ADR-0024 D2). The {@code <domain>}
 * segment of every canonical code must be one of these — the build-time format gate (ADR-0024 D5-2)
 * rejects any code whose domain is unregistered. This closes the legacy class of bug where a typo
 * (e.g. {@code dls.} for {@code dsl.}) silently minted a brand-new namespace.
 *
 * <p>An enum (not a free-text file) keeps the registry native-clean and compile-checked: zero
 * runtime reflection, zero I/O. Adding a domain = adding a constant here + review.
 */
public enum Domain {
    DSL,
    CLI,
    CORE,
    CATALOG,
    SCHEMA,
    // core ring: pipeline lifecycle state machine (illegal transitions)
    LIFECYCLE,
    // service assembly root: role selection and startup-fatal failures (app)
    ROLE,
    BOOT,
    // service assembly root: resolving a pipeline's runnable topology from its stored artifact at
    // actuation time -- a missing artifact, or one that is not a pipeline (app)
    ACTUATION,
    // storage connectivity: reaching the backing store and its replica-set requirement (adapters)
    STORE,
    // pdk bridge: loading, level-gating, driving and projecting a connector (adapters)
    CONNECTOR,
    // stateless row transforms: evaluating an author's CEL expression or js script against an event
    // (adapters)
    TRANSFORM,
    // storage data-plane: operating on the backing store at runtime and reading its stored documents
    // back — distinct from STORE, which polices reaching the store at startup (adapters)
    IO,
    // control layer: the resource-type-agnostic verb layer (apply / audit / auth); diagnosable
    // failures such as an operation refused because its mandatory audit record could not be written
    CONTROL,
    // runtime execution: driving the Jet job that runs a pipeline (submit / suspend / resume /
    // cancel), and the machinery the running job itself depends on — carrying a position through the
    // engine, propagating a frontier; diagnosable failures such as acting on a pipeline that has no
    // running job, or a position the frontier's encoding cannot represent (runtime)
    ENGINE,
    // stateful assembly of nested documents: checking a nest tree, sizing its state, and the runtime
    // limits and unresolvable references that stop or dead-letter part of it (runtime)
    NEST,
    // observation read faces: reading a pipeline's store-backed status / metrics / snapshot;
    // diagnosable failures such as reading a pipeline that has published no observation (control)
    MONITOR,
    // source-specific control operations: identity, optimistic concurrency and reference protection
    SOURCE,
    // local MCP presentation: sidecar input, connector-spec and upstream-response failures
    MCP,
    // runtime data plane: reading a source's snapshot / cdc into the replay store — diagnosable
    // capture-configuration faults such as an unparsable consumption start point (runtime)
    CAPTURE;


    /** The lower-case identifier used as the {@code <domain>} segment of a canonical code. */
    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Whether {@code domain} is a registered domain id (exact, case-sensitive). */
    public static boolean isRegistered(String domain) {
        for (Domain d : values()) {
            if (d.id().equals(domain)) {
                return true;
            }
        }
        return false;
    }

    /** All registered domain ids. */
    public static Set<String> ids() {
        Set<String> ids = new TreeSet<>();
        for (Domain d : values()) {
            ids.add(d.id());
        }
        return ids;
    }
}
