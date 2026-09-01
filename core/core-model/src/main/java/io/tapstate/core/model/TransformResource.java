package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@code kind: transform} — reusable definition body (ADR-0016 §1/§5, X19): pure logic,
 * {@code from:} is forbidden (wiring belongs to the referencing pipeline step).
 */
@Doc("A reusable transform definition holding pure logic that pipeline steps can reference; it cannot declare its own input wiring.")
public record TransformResource(
        @Doc(value = "Unique resource id across the workspace; must not contain a dot.", required = true)
        String id,
        @Doc("Optional labels and free-text description.")
        Metadata metadata,
        @YamlFlatten TransformBody body,
        @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
        Map<String, Object> experimental)
        implements Resource {

    public TransformResource {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(body, "body");
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    @Override
    public String kind() {
        return "transform";
    }
}
