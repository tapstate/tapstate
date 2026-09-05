package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@code kind: view} — reusable MDM sink definition body (ADR-0016 §7, X19): pure
 * declaration of where/how to materialize; no {@code from:} (wiring belongs to the pipeline).
 */
@Doc("A reusable view: declares where and how to materialize data, without any inbound wiring.")
public record ViewResource(
        @Doc(value = "Unique resource id across the workspace; must not contain a dot.", required = true)
        String id,
        @Doc("Optional labels and free-text description.")
        Metadata metadata,
        @Doc(value = "Name of the column used as the view's primary key.", required = true)
        String primaryKey,
        @Doc("Where and how the view's data is materialized.")
        Storage storage,
        @Doc("Column definitions of the view's output schema.")
        ViewSchema schema,
        @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
        Map<String, Object> experimental)
        implements Resource {

    public ViewResource {
        Objects.requireNonNull(id, "id");
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    @Override
    public String kind() {
        return "view";
    }
}
