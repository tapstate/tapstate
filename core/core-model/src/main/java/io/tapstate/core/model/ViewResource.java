package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@code kind: view} — reusable MDM sink definition body (§7, X19): pure
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
        @Doc(value = "How rows are written to the view — for example upsert or append.", def = "upsert")
        WriteMode writeMode,
        @Doc(value = "Treatment of existing view rows before a new full load; resume, recovery and CDC-only never clear rows.",
                def = "append")
        OnFullLoad onFullLoad,
        @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
        Map<String, Object> experimental)
        implements Resource {

    public ViewResource {
        Objects.requireNonNull(id, "id");
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    /** A view definition written with the default write settings: upsert, and keep rows on a full load. */
    public ViewResource(String id, Metadata metadata, String primaryKey, Storage storage,
            Map<String, Object> experimental) {
        this(id, metadata, primaryKey, storage, null, null, experimental);
    }

    @Override
    public String kind() {
        return "view";
    }
}
