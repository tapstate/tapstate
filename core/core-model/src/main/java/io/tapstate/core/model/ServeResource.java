package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code kind: serve} — reusable publish-surface definition body (§8, X19):
 * sync / query / push declarations without {@code from:} wiring.
 */
@Doc("Reusable publish-surface definition holding sync, query and push declarations without source wiring.")
public record ServeResource(
        @Doc(value = "Unique resource id across the workspace; must not contain a dot.", required = true)
        String id,
        @Doc("Optional labels and free-text description.")
        Metadata metadata,
        @Doc("Sync publish declarations exposed by this serve surface.")
        List<SyncElement> sync,
        @Doc("Query publish declarations exposed by this serve surface.")
        List<QueryElement> query,
        @Doc("Push publish declarations exposed by this serve surface.")
        List<PushElement> push,
        @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
        Map<String, Object> experimental) implements Resource {

    public ServeResource {
        Objects.requireNonNull(id, "id");
        sync = sync == null ? null : List.copyOf(sync);
        query = query == null ? null : List.copyOf(query);
        push = push == null ? null : List.copyOf(push);
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    @Override
    public String kind() {
        return "serve";
    }
}
