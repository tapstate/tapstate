package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code kind: pipeline} — the composing runnable unit (ADR-0016 §1, X17): references
 * pre-created sources by id (never inline), wires transforms / view / serve, carries
 * task-level settings. Minimal composition (source + view/serve) is a validate-layer rule.
 */
@Doc("Composing runnable unit that references pre-created sources by id and wires transforms, view and serve into a task.")
public record PipelineResource(
        @Doc(value = "Unique resource id across the workspace; must not contain a dot.", required = true)
        String id,
        @Doc("Optional labels and free-text description.")
        Metadata metadata,
        @Doc(value = "Ids of pre-created sources this pipeline reads from; blank drafts may omit them.",
                required = true, key = "source")
        @YamlScalarOrList
        List<String> sources,
        @Doc("Ordered transform steps applied to the source data.")
        List<Step> transforms,
        @Doc("View configuration that shapes the pipeline output into a queryable result.")
        ViewBlock view,
        @Doc("Serve configuration that exposes the pipeline output downstream.")
        ServeBlock serve,
        @Doc("Task-level settings for this pipeline.")
        Settings settings,
        @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
        Map<String, Object> experimental)
        implements Resource {

    public PipelineResource {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(sources, "sources");
        if (sources.isEmpty() && (transforms != null && !transforms.isEmpty() || view != null || serve != null)) {
            throw new IllegalArgumentException("source: must reference at least one source (X17)");
        }
        sources = List.copyOf(sources);
        transforms = transforms == null ? null : List.copyOf(transforms);
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    @Override
    public String kind() {
        return "pipeline";
    }
}
