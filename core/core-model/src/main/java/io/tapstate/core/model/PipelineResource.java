package io.tapstate.core.model;

import java.util.ArrayList;
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
        @Doc(value = "The sources this pipeline reads from; at least one is required. Each is a bare "
                + "source id, or an object carrying this pipeline's own srs switch for that source.",
                required = true, key = "source")
        @YamlScalarOrList
        List<SourceRef> sources,
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
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("source: must reference at least one source (X17)");
        }
        sources = List.copyOf(sources);
        transforms = transforms == null ? null : List.copyOf(transforms);
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    /**
     * The ids alone, in declaration order. Most callers only need to know which sources this
     * pipeline reads; only the capture path needs the srs switch that rides along with them.
     */
    public List<String> sourceIds() {
        List<String> ids = new ArrayList<>(sources.size());
        for (SourceRef ref : sources) {
            ids.add(ref.id());
        }
        return Collections.unmodifiableList(ids);
    }

    @Override
    public String kind() {
        return "pipeline";
    }
}
