package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code kind: source} — independent collection/mining resource owning connection config
 * and (for CDC) the SRS (ADR-0016 §3/§4). Dual role (X18): when referenced purely as a
 * connection supplier by sync/push elements, {@code mode}/{@code tables} may be absent;
 * conditional requiredness is a validate-layer rule.
 */
@Doc("kind: source — an independent collection/mining resource owning connection config and, "
        + "for CDC, the Shared Record Store.")
public record SourceResource(
        @Doc(value = "Unique resource id across the workspace; must not contain a dot.",
                required = true)
        String id,
        @Doc("Optional labels and free-text description.")
        Metadata metadata,
        @Doc(value = "Id of the connector this source reads through (e.g. mysql, kafka).",
                required = true)
        String connector,
        @Doc("Connector connection config; keys are connector-specific.")
        Map<String, Object> config,
        @Doc("Read mode; may be omitted when the source is only a connection supplier.")
        SourceMode mode,
        @Doc("Tables to read: bare names, /regex/ patterns, or per-table objects.")
        List<TableRef> tables,
        @Doc("Connector-specific source options; the read mode and start position live in pipeline settings.")
        Map<String, Object> options,
        @Doc("Shared Record Store configuration; only valid on cdc sources.")
        Srs srs,
        @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
        Map<String, Object> experimental) implements Resource {

    public SourceResource {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(connector, "connector");
        config = config == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(config));
        tables = tables == null ? null : List.copyOf(tables);
        options = options == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(options));
        experimental = experimental == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<>(experimental));
    }

    @Override
    public String kind() {
        return "source";
    }

    /**
     * Whether this source buffers its changes through the shared replay store.
     *
     * <p>On unless the source says otherwise: a source with no {@code srs} block, or one whose block
     * leaves {@code enabled} unset, is buffered. Only an explicit {@code enabled: false} turns it off.
     *
     * <p>It lives on the resource because more than one layer asks -- the capture side to decide how to
     * run, the control side to decide whether the answer may change right now -- and two readings of a
     * default are two things that can disagree about a source nobody wrote the field on.
     */
    public boolean srsEnabled() {
        return srs == null || srs.enabled() == null || srs.enabled();
    }
}
