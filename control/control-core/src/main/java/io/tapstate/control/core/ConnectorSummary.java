package io.tapstate.control.core;

import java.util.List;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.model.SourceMode;

/**
 * A connector as the online catalog lists it: identity, organising group, the source modes it may be
 * paired with, its discovery axis, whether it can be a sink or a push target, and whether it came from
 * the bundled snapshot or was registered at runtime. A control-ring projection of a
 * {@link ConnectorCatalogEntry} — a face renders this, never the store type.
 */
public record ConnectorSummary(
        String id,
        String name,
        String icon,
        String group,
        List<String> modes,
        String discovery,
        boolean sink,
        boolean pushOut,
        String origin) {

    /** Projects a catalog row to a summary, tagging where it came from ({@code bundled} or {@code registered}). */
    public static ConnectorSummary of(ConnectorCatalogEntry entry, String origin) {
        return new ConnectorSummary(
                entry.id(),
                entry.displayName(),
                entry.icon(),
                entry.group().yaml(),
                entry.modes().stream().map(SourceMode::yaml).toList(),
                entry.discovery().yaml(),
                entry.sink().capable(),
                entry.pushOut(),
                origin);
    }
}
