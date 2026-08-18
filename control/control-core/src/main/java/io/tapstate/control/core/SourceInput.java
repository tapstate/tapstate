package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;

import java.util.List;
import java.util.Map;

/** Structured JSON input for the persistent Source projection. */
public record SourceInput(
        String id,
        Metadata metadata,
        String connector,
        Map<String, Object> config,
        String mode,
        List<SourceTableDraft> tables,
        Map<String, Object> options,
        SourceDraft.SourceSrs srs,
        Map<String, Object> experimental,
        List<String> clearSecrets) {

    public SourceInput {
        // SourceDraft owns the defensive JSON copies shared by the draft and persistent input faces.
        new SourceDraft(id, metadata, connector, config, mode, tables, options, srs, experimental, clearSecrets);
        config = SourceDraft.copyJsonMap(config, false);
        tables = tables == null ? null : List.copyOf(tables);
        options = SourceDraft.copyJsonMap(options, true);
        experimental = SourceDraft.copyJsonMap(experimental, true);
        clearSecrets = clearSecrets == null ? List.of() : List.copyOf(clearSecrets);
    }

    @Override
    public String toString() {
        return "SourceInput[id=" + id
                + ", connector=" + connector
                + ", configKeys=" + config.keySet()
                + ", clearSecrets=" + clearSecrets
                + "]";
    }

    SourceDraft asDraft() {
        return new SourceDraft(id, metadata, connector, config, mode, tables, options, srs, experimental, clearSecrets);
    }
}
