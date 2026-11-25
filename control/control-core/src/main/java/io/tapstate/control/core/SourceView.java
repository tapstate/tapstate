package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Secret-redacted structured Source returned by the control layer. */
public record SourceView(
        String id,
        Metadata metadata,
        String connector,
        Map<String, Object> config,
        List<String> configuredSecrets,
        String mode,
        List<SourceTableView> tables,
        Map<String, Object> options,
        SourceDraft.SourceSrs srs,
        Map<String, Object> execution,
        Map<String, Object> experimental,
        String contentHash) {

    public SourceView(String id, Metadata metadata, String connector, Map<String, Object> config,
            List<String> configuredSecrets, String mode, List<SourceTableView> tables, Map<String, Object> options,
            SourceDraft.SourceSrs srs, Map<String, Object> experimental, String contentHash) {
        this(id, metadata, connector, config, configuredSecrets, mode, tables, options, srs, null, experimental,
                contentHash);
    }


    public SourceView {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(contentHash, "contentHash");
        config = SourceDraft.copyJsonMap(config, false);
        configuredSecrets = configuredSecrets == null ? List.of() : List.copyOf(configuredSecrets);
        tables = tables == null ? null : List.copyOf(tables);
        options = SourceDraft.copyJsonMap(options, true);
        execution = SourceDraft.copyJsonMap(execution, true);
        experimental = SourceDraft.copyJsonMap(experimental, true);
    }

    @Override
    public String toString() {
        return "SourceView[id=" + id
                + ", connector=" + connector
                + ", configuredSecrets=" + configuredSecrets
                + ", contentHash=" + contentHash
                + "]";
    }
}
