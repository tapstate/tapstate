package io.tapstate.spi.store;

import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import java.util.*;

/** Frozen authoring hashes and resolved source selections; no connection secrets are persisted here. */
public record PipelinePreparationInputs(String pipelineHash,Map<String,String> sourceHashes,
        Map<String,Set<String>> selectedTables,List<DirectChannelPreparation> directChannels) {
    public PipelinePreparationInputs {
        sourceHashes = Map.copyOf(sourceHashes);
        var selections = new HashMap<String,Set<String>>(); selectedTables.forEach((source,tables) -> selections.put(source,Set.copyOf(tables)));
        selectedTables = Map.copyOf(selections);
        directChannels = directChannels.stream().sorted(Comparator.comparing(value -> value.context().sourceId())).toList();
        if (!hash(pipelineHash) || sourceHashes.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || !hash(entry.getValue()))
                || !selectedTables.keySet().equals(sourceHashes.keySet()) || selectedTables.values().stream().anyMatch(tables -> tables.stream().anyMatch(String::isBlank))
                || new HashSet<>(directChannels.stream().map(value -> value.context().sourceId()).toList()).size() != directChannels.size()
                || !sourceHashes.keySet().containsAll(directChannels.stream().map(value -> value.context().sourceId()).toList()))
            throw new IllegalArgumentException("invalid frozen Pipeline preparation inputs");
        for (var direct : directChannels) if (!direct.tables().equals(selectedTables.get(direct.context().sourceId())))
            throw new IllegalArgumentException("private channel does not match the fixed source selection");
    }
    public static PipelinePreparationInputs of(PipelineIntentScope scope,PipelineResource pipeline,Map<String,SourceResource> sources,Map<String,Set<String>> selections) {
        if (!scope.pipelineId().equals(pipeline.id())) throw new IllegalArgumentException("Pipeline snapshot belongs to another scope");
        var hashes = new HashMap<String,String>(); var direct = new ArrayList<DirectChannelPreparation>();
        sources.forEach((id,source) -> { if (!id.equals(source.id())) throw new IllegalArgumentException("source snapshot identity mismatch"); hashes.put(id,CanonicalHash.of(source)); });
        if (!new HashSet<>(pipeline.sourceIds()).equals(hashes.keySet()) || !selections.keySet().equals(hashes.keySet()))
            throw new IllegalArgumentException("source snapshots and selections must close the Pipeline references");
        var settings = pipeline.settings(); ReadMode mode = settings == null || settings.readMode() == null ? ReadMode.SNAPSHOT_AND_CDC : settings.readMode();
        String start = DirectChannelPreparation.checkedStart(scope,settings == null ? null : settings.startFrom());
        for (var reference : pipeline.sources()) {
            if (!(reference instanceof SourceRef.Spec spec)) throw new IllegalStateException("a prepared Pipeline requires materialized source switches");
            var tables = selections.get(reference.id());
            if (!spec.srs() && mode != ReadMode.SNAPSHOT_ONLY && !tables.isEmpty())
                direct.add(DirectChannelPreparation.of(scope,sources.get(reference.id()),mode,start,tables));
        }
        return new PipelinePreparationInputs(CanonicalHash.of(pipeline),hashes,selections,direct);
    }
    public boolean sameRecovery(PipelinePreparationInputs other) { return directChannels.equals(Objects.requireNonNull(other,"other").directChannels); }
    private static boolean hash(String value) { return value != null && value.matches("[0-9a-f]{64}"); }
}
