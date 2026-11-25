package io.tapstate.app;

import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One immutable artifact-store reading used throughout a single pipeline start attempt. */
final class ReadOnlyArtifactSnapshot implements ArtifactStore {

    private final Optional<String> managedDefault;
    private final List<String> canonical;
    private final Map<String, String> byId;

    private ReadOnlyArtifactSnapshot(List<Resource> resources, Optional<String> managedDefault) {
        this.managedDefault = managedDefault;
        var encoded = new java.util.ArrayList<String>();
        Map<String, String> indexed = new LinkedHashMap<>();
        var writer = new io.tapstate.core.model.canonical.CanonicalWriter();
        for (Resource resource : List.copyOf(resources)) {
            String text = writer.write(resource);
            String previous = indexed.put(resource.id(), text);
            if (previous != null) {
                throw new IllegalStateException("artifact snapshot contains duplicate id " + resource.id());
            }
            encoded.add(text);
        }
        this.canonical = List.copyOf(encoded);
        this.byId = Map.copyOf(indexed);
    }

    static ReadOnlyArtifactSnapshot capture(ArtifactStore source) {
        return new ReadOnlyArtifactSnapshot(Objects.requireNonNull(source, "source").list(), source.managedDefaultSourceId());
    }

    /** An already bounded native validation reading; it never broadens to an ambient store list. */
    static ReadOnlyArtifactSnapshot validation(io.tapstate.spi.store.ValidationTaskStore.Inputs inputs, String managedDefault) {
        Objects.requireNonNull(inputs);
        return validation(inputs.canonicalResources(),inputs.task().comparison().dependencyHashes(),managedDefault);
    }

    static ReadOnlyArtifactSnapshot validation(io.tapstate.spi.store.ValidationTaskStore.DeclarationInputs inputs,String managedDefault){
        Objects.requireNonNull(inputs);return validation(inputs.canonicalResources(),inputs.dependencyHashes(),managedDefault);
    }
    private static ReadOnlyArtifactSnapshot validation(Map<String,String> canonical,Map<String,String> expected,String managedDefault){
        var parser = new io.tapstate.core.dsl.DslParser();
        var owned = new java.util.TreeMap<String, String>(canonical);
        for (var entry : owned.entrySet()) {
            Resource resource;
            try { resource = parser.parse(entry.getValue()); }
            catch (io.tapstate.core.common.TapstateException invalid) { throw invalidValidationInputs(); }
            if (!entry.getKey().equals(resource.id()) || !io.tapstate.core.model.canonical.CanonicalHash.of(resource).equals(expected.get(entry.getKey())))
                throw invalidValidationInputs();
        }
        return new ReadOnlyArtifactSnapshot(owned, Optional.ofNullable(managedDefault));
    }

    private ReadOnlyArtifactSnapshot(Map<String, String> canonical, Optional<String> managedDefault) {
        this.managedDefault = managedDefault; this.byId = Map.copyOf(canonical); this.canonical = List.copyOf(canonical.values());
    }

    private static io.tapstate.core.common.TapstateException invalidValidationInputs() {
        return new io.tapstate.core.common.TapstateException(io.tapstate.core.validation.ValidationError.INVALID_CONTRACT,
                Map.of("field", "configuration.validationInputs"), null);
    }

    /** Overlays one validated candidate closure without mutating the underlying artifact store. */
    static ReadOnlyArtifactSnapshot overlay(ArtifactStore source, List<Resource> overrides) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(overrides, "overrides");
        Map<String, Resource> merged = new LinkedHashMap<>();
        source.list().forEach(resource -> merged.put(resource.id(), resource));
        overrides.forEach(resource -> merged.put(resource.id(), resource));
        return new ReadOnlyArtifactSnapshot(List.copyOf(merged.values()), source.managedDefaultSourceId());
    }

    @Override public Optional<String> managedDefaultSourceId() { return managedDefault; }

    @Override
    public void saveAll(List<Resource> artifacts) {
        throw new UnsupportedOperationException("an artifact snapshot is read-only");
    }

    @Override
    public Optional<Resource> get(String id) {
        return Optional.ofNullable(byId.get(id)).map(ReadOnlyArtifactSnapshot::decode);
    }

    @Override
    public List<Resource> list() {
        return canonical.stream().map(ReadOnlyArtifactSnapshot::decode).toList();
    }

    // Every result owns its complete value tree; callers cannot mutate the stored snapshot through a DTO.
    private static Resource decode(String canonical) { return new io.tapstate.core.dsl.DslParser().parse(canonical); }
}
