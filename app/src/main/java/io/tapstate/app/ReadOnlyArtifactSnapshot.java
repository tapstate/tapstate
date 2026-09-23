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

    private final List<Resource> resources;
    private final Map<String, Resource> byId;

    private ReadOnlyArtifactSnapshot(List<Resource> resources) {
        this.resources = List.copyOf(resources);
        Map<String, Resource> indexed = new LinkedHashMap<>();
        for (Resource resource : this.resources) {
            Resource previous = indexed.put(resource.id(), resource);
            if (previous != null) {
                throw new IllegalStateException("artifact snapshot contains duplicate id " + resource.id());
            }
        }
        this.byId = Map.copyOf(indexed);
    }

    static ReadOnlyArtifactSnapshot capture(ArtifactStore source) {
        return new ReadOnlyArtifactSnapshot(Objects.requireNonNull(source, "source").list());
    }

    @Override
    public void saveAll(List<Resource> artifacts) {
        throw new UnsupportedOperationException("an artifact snapshot is read-only");
    }

    @Override
    public Optional<Resource> get(String id) {
        return Optional.ofNullable(byId.get(id));
    }

    @Override
    public List<Resource> list() {
        return resources;
    }
}
