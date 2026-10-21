package io.tapstate.app;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReadOnlyArtifactSnapshotTest {

    @Test
    void overlaysCandidatesWithoutMutatingBaseAndRefusesWrites() {
        SourceResource stored = source("source", "mysql");
        SourceResource replacement = source("source", "postgres");
        SourceResource added = source("added", "csv");
        ArtifactStore base = store(List.of(stored));

        ReadOnlyArtifactSnapshot snapshot = ReadOnlyArtifactSnapshot.overlay(base, List.of(replacement, added));

        assertThat(snapshot.list()).containsExactly(replacement, added);
        assertThat(snapshot.get("source")).contains(replacement);
        assertThat(snapshot.get("missing")).isEmpty();
        assertThat(base.list()).containsExactly(stored);
        assertThatThrownBy(() -> snapshot.saveAll(List.of(added))).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void refusesDuplicateIdsWhenCapturingAStoredSnapshot() {
        assertThatThrownBy(() -> ReadOnlyArtifactSnapshot.capture(store(List.of(
                source("duplicate", "mysql"), source("duplicate", "csv")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate id");
    }

    private static SourceResource source(String id, String connector) {
        return new SourceResource(id, null, connector, java.util.Map.of(), SourceMode.CDC, null, null, null);
    }

    private static ArtifactStore store(List<Resource> resources) {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Resource> get(String id) {
                return resources.stream().filter(resource -> resource.id().equals(id)).findFirst();
            }

            @Override
            public List<Resource> list() {
                return resources;
            }
        };
    }
}
