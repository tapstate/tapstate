package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SourceConnectionResolverTest {

    @Test
    void preservesDraftValuesAndRestoresOnlyTheMissingSecret() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql",
                Map.of("host", "production", "database", "orders", "password", "saved-secret"),
                null, null, null, null));

        ConnectionConfig resolved = new SourceConnectionResolver(artifacts)
                .resolve("source-1", "mysql", Map.of("host", "staging"));

        assertThat(resolved.settings())
                .containsEntry("host", "staging")
                .containsEntry("password", "saved-secret")
                .doesNotContainKey("database");
    }

    @Test
    void doesNotResolveAStoredSourceWhenTheConnectorChanges() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql", Map.of("host", "production", "password", "saved-secret"),
                null, null, null, null));

        ConnectionConfig resolved = new SourceConnectionResolver(artifacts)
                .resolve("source-1", "postgres", Map.of("host", "staging"));

        assertThat(resolved).isEqualTo(
                new ConnectionConfig("source-1", "postgres", Map.of("host", "staging")));
    }

    @Test
    void normalizesNullSettingsAndRestoresThePersistedSecret() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql", Map.of("host", "production", "password", "saved-secret"),
                null, null, null, null));

        ConnectionConfig resolved = new SourceConnectionResolver(artifacts)
                .resolve("source-1", "mysql", null);

        assertThat(resolved.settings()).containsEntry("password", "saved-secret")
                .doesNotContainKey("host");
    }

    private static final class InMemoryArtifactStore implements ArtifactStore {
        private final List<Resource> resources = new ArrayList<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            resources.clear();
            resources.addAll(artifacts);
        }

        @Override
        public Optional<Resource> get(String id) {
            return resources.stream().filter(resource -> resource.id().equals(id)).findFirst();
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(resources);
        }
    }
}
