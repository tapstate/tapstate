package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    @Test
    void restoresSharedTlsAndSshSecretsForConnectionTests() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql",
                Map.of(
                        "host", "production",
                        "sslKey", "saved-key",
                        "sslKeyPassword", "saved-key-password",
                        "__TAPDATA", Map.of("sshHost", "saved-bastion", "sshPassword", "saved-ssh-password")),
                null, null, null, null));

        ConnectionConfig resolved = new SourceConnectionResolver(artifacts)
                .resolve("source-1", "mysql", Map.of(
                        "host", "staging",
                        "__TAPDATA", Map.of("sshHost", "staging-bastion", "sshPort", 2222)));

        assertThat(resolved.settings())
                .containsEntry("host", "staging")
                .containsEntry("sslKey", "saved-key")
                .containsEntry("sslKeyPassword", "saved-key-password")
                .containsEntry("__TAPDATA", Map.of(
                        "sshHost", "staging-bastion",
                        "sshPort", 2222,
                        "sshPassword", "saved-ssh-password"));
    }

    @Test
    void rejectsExplicitNullSecretsConsistentlyWithSourceUpdates() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql",
                Map.of(
                        "sslKey", "saved-key",
                        "__TAPDATA", Map.of("sshHost", "bastion", "sshPassword", "saved-ssh-password")),
                null, null, null, null));
        Map<String, Object> sshSettings = new LinkedHashMap<>();
        sshSettings.put("sshHost", "bastion");
        sshSettings.put("sshPassword", null);
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sslKey", null);
        request.put("__TAPDATA", sshSettings);

        assertThatThrownBy(() -> new SourceConnectionResolver(artifacts)
                .resolve("source-1", "mysql", request))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.args().get("reason"))
                                .isEqualTo("a supplied secret cannot be null: sslKey"));
    }

    @Test
    void keepsReplacementSharedSecretsFromTheConnectionTestRequest() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql",
                Map.of("sslKey", "saved-key", "__TAPDATA", Map.of("sshPassword", "saved-ssh-password")),
                null, null, null, null));

        ConnectionConfig resolved = new SourceConnectionResolver(artifacts)
                .resolve("source-1", "mysql", Map.of(
                        "sslKey", "replacement-key",
                        "__TAPDATA", Map.of("sshPassword", "replacement-ssh-password")));

        assertThat(resolved.settings())
                .containsEntry("sslKey", "replacement-key")
                .containsEntry("__TAPDATA", Map.of("sshPassword", "replacement-ssh-password"));
    }

    @Test
    void resolvesConfigAndSecretValuesFromOneCatalogSnapshot() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "source-1", null, "mysql",
                Map.of("password", "saved-db-password", "sslKey", "saved-private-key",
                        "__TAPDATA", Map.of("sshPassword", "saved-ssh-password")),
                null, null, null, null));
        var catalog = io.tapstate.core.catalog.TapstateCatalog.load();
        java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        SourceConnectionResolver resolver = new SourceConnectionResolver(artifacts, () -> {
            reads.incrementAndGet();
            return catalog;
        });

        SourceConnectionResolver.ResolvedConnection resolved = resolver.resolveWithSecrets(
                "source-1", "mysql", Map.of("host", "staging"));

        assertThat(reads).hasValue(1);
        assertThat(resolved.config().settings())
                .containsEntry("password", "saved-db-password")
                .containsEntry("sslKey", "saved-private-key")
                .containsEntry("__TAPDATA", Map.of("sshPassword", "saved-ssh-password"));
        assertThat(resolved.secretValues())
                .contains("saved-db-password", "saved-private-key", "saved-ssh-password");
    }

    @Test
    void restoresOnlyTheMatchingAtlasUriDisplayForConnectionOperations() {
        String original = "mongodb+srv://alice:pa%40ss@cluster.example/test";
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "atlas", null, "mongodb-atlas", Map.of("isUri", true, "uri", original),
                null, null, null, null));
        SourceConnectionResolver resolver = new SourceConnectionResolver(artifacts);

        ConnectionConfig kept = resolver.resolve("atlas", "mongodb-atlas",
                Map.of("isUri", true, "uri", "mongodb+srv://<redacted>@cluster.example/test"));
        assertThat(kept.settings()).containsEntry("uri", original);
        assertThat(resolver.resolve("atlas", "mongodb-atlas", Map.of("isUri", true)).settings())
                .containsEntry("uri", original);

        assertThatThrownBy(() -> resolver.resolve("atlas", "mongodb-atlas",
                Map.of("isUri", true, "uri", "mongodb+srv://<redacted>@other.example/test")))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
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
