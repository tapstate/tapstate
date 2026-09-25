package io.tapstate.app;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SecretTrackingArtifactStoreTest {

    @Test
    void registersExistingAndSuccessfullyWrittenSourceSecrets() {
        RecordingStore delegate = new RecordingStore();
        delegate.save(source("existing", "before-start"));
        SecretRedactor redactor = new SecretRedactor();

        SecretTrackingArtifactStore store = tracking(delegate, redactor);
        store.saveAll(List.of(source("applied", "from-apply")));
        store.save(source("created", "from-create"));

        assertThat(redactor.redact("before-start from-apply from-create"))
                .isEqualTo("******** ******** ********");
    }

    @Test
    void tracksOnlyClearlySecretValuesWhenAStoredConnectorIsNoLongerInTheCatalog() {
        RecordingStore delegate = new RecordingStore();
        SecretRedactor redactor = new SecretRedactor();
        SecretTrackingArtifactStore store = tracking(delegate, redactor);
        SourceResource source = new SourceResource(
                "legacy", null, "removed-connector",
                Map.of("host", "db.internal", "port", 3306,
                        "credentials", Map.of("password", "nested-secret")),
                null, null, null, null);

        store.save(source);

        assertThat(redactor.redact("db.internal 3306 nested-secret"))
                .isEqualTo("db.internal 3306 ********");
    }

    @Test
    void pipelineIncarnationCandidatesAndReadsPassThroughTheProcessDecorator() {
        RecordingStore delegate = new RecordingStore();
        SecretTrackingArtifactStore store = tracking(delegate, new SecretRedactor());
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: upstream
                """);

        assertThat(store.saveAll(List.of(pipeline), Map.of(), Map.of("orders", "created-identity")))
                .isEmpty();
        assertThat(store.pipelineIncarnationId("orders")).contains("created-identity");
        assertThat(store.ensurePipelineIncarnationId("orders", "retry-identity"))
                .contains("created-identity");
        assertThat(delegate.incarnations).containsEntry("orders", "created-identity");
    }

    private static SecretTrackingArtifactStore tracking(ArtifactStore delegate, SecretRedactor redactor) {
        return new SecretTrackingArtifactStore(delegate, TapstateCatalog::load, redactor);
    }

    private static SourceResource source(String id, String password) {
        return new SourceResource(
                id, null, "mysql", Map.of("host", "localhost", "password", password),
                null, null, null, null);
    }

    private static final class RecordingStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();
        private final Map<String, String> incarnations = new LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(artifact -> resources.put(artifact.id(), artifact));
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes,
                Map<String, String> pipelineIncarnationCandidates) {
            pipelineIncarnationCandidates.forEach(incarnations::putIfAbsent);
            saveAll(artifacts);
            return Optional.empty();
        }

        @Override
        public Optional<String> pipelineIncarnationId(String pipelineId) {
            return Optional.ofNullable(incarnations.get(pipelineId));
        }

        @Override
        public Optional<String> ensurePipelineIncarnationId(String pipelineId, String candidate) {
            if (!resources.containsKey(pipelineId)) {
                return Optional.empty();
            }
            return Optional.of(incarnations.computeIfAbsent(pipelineId, ignored -> candidate));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(resources.values());
        }
    }
}
