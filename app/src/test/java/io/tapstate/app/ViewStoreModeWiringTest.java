package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

class ViewStoreModeWiringTest {
    private static final String OLD = "mongodb://old-user:old-secret@mongo:27017/metadata?authSource=admin";
    private static final String NEW = "mongodb://new-user:new-secret@mongo:27017/metadata?authSource=admin";

    @ParameterizedTest
    @ValueSource(strings = {
            "mongodb://user:secret@mongo:27017/metadata?authSource=admin",
            "mongodb+srv://user:secret@atlas.example/metadata"
    })
    void cloudStartupDoesNotEvenReadOrWriteTheArtifactStore(String metadataUri) {
        ArtifactStore artifacts = mock(ArtifactStore.class);
        MongoProperties mongo = new MongoProperties();
        mongo.setUri(null);
        mongo.setTlsCaFile("/unused/private-ca.pem");

        try (var context = start(artifacts, mongo, cloud(metadataUri))) {
            context.getBean(ViewStoreSeedRunner.class).seed();
        }

        verifyNoInteractions(artifacts);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cloudRestartAndCredentialChangesPreserveExistingSources(boolean authored) {
        InMemoryStorePort store = new InMemoryStorePort();
        SourceResource existing = new SourceResource("views",
                authored ? new Metadata(Map.of(), "authored", true, "verified-user") : null,
                authored ? "mongodb-atlas" : "mongodb",
                Map.of("isUri", true, "uri", "mongodb://old-user:old-secret@mongo:27017/metadata_views?authSource=admin"),
                authored ? SourceMode.CDC : null, null, null, null);
        store.artifacts().create(existing);
        String hash = CanonicalHash.of(existing);

        for (String metadataUri : new String[] {OLD, NEW}) {
            try (var context = start(store.artifacts(), new MongoProperties(), cloud(metadataUri))) {
                assertThat(store.artifacts().get("views")).contains(existing);
                assertThat(CanonicalHash.of(store.artifacts().get("views").orElseThrow())).isEqualTo(hash);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "mongodb://mongo:27017/tapstate",
            "mongodb+srv://user:secret@atlas.example/onprem_metadata"
    })
    void onPremStillSeedsFromItsOwnMongoUriBeforeTheStartPhase(String metadataUri) {
        InMemoryStorePort store = new InMemoryStorePort();
        MongoProperties mongo = new MongoProperties();
        mongo.setUri(metadataUri);
        AtomicBoolean presentAtStart = new AtomicBoolean();

        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean(ViewStoreSeedRunner.class, () -> runner(store.artifacts(), mongo,
                    CloudRuntimeSettings.resolve(new CloudProperties())));
            context.registerBean(SurfaceStart.class, () -> new SurfaceStart(() -> presentAtStart.set(
                    store.artifacts().get("views").isPresent())));
            context.refresh();
            SourceResource source = (SourceResource) store.artifacts().get("views").orElseThrow();
            assertThat(source.config().get("uri")).isEqualTo(ViewStoreSeedRunner.viewsUri(metadataUri));
            assertThat(source.metadata()).isNull();
            assertThat(source.mode()).isNull();
            assertThat(source.tables()).isNull();
        }

        assertThat(presentAtStart.get()).isTrue();
    }

    @Test
    void onPremRestartStillPreservesAnAuthoredViewsConnection() {
        InMemoryStorePort store = new InMemoryStorePort();
        SourceResource existing = new SourceResource("views", null, "mongodb",
                Map.of("isUri", true, "uri", "mongodb://elsewhere:27017/owned"), null, null, null, null);
        store.artifacts().create(existing);

        try (var context = start(store.artifacts(), new MongoProperties(),
                CloudRuntimeSettings.resolve(new CloudProperties()))) {
            assertThat(store.artifacts().get("views")).contains(existing);
        }
    }

    private static ViewStoreSeedRunner runner(ArtifactStore artifacts, MongoProperties mongo,
            CloudRuntimeSettings settings) {
        return new ControlPlaneConfiguration().viewStoreSeedRunner(artifacts, mongo, settings);
    }

    private static AnnotationConfigApplicationContext start(ArtifactStore artifacts, MongoProperties mongo,
            CloudRuntimeSettings settings) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(ViewStoreSeedRunner.class, () -> runner(artifacts, mongo, settings));
        context.refresh();
        return context;
    }

    private static CloudRuntimeSettings cloud(String metadataUri) {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example.invalid");
        properties.setToken("controlled-status-token");
        properties.setClusterId("controlled-views-cluster");
        properties.setAtlasUri(metadataUri);
        return CloudRuntimeSettings.resolve(properties);
    }

    private static final class SurfaceStart implements SmartLifecycle {
        private final Runnable onStart;
        private boolean running;

        private SurfaceStart(Runnable onStart) {
            this.onStart = onStart;
        }

        @Override
        public void start() {
            onStart.run();
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }
    }
}
