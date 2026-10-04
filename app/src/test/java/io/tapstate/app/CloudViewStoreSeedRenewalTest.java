package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StoredArtifactRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudViewStoreSeedRenewalTest {
    private static final String OLD = "mongodb://old-user:old-secret@mongo:27017/metadata?authSource=admin";
    private static final String NEW = "mongodb://new-user:new-secret@mongo:27017/metadata?authSource=admin";

    @Test
    void cloudRefreshesCredentialsAndSecretTrackingTogether() {
        InMemoryStorePort store = new InMemoryStorePort();
        SecretRedactor redactor = new SecretRedactor();
        ArtifactStore tracked = new SecretTrackingArtifactStore(store.artifacts(), TapstateCatalog::load, redactor);
        seed(tracked, OLD);
        SourceResource before = (SourceResource) store.artifacts().get("views").orElseThrow();
        seed(tracked, NEW);
        SourceResource after = (SourceResource) store.artifacts().get("views").orElseThrow();
        assertThat(after.config()).isEqualTo(source(NEW).config());
        assertThat(CanonicalHash.of(after)).isNotEqualTo(CanonicalHash.of(before));
        assertThat(redactor.redact((String) after.config().get("uri"))).doesNotContain("new-secret", "new-user");
        seed(tracked, NEW);
        assertThat(store.artifacts().get("views")).contains(after);
    }

    @Test
    void theSameCloudRenewalDoesNotOverwriteAnOnPremConnection() {
        InMemoryStorePort store = new InMemoryStorePort();
        new ViewStoreSeedRunner(store.artifacts(), OLD, null).seed();
        Resource before = store.artifacts().get("views").orElseThrow();
        new ViewStoreSeedRunner(store.artifacts(), NEW, null).seed();
        assertThat(store.artifacts().get("views")).contains(before);
    }

    @Test
    void conflictingTargetsOrRecognizableAuthoredResourcesArePreserved() {
        SourceResource legacy = source(OLD);
        for (SourceResource conflict : List.of(
                source("mongodb://old-user:old-secret@elsewhere:27017/metadata?authSource=admin"),
                targetSource("mongodb://old-user:old-secret@mongo:27017/other_views?authSource=admin"),
                source("mongodb://old-user:old-secret@mongo:27017/metadata?authSource=other"),
                source("mongodb://old-user:old-secret@mongo:27017/metadata?authSource=admin&tls=true"),
                source("mongodb://old-user:old-secret@mongo:27017/metadata?authSource=admin#fragment"),
                source("mongodb://old-user:old-secret@mongo:27017/metadata?invalid=%notvalid"),
                new SourceResource("views", new Metadata(Map.of(), "authored"), "mongodb", legacy.config(), null, null, null, null),
                new SourceResource("views", new Metadata(Map.of(), null, true, "verified-user"), "mongodb", legacy.config(), null, null, null, null),
                new SourceResource("views", null, "mongodb", legacy.config(), SourceMode.CDC, null, null, null),
                new SourceResource("views", null, "mongodb", legacy.config(), null, List.of(), null, null),
                new SourceResource("views", null, "mongodb-atlas", legacy.config(), null, null, null, null),
                new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", legacy.config().get("uri"), "extra", "owned"), null, null, null, null),
                new SourceResource("views", null, "mongodb", Map.of("isUri", "true", "uri", legacy.config().get("uri")), null, null, null, null),
                new SourceResource("views", null, "mongodb", legacy.config(), null, null, null, Map.of("owned", true)))) {
            InMemoryStorePort store = new InMemoryStorePort();
            store.artifacts().create(conflict);
            assertConflict(() -> seed(store.artifacts(), NEW));
            assertThat(store.artifacts().get("views")).contains(conflict);
        }
    }

    @Test
    void anExplicitReplayConfigurationIsNotAdoptedAsAConnectionSeed() {
        SourceResource capture = new SourceResource("views", null, "mongodb", source(OLD).config(),
                null, null, new Srs(null, null, null, null, false), null);
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().create(capture);
        assertConflict(() -> seed(store.artifacts(), NEW));
        assertThat(store.artifacts().get("views")).contains(capture);
    }

    @Test
    void aCompetingDifferentConfigurationCannotBeOverwrittenByRetry() {
        SourceResource observed = source(OLD);
        SourceResource competitor = source("mongodb://third-user:third-secret@mongo:27017/metadata?authSource=admin");
        ArtifactStore artifacts = conflictStore(observed, competitor, ArtifactMutation.VERSION_CONFLICT);
        assertConflict(() -> seed(artifacts, NEW));
        verify(artifacts).replace("views", CanonicalHash.of(observed), source(NEW));
        verify(artifacts, never()).saveAll(any());
        assertThat(artifacts.get("views")).contains(competitor);
    }

    @Test
    void aConcurrentWriterOfExactlyTheDesiredConnectionIsAccepted() {
        SourceResource observed = source(OLD);
        ArtifactStore artifacts = conflictStore(observed, source(NEW), ArtifactMutation.VERSION_CONFLICT);
        seed(artifacts, NEW);
        verify(artifacts).replace("views", CanonicalHash.of(observed), source(NEW));
    }

    @Test
    void disappearanceDoesNotRecreateAnArtifactWithLostOwnership() {
        SourceResource observed = source(OLD);
        ArtifactStore artifacts = conflictStore(observed, source(NEW), ArtifactMutation.NOT_FOUND);
        assertConflict(() -> seed(artifacts, NEW));
        verify(artifacts).create(source(NEW));
        verify(artifacts).replace("views", CanonicalHash.of(observed), source(NEW));
    }

    @Test
    void anUnchangedConnectionDoesNotRewriteItsCiphertextOrHash() {
        SourceResource existing = source(NEW);
        ArtifactStore artifacts = conflictStore(existing, existing, ArtifactMutation.VERSION_CONFLICT);
        seed(artifacts, NEW);
        verify(artifacts, never()).replace(anyString(), anyString(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anAcceptedOtherNodeWinnerRefreshesThisNodesSecretTracking(boolean afterConflict) {
        SourceResource winner = source(NEW);
        ArtifactStore delegate = conflictStore(afterConflict ? source(OLD) : winner,
                winner, ArtifactMutation.VERSION_CONFLICT);
        when(delegate.listStored()).thenReturn(List.of(StoredArtifactRecord.of(source(OLD))));
        SecretRedactor redactor = new SecretRedactor();
        ArtifactStore tracked = new SecretTrackingArtifactStore(delegate, TapstateCatalog::load, redactor);
        assertThat(redactor.redact("old-secret new-secret")).isEqualTo("******** new-secret");
        seed(tracked, NEW);
        assertThat(redactor.redact((String) winner.config().get("uri")))
                .isEqualTo("mongodb://********@mongo:27017/metadata_views?authSource=admin");
        if (afterConflict) verify(delegate).replace("views", CanonicalHash.of(source(OLD)), winner);
        else verify(delegate, never()).replace(anyString(), anyString(), any());
    }

    @Test
    void refusedCandidatesAreNotRegisteredAsStoredSecrets() {
        ArtifactStore delegate = conflictStore(source(OLD),
                source("mongodb://third-user:third-secret@mongo:27017/metadata?authSource=admin"),
                ArtifactMutation.VERSION_CONFLICT);
        when(delegate.listStored()).thenReturn(List.of(StoredArtifactRecord.of(source(OLD))));
        SecretRedactor redactor = new SecretRedactor();
        ArtifactStore tracked = new SecretTrackingArtifactStore(delegate, TapstateCatalog::load, redactor);
        assertConflict(() -> seed(tracked, NEW));
        assertThat(redactor.redact("old-secret new-secret")).isEqualTo("******** new-secret");
    }

    @Test
    void srvRenewalPreservesOptionsAndTracksEncodedAndDecodedPasswords() {
        InMemoryStorePort store = new InMemoryStorePort();
        SecretRedactor redactor = new SecretRedactor();
        ArtifactStore tracked = new SecretTrackingArtifactStore(store.artifacts(), TapstateCatalog::load, redactor);
        String oldUri = "mongodb+srv://old-user:old%40secret@CLUSTER.example/metadata?retryWrites=true&w=majority&authSource=admin";
        String newUri = "mongodb+srv://new-user:new%40secret@cluster.example/metadata?retryWrites=true&w=majority&authSource=admin";
        seed(tracked, oldUri);
        seed(tracked, newUri);
        SourceResource accepted = (SourceResource) store.artifacts().get("views").orElseThrow();
        assertThat(accepted.config().get("uri")).isEqualTo(ViewStoreSeedRunner.viewsUri(newUri, "metadata_views"));
        assertThat(redactor.redact(accepted.config().get("uri") + " decoded=new@secret"))
                .isEqualTo("mongodb+srv://********@cluster.example/metadata_views?retryWrites=true&w=majority&authSource=admin decoded=********");
    }

    @Test
    void theTestFixtureRefusesMissingIdentityAndStaleVersionsInsteadOfUpserting() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        SourceResource before = source(OLD);
        SourceResource after = source(NEW);
        assertThat(store.replace("views", CanonicalHash.of(before), after)).isEqualTo(ArtifactMutation.NOT_FOUND);
        store.create(before);
        assertThat(store.replace("views", CanonicalHash.of(after), after)).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
        assertThat(store.get("views")).contains(before);
        assertThat(store.replace("views", CanonicalHash.of(before), after)).isEqualTo(ArtifactMutation.REPLACED);
        assertThat(store.replace("views", CanonicalHash.of(before), before)).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
        assertThatThrownBy(() -> store.replace("another-id", CanonicalHash.of(after), after))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.get("views")).contains(after);
    }

    private static ArtifactStore conflictStore(SourceResource observed, SourceResource competitor, ArtifactMutation outcome) {
        ArtifactStore artifacts = mock(ArtifactStore.class);
        when(artifacts.create(any())).thenReturn(ArtifactMutation.ALREADY_EXISTS);
        when(artifacts.get("views")).thenReturn(java.util.Optional.of(observed), java.util.Optional.of(competitor));
        when(artifacts.replace(anyString(), anyString(), any())).thenReturn(outcome);
        return artifacts;
    }

    private static SourceResource source(String metadataUri) {
        return targetSource(ViewStoreSeedRunner.viewsUri(metadataUri, "metadata_views"));
    }

    private static SourceResource targetSource(String uri) {
        return new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", uri),
                null, null, null, null);
    }

    private static void seed(ArtifactStore artifacts, String metadataUri) {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example.invalid");
        properties.setToken("controlled-status-token");
        properties.setClusterId("controlled-renewal-cluster");
        properties.setAtlasUri(metadataUri);
        new ControlPlaneConfiguration().viewStoreSeedRunner(artifacts, new MongoProperties(),
                CloudRuntimeSettings.resolve(properties)).seed();
    }

    private static void assertConflict(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(BootError.CLOUD_VIEW_STORE_CONFLICT);
            assertThat(error.args()).containsExactlyEntriesOf(Map.of("store", "views"));
            assertThat(error.getCause()).isNull();
        });
    }
}
