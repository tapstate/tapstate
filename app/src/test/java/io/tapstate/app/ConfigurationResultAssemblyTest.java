package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.*;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfigurationResultAssemblyTest {
    @Test void durableResultsAndCommittedRetriesDoNotRequireOrCreateBusiness() {
        var root = mock(StorageRoot.class); var runtime = mock(StorageRuntime.class);
        var configuration = mock(MongoConfigurationDomain.class); var imports = mock(ConfigurationImportStore.class);
        when(root.runtime()).thenReturn(runtime); when(runtime.requireConfiguration()).thenReturn(configuration);
        when(configuration.imports()).thenReturn(imports); when(imports.guardedBy(any())).thenReturn(imports);
        when(configuration.audit()).thenReturn(record -> {});
        var artifacts = mock(ArtifactStore.class); when(configuration.artifacts()).thenReturn(artifacts);
        when(runtime.business()).thenThrow(new AssertionError("result lookup must not touch business"));
        var setup = new StorageSetupScope("cluster", "installation", "setup", 1);
        var scope = new ConfigurationImportStore.Scope("cluster", 1, StorageBinding.Mode.OP, null);
        String id = UUID.randomUUID().toString(); Instant now = Instant.now();
        var ticket = new ConfigurationImportStore.Ticket(id, scope, "a".repeat(64), "b".repeat(64), 2, now, now.plusSeconds(1800));
        var receipt = new ConfigurationImportStore.Receipt(id, "a".repeat(64), 3, Map.of("source", "c".repeat(64)), Set.of(), Set.of());
        when(imports.find(scope, id)).thenReturn(Optional.of(new ConfigurationImportStore.Status(ticket, ConfigurationImportStore.Phase.COMMITTED, 1, receipt)));
        when(imports.commit(eq(ticket), any())).thenReturn(receipt);
        var caller = StorageControlAccess.ordinary(StorageBinding.Mode.OP, setup,
                new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1),
                TapstatePrincipal.humanJwt(new VerifiedToken("admin", Scope.ADMIN)), null, () -> {});
        var dispatcher = new StorageRootConfiguration().storageControlDispatcher(ControlOperations.registry(), empty(), empty(), empty(),
                (ignored, guard) -> { throw new AssertionError("no business snapshot"); }, root, Clock.systemUTC());
        assertThat(dispatcher.dispatch("configuration.import.status", caller, Map.of("importId", id))).containsEntry("phase", "COMMITTED");
        var payload = new ConfigurationImportStore.Payload(List.of(new SourceResource("source", null, "mongodb", Map.of("uri", "mongodb://host/source"), null, null, null, null)),
                List.of(), List.of(), Map.of(), Map.of());
        String key = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        String prepared = new EncryptedConfigurationTransfer().seal(payload, key);
        assertThat(dispatcher.dispatch("configuration.import.commit", caller,
                Map.of("importId", id, "transfer", prepared, "transferKey", key, "defaultSourceMapping", Map.of())))
                .containsEntry("id", id).containsEntry("configurationRevision", 3L);
        verify(runtime, never()).business(); verifyNoInteractions(artifacts);
    }
    @SuppressWarnings("unchecked") private static <T> ObjectProvider<T> empty() { return mock(ObjectProvider.class); }
}
