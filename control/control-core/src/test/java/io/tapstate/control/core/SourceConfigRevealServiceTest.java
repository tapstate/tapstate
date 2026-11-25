package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceConfigRevealServiceTest {

    @Test
    void productionPolicyRefusesBeforeAnyStoredSourceIsRead() {
        RecordingStore store = new RecordingStore(source());
        SourceConfigRevealService service =
                new SourceConfigRevealService(SourceConfigRevealAuthorizer.denyAll(), store);

        assertThatThrownBy(() -> service.reveal(
                "admin", new SourceConfigRevealRequest("orders", "one-time-grant")))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(ControlError.SOURCE_CONFIG_REVEAL_UNAVAILABLE));
        assertThat(store.reads).hasValue(0);
    }

    @Test
    void reservedOperationHasDedicatedSchemaAndNoPublishedFrontend() {
        Operation operation = ControlOperations.registry().resolve("source.reveal-config");

        assertThat(operation.scope()).isEqualTo(Scope.ADMIN);
        assertThat(operation.audited()).isTrue();
        assertThat(operation.exposure()).isEmpty();
        assertThat(ControlApiSchema.resolve(operation.schema().params()).get("required"))
                .isEqualTo(List.of("id", "grant"));
    }

    @Test
    void aFutureAuthorizerCanReleaseOnlyTheRequestedConfigWithoutAnyKeyMaterial() {
        RecordingStore store = new RecordingStore(source());
        SourceConfigRevealService service = new SourceConfigRevealService(
                (principal, sourceId, grant) -> {
                    assertThat(principal).isEqualTo("stepped-up-admin");
                    assertThat(sourceId).isEqualTo("orders");
                    assertThat(grant).isEqualTo("one-time-grant");
                }, store);

        SourceConfigRevealResult result = service.reveal(
                "stepped-up-admin", new SourceConfigRevealRequest("orders", "one-time-grant"));

        assertThat(result.id()).isEqualTo("orders");
        assertThat(result.config()).containsEntry("password", "stored-secret");
        assertThat(result.config()).doesNotContainKeys("key", "keyring", "envelope");
        assertThat(store.reads).hasValue(1);
    }

    private static SourceResource source() {
        return new SourceResource("orders", null, "mysql", Map.of("password", "stored-secret"),
                null, null, null, null);
    }

    private static final class RecordingStore implements ArtifactStore {
        private final Resource resource;
        private final AtomicInteger reads = new AtomicInteger();

        private RecordingStore(Resource resource) {
            this.resource = resource;
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Resource> get(String id) {
            reads.incrementAndGet();
            return resource.id().equals(id) ? Optional.of(resource) : Optional.empty();
        }

        @Override
        public List<Resource> list() {
            return List.of(resource);
        }
    }
}
