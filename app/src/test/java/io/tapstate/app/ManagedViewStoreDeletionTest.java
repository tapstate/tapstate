package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ManagedViewStoreDeletionTest {

    @Test
    void refusesDeletingTheStoreUsedByAnInlineView() {
        InMemoryStorePort store = new InMemoryStorePort();
        ArtifactStore artifacts = new DeletableArtifacts(store.artifacts());
        List<AuditRecord> audits = new ArrayList<>();
        ArtifactMutationService mutations = new ArtifactMutationService(
                artifacts, store.desired(), store.state(), store.observations(), store.meta(),
                store.derivedSchemas(), new AuditGate(audits::add, Clock.systemUTC()), id -> { });
        ViewStoreSeedRunner seed = new ViewStoreSeedRunner(
                artifacts, "mongodb://mongo:27017/tapstate", null);
        String storeId = ViewTargetResolver.STATE_STORE_SOURCE_ID;
        String defaultUri = "mongodb://mongo:27017/views?authSource=tapstate";
        String customUri = "mongodb://mongo:27017/my_views?authSource=tapstate";

        seed.seed();
        assertThat(((SourceResource) artifacts.get(storeId).orElseThrow()).config())
                .containsEntry("uri", defaultUri);
        SourceResource customized = new SourceResource(
                storeId, null, "mongodb", Map.of("isUri", true, "uri", customUri),
                null, null, null, null);
        artifacts.save(customized);
        SourceResource orders = new SourceResource(
                "orders_src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null);
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.spec("orders_src", true)), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "id", null),
                null, null, null);
        artifacts.saveAll(List.of(orders, pipeline));

        assertThatCode(() -> buildDag(artifacts)).doesNotThrowAnyException();
        assertThatThrownBy(() -> mutations.delete("operator", orders.id(), CanonicalHash.of(orders)))
                .isInstanceOfSatisfying(TapstateException.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(ArtifactError.IN_USE);
                    assertThat(refusal.args())
                            .isEqualTo(Map.of("id", "orders_src", "referrers", List.of("p")));
                });
        assertThat(artifacts.get(orders.id())).contains(orders);
        seed.seed();
        assertThat(artifacts.get(storeId)).contains(customized);

        String currentHash = CanonicalHash.of(artifacts.get(storeId).orElseThrow());
        Throwable refusal = catchThrowable(() -> mutations.delete("operator", storeId, currentHash));
        if (refusal == null) {
            // Capture the consequences before the expected refusal assertion fails.
            assertThat(artifacts.get(storeId)).isEmpty();
            assertThatThrownBy(() -> buildDag(artifacts))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(ActuationError.VIEW_STORE_NOT_CONFIGURED);
                        assertThat(failure.args()).isEqualTo(Map.of("store", storeId));
                    });
            seed.seed();
            SourceResource reseeded = (SourceResource) artifacts.get(storeId).orElseThrow();
            assertThat(reseeded.config().get("uri")).isEqualTo(defaultUri).isNotEqualTo(customUri);
        }

        assertThat(refusal)
                .as("deleting the store used by p's inline view must be refused with artifact.in-use; "
                        + "an accepted delete breaks the next start and loses the custom URI on restart")
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ArtifactError.IN_USE);
                    assertThat(failure.args())
                            .isEqualTo(Map.of("id", storeId, "referrers", List.of("p")));
                });
        assertThat(artifacts.get(storeId)).contains(customized);
        assertThat(artifacts.get(pipeline.id())).contains(pipeline);
    }

    private static void buildDag(ArtifactStore artifacts) {
        // Build from the current inventory, including the wrapper's conditional deletion.
        InMemoryArtifactStore snapshot = new InMemoryArtifactStore();
        snapshot.saveAll(artifacts.list());
        new StoreBackedDagSource(new InMemoryStorePort(snapshot)).dagFor("p");
    }

    /** Adds conditional deletion to the existing artifact fixture for this single case. */
    private static final class DeletableArtifacts implements ArtifactStore {

        private final ArtifactStore delegate;
        private final Set<String> deleted = new HashSet<>();

        private DeletableArtifacts(ArtifactStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void saveAll(List<Resource> resources) {
            delegate.saveAll(resources);
            resources.forEach(resource -> deleted.remove(resource.id()));
        }

        @Override
        public synchronized ArtifactMutation create(Resource resource) {
            if (deleted.contains(resource.id())) {
                save(resource);
                return ArtifactMutation.CREATED;
            }
            return delegate.create(resource);
        }

        @Override
        public synchronized Optional<Resource> get(String id) {
            return deleted.contains(id) ? Optional.empty() : delegate.get(id);
        }

        @Override
        public synchronized List<Resource> list() {
            return delegate.list().stream().filter(resource -> !deleted.contains(resource.id())).toList();
        }

        @Override
        public synchronized ArtifactMutation delete(String id, String expectedContentHash) {
            Optional<Resource> current = get(id);
            if (current.isEmpty()) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!CanonicalHash.of(current.orElseThrow()).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            deleted.add(id);
            return ArtifactMutation.DELETED;
        }
    }
}
