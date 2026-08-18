package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class SourceProjectionServiceTest {

    private final TapstateCatalog catalog = TapstateCatalog.load();
    private final RecordingArtifactStore store = new RecordingArtifactStore();
    private final SourceProjectionService sources = new SourceProjectionService(
            new ApplyService(() -> catalog, store, new AuditGate(record -> { }, Clock.systemUTC()),
                    new EmptySchemaStore(), PlanAdvisories.none()),
            new ArtifactQueryService(store),
            new ArtifactMutationService(store, new EmptyDesiredStore(), new EmptyStateStore(),
                    new EmptyObservationStore(), new EmptySrsMetaStore(),
                    new AuditGate(record -> { }, Clock.systemUTC())),
            new SourceRepresentation(() -> catalog));

    @Test
    void createAndReplaceUseGenericConditionalWrites() {
        SourceView created = sources.create("alice", input("orders", "before"));

        assertThat(store.batchWrites).singleElement().satisfies(write -> {
            assertThat(write.resource().id()).isEqualTo("orders");
            assertThat(write.intent()).isEqualTo(ArtifactWrite.Intent.CREATE_ONLY);
        });

        store.batchWrites.clear();
        SourceView replaced = sources.replace(
                "alice", "orders", created.contentHash(), input("orders", "after"));

        assertThat(replaced.metadata().description()).isEqualTo("after");
        assertThat(store.batchWrites).singleElement().satisfies(write -> {
            assertThat(write.intent()).isEqualTo(ArtifactWrite.Intent.REPLACE_ONLY);
            assertThat(write.expectedContentHash()).isEqualTo(created.contentHash());
        });
    }

    @Test
    void candidateWorkspaceFailureWritesNothing() {
        SourceView created = sources.create("alice", input("orders", "before"));
        store.seed((Resource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: nightly
                source: orders
                settings: { schedule: "0 2 * * *" }
                transforms:
                  - id: selected
                    type: filter
                    from: [orders]
                    expr: "op != 'd'"
                view:
                  id: orders_view
                  from: selected
                  primary_key: id
                  storage:
                    warm:
                      collection: orders_view
                """));
        store.batchWrites.clear();

        Throwable failure = catchThrowable(() -> sources.replace(
                "alice", "orders", created.contentHash(), input("orders", "after", "cdc")));

        assertThat(failure).isInstanceOf(TapstateException.class);
        TapstateException diagnostic = (TapstateException) failure;
        assertThat(diagnostic.code()).isEqualTo(DslError.MODE_MISMATCH);
        assertThat(diagnostic.args())
                .containsEntry("field", "schedule")
                .containsEntry("mode", "cdc")
                .containsEntry("path", "settings.schedule");
        assertThat(store.batchWrites).isEmpty();
        assertThat(hash(store.get("orders").orElseThrow())).isEqualTo(created.contentHash());
    }

    @Test
    void deleteDelegatesToTheGenericArtifactDeletionPath() {
        SourceView created = sources.create("alice", input("orders", "before"));

        sources.delete("alice", "orders", created.contentHash());

        assertThat(store.deletes).containsExactly(created.contentHash());
        assertThat(store.get("orders")).isEmpty();
    }

    private static SourceInput input(String id, String description) {
        return input(id, description, "snapshot");
    }

    private static SourceInput input(String id, String description, String mode) {
        return new SourceInput(
                id,
                new Metadata(Map.of("team", "data"), description),
                "mysql",
                Map.of("host", "localhost", "port", "3306", "database", "orders", "username", "app"),
                mode,
                List.of(new SourceTableDraft("literal", "orders", null, null, null, null)),
                Map.of(),
                null,
                Map.of(),
                List.of());
    }

    private static String hash(Resource resource) {
        return CanonicalHash.of(new CanonicalWriter().write(resource));
    }

    private static final class RecordingArtifactStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();
        private final List<ArtifactWrite> batchWrites = new ArrayList<>();
        private final List<String> deletes = new ArrayList<>();

        void seed(Resource resource) {
            resources.put(resource.id(), resource);
        }

        @Override
        public synchronized ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
            batchWrites.addAll(writes);
            for (ArtifactWrite write : writes) {
                Resource current = resources.get(write.resource().id());
                if (write.intent() == ArtifactWrite.Intent.CREATE_ONLY && current != null) {
                    return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.ALREADY_EXISTS);
                }
                if (write.intent() == ArtifactWrite.Intent.REPLACE_ONLY) {
                    if (current == null) {
                        return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.NOT_FOUND);
                    }
                    if (!hash(current).equals(write.expectedContentHash())) {
                        return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.VERSION_CONFLICT);
                    }
                }
            }
            writes.forEach(write -> resources.put(write.resource().id(), write.resource()));
            return ArtifactBatchWrite.applied();
        }

        @Override
        public synchronized ArtifactMutation delete(String id, String expectedContentHash) {
            deletes.add(expectedContentHash);
            Resource current = resources.get(id);
            if (current == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(current).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            resources.remove(id);
            return ArtifactMutation.DELETED;
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(resource -> resources.put(resource.id(), resource));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(resources.values());
        }

        private static String hash(Resource resource) {
            return CanonicalHash.of(new CanonicalWriter().write(resource));
        }
    }

    private static final class EmptySchemaStore implements SchemaStore {
        @Override
        public Optional<io.tapstate.spi.store.DiscoveredSourceModel> get(String sourceId) {
            return Optional.empty();
        }

        @Override
        public void save(io.tapstate.spi.store.DiscoveredSourceModel model) {
        }
    }

    private static final class EmptyDesiredStore implements DesiredStore {
        @Override public Optional<DesiredState> read(String pipelineId) { return Optional.empty(); }
        @Override public void save(DesiredState desired) { }
        @Override public List<String> pipelineIds() { return List.of(); }
        @Override public void delete(String pipelineId) { }
    }

    private static final class EmptyStateStore implements StateStore {
        @Override public Optional<CheckpointDoc> read(String pipelineId) { return Optional.empty(); }
        @Override public void create(String pipelineId, String stateJson, Instant touchTime) { }
        @Override public CasOutcome compareAndSwap(
                String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            throw new UnsupportedOperationException();
        }
        @Override public void delete(String pipelineId) { }
    }

    private static final class EmptyObservationStore implements ObservationStore {
        @Override public Optional<Observation> read(String pipelineId) { return Optional.empty(); }
        @Override public void save(Observation observation) { }
        @Override public void delete(String pipelineId) { }
    }

    private static final class EmptySrsMetaStore implements SrsMetaStore {
        @Override public Optional<SrsMeta> read(String miningChainId) { return Optional.empty(); }
        @Override public void create(String miningChainId, String retention) { }
        @Override public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) { }
        @Override public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) { }
        @Override public void advanceConsumerReadSeq(
                String miningChainId, String pipelineId, String table, long lastReadSeq) { }
        @Override public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) { }
        @Override public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) { }
        @Override public long openEpoch(String miningChainId) { return 0; }
        @Override public void appendSchemaVersion(String miningChainId, SchemaVersion version) { }
        @Override public void markSnapshotComplete(String miningChainId, String table) { }
        @Override public List<String> miningChainIdsWithConsumer(String pipelineId) { return List.of(); }
        @Override public void detachConsumer(String miningChainId, String pipelineId) { }
    }
}
