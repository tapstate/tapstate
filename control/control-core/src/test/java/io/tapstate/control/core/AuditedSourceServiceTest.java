package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AuditedSourceServiceTest {

    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-07-13T08:00:00Z"), ZoneOffset.UTC);

    private final TapstateCatalog catalog = TapstateCatalog.load();
    private final SourceRepresentation representation = new SourceRepresentation(catalog);

    @Test
    void writesRecordAuditBeforeDelegatingToTheSourceService() {
        List<String> events = new ArrayList<>();
        RecordingArtifactStore store = new RecordingArtifactStore(events);
        List<AuditRecord> records = new ArrayList<>();
        AuditStore auditStore = record -> {
            events.add("audit:" + record.operationId());
            records.add(record);
        };
        AuditedSourceService service = service(store, auditStore);

        SourceView created = service.create("alice", draft("orders", "before"));
        assertThat(events).containsExactly("audit:source.create", "store:list", "store:create");

        events.clear();
        SourceView updated = service.replace(
                "bob", "orders", created.contentHash(), draft("orders", "after"));
        assertThat(events).containsExactly("audit:source.update", "store:list", "store:replace");

        events.clear();
        service.delete("carol", "orders", updated.contentHash());
        assertThat(events).containsExactly("audit:source.delete", "store:list", "store:delete");

        assertThat(records).extracting(AuditRecord::operationId)
                .containsExactly("source.create", "source.update", "source.delete");
        assertThat(records).extracting(AuditRecord::principal)
                .containsExactly("alice", "bob", "carol");
        assertThat(records).extracting(AuditRecord::resourceId)
                .containsExactly("orders", "orders", "orders");
    }

    @Test
    void anAuditFailurePreventsEveryWriteFromReachingTheArtifactStore() {
        List<String> events = new ArrayList<>();
        RecordingArtifactStore store = new RecordingArtifactStore(events);
        Resource existing = representation.toModel(draft("orders", "before"), null);
        store.seed(existing);
        String hash = CanonicalHash.of(existing);
        AuditStore failingAuditStore = record -> {
            throw new IllegalStateException("audit unavailable");
        };
        AuditedSourceService service = service(store, failingAuditStore);

        assertThatThrownBy(() -> service.create("alice", draft("customers", "new")))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(events).isEmpty();

        assertThatThrownBy(() -> service.replace(
                "alice", "orders", hash, draft("orders", "after")))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(events).isEmpty();

        assertThatThrownBy(() -> service.delete("alice", "orders", hash))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(events).isEmpty();
    }

    @Test
    void readsDelegateDirectlyWithoutInvokingTheAuditStore() {
        List<String> events = new ArrayList<>();
        RecordingArtifactStore store = new RecordingArtifactStore(events);
        store.seed(representation.toModel(draft("orders", "before"), null));
        List<AuditRecord> records = new ArrayList<>();
        AuditedSourceService service = service(store, records::add);

        assertThat(service.list()).extracting(SourceView::id).containsExactly("orders");
        assertThat(service.get("orders").metadata().description()).isEqualTo("before");

        assertThat(records).isEmpty();
        assertThat(events).containsExactly("store:list", "store:get");
    }

    private AuditedSourceService service(ArtifactStore store, AuditStore auditStore) {
        SourceService sourceService = new SourceService(catalog, store, representation, DataBrowserFollows.NONE);
        return new AuditedSourceService(sourceService, new AuditGate(auditStore, CLOCK));
    }

    private static SourceDraft draft(String id, String description) {
        return new SourceDraft(
                id,
                new Metadata(Map.of(), description),
                "mysql",
                Map.of("host", "localhost", "port", "3306", "database", "orders", "username", "app"),
                "snapshot",
                List.of(new SourceTableDraft("literal", "orders", null, null, null, null)),
                Map.of(),
                null,
                Map.of(),
                List.of());
    }

    private static final class RecordingArtifactStore implements ArtifactStore {

        private final List<String> events;
        private final Map<String, Resource> artifacts = new LinkedHashMap<>();

        private RecordingArtifactStore(List<String> events) {
            this.events = events;
        }

        private void seed(Resource resource) {
            artifacts.put(resource.id(), resource);
        }

        @Override
        public ArtifactMutation create(Resource artifact) {
            events.add("store:create");
            if (artifacts.putIfAbsent(artifact.id(), artifact) != null) {
                return ArtifactMutation.ALREADY_EXISTS;
            }
            return ArtifactMutation.CREATED;
        }

        @Override
        public ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
            events.add("store:replace");
            Resource existing = artifacts.get(id);
            if (existing == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(existing).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            artifacts.put(id, replacement);
            return ArtifactMutation.REPLACED;
        }

        @Override
        public ArtifactMutation delete(String id, String expectedContentHash) {
            events.add("store:delete");
            Resource existing = artifacts.get(id);
            if (existing == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(existing).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            artifacts.remove(id);
            return ArtifactMutation.DELETED;
        }

        @Override
        public void saveAll(List<Resource> resources) {
            events.add("store:saveAll");
            resources.forEach(this::seed);
        }

        @Override
        public Optional<Resource> get(String id) {
            events.add("store:get");
            return Optional.ofNullable(artifacts.get(id));
        }

        @Override
        public List<Resource> list() {
            events.add("store:list");
            return new ArrayList<>(artifacts.values());
        }

        private static String hash(Resource resource) {
            return CanonicalHash.of(resource);
        }
    }
}
