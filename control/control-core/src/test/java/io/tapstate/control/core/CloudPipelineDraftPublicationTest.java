package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudPipelineDraftPublicationTest {

    private static final Instant NOW = Instant.parse("2026-10-10T04:00:00Z");
    private static final String OWNER = "verified-cloud-user-a";
    private static final String EDITOR = "verified-cloud-user-b";
    private static final CanonicalWriter WRITER = new CanonicalWriter();

    @Test
    void previewUsesTheSameDependencySliceAsPublicationWithoutScanningUnrelatedArtifacts() {
        var fixture = new Fixture(true, PipelineDraft.Mode.WIZARD);
        var expected = fixture.service.preview("orders", 1);
        fixture.artifacts.save(new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: unrelated
                connector: mongodb
                config: { uri: "mongodb://localhost:27017/unrelated" }
                """));
        fixture.artifacts.useSlice = true;
        fixture.artifacts.scans = 0;
        assertThat(fixture.service.preview("orders", 1)).isEqualTo(expected);
        assertThat(fixture.artifacts.scans).as("preview must not hydrate the entire workspace").isZero();
        assertThat(fixture.artifacts.sliceReads).isEqualTo(1);
        assertThat(fixture.drafts.lastPublication).isNull();
    }

    @Test
    void publicationPassesThePlannedSourceSchemaToDerivation() {
        var reads = new LinkedHashMap<String, Integer>();
        var model = new io.tapstate.spi.store.DiscoveredSourceModel("mongo_input", "mongodb", NOW.toEpochMilli(),
                new io.tapstate.spi.store.SourceModel(List.of(new io.tapstate.spi.store.SourceTable("orders", List.of(
                        new io.tapstate.spi.store.SourceField("_id", "int", io.tapstate.core.common.TapstateType.INT64)),
                        List.of("_id"), List.of()))));
        io.tapstate.spi.store.SchemaStore schemas = new io.tapstate.spi.store.SchemaStore() {
            @Override public void save(io.tapstate.spi.store.DiscoveredSourceModel ignored) {
                throw new AssertionError("read only");
            }
            @Override public Optional<io.tapstate.spi.store.DiscoveredSourceModel> get(String id) {
                reads.merge(id, 1, Integer::sum);
                return id.equals("mongo_input") ? Optional.of(model) : Optional.empty();
            }
        };
        var captured = new java.util.concurrent.atomic.AtomicReference<SchemaDerivation.PublishedInputs>();
        SchemaDerivation derivation = new SchemaDerivation() {
            @Override public void derive(String id) { throw new AssertionError("expected publication inputs"); }
            @Override public void derivePublishedDraft(String id, PublishedInputs inputs, long generation) {
                captured.set(inputs);
            }
        };
        var fixture = new Fixture(true, PipelineDraft.Mode.DAG, schemas, derivation);

        assertThat(fixture.service.publish("orders", 1, null, OWNER).published()).isTrue();
        assertThat(reads).containsExactlyInAnyOrderEntriesOf(Map.of("mongo_input", 1, "mongo_target", 1));
        assertThat(captured.get().models()).containsEntry("mongo_input", Optional.of(model));
        assertThat(captured.get().definitions()).containsOnlyKeys("orders", "mongo_input", "mongo_target");
        assertThat(captured.get().definitions().get("orders")).isEqualTo(fixture.drafts.lastPublication.artifact());
    }

    @ParameterizedTest
    @EnumSource(PipelineDraft.Mode.class)
    void cloudPublicationAttributesTheArtifactToTheVerifiedPublisher(PipelineDraft.Mode mode) {
        Fixture fixture = new Fixture(true, mode);
        String preview = WRITER.write(fixture.service.preview("orders", 1));

        PipelineDraftService.PublishResult published = fixture.service.publish("orders", 1, null, OWNER);

        assertThat(published.published()).isTrue();
        assertThat(published.artifact().metadata().cloud()).isTrue();
        assertThat(published.artifact().metadata().userId()).isEqualTo(OWNER);
        fixture.assertPublished(published);
        assertThat(WRITER.write(fixture.service.preview("orders", 1))).isEqualTo(preview);
    }

    @ParameterizedTest
    @EnumSource(PipelineDraft.Mode.class)
    void publishingAnEditKeepsTheOriginalCreator(PipelineDraft.Mode mode) {
        Fixture fixture = new Fixture(true, mode);
        PipelineResource original = (PipelineResource) ResourceAttributionPolicy.managedCloud()
                .attribute(OWNER, fixture.service.preview("orders", 1), null);
        fixture.artifacts.save(original);
        String originalHash = CanonicalHash.of(original);
        fixture.drafts.current = draft(mode, "Changed by the second user", originalHash);

        PipelineDraftService.PublishResult published = fixture.service.publish("orders", 1, originalHash, EDITOR);

        assertThat(published.published()).isTrue();
        assertThat(published.artifact().metadata().description()).isEqualTo("Changed by the second user");
        assertThat(published.artifact().metadata().cloud()).isTrue();
        assertThat(published.artifact().metadata().userId()).isEqualTo(OWNER);
        assertThat(fixture.drafts.lastPublication.updatedBy()).isEqualTo(EDITOR);
        fixture.assertPublished(published);

        PipelineDraftService.PublishResult replay = fixture.service.publish(
                "orders", 1, published.artifactHash(), "verified-cloud-user-c");
        assertThat(replay.published()).isTrue();
        assertThat(replay.artifact().metadata().userId()).isEqualTo(OWNER);
        assertThat(replay.artifactHash()).isEqualTo(published.artifactHash());
        fixture.assertPublished(replay);
    }

    @ParameterizedTest
    @EnumSource(PipelineDraft.Mode.class)
    void anExistingUnattributedArtifactRemainsUnknown(PipelineDraft.Mode mode) {
        Fixture fixture = new Fixture(true, mode);
        PipelineResource original = fixture.service.preview("orders", 1);
        fixture.artifacts.save(original);
        String originalHash = CanonicalHash.of(original);
        fixture.drafts.current = draft(mode, "Edited legacy pipeline", originalHash);

        PipelineDraftService.PublishResult published = fixture.service.publish("orders", 1, originalHash, EDITOR);

        assertThat(published.published()).isTrue();
        assertThat(published.artifact().metadata().cloud()).isNull();
        assertThat(published.artifact().metadata().userId()).isNull();
        fixture.assertPublished(published);
    }

    @ParameterizedTest
    @EnumSource(PipelineDraft.Mode.class)
    void onPremPublicationDoesNotGenerateCloudAttribution(PipelineDraft.Mode mode) {
        Fixture fixture = new Fixture(false, mode);
        String preview = WRITER.write(fixture.service.preview("orders", 1));

        PipelineDraftService.PublishResult published = fixture.service.publish("orders", 1, null, "local-admin");

        assertThat(published.published()).isTrue();
        assertThat(WRITER.write(published.artifact())).isEqualTo(preview);
        assertThat(published.artifact().metadata().cloud()).isNull();
        assertThat(published.artifact().metadata().userId()).isNull();
        fixture.assertPublished(published);
    }

    @ParameterizedTest
    @EnumSource(PipelineDraft.Mode.class)
    void cloudPreviewDoesNotInventAttributionOrWritePublicationMarkers(PipelineDraft.Mode mode) {
        Fixture fixture = new Fixture(true, mode);
        PipelineDraft original = fixture.drafts.current;

        PipelineResource first = fixture.service.preview("orders", 1);
        PipelineResource second = fixture.service.preview("orders", 1);

        assertThat(WRITER.write(second)).isEqualTo(WRITER.write(first));
        assertThat(first.metadata().cloud()).isNull();
        assertThat(first.metadata().userId()).isNull();
        assertThat(fixture.drafts.current).isEqualTo(original);
        assertThat(fixture.drafts.lastPublication).isNull();
        assertThat(fixture.artifacts.get("orders")).isEmpty();
    }

    @Test
    void publicationPlanningRejectsCallerChosenAttributionOnCreateAndEdit() {
        Fixture fixture = new Fixture(true, PipelineDraft.Mode.DAG);
        PipelineResource candidate = fixture.service.preview("orders", 1);
        PipelineResource forged = withMetadata(candidate, new Metadata(Map.of(), "Forged", true, EDITOR));

        assertThatThrownBy(() -> fixture.apply.planDraftPublication(OWNER, forged))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        assertThat(fixture.artifacts.get("orders")).isEmpty();
        assertThat(fixture.drafts.lastPublication).isNull();

        Resource original = ResourceAttributionPolicy.managedCloud().attribute(OWNER, candidate, null);
        fixture.artifacts.save(original);
        assertThatThrownBy(() -> fixture.apply.planDraftPublication(EDITOR, forged))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        assertThat(fixture.artifacts.get("orders").map(WRITER::write)).contains(WRITER.write(original));
        assertThat(fixture.drafts.lastPublication).isNull();
    }

    private static PipelineResource withMetadata(PipelineResource resource, Metadata metadata) {
        return new PipelineResource(resource.id(), metadata, resource.sources(), resource.transforms(),
                resource.view(), resource.serve(), resource.settings(), resource.experimental());
    }

    private static PipelineDraft draft(PipelineDraft.Mode mode, String description, String baseHash) {
        PipelineDraft.Graph graph = mode == PipelineDraft.Mode.DAG
                ? new PipelineDraft.Graph(List.of(
                        new PipelineDraft.Node("input", "source", "mongo_input", "orders", Map.of(), Map.of()),
                        new PipelineDraft.Node("output", "target", "mongo_target", "orders", Map.of(),
                                Map.of("writeMode", "append"))),
                        List.of(new PipelineDraft.Edge("input-output", "input", "output")),
                        new PipelineDraft.Viewport(0, 0, 1)) : null;
        PipelineDraft.Wizard wizard = mode == PipelineDraft.Mode.WIZARD
                ? new PipelineDraft.Wizard(new PipelineDraft.Root("root", "mongo_input", "orders",
                        List.of(), List.of()), List.of(), List.of(), new PipelineDraft.Output("source",
                        Map.of("sourceId", "mongo_target", "table", "orders", "writeMode", "append"))) : null;
        return new PipelineDraft("orders", 1, 1, mode, "Orders", description, graph, wizard,
                baseHash, null, null, NOW, NOW, "untrusted-draft-actor");
    }

    private static final class Fixture {
        private final CanonicalStore artifacts = new CanonicalStore();
        private final RecordingDraftStore drafts = new RecordingDraftStore(artifacts);
        private final ApplyService apply;
        private final PipelineDraftService service;

        private Fixture(boolean cloud, PipelineDraft.Mode mode) {
            this(cloud, mode, new EmptySchemaStore(), SchemaDerivation.none());
        }

        private Fixture(boolean cloud, PipelineDraft.Mode mode, io.tapstate.spi.store.SchemaStore schemas,
                SchemaDerivation derivation) {
            DslParser parser = new DslParser();
            artifacts.save(parser.parse("""
                    version: tapstate/v1
                    kind: source
                    id: mongo_input
                    connector: mongodb
                    config: { uri: "mongodb://localhost:27017/input" }
                    mode: cdc
                    tables: [orders]
                    """));
            artifacts.save(parser.parse("""
                    version: tapstate/v1
                    kind: source
                    id: mongo_target
                    connector: mongodb
                    config: { uri: "mongodb://localhost:27017/output" }
                    """));
            Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
            AuditGate gate = new AuditGate(record -> {}, clock);
            apply = new ApplyService(TapstateCatalog::load, artifacts, gate,
                    schemas, PlanAdvisories.none(), derivation, null,
                    cloud ? ResourceAttributionPolicy.managedCloud() : ResourceAttributionPolicy.onPrem(),
                    cloud ? StateDatabasePolicy.CLOUD : StateDatabasePolicy.ON_PREM);
            service = new PipelineDraftService(drafts, new PipelineDraftCompiler(), WRITER, clock, gate, apply);
            drafts.current = draft(mode, "Initial pipeline", null);
        }

        private void assertPublished(PipelineDraftService.PublishResult result) {
            PipelineDraft.Publication publication = drafts.lastPublication;
            assertThat(publication.artifact()).isEqualTo(result.artifact());
            assertThat(result.artifactHash()).isEqualTo(
                    CanonicalHash.of(new DslParser().parse(WRITER.write(result.artifact()))));
            assertThat(publication.publishedArtifactHash()).isEqualTo(result.artifactHash());
            assertThat(artifacts.get("orders").map(WRITER::write)).contains(WRITER.write(result.artifact()));
            assertThat(drafts.current.baseArtifactHash()).isEqualTo(result.artifactHash());
            assertThat(drafts.current.publishedArtifactHash()).isEqualTo(result.artifactHash());
            assertThat(drafts.current.publishedDraftRevision()).isEqualTo(1L);
            assertThat(publication.workspacePreconditions()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "mongo_input", CanonicalHash.of(artifacts.get("mongo_input").orElseThrow()),
                    "mongo_target", CanonicalHash.of(artifacts.get("mongo_target").orElseThrow())));
        }
    }

    private static final class CanonicalStore implements ArtifactStore {
        private final Map<String, String> canonical = new LinkedHashMap<>();
        private final DslParser parser = new DslParser();
        private boolean useSlice;
        private int scans;
        private int sliceReads;

        @Override public Optional<ReadableInventory> draftPublicationInventory(PipelineResource candidate) {
            if (!useSlice) return Optional.empty();
            sliceReads++;
            var ids = new java.util.LinkedHashSet<String>(); ids.add(candidate.id());
            io.tapstate.core.dsl.ReferenceGraph.declaredReferences(candidate).forEach(edge -> ids.add(edge.id()));
            var resources = ids.stream().flatMap(id -> get(id).stream()).toList();
            return Optional.of(new ReadableInventory(resources, Optional.empty(), Optional.empty(), ids));
        }

        @Override
        public void saveAll(List<Resource> resources) {
            resources.forEach(resource -> canonical.put(resource.id(), WRITER.write(resource)));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(canonical.get(id)).map(parser::parse);
        }

        @Override
        public List<Resource> list() {
            scans++;
            return canonical.values().stream().map(parser::parse).toList();
        }
    }

    /** Records the single conditional publication handed to the transactional store boundary. */
    private static final class RecordingDraftStore implements PipelineDraftStore {
        private final CanonicalStore artifacts;
        private PipelineDraft current;
        private PipelineDraft.Publication lastPublication;

        private RecordingDraftStore(CanonicalStore artifacts) {
            this.artifacts = artifacts;
        }

        @Override
        public Optional<PipelineDraft> get(String pipelineId) {
            return Optional.ofNullable(current).filter(draft -> draft.pipelineId().equals(pipelineId));
        }

        @Override
        public List<PipelineDraft> list() {
            return current == null ? List.of() : List.of(current);
        }

        @Override
        public PipelineDraftMutation create(PipelineDraft draft) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineDraftMutation delete(String pipelineId, long expectedRevision) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
            lastPublication = publication;
            if (current.revision() != publication.expectedDraftRevision()) {
                return PipelineDraftMutation.REVISION_CONFLICT;
            }
            String actualHash = artifacts.get(publication.pipelineId()).map(CanonicalHash::of).orElse(null);
            if (!Objects.equals(actualHash, publication.expectedArtifactHash())) {
                return PipelineDraftMutation.ARTIFACT_CONFLICT;
            }
            for (var expected : publication.workspacePreconditions().entrySet()) {
                if (!artifacts.get(expected.getKey()).map(CanonicalHash::of).filter(expected.getValue()::equals)
                        .isPresent()) {
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
            }
            artifacts.save(publication.artifact());
            current = new PipelineDraft(current.pipelineId(), current.schemaVersion(), current.revision(),
                    current.mode(), current.name(), current.description(), current.graph(), current.wizard(),
                    publication.publishedArtifactHash(), publication.expectedDraftRevision(),
                    publication.publishedArtifactHash(), current.createdAt(), publication.publishedAt(),
                    publication.updatedBy());
            return PipelineDraftMutation.PUBLISHED;
        }
    }
}
