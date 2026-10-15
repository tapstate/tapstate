package io.tapstate.adapters.mongostore;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Witnesses draft CAS and publication preconditions against a real Mongo replica-set. */
@RequiresDocker
class MongoPipelineDraftStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void publishesArtifactAndMarkersTogetherAndRejectsAStaleArtifact() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        try (MongoClient client = MongoClients.create(uri)) {
            var database = client.getDatabase("tapstate_pipeline_draft_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();

            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);
            PipelineDraft initial = draft("orders", 1, PipelineDraft.Mode.DAG);
            assertThat(store.create(initial)).isEqualTo(PipelineDraftMutation.CREATED);

            Resource artifact = artifact("orders");
            String artifactHash = CanonicalHash.of(artifact);
            assertThat(store.publish(new PipelineDraft.Publication("orders", 1, null, artifact, artifactHash,
                    Instant.parse("2026-09-21T01:00:00Z"), "publisher")))
                    .isEqualTo(PipelineDraftMutation.PUBLISHED);

            Document storedArtifact = artifacts.find(new Document("_id", "orders")).first();
            assertThat(storedArtifact).isNotNull();
            assertThat(storedArtifact.getString("contentHash")).isEqualTo(artifactHash);
            assertThat(store.get("orders").orElseThrow().publishedDraftRevision()).isEqualTo(1L);

            assertThat(store.replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);

            assertThat(store.replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG,
                    artifactHash, 1L, artifactHash)))
                    .isEqualTo(PipelineDraftMutation.REPLACED);
            assertThat(store.publish(new PipelineDraft.Publication("orders", 2, "stale-hash", artifact,
                    artifactHash, Instant.parse("2026-09-21T02:00:00Z"), "publisher")))
                    .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
            assertThat(artifacts.countDocuments()).isEqualTo(1);
            assertThat(store.get("orders").orElseThrow().publishedDraftRevision()).isEqualTo(1L);
        }
    }

    @Test
    void createListReplaceAndDeleteUseRevisionAndModePreconditions() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_pipeline_draft_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();
            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);

            PipelineDraft initial = draft("orders", 1, PipelineDraft.Mode.DAG);
            assertThat(store.create(initial)).isEqualTo(PipelineDraftMutation.CREATED);
            assertThat(store.create(initial)).isEqualTo(PipelineDraftMutation.ALREADY_EXISTS);
            assertThat(store.create(draft("customers", 1, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.CREATED);
            assertThat(store.get("missing")).isEmpty();
            assertThat(store.list()).extracting(PipelineDraft::pipelineId).containsExactly("customers", "orders");

            assertThat(store.replace("missing", 1, draft("missing", 2, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.NOT_FOUND);
            assertThat(store.replace("orders", 2, draft("orders", 3, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.WIZARD)))
                    .isEqualTo(PipelineDraftMutation.MODE_CONFLICT);
            assertThat(store.replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG,
                    null, null, null)))
                    .isEqualTo(PipelineDraftMutation.REPLACED);
            assertThat(store.get("orders").orElseThrow().revision()).isEqualTo(2);

            assertThat(store.delete("orders", 1)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.delete("orders", 2)).isEqualTo(PipelineDraftMutation.DELETED);
            assertThat(store.delete("orders", 2)).isEqualTo(PipelineDraftMutation.NOT_FOUND);
            assertThat(store.list()).extracting(PipelineDraft::pipelineId).containsExactly("customers");

            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.CREATED);
            assertThat(store.get("orders").orElseThrow().revision()).isEqualTo(3);
            assertThat(store.replace("orders", 2, draft("orders", 3, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            Resource ordersArtifact = artifact("orders");
            assertThat(store.publish(new PipelineDraft.Publication("orders", 2, null, ordersArtifact,
                    CanonicalHash.of(ordersArtifact), Instant.parse("2026-09-21T03:00:00Z"), "stale-publisher")))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        }
    }

    @Test
    void rebaseChangesOnlyThePublicationBaseWithThePriorBaseAsPrecondition() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_pipeline_draft_rebase_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();
            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);
            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.DAG,
                    "old-hash", 1L, "published-hash"))).isEqualTo(PipelineDraftMutation.CREATED);

            PipelineDraft rebased = draft("orders", 2, PipelineDraft.Mode.DAG,
                    "new-hash", 1L, "published-hash");
            assertThat(store.replace("orders", 1, rebased)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.rebase("orders", 1, "wrong-hash", rebased))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.rebase("orders", 1, "old-hash", rebased))
                    .isEqualTo(PipelineDraftMutation.REPLACED);
            assertThat(store.get("orders").orElseThrow().baseArtifactHash()).isEqualTo("new-hash");
            assertThat(store.get("orders").orElseThrow().publishedArtifactHash()).isEqualTo("published-hash");
            assertThat(store.rebase("orders", 1, "old-hash", rebased))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        }
    }

    @Test
    void publishDistinguishesMissingAndStaleDraftRevisions() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_pipeline_draft_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();
            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);
            Resource missingArtifact = artifact("missing");
            PipelineDraft.Publication missing = new PipelineDraft.Publication("missing", 1, null, missingArtifact,
                    CanonicalHash.of(missingArtifact), Instant.parse("2026-09-21T01:00:00Z"), "publisher");
            assertThat(store.publish(missing)).isEqualTo(PipelineDraftMutation.NOT_FOUND);

            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.CREATED);
            Resource ordersArtifact = artifact("orders");
            PipelineDraft.Publication stale = new PipelineDraft.Publication("orders", 2, null, ordersArtifact,
                    CanonicalHash.of(ordersArtifact), Instant.parse("2026-09-21T01:00:00Z"), "publisher");
            assertThat(store.publish(stale)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(artifacts.countDocuments()).isZero();
        }
    }

    @Test
    void publishRejectsDependenciesThatChangedAfterValidation() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_pipeline_draft_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();
            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);
            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.CREATED);
            artifacts.insertOne(new Document("_id", "crm").append("contentHash", "new-source-hash"));

            Resource ordersArtifact = artifact("orders");
            PipelineDraft.Publication staleDependency = new PipelineDraft.Publication("orders", 1, null,
                    ordersArtifact, CanonicalHash.of(ordersArtifact), Instant.parse("2026-09-21T04:00:00Z"),
                    "publisher", Map.of("crm", "hash-used-during-validation"));

            assertThat(store.publish(staleDependency)).isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
            assertThat(artifacts.find(new Document("_id", "orders")).first()).isNull();
            assertThat(store.get("orders").orElseThrow().publishedDraftRevision()).isNull();
        }
    }

    @Test
    void refusesToReplaceAnArtifactOfAnotherKind() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_pipeline_draft_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();
            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);
            assertThat(store.create(draft("mysql", 1, PipelineDraft.Mode.DAG)))
                    .isEqualTo(PipelineDraftMutation.CREATED);
            artifacts.insertOne(new Document("_id", "mysql").append("kind", "source")
                    .append("contentHash", "source-hash"));
            Resource pipeline = artifact("mysql");

            assertThat(store.publish(new PipelineDraft.Publication("mysql", 1, null, pipeline,
                    CanonicalHash.of(pipeline), Instant.parse("2026-09-21T01:00:00Z"), "publisher")))
                    .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
            assertThat(artifacts.find(new Document("_id", "mysql")).first().getString("kind"))
                    .isEqualTo("source");
        }
    }

    @Test
    void replacesAStoredDraftAfterComparingItsMigratedModeAndSkipsUnreadableRowsInLists() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_pipeline_draft_it");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            drafts.drop();
            artifacts.drop();
            MongoPipelineDraftStore store = new MongoPipelineDraftStore(client, drafts, artifacts);
            drafts.insertOne(new Document("_id", "legacy").append("revision", 1L)
                    .append("name", "Legacy").append("updatedBy", "migration"));
            assertThat(store.replace("legacy", 1, draft("legacy", 2, PipelineDraft.Mode.DAG,
                    null, null, null)))
                    .isEqualTo(PipelineDraftMutation.REPLACED);

            drafts.insertOne(new Document("_id", "corrupt").append("schemaVersion", 99)
                    .append("revision", 1L).append("mode", "unknown"));
            assertThat(store.list()).extracting(PipelineDraft::pipelineId).containsExactly("legacy");
            assertThat(store.listSummaries()).extracting(summary -> summary.pipelineId()).containsExactly("legacy");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.get("corrupt"))
                    .isInstanceOf(TapstateException.class)
                    .extracting(error -> ((TapstateException) error).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        }
    }

    private static PipelineDraft draft(String id, long revision, PipelineDraft.Mode mode) {
        return draft(id, revision, mode, revision == 1 ? null : "artifact-hash", null, null);
    }

    private static PipelineDraft draft(String id, long revision, PipelineDraft.Mode mode,
            String baseArtifactHash, Long publishedDraftRevision, String publishedArtifactHash) {
        Instant now = Instant.parse("2026-09-21T00:00:00Z");
        boolean dag = mode == PipelineDraft.Mode.DAG;
        PipelineDraft.Graph graph = dag
                ? new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1)) : null;
        PipelineDraft.Wizard wizard = dag ? null : new PipelineDraft.Wizard(
                new PipelineDraft.Root(id, "crm", "orders", List.of(), List.of()), List.of(), List.of(), null);
        return new PipelineDraft(id, 1, revision, mode, id, "", graph, wizard,
                baseArtifactHash, publishedDraftRevision, publishedArtifactHash, now, now, "author");
    }

    private static Resource artifact(String id) {
        return new PipelineResource(id, null, List.of(SourceRef.bare("crm")), List.of(), null, null, null,
                Map.of());
    }
}
