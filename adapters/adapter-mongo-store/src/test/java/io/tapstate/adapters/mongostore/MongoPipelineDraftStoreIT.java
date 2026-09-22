package io.tapstate.adapters.mongostore;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
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
            PipelineDraft initial = draft(1, "Orders");
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

            assertThat(store.replace("orders", 1, draft(2, "Orders changed")))
                    .isEqualTo(PipelineDraftMutation.REPLACED);
            assertThat(store.publish(new PipelineDraft.Publication("orders", 2, "stale-hash", artifact,
                    artifactHash, Instant.parse("2026-09-21T02:00:00Z"), "publisher")))
                    .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
            assertThat(artifacts.countDocuments()).isEqualTo(1);
            assertThat(store.get("orders").orElseThrow().publishedDraftRevision()).isEqualTo(1L);
        }
    }

    private static PipelineDraft draft(long revision, String name) {
        Instant now = Instant.parse("2026-09-21T00:00:00Z");
        return new PipelineDraft("orders", 1, revision, PipelineDraft.Mode.DAG, name, "",
                new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1)), null,
                revision == 1 ? null : "artifact-hash", 1L, "artifact-hash", now, now, "author");
    }

    private static Resource artifact(String id) {
        return new PipelineResource(id, null, List.of(SourceRef.bare("crm")), List.of(), null, null, null,
                Map.of());
    }
}
