package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoPipelineDraftStore;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The specification vocabulary cannot inject a MongoDB commit acknowledgement failure. */
@RequiresDocker
class PipelineDraftCommitOutcomeIT {

    @Test
    void publishReportsAnUnconfirmedCommitThatMayHaveLanded() {
        try (var mongo = NetworkedMongo.startWithTestCommands();
                var client = MongoClients.create(mongo.uriForThisHost("tapstate"))) {
            var database = client.getDatabase("tapstate");
            var drafts = database.getCollection("pipeline_drafts");
            var artifacts = database.getCollection("artifacts");
            var store = new MongoPipelineDraftStore(client, drafts, artifacts);
            String pipeline = "orders";
            var draft = new PipelineDraft(pipeline, PipelineDraft.CURRENT_SCHEMA_VERSION, 1, PipelineDraft.Mode.DAG,
                    "Orders", "", new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1)),
                    null, null, null, null, Instant.EPOCH, Instant.EPOCH, "author");
            assertThat(store.create(draft)).isEqualTo(PipelineDraftMutation.CREATED);

            var artifact = new PipelineResource(pipeline, null, List.of(SourceRef.bare("input")), List.of(),
                    null, null, null, Map.of());
            String hash = CanonicalHash.of(artifact);
            var publication = new PipelineDraft.Publication(pipeline, 1, null, artifact, hash, Instant.EPOCH, "publisher");
            var admin = client.getDatabase("admin");
            // A write concern error lets the commit execute while withholding its acknowledgement.
            // The finite fault outlasts bounded retries and lets the old loop eventually report success.
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 20))
                    .append("data", new Document("failCommands", List.of("commitTransaction"))
                            .append("writeConcernError", new Document("code", 64).append("errmsg", "test acknowledgement failure"))
                            .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
            try {
                assertThatThrownBy(() -> store.publish(publication))
                        .isInstanceOfSatisfying(TapstateException.class, error -> {
                            assertThat(error.code()).isEqualTo(IoError.PIPELINE_COMMIT_OUTCOME_UNKNOWN);
                            assertThat(error.args()).containsExactlyEntriesOf(Map.of("pipeline", pipeline));
                        });
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }

            assertThat(artifacts.find(new Document("_id", pipeline)).first()).containsEntry("contentHash", hash);
            var published = store.get(pipeline).orElseThrow();
            assertThat(published.publishedDraftRevision()).isEqualTo(1L);
            assertThat(published.publishedArtifactHash()).isEqualTo(hash);
        }
    }
}
