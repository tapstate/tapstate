package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deletion removes the effective intent and observation while retaining the original retirement fence.
 * A missing artifact alone cannot prove that convergence stopped or that a late original writer is denied.
 */
class ArtifactDeleteReclaimsDependentDocsIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void removingAPipelineRetiresItsIntentAndCheckpointAndRemovesItsObservation(Tiers tier, @TempDir Path directory)
            throws Exception {
        verifyDeletion(tier, directory, false);
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aCompletedFullStopCanDeleteItsConfirmedSuccessorGeneration(Tiers tier, @TempDir Path directory)
            throws Exception {
        verifyDeletion(tier, directory, true);
    }

    private static void verifyDeletion(Tiers tier, Path directory, boolean purge) throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = tier.launch(storage);
                MongoConfigurationDomain configuration = MongoConfigurationDomain.openExisting(
                        new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)));
                var raw = MongoClients.create(storage.cacheUri())) {
            try {
                RunningPipeline running = RunningPipeline.started(server, directory, storage);
                ControlPlane control = running.control();
                String pipelineId = running.pipelineId();
                var cache = raw.getDatabase(storage.cacheDatabase());
                var states = SystemCollections.CACHE_PIPELINE_STATE.on(cache);
                var observations = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(cache);
                var id = new Document("_id", pipelineId);

                Await.until("all original bookkeeping to exist before deletion",
                        () -> configuration.intents().load(pipelineId).filter(intent -> !intent.deleted()).isPresent()
                                && states.find(id).first() != null && observations.find(id).first() != null,
                        () -> "intent=" + configuration.intents().load(pipelineId)
                                + " state=" + states.find(id).first() + " observation=" + observations.find(id).first());
                assertThat(configuration.intents().pipelineIds()).contains(pipelineId);

                var initialScope = states.find(id).first().get("scope", Document.class);
                running.stopAndSettle(purge);
                var before = configuration.intents().load(pipelineId).orElseThrow();
                var checkpoint = states.find(id).first();
                assertThat(before.deleted()).isFalse();
                assertThat(checkpoint).isNotNull();
                var published = configuration.pipelineStates().read(pipelineId).orElseThrow();
                assertThat(published.phase()).isEqualTo(PipelineStateMapping.Phase.ACTIVE);
                assertThat(published.stateGeneration()).isEqualTo(initialScope.getLong("stateGeneration") + (purge ? 1 : 0));
                assertThat(before.desired().targetState()).isEqualTo(PipelineState.STOPPED);
                assertThat(before.desired().purgeState()).isFalse();
                var prepared = configuration.preparations().read(pipelineId).orElseThrow();
                assertThat(prepared.phase()).isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE);
                assertThat(prepared.scope()).isEqualTo(before.scope());
                assertThat(checkpoint.get("scope", Document.class).getLong("stateGeneration")).isEqualTo(published.stateGeneration());
                if (purge) {
                    var row = SystemCollections.OPERATOR_GENERATIONS.on(raw.getDatabase(storage.configurationDatabase())).find(id).first();
                    var completed = row.get("stateRebuild", Document.class);
                    assertThat(completed.get("originalScope")).isEqualTo(initialScope);
                    assertThat(completed.get("targetScope")).isEqualTo(checkpoint.get("scope"));
                    assertThat(completed.get("result", Document.class)).containsEntry("phase", "COMPLETE")
                            .containsEntry("checkpointEpoch", checkpoint.getLong("epoch"));
                }
                assertThat(configuration.pipelineStates().read(pipelineId)).contains(published);
                try {
                    control.deleteArtifact(pipelineId, control.contentHash(pipelineId));
                } catch (RuntimeException | Error failure) {
                    FailureScene.writeThreads(storage.clusterId(), server);
                    throw failure;
                }

                assertThat(control.artifact(pipelineId)).isEmpty();
                var tombstone = configuration.intents().load(pipelineId).orElseThrow();
                assertThat(tombstone.deleted()).isTrue();
                assertThat(tombstone.scope()).isEqualTo(before.scope());
                assertThat(tombstone.intentRevision()).isGreaterThan(before.intentRevision());
                assertThat(configuration.intents().pipelineIds())
                        .as("a retired intent cannot keep the converger chasing a missing artifact")
                        .doesNotContain(pipelineId);
                assertThat(configuration.intents().compareAndSet(before.scope(), before.intentRevision(), before.desired()))
                        .as("a delayed original intent write cannot resurrect the deleted pipeline").isFalse();
                assertThat(configuration.intents().load(pipelineId)).contains(tombstone);

                var mapping = configuration.pipelineStates().read(pipelineId).orElseThrow();
                assertThat(mapping.phase()).isEqualTo(PipelineStateMapping.Phase.RETIRED);
                var receipt = configuration.retirements().completed(pipelineId, mapping.operationId()).orElseThrow();
                assertThat(receipt.scope()).isEqualTo(before.scope());
                var retired = receipt.cache().orElseThrow();
                var retained = states.find(id).first();
                assertThat(retained).isNotNull();
                assertThat(retained.get("scope")).isEqualTo(checkpoint.get("scope"));
                assertThat(retained.getString("stateJson")).isEqualTo(StateJson.of(PipelineState.STOPPED));
                assertThat(retained.getLong("epoch")).isEqualTo(retired.epoch()).isGreaterThan(checkpoint.getLong("epoch"));
                assertThat(retained.getLong("runtimeWriteFence")).isEqualTo(retired.writeFence())
                        .isGreaterThan(checkpoint.getLong("runtimeWriteFence"));
                assertThat(observations.find(id).first()).as("the obsolete published observation is removed").isNull();
                assertThat(control.stateExpectingRefusal(pipelineId).code()).isEqualTo("lifecycle.unknown-pipeline");
            } finally {
                if (server instanceof RealProcessServer process)
                    FailureScene.writeMemberLogs(storage.clusterId(), java.util.Map.of("member", process.output()));
            }
        }
    }
}
