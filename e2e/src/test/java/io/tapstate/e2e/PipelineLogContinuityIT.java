package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.pdk.PipelineLogEmitterFixture;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class PipelineLogContinuityIT {
    private static final Duration WAIT = Duration.ofSeconds(100);
    private static final String BEFORE = "trace-generated-before-pause";
    private static final String LATE = "trace-late-from-retired-handle";
    private static final String NEWER = "trace-generated-by-successor";

    @Test
    void pauseResumeAndRebuildKeepLogsButARecreatedPipelineRejectsItsOldLogger(@TempDir Path tree)
            throws Exception {
        try (var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
             var mongo = MongoClients.create(storage.configurationUri())) {
            var database = mongo.getDatabase(new ConnectionString(storage.configurationUri()).getDatabase());
            var running = RunningPipeline.started(server, tree, storage);
            String id = running.pipelineId();
            var control = running.control();
            String originalHash = control.contentHash(id);
            long opening = generation(database,id);
            var old = PipelineLogEmitterFixture.boundTo(id, opening);
            old.accept(BEFORE);
            assertThat(control.logs(id)).startsWith("200 ").contains(BEFORE);

            control.lifecycle(id, LifecycleVerb.PAUSE);
            awaitState(control,id,PipelineState.PAUSED);
            assertThat(control.logs(id)).startsWith("200 ").contains(BEFORE);
            control.lifecycle(id, LifecycleVerb.RESUME);
            awaitState(control,id,PipelineState.RUNNING);
            long resumed = generation(database,id);
            assertThat(resumed).isEqualTo(opening);
            assertThat(control.logs(id)).startsWith("200 ").contains(BEFORE);

            running.stopAndSettle(true);
            Await.until("physical state generation has advanced without deleting the authoring artifact",WAIT,
                    () -> generation(database,id) > opening,() -> "generation=" + generation(database,id));
            long rebuilt = generation(database,id);
            assertThat(control.contentHash(id)).isEqualTo(originalHash);
            assertThat(control.logs(id)).startsWith("200 ").contains(BEFORE);

            PipelineLogLifetimeIT.deleteAtRest(control,id,originalHash);
            assertThat(control.artifact(id)).isEmpty();
            control.apply(Map.of(
                    "source.tap.yml",Workspaces.cdcSourceYaml(running.sourceId(),tree.resolve(running.sourceId())),
                    "target.tap.yml",Workspaces.targetYaml(running.targetId(),running.targetDirectory()),
                    "pipeline.tap.yml",Workspaces.pipelineYaml(id,running.sourceId(),running.targetId(),"orders")));
            long successor = generation(database,id);
            assertThat(successor).isGreaterThan(rebuilt);
            control.lifecycle(id,LifecycleVerb.START);
            awaitState(control,id,PipelineState.RUNNING);
            PipelineLogEmitterFixture.boundTo(id, successor).accept(NEWER);
            // The old handler emits on a different thread, after the successor is active and logged.
            var delivered = new java.util.concurrent.CompletableFuture<Void>();
            Thread.startVirtualThread(() -> {
                try {
                    old.accept(LATE);
                    delivered.complete(null);
                } catch (Throwable failure) {
                    delivered.completeExceptionally(failure);
                }
            });
            delivered.get(5, java.util.concurrent.TimeUnit.SECONDS);
            String actual = control.logs(id);
            assertThat(actual).startsWith("200 ").contains(NEWER).doesNotContain(BEFORE, LATE);
            System.out.printf("LOG_CONTINUITY pauseResume=true physicalRebuild=true lateRejected=true "
                    + "opening=%d resumed=%d rebuilt=%d successor=%d%n",opening,resumed,rebuilt,successor);
        }
    }

    private static long generation(MongoDatabase database,String id) {
        var result = SystemCollections.OPERATOR_GENERATIONS.on(database).find(new Document("_id",id)).first();
        return result==null?-1:result.getLong("stateGeneration");
    }

    private static void awaitState(ControlPlane control,String id,PipelineState expected) {
        Await.until("pipeline reports " + expected, WAIT, () -> control.state(id).filter(expected::equals).isPresent(),
                () -> "actual=" + control.state(id));
    }
}
