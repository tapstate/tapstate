package io.tapstate.e2e;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Delays only the native full operation's CONFIG publication; no stored value is committed. */
final class ClearStateCompletionInterleaving implements AutoCloseable {
    static MongoDBContainer startMongo() {
        var mongo = new MongoDBContainer(DockerImageName.parse(System.getProperty("tapstate.e2e.mongo-image", "mongo:7.0")));
        String owner = System.getProperty("tapstate.e2e.resource-owner");
        if (owner != null && !owner.isBlank()) mongo.withLabel(System.getProperty("tapstate.e2e.resource-owner-label",
                "io.tapstate.e2e.resource-owner"), owner);
        if (Boolean.getBoolean("tapstate.e2e.mongo-tmpfs")) {
            mongo.withTmpFs(java.util.Map.of("/data/db", "rw,size=1g", "/data/configdb", "rw,size=16m"));
            var command = new java.util.ArrayList<>(Arrays.asList(mongo.getCommandParts()));
            command.addAll(java.util.List.of("--wiredTigerCacheSizeGB", "0.25", "--oplogSize", "16"));
            mongo.withCommand(command.toArray(String[]::new));
            mongo.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(1L << 30).withNanoCPUs(1_000_000_000L));
        }
        mongo.start();
        return mongo;
    }

    private final MongoClient client;
    private final PreparedNativeStore storage;
    private final String pipeline;
    private final Runnable pass;
    private final AtomicReference<Thread> worker = new AtomicReference<>();
    private final ScheduledExecutorService convergence = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "clear-state-completion-probe");
        worker.set(thread);
        return thread;
    });
    private ClientSession held;

    ClearStateCompletionInterleaving(InProcessServer server, PreparedNativeStore storage, String pipeline) {
        this.storage = storage;
        this.pipeline = pipeline;
        pass = server.publishingObservations();
        assertThat(server.stopPublishingObservations()).isEqualTo(1);
        client = MongoClients.create(storage.configurationUri());
    }

    void holdBeforeReplacement() {
        var mappings = SystemCollections.OPERATOR_GENERATIONS.on(client.getDatabase(storage.configurationDatabase()));
        var original = mappings.find(new Document("_id", pipeline)).first();
        assertThat(original).containsEntry("stateGeneration", 1L).containsEntry("phase", "ACTIVE");
        held = client.startSession();
        held.startTransaction();
        assertThat(mappings.updateOne(held, original,
                new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount()).isEqualTo(1);
        convergence.scheduleWithFixedDelay(pass, 0, 1, TimeUnit.SECONDS);
        Await.until("the native replacement to contend on its original CONFIG mapping", Duration.ofSeconds(20),
                () -> worker.get() != null && Arrays.stream(worker.get().getStackTrace()).anyMatch(frame ->
                        frame.getClassName().equals("io.tapstate.adapters.mongostore.MongoPipelineStateRebuilds")
                                && frame.getMethodName().equals("begin")),
                () -> worker.get() == null ? "no native pass" : Arrays.toString(worker.get().getStackTrace()));
        assertThat(mappings.find(new Document("_id", pipeline)).first()).isEqualTo(original);
    }

    void observed(boolean stopped) {
        if (held == null) return;
        assertThat(stopped).as("the original generation's STOPPED is still visible before replacement").isTrue();
        var pending = SystemCollections.PIPELINE_INTENTS.on(client.getDatabase(storage.configurationDatabase()))
                .find(new Document("_id", pipeline)).first();
        assertThat(pending).containsEntry("stateGeneration", 1L).containsEntry("purgeState", true);
        System.out.println("clear-state probe: old STOPPED observed with pending clear intent " + pending);
        release();
        System.out.println("clear-state probe: CONFIG lock aborted before START; native completion is free to proceed");
    }

    private void release() {
        if (held == null) return;
        try { held.abortTransaction(); }
        finally { held.close(); held = null; }
    }

    @Override public void close() {
        release();
        convergence.shutdown();
        try {
            if (!convergence.awaitTermination(30, TimeUnit.SECONDS))
                throw new AssertionError("native convergence did not finish after releasing the probe transaction");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        } finally {
            try {
                var mapping = SystemCollections.OPERATOR_GENERATIONS.on(client.getDatabase(storage.configurationDatabase()))
                        .find(new Document("_id", pipeline)).first();
                System.out.println("clear-state probe: released operation mapping=" + mapping);
            } catch (MongoException diagnostic) {
                System.out.println("clear-state probe: released operation mapping unavailable: "
                        + diagnostic.getClass().getSimpleName());
            } finally {
                client.close();
            }
        }
    }
}
