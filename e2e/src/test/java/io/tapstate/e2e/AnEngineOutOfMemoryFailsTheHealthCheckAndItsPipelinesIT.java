package io.tapstate.e2e;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A server whose engine has been shut down for want of memory stops passing its health check, and stops
 * reporting its pipelines as running.
 *
 * <p>The engine is a member embedded in the server's own process. When the heap runs out, it is the member's
 * own out-of-memory handling that shuts it down, and the process around it stays up and goes on serving
 * HTTP. That process used to go on saying everything was fine: the health check the image runs kept
 * passing, so a restart policy watching it had nothing to act on, and a pipeline that had been running kept
 * reading {@code RUNNING} while every attempt to reach its job was refused by a member that no longer
 * existed. Nothing moved again until somebody restarted the server by hand.
 *
 * <p><b>The healthy half is asserted first.</b> "The check fails" is also satisfied by a check that never
 * passed, and "the pipeline is not running" by a pipeline that never started. So the same server is read
 * while its engine is up -- the run has carried its row, it reads {@code RUNNING}, and the check passes --
 * and only then is the engine run out of memory.
 *
 * <p><b>How the engine runs out of memory, and why it is done from inside.</b> Exhausting the heap for real
 * is the report's own route and not a usable one here: which thread the collector refuses first is not
 * under anybody's control, and a heap exhausted in this JVM starves the harness along with the product. So
 * the case starts from the moment the error reaches the engine: an {@link OutOfMemoryError} escapes one of
 * the engine's own threads, and the engine's out-of-memory handling decides what becomes of the member.
 * The member is therefore shut down by the engine's own code, as it was in the report, and nothing here
 * shuts it down directly. Reaching one of those threads means reaching inside the running process, which is
 * why this case names the in-process tier rather than driving {@link ServerHandle}. And it insists that the
 * member really is down before it measures anything: a member that survived would leave every assertion
 * after it measuring a healthy server.
 *
 * <p><b>What "fails" means is the image's own check.</b> It runs {@code curl -f} against {@code /healthz},
 * so an answer of 400 or above fails it, and so does no answer within its timeout. Either gives a restart
 * policy something to act on, so either is accepted. A 200 with some other word in it is not accepted,
 * because curl would pass it.
 */
@DisplayName("an engine shut down for want of memory takes the health check and its pipelines down with it")
class AnEngineOutOfMemoryFailsTheHealthCheckAndItsPipelinesIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    /**
     * Named without the word the status is asked to say. The failure message carries the pipeline's name, so
     * a name that said "memory" would satisfy the last assertion whatever the cause it gave.
     */
    private static final String PIPELINE = "engine_lost_witness";
    private static final String TABLE = "orders";

    /**
     * The error the collector raises when it gives up, rather than the one raised when a single allocation
     * is refused. The engine's stock handling weighs a refused allocation against how full the heap is
     * before it acts, and this JVM's heap is not full; a collector that gave up is proof enough on its own.
     * Both are the heap running out, and either one reaching the engine is what the report is about.
     */
    private static final String THE_COLLECTOR_GAVE_UP = "GC overhead limit exceeded";

    /** How long the image's health check gives one run of itself. An answer slower than this fails it too. */
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(3);

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(HEALTH_CHECK_TIMEOUT).build();

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theHealthCheckFailsAndThePipelineReadsFailedForWantOfMemory(
            @TempDir Path source, @TempDir Path target, @TempDir Path jars) {
        FileEndpoints.replaceTable(source.resolve(TABLE + ".csv"), "id,amount\n1,12\n");

        Set<HazelcastInstance> runningBefore = Hazelcast.getAllHazelcastInstances();
        try (InProcessServer server = InProcessServer.start(SharedMongo.replicaSetUrl("engine_out_of_memory"))) {
            HazelcastInstance engine = theMemberThisServerStarted(runningBefore);
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, read(E2eConnectorJar.buildInto(jars)));
            control.apply(Map.of(
                    "src_file.tap.yml", sourceYaml(source),
                    "tgt_file.tap.yml", targetYaml(target),
                    "pipeline.tap.yml", pipelineYaml()));
            control.discoverSchema("src_file", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", source.toString()));

            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the run to carry its row, so this is a working pipeline and not a stuck one",
                    Duration.ofSeconds(90),
                    () -> Files.exists(target.resolve(TABLE + ".csv")),
                    () -> "the target file is still absent");
            Await.until("the run to report itself running", Duration.ofSeconds(90),
                    () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE)));

            // The healthy reading. Without it, the failing check asserted below is also satisfied by a check
            // that never passed at all.
            HealthCheck whileTheEngineRuns = healthCheck(server.baseUrl());
            assertThat(whileTheEngineRuns.passes())
                    .as("while its engine is up, the server passes its own health check: %s", whileTheEngineRuns)
                    .isTrue();

            // The out-of-memory handling is handed every member this JVM runs, not just this server's. A member
            // some other case left running would be taken down along with it, breaking that case instead.
            assertThat(runningMembers())
                    .as("this server's member is the only one running in this JVM when the heap runs out")
                    .containsExactly(engine);
            runOutOfMemoryOnAnEngineThread();
            Await.until("the engine's own out-of-memory handling to shut its member down; nothing after this "
                            + "measures anything while the member is still up",
                    () -> !isRunning(engine),
                    () -> "the member is still running");

            String credential = control.credential();
            Await.until("the server to stop passing its health check, and to stop reporting the pipeline as "
                            + "running, now that the engine that carried it is gone",
                    () -> !healthCheck(server.baseUrl()).passes()
                            && PipelineState.FAILED.name().equals(stateIn(status(server.baseUrl(), credential))),
                    () -> "health check " + healthCheck(server.baseUrl())
                            + "; status " + status(server.baseUrl(), credential)
                            + "; why " + get(server.baseUrl(), "/api/pipelines/" + PIPELINE + "/explain", credential));

            Answer failed = status(server.baseUrl(), credential);
            assertThat(failureMessageIn(failed))
                    .as("and the status names what took the engine down: %s", failed)
                    .containsIgnoringCase("memory");
        }
    }

    /**
     * The engine member this server started: the one running member that was not there before it started.
     * Insisted on as exactly one, because the out-of-memory handling below belongs to the process rather than
     * to any one member, and a case that could not name the member it is about could not say that member
     * went down.
     */
    private static HazelcastInstance theMemberThisServerStarted(Set<HazelcastInstance> runningBefore) {
        List<HazelcastInstance> started = Hazelcast.getAllHazelcastInstances().stream()
                .filter(member -> !runningBefore.contains(member))
                .filter(AnEngineOutOfMemoryFailsTheHealthCheckAndItsPipelinesIT::isRunning)
                .toList();
        assertThat(started).as("the server starts exactly one engine member of its own").hasSize(1);
        return started.getFirst();
    }

    /**
     * Lets an out-of-memory error escape one of the engine's own threads. That is how the engine hears the
     * heap has run out: its threads hand such an error to its out-of-memory handling, and that handling, not
     * this case, decides what becomes of the member.
     */
    private static void runOutOfMemoryOnAnEngineThread() {
        Thread engineThread = new HazelcastManagedThread(() -> {
            throw new OutOfMemoryError(THE_COLLECTOR_GAVE_UP);
        }, "e2e-engine-out-of-memory");
        engineThread.start();
        Await.until("the engine thread to hand its out-of-memory error over and end",
                () -> !engineThread.isAlive(),
                () -> "the thread is still " + engineThread.getState());
    }

    private static List<HazelcastInstance> runningMembers() {
        return Hazelcast.getAllHazelcastInstances().stream()
                .filter(AnEngineOutOfMemoryFailsTheHealthCheckAndItsPipelinesIT::isRunning)
                .toList();
    }

    private static boolean isRunning(HazelcastInstance member) {
        try {
            return member.getLifecycleService().isRunning();
        } catch (HazelcastInstanceNotActiveException gone) {
            return false;
        }
    }

    /** One run of the image's health check: whether it passed, and what it was answered. */
    private record HealthCheck(boolean passes, String answer) {

        @Override
        public String toString() {
            return (passes ? "passes" : "fails") + " (" + answer + ")";
        }
    }

    /** The image's health check, run the way the image runs it: {@code curl -f} against {@code /healthz}. */
    private static HealthCheck healthCheck(URI baseUrl) {
        HttpRequest probe = HttpRequest.newBuilder(baseUrl.resolve("/healthz"))
                .timeout(HEALTH_CHECK_TIMEOUT)
                .GET()
                .build();
        try {
            HttpResponse<String> answer = HTTP.send(probe, HttpResponse.BodyHandlers.ofString());
            return new HealthCheck(answer.statusCode() < 400, "HTTP " + answer.statusCode() + " " + answer.body());
        } catch (IOException noAnswer) {
            return new HealthCheck(false, "no answer: " + noAnswer);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while running the health check", interrupted);
        }
    }

    /** An answer from the server as a reader receives it, or the reason there was none. */
    private record Answer(int status, String body) {

        @Override
        public String toString() {
            return status < 0 ? body : "HTTP " + status + " " + body;
        }
    }

    private static Answer status(URI baseUrl, String credential) {
        return get(baseUrl, "/api/pipelines/" + PIPELINE + "/status", credential);
    }

    private static Answer get(URI baseUrl, String path, String credential) {
        HttpRequest request = HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(READ_TIMEOUT)
                .header("Authorization", "Bearer " + credential)
                .GET()
                .build();
        try {
            HttpResponse<String> answer = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            return new Answer(answer.statusCode(), answer.body());
        } catch (IOException noAnswer) {
            return new Answer(-1, "no answer: " + noAnswer);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while reading " + path, interrupted);
        }
    }

    /** The lifecycle state a status answer carries, or null when it carries none. */
    private static String stateIn(Answer status) {
        if (status.status() != 200 || !(JsonReader.parse(status.body()) instanceof Map<?, ?> answer)) {
            return null;
        }
        return answer.get("state") instanceof String state ? state : null;
    }

    /** The rendered message of the failure a status answer carries, or null when it carries none. */
    private static String failureMessageIn(Answer status) {
        if (status.status() != 200 || !(JsonReader.parse(status.body()) instanceof Map<?, ?> answer)
                || !(answer.get("failure") instanceof Map<?, ?> failure)) {
            return null;
        }
        return failure.get("message") instanceof String message ? message : null;
    }

    private static String sourceYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: src_file
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(E2eConnectorJar.CONNECTOR_ID, directory, TABLE);
    }

    private static String targetYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_file
                connector: %s
                config: { uri: "%s" }
                """.formatted(E2eConnectorJar.CONNECTOR_ID, directory);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_file
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: %s
                  sync:
                    - source: tgt_file
                """.formatted(PIPELINE, TABLE);
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException cannotRead) {
            throw new UncheckedIOException("cannot read the connector jar", cannotRead);
        }
    }
}
