package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A run whose publisher has stopped does not read like a healthy run whose state has not changed.
 *
 * <p>These two situations used to be byte-identical on the status face. A pipeline sitting at
 * {@code RUNNING} with nothing new to carry, and a pipeline whose server stopped saying anything about
 * it minutes ago, both answered {@code RUNNING} and nothing else -- so the operator's only way to tell
 * them apart was to know, from somewhere else entirely, whether the server was still converging. What
 * separates them now is that the answer says how old it is.
 *
 * <p><b>The steady half is asserted first, and it is not decoration.</b> "The age grows past a bound"
 * is satisfied by an age that was always past it, which is what a broken clock or a timestamp fixed at
 * the epoch would produce. So the same pipeline is read while its publisher is running, and that
 * reading has to be small; only then is the same reading taken again after the publisher stops.
 *
 * <p><b>What it takes to stop a publisher, and why it is done from inside.</b> There is one supported
 * role and it both converges and serves reads, and one store behind both, so there is no half of this
 * product that can be stopped from outside while the other half keeps answering -- stopping the server
 * takes the status face down with it, and making the store unreachable takes it down twice. The
 * scheduled pass is cancelled in the running application instead, which is why this case names the
 * in-process tier rather than driving {@link ServerHandle}. The cancel reports how many tasks it
 * cancelled and this case insists on exactly one: matching the publisher by name is otherwise a silent
 * no-op the moment it is renamed, and a no-op here produces a healthy run that the assertions below
 * would be measuring instead.
 *
 * <p><b>Discrimination.</b> Take the age away -- the observation no longer records when it was taken --
 * and every assertion below has nothing to read: the two situations go back to being one word, and the
 * case cannot be written at all, let alone passed.
 */
@DisplayName("a run whose publisher stopped is told apart from a steady run")
class AStalledPublisherIsToldApartFromASteadyRunIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final String PIPELINE = "publisher_stall_witness";
    private static final String TABLE = "orders";

    /** How long a reading may be while the publisher is running. The pass runs every second by default. */
    private static final Duration FRESH_ENOUGH = Duration.ofSeconds(15);

    /**
     * How far past that the age must climb once the publisher stops. Comfortably above the steady bound,
     * so a slow pass cannot be mistaken for a stalled one, and comfortably under the wait that follows.
     */
    private static final Duration CLEARLY_STALE = Duration.ofSeconds(25);

    /** Readings taken after the stop, far enough apart that a growing age is visible between them. */
    private static final int READINGS = 3;
    private static final Duration BETWEEN_READINGS = Duration.ofSeconds(10);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theAnswerGrowsOldWhileTheStateWordDoesNotChange(
            @TempDir Path source, @TempDir Path target, @TempDir Path jars) {
        FileEndpoints.replaceTable(source.resolve(TABLE + ".csv"), "id,amount\n1,12\n");

        try (InProcessServer server = InProcessServer.start(SharedMongo.replicaSetUrl("publisher_stall"))) {
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

            // The steady reading. Without it, every assertion below is also satisfied by an age that was
            // never small -- a clock at the epoch reads as stale from the first request.
            long whileSteady = ageOf(control);
            assertThat(whileSteady)
                    .as("while its publisher is running, the answer is recent")
                    .isLessThan(FRESH_ENOUGH.toMillis());
            PipelineState steadyState = control.state(PIPELINE).orElseThrow();

            assertThat(server.stopPublishingObservations())
                    .as("the publisher must actually have been stopped; nothing below measures anything "
                            + "if this cancelled no task")
                    .isEqualTo(1);

            List<Long> ages = new ArrayList<>();
            for (int reading = 0; reading < READINGS; reading++) {
                letTimePass(BETWEEN_READINGS);
                ages.add(ageOf(control));
            }

            assertThat(ages)
                    .as("with nobody publishing, each reading is older than the one before it: %s", ages)
                    .isSorted()
                    .doesNotHaveDuplicates();
            assertThat(ages.getLast())
                    .as("and the age has climbed clear of anything a running publisher produces")
                    .isGreaterThan(CLEARLY_STALE.toMillis());
            assertThat(control.state(PIPELINE))
                    .as("while the state word itself does not change -- nothing has seen the job die, and "
                            + "guessing it into a failure would be the dishonest half of this")
                    .contains(steadyState);
        }
    }

    /** The product's own account of how old its answer is; absent means it published nothing, which is a failure here. */
    private static long ageOf(ControlPlane control) {
        return control.observedAgeMillis(PIPELINE)
                .orElseThrow(() -> new AssertionError("the status face published no observation to age"));
    }

    /** A gap between readings, waited for as a condition so this module's no-fixed-sleep gate stays honest. */
    private static void letTimePass(Duration howLong) {
        long deadline = System.nanoTime() + howLong.toNanos();
        Await.until("the gap between two readings", howLong.plusSeconds(20),
                () -> System.nanoTime() - deadline >= 0, () -> "waiting between readings");
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
