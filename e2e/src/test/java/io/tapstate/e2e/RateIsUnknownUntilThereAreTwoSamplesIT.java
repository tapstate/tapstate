package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shipped command line says a pipeline's rate is not known until it has two readings, and then says
 * a number -- never a nought in place of the first, and never a "not known" that outlives the second.
 *
 * <p>Two verbs, two shapes of the same rule. A watch opens with one reading and says so on its first
 * line; after rows move and its next reading lands, it prints rows per second. A one-shot status takes
 * both readings itself, waiting for the server to publish a newer observation, and prints rows per second
 * in the one answer -- with a number, which may be nought when nothing moved, but only ever after two
 * readings.
 *
 * <p>The first line of the watch is what decides the first half: a watch that printed {@code 0.0 rows/s}
 * there would read as a pipeline moving nothing, which is what a stalled one looks like, and the two call
 * for different next steps. Rows are then moved on purpose, so the number that follows is a positive one
 * and not a nought that a broken rate could also print.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else; the CLI runs
 * as its own process on the classpath the build recorded.
 */
class RateIsUnknownUntilThereAreTwoSamplesIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final long ROWS_TO_MOVE = 200;

    /** The outbound rate on a movement line; the line names every direction it counts, in name order. */
    private static final Pattern RATE = Pattern.compile("moving  .*\\bout (\\d+\\.\\d) rows/s");

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aWatchOpensNotKnowingAndThenSaysANumberAndAOneShotStatusSaysOne(@TempDir Path directory) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("rate_two_samples_state");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            RunningPipeline running = RunningPipeline.started(server, directory);
            String pipelineId = running.pipelineId();

            try (CliProcess watch = CliProcess.onAPipe(Map.of("TAPSTATE_PASSWORD", PASSWORD),
                    "-c", server.baseUrl().toString(), "-u", USER,
                    "status", pipelineId, "--watch")) {
                String opened = watch.awaitOutput(seen -> seen.contains(pipelineId + "  moving  not known"), TIMEOUT,
                        "the watch's first movement line, which has one reading and says so");
                assertThat(opened).as("one reading is not a rate of nought").doesNotContain("rows/s");

                running.insertAtSource(ROWS_TO_MOVE);
                String moved = watch.awaitOutput(seen -> positiveRate(seen) != null, TIMEOUT,
                        "a rate off two readings after rows moved");
                assertThat(moved.indexOf("moving  not known"))
                        .as("the watch said it did not know before it said a number")
                        .isLessThan(moved.indexOf(positiveRate(moved)));
            }

            // Asked for, because this session's output is a pipe rather than a terminal: the second
            // reading a rate is made of costs a wait, and a verb people run in a loop does not spend it
            // unless somebody is looking or somebody asks. Both halves are witnessed here, since a flag
            // nobody exercises is a flag that stops working.
            CliOnce.Run status = CliOnce.runSession(PASSWORD, "status " + pipelineId + " --rate\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);
            assertThat(status.exitCode())
                    .as("the session must have run; stdout was:%n%s%nstderr was:%n%s", status.stdout(), status.stderr())
                    .isZero();
            // The evidence lines, printed: what the one-shot answer actually said, beside the assertion on it.
            status.stdout().lines().filter(line -> line.startsWith("moving") || line.startsWith("lag"))
                    .forEach(line -> System.out.println("one-shot status said: " + line));
            assertThat(status.stdout())
                    .as("a one-shot status asked for a rate takes its second reading itself")
                    .containsPattern("moving     .*\\bout \\d+\\.\\d rows/s \\(over \\d+\\.\\ds of the pipeline's own time\\)")
                    .contains("lag        ");

            CliOnce.Run plain = CliOnce.runSession(PASSWORD, "status " + pipelineId + "\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);
            assertThat(plain.exitCode()).isZero();
            plain.stdout().lines().filter(line -> line.startsWith("moving"))
                    .forEach(line -> System.out.println("one-shot status without --rate said: " + line));
            assertThat(plain.stdout())
                    .as("without a terminal and without the flag it answers at once, and says how to ask")
                    .contains("moving     not known -- one reading")
                    .contains("--rate")
                    .doesNotContain("rows/s");
        }
    }

    /** The first rate line whose number is above nought, or null when none has been printed yet. */
    private static String positiveRate(String output) {
        Matcher matcher = RATE.matcher(output);
        while (matcher.find()) {
            if (Double.parseDouble(matcher.group(1)) > 0) {
                return matcher.group();
            }
        }
        return null;
    }
}
