package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
 * {@code tail} shows a change to any row, and gives back what it held when the client goes.
 *
 * <p>Two commands watch a collection and they differ in what they watch: {@code watch} holds one row
 * and redraws it, {@code tail} reports every change to the whole collection. Said that way the
 * difference sounds obvious, and it is exactly the kind of difference an implementation can lose while
 * both commands still appear to work - a {@code tail} built by polling one row would look right until
 * somebody changed a different one.
 *
 * <p>So the row that changes is deliberately not the one {@code watch} would be holding. That makes
 * the two commands answer differently about the same event, which is the only way to show from outside
 * that the difference is real rather than a difference in what the help text says.
 *
 * <p>The third assertion is about what is left behind. A follow holds a connector instance for as long
 * as it is open, and instances are capped for the host; the leak this guards - a client that walked
 * away leaving its follow running - reports nothing at all when it happens. It surfaces much later, as
 * somebody else being refused, with nothing connecting the two. There is no count to read from out
 * here, so the cap itself is made to answer: every place is taken, given back, and then asked for
 * again. A follow that outlives its client leaves that last request refused.
 */
class TailIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final String SOURCE_ID = "src_rows";
    private static final String COLLECTION = "orders";

    /** How many rows are there to change; the one changed is deliberately not the first. */
    private static final int SEEDED_ROWS = 5;
    private static final int CHANGED_ROW = 4;

    /**
     * The host's ceiling on live connector instances. Named here rather than read from the product: a
     * test that asked the product how many it allows would agree with it however wrong it was.
     */
    private static final int CEILING = 16;

    /**
     * Cursor movement up over the lines already drawn - what the in-place view writes to put its next
     * frame over the last one. A pattern rather than a literal because the count is the height of the
     * frame before it, which is a fact about the row rather than about redrawing.
     */
    private static final String CURSOR_UP = "\\[\\d+A";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path dataDirectory;

    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        E2eConnectorJar.buildInto(connectorJars, E2eConnectorJar.BROWSABLE_CONNECTOR_ID);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @Test
    void reportsAChangeToARowNothingWasHoldingAndGivesTheInstanceBackAfterwards() {
        seed(SEEDED_ROWS);

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_tail"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);
            String connector = E2eConnectorJar.BROWSABLE_CONNECTOR_ID;
            control.registerConnector(connector, ConnectorJars.bytesFor(connector));
            control.apply(Map.of("src_rows.tap.yml", sourceYaml()));

            // (1) A change to a row nobody named reaches a tail. Driven on a pipe on purpose: this is
            // the command that is meant to work in one, and it is the difference the refusal that
            // watch gives in a pipe points at.
            try (CliProcess tail = CliProcess.onAPipe(Map.of("TAPSTATE_PASSWORD", PASSWORD),
                    "-c", server.baseUrl().toString(), "-u", USER,
                    "tail", SOURCE_ID + "." + COLLECTION)) {
                awaitFollowing(control);
                change(CHANGED_ROW);
                tail.awaitOutput(seen -> seen.contains(marker(CHANGED_ROW)), TIMEOUT,
                        "the change to row " + CHANGED_ROW + " to reach a tail of the whole collection");
            }

            // (2) The control group, and the reason (1) says anything. The same event, watched by the
            // other command holding a different row, must not show up - otherwise "tail follows the
            // whole collection" is a sentence about a command that behaves identically to its
            // neighbour. On a terminal because watch refuses a pipe, which is its own case.
            // No filter: unasked, watch holds the first row the database hands back, which for this
            // connector is the first in the file. Which row that is does not have to be guaranteed for
            // the assertion below to hold - it only has to not be the one that changes.
            try (CliProcess watch = CliProcess.onATerminal(Map.of("TAPSTATE_PASSWORD", PASSWORD),
                    "-c", server.baseUrl().toString(), "-u", USER,
                    "watch", SOURCE_ID + "." + COLLECTION)) {
                String firstFrame = watch.awaitOutput(seen -> seen.contains("row-1"), TIMEOUT,
                        "watch to draw the row it was asked to hold");
                // The first frame is drawn where the prompt left off, so it carries no cursor movement.
                // Asserted so that the one below is read off a frame that really did replace this one.
                assertThat(firstFrame)
                        .as("the first frame this command draws")
                        .doesNotContainPattern(CURSOR_UP);

                change(CHANGED_ROW);
                // Then change the row it *is* holding. Its arrival is what makes the absence above a
                // fact rather than a wait: the two changes are ordered, so once the second has been
                // drawn the first has already been seen and not drawn.
                String held = "held-and-changed";
                change(1, held);
                String shown = watch.awaitOutput(seen -> seen.contains(held), TIMEOUT,
                        "watch to redraw the row it is holding after that row changed");
                assertThat(shown)
                        .as("what watch drew while a row it was not holding changed")
                        .doesNotContain(marker(CHANGED_ROW));
                // It redrew rather than appended. This is the one place that says so on the lane every
                // change runs through: the witness that drives this command against a pipeline needs real
                // connector jars, so on an ordinary run it does not execute at all - and the bytes being
                // asserted here are the whole of drawing in place, and the reason this command refuses to
                // run down a pipe.
                assertThat(shown)
                        .as("what watch wrote to put the new frame over the old one")
                        .containsPattern(CURSOR_UP);
            }

            // (3) The place it held is given back. Taking every place, releasing them, and asking again
            // is the only way to read a count this face does not publish - and it is the shape the leak
            // actually has, since what a leaked follow costs is somebody else's request, later.
            List<ControlPlane.Follow> holding = new ArrayList<>();
            try {
                for (int taken = 0; taken < CEILING; taken++) {
                    holding.add(control.follow(SOURCE_ID, COLLECTION, null));
                }
                // The cap is real, which is what makes releasing it mean something - without this the
                // last assertion would pass just as well on a host that had no cap at all. Read off
                // the close rather than the open: the handshake succeeds and the refusal follows it,
                // so a caller expecting the connection to fail would see nothing wrong here.
                try (ControlPlane.Follow refused = control.follow(SOURCE_ID, COLLECTION, null)) {
                    assertThat(refused.awaitClose(TIMEOUT, "the refusal of a follow past the ceiling"))
                            .as("what the product said when every place was already taken")
                            .contains("connector.instance-limit-reached");
                }
            } finally {
                holding.forEach(ControlPlane.Follow::close);
            }
            // Not merely accepted - actually working. A follow that opened and then sat deaf would
            // pass an "it was not refused" assertion while holding nothing anybody could use.
            try (ControlPlane.Follow afterwards = control.follow(SOURCE_ID, COLLECTION, null)) {
                change(2);
                afterwards.awaitFrame(frame -> true, TIMEOUT,
                        "a change on a follow opened after every earlier one was closed");
            }
        }
    }

    /**
     * Waits until the follow behind the command is actually running, by changing a row it is not being
     * asked about and watching that arrive.
     *
     * <p>Without this the change under test could be made before the stream started, and the case would
     * fail reporting that a tail does not report changes - which would be a true sentence about a
     * product that works. The row used is the first one, and the assertion below is about the fourth.
     */
    private void awaitFollowing(ControlPlane control) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        try (ControlPlane.Follow probe = control.follow(SOURCE_ID, COLLECTION, null)) {
            int nudge = 0;
            while (System.nanoTime() - deadline < 0) {
                change(1, "waking-" + nudge++);
                if (!probe.frames().isEmpty()) {
                    return;
                }
                sleep();
            }
        }
        throw new AssertionError("no change reached a follow within " + TIMEOUT
                + ", so nothing here could tell a stream that had not started from one with nothing to say");
    }

    private void seed(int rows) {
        StringBuilder csv = new StringBuilder("id,note\n");
        for (int row = 1; row <= rows; row++) {
            csv.append(row).append(',').append("row-").append(row).append('\n');
        }
        write(csv.toString());
    }

    /** Rewrites the collection with one row carrying a value nothing else in it has. */
    private void change(int row) {
        change(row, marker(row));
    }

    private void change(int row, String note) {
        StringBuilder csv = new StringBuilder("id,note\n");
        for (int each = 1; each <= SEEDED_ROWS; each++) {
            csv.append(each).append(',').append(each == row ? note : "row-" + each).append('\n');
        }
        write(csv.toString());
    }

    private static String marker(int row) {
        return "changed-row-" + row;
    }

    private void write(String csv) {
        try {
            Files.writeString(dataDirectory.resolve(COLLECTION + ".csv"), csv);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String sourceYaml() {
        return """
                version: tapstate/v1
                kind: source
                id: src_rows
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(dataDirectory);
    }

    private static void sleep() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for a follow to come up", e);
        }
    }
}
