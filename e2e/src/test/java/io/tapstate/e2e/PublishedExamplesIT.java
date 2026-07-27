package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs every published example, on every tier.
 *
 * <p>The sweep is what makes an example real. Naming the examples here instead - one constant per test -
 * would leave two sets that nothing reconciles: the ones published under {@code examples/} and the ones
 * some test happens to name. A specification could then sit in the working tree, be parsed, be validated
 * against the schema, be read and copied by an author as a working sample, and never once be run. It
 * could name a read the runtime ignores or a step the product renamed and stay green forever, looking
 * exactly like the examples that do run. Discovering them is what forbids that: to publish one is to run
 * it.
 *
 * <p>Executing the specification is most of the assertion. Its awaits are bounded and read the target
 * through the harness, so a run that returns is a run whose every await held; the executor throws
 * otherwise. What each example is for, and why its numbers are what they are, is written in the example
 * itself - it is the file an author opens.
 *
 * <p>The count it settles on is then read again, by the path this class handed out rather than by the
 * alias the example named. Every await resolves its address out of the example's own resource, so an
 * example whose target names the source's address counts the rows the harness seeded itself: it settles
 * on the first poll, before the product has emitted anything, and is green with the connector deleted.
 * Resolving faithfully is not enough when the resource is what is wrong - a faithful lookup returns the
 * wrong directory just as faithfully - so the last word belongs to an address the example cannot name.
 *
 * <p>That read is what obliges an example to settle on a count, and to mean the target by it. The
 * obligation is small and it is checked: an example that never settles fails before it runs, rather than
 * quietly skipping the one assertion that does not take its word for anything.
 */
class PublishedExamplesIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(200);

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        E2eConnectorJar.buildInto(connectorJars);
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

    static Stream<Arguments> everyPublishedExampleOnEveryTier() {
        return Examples.specifications().stream()
                .flatMap(specification -> Stream.of(Tiers.values())
                        .map(tier -> Arguments.of(specification, tier)));
    }

    @ParameterizedTest(name = "{0} on {1}")
    @MethodSource("everyPublishedExampleOnEveryTier")
    void thePublishedExampleRuns(Path specification, Tiers tier) {
        Path workspace = specification.getParent();
        Envelope envelope = EnvelopeParser.parse(Examples.read(specification));

        Map<TableAlias, Long> settled = theCountItSettlesOn(envelope);
        assertThat(settled)
                .as("%s never settles on a count, so nothing outside its own addressing could check it",
                        specification)
                .isNotEmpty();

        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(store(workspace, tier)));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.CONNECTOR_ID, files), env());

            new E2eExecutor(binding, new FilePipelineLoader(workspace), TIMEOUT, POLL).execute(envelope);

            settled.forEach((alias, rows) -> assertThat(files.count(EndpointAddress.uri(targetDirectory.toString()), alias.table()))
                    .as("%s settles on %s rows in %s, read there by the address it named; this reads the "
                            + "target this run handed out, which it cannot name", specification, rows, alias)
                    .isEqualTo(rows));
        }
    }

    /**
     * The count an example settles on: the last one it awaits or asserts, which is the reading it claims
     * is final. Read backwards because the earlier counts are waypoints - the first example passes through
     * two on the way to three - and only the last one describes a target at rest.
     */
    private static Map<TableAlias, Long> theCountItSettlesOn(Envelope envelope) {
        for (int index = envelope.steps().size() - 1; index >= 0; index--) {
            Matcher matcher = switch (envelope.steps().get(index)) {
                case Step.Await await -> await.matcher();
                case Step.Assertion assertion -> assertion.matcher();
                case Step.Lifecycle ignored -> null;
                case Step.Cdc ignored -> null;
            };
            if (matcher instanceof Matcher.Count count) {
                return count.expected();
            }
        }
        return Map.of();
    }

    /** Mongo refuses a database name longer than this, and says so only once a run is already underway. */
    private static final int NAME_LIMIT = 63;

    /**
     * One database per example per tier. Sharing one would leave the previous example's resources behind,
     * and the driver reconciling them would dial the temporary directories of a run that has ended.
     *
     * <p>An example names itself as long as it likes, so the name is trimmed to fit and a digest of the
     * whole of it is appended - two examples that trim to the same prefix still get a database each.
     */
    private static String store(Path workspace, Tiers tier) {
        String head = "e2e_";
        String tail = "_" + tier.name().toLowerCase(Locale.ROOT);
        String name = workspace.getFileName().toString().toLowerCase(Locale.ROOT).replace('-', '_');
        int room = NAME_LIMIT - head.length() - tail.length();
        if (name.length() > room) {
            String digest = Integer.toHexString(name.hashCode());
            name = name.substring(0, room - digest.length() - 1) + "_" + digest;
        }
        return head + name + tail;
    }

    /**
     * The harness is the client, so the addresses the published references resolve to are its own. The two
     * directories are deliberately different: a sink names its target table after the source row's table,
     * so one directory would have a pipeline write back over the file the harness seeded, and a count would
     * then read the harness's own rows without a single row having crossed the product.
     */
    private UnaryOperator<String> env() {
        return Map.of(
                "SRC_DIR", sourceDirectory.toString(),
                "TGT_DIR", targetDirectory.toString())::get;
    }
}
