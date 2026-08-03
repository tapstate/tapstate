package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * <p>The count it settles on is then read again, from outside the example's own addressing. Every await
 * resolves its address out of the example's own resource, so an example whose target names the source's
 * address counts the rows the harness seeded itself: it settles on the first poll, before the product has
 * emitted anything, and is green with the connector deleted. Resolving faithfully is not enough when the
 * resource is what is wrong - a faithful lookup returns the wrong directory just as faithfully - so the
 * last word belongs to a reading the example cannot influence. For a file example that is a count over
 * the target directory this run handed out. For an example that asked for stores, it is two facts read
 * over the run's own handles: the store its settled table actually landed on is not one the seed went
 * into, and that store's own count agrees - together they refuse the seeded rows counted back, however
 * the resource came to point there.
 *
 * <p>That read is what obliges an example to settle on a count, and to mean the target by it. The
 * obligation is small and it is checked: an example that never settles fails before it runs, rather than
 * quietly skipping the one assertion that does not take its word for anything.
 *
 * <p>An example naming real connectors is gated the way any real-connector witness is: naming no
 * connectors directory skips it, a directory whose jars do not resolve fails it. The gate reads the
 * directory named from outside before this class points the registration path at its own staging
 * directory, so the decision is about the operator's intent, not about the staging.
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
    private boolean connectorsDirOverridden;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    /**
     * Stages every jar the example registers into a directory of this run's own, then points the
     * registration path there. The synthetic connector is built fresh; real connectors are copied from
     * the directory named from outside, gated first - so a developer machine skips a real-connector
     * example the way it skips any real-connector witness, and a named-but-broken directory fails it.
     * The gate must run before the property is overridden: it is asking about the outside directory.
     */
    private void stageConnectorJars(List<String> connectorIds) {
        List<String> real = connectorIds.stream()
                .filter(connectorId -> !E2eConnectorJar.CONNECTOR_ID.equals(connectorId))
                .toList();
        if (!real.isEmpty()) {
            RealConnectorGate.require(real.toArray(String[]::new));
            try {
                for (String connectorId : real) {
                    Files.write(connectorJars.resolve(connectorId + "-connector.jar"),
                            ConnectorJars.bytesFor(connectorId));
                }
            } catch (IOException e) {
                throw new UncheckedIOException("cannot stage real connector jars for this run", e);
            }
        }
        E2eConnectorJar.buildInto(connectorJars);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
        connectorsDirOverridden = true;
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (!connectorsDirOverridden) {
            return;
        }
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

        stageConnectorJars(envelope.setup().connectors());

        String run = store(workspace, tier);
        // The stores the example asked for come up before anything else: a resource cannot be applied
        // before the endpoint whose address it interpolates exists.
        try (ProvisionedStores stores = ProvisionedStores.provision(envelope.setup().databases(), run);
                ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(run));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, drivers(files, stores), env(stores));

            new E2eExecutor(binding, new FilePipelineLoader(workspace), TIMEOUT, POLL).execute(envelope);

            if (envelope.setup().databases().isEmpty()) {
                settled.forEach((alias, rows) -> assertThat(
                                files.count(EndpointAddress.uri(targetDirectory.toString()), alias.table()))
                        .as("%s settles on %s rows in %s, read there by the address it named; this reads the "
                                + "target this run handed out, which it cannot name", specification, rows, alias)
                        .isEqualTo(rows));
            } else {
                theSettledCountIsInAStoreTheSeedNeverTouched(specification, envelope, settled, binding, stores);
            }
        }
        // Last line on purpose: the ledger vouches only for a run that held every assertion above,
        // including the independent read. The release gate reads absence from it, so nothing may be
        // recorded on a path that can be reached without the assertions.
        WitnessLedger.record(workspace.getFileName().toString(), tier);
    }

    /**
     * The independent read for an example that asked for stores. The example's own awaits already read
     * through its resources; what remains unproven is that those resources point where they claim. The
     * bridge from a resource to a store is the interpolation under test, so it is not consulted: the
     * store an address lands on is told by the database name this run minted, and the closing count is
     * taken over the run's own handle. An example whose target interpolated the source's references
     * settles its count in the very store the seed went into - the first assertion refuses exactly that.
     */
    private void theSettledCountIsInAStoreTheSeedNeverTouched(
            Path specification,
            Envelope envelope,
            Map<TableAlias, Long> settled,
            HttpTierBinding binding,
            ProvisionedStores stores) {
        Set<String> seeded = new LinkedHashSet<>();
        for (Seed seed : envelope.seed()) {
            EndpointAddress address = binding.addressOf(seed.table());
            Optional<String> holder = stores.storeHolding(address);
            // A seed that landed on nothing this run handed out is not one to skip over. Its resource is
            // pointing at an endpoint the run did not provision, which is the same failure of
            // interpolation this guard exists to catch - and skipping it would quietly shrink the set
            // below, so a target sharing the source's store would then pass unremarked. The reading is
            // taken here rather than from the resource text, which is the thing under test.
            assertThat(holder.isPresent() || namesSomethingThisRunHandedOut(address))
                    .as("%s seeds %s at %s, which is neither a store this run provisioned nor a directory "
                            + "it handed out: the resource is not pointing where the run put its endpoint",
                            specification, seed.table(), address.settings())
                    .isTrue();
            holder.ifPresent(seeded::add);
        }
        settled.forEach((alias, rows) -> {
            Optional<String> holder = stores.storeHolding(binding.addressOf(alias));
            assertThat(holder)
                    .as("%s settles on %s, whose resource names none of the stores this run brought up",
                            specification, alias)
                    .isPresent();
            assertThat(holder.get())
                    .as("%s settles its count in the store its seed went into: the rows it counted are "
                            + "the harness's own, not rows the product moved", specification)
                    .isNotIn(seeded);
            assertThat(stores.count(holder.get(), alias.table()))
                    .as("%s settles on %s rows in %s; this reads the same store by the handle this run "
                            + "kept for itself, which the example cannot name", specification, rows, alias)
                    .isEqualTo(rows);
        });
    }

    /**
     * The file driver plus one per store the example asked for. The file driver is always present because
     * the synthetic connector needs no provisioning - it reads a directory this run made.
     */
    private static Map<String, Endpoints> drivers(Endpoints files, ProvisionedStores stores) {
        Map<String, Endpoints> drivers = new LinkedHashMap<>();
        drivers.put(E2eConnectorJar.CONNECTOR_ID, files);
        drivers.putAll(stores.driversByConnector());
        return drivers;
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
            // The same digest the database naming uses, and for the same reason: this id is what keeps
            // two runs off each other's data, so what shortens it has to keep it unique. A 32-bit
            // hashCode does not - pairs that agree on it can be constructed - and a collision here
            // would be inherited by every database name built from this id.
            String digest = ProvisionedStores.digest(name);
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
    /** Whether an address names one of the two directories this run handed out for the file store. */
    private boolean namesSomethingThisRunHandedOut(EndpointAddress address) {
        return namesOneOf(address, List.of(sourceDirectory.toString(), targetDirectory.toString()));
    }

    /**
     * The same reading {@code storeHolding} takes of a provisioned database, applied to the run's own
     * directories: an address is one of them when a setting carries the path. Held apart from the
     * caller so it can be checked without a run.
     */
    static boolean namesOneOf(EndpointAddress address, List<String> handedOut) {
        return address.settings().values().stream()
                .filter(java.util.Objects::nonNull)
                .anyMatch(value -> handedOut.stream().anyMatch(value.toString()::contains));
    }

    private UnaryOperator<String> env(ProvisionedStores stores) {
        Map<String, String> environment = new LinkedHashMap<>(stores.environment());
        environment.put("SRC_DIR", sourceDirectory.toString());
        environment.put("TGT_DIR", targetDirectory.toString());
        return environment::get;
    }
}
