package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.SystemMetaStore;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A store a released build wrote, opened by this one, ending up as what a fresh install of this one
 * would have built.
 *
 * <p>Everything else about the migration is witnessed against a store this build made itself, which
 * cannot answer the question the migration exists for: whether it can read what somebody actually has
 * on disk. The older shape here is not a fixture written to look old -- a fixture is written from the
 * same understanding as the code that reads it, so the two agree by construction and agree just as
 * happily when both are wrong. It is written by the released build, running as its published image,
 * driving the same specification through the whole product.
 *
 * <p>The two stores are compared rather than the migrated one being checked against expectations.
 * Expectations are the thing a missed changeset also gets to write. A store built fresh by this build
 * is the only description of "right" that no mistake in the migration can reach, so the claim is that
 * upgrading and installing arrive at the same place.
 *
 * <p>Both servers reach the same files because the workspace is mounted into the container at the
 * path it has out here. A file endpoint's address is a path and it is stored inside the resource, so
 * any other mount point would leave the older build's resources naming somewhere the newer build
 * cannot open -- and the upgrade would look like it had lost the pipeline.
 *
 * <p><b>What comparing against a fresh install structurally cannot see.</b> Both stores are brought
 * forward by the same runner from the same starting version, so a fault that hits the upgrade and the
 * fresh install equally cancels out of the comparison and this passes. Measured, not reasoned about:
 * making the runner jump to the newest pending changeset instead of applying each in turn -- the
 * "upgrading two releases at once only runs the last script" fault -- leaves this green, because a
 * fresh install skips the same steps and the two agree on having skipped them. Three cases a level
 * down go red on it.
 *
 * <p>So this case is not where stepwise application is held, and nothing here should be read as
 * covering it. What it holds is the half no unit-level case can reach: that the data a released build
 * actually wrote is readable, convertible, and still serves a running pipeline afterwards.
 */
class UpgradeFromPublishedImageIT {

    /**
     * The released build to upgrade from, taken from the stack that ships with this checkout rather
     * than named here. A version written into a test is right until the next release and then quietly
     * wrong; the shipped stack has to name the current release, and a gate on the release path fails
     * when it does not, so reading it here cannot drift.
     */
    private static final Path SHIPPED_STACK = Path.of("..", "deploy", "quickstart", "docker-compose.yml");

    private static final Pattern IMAGE = Pattern.compile("image:\\s*(ghcr\\.io/tapstate/tapstate:\\S+)");

    /** The published example this drives. File-ended on purpose: see the class note about paths. */
    private static final String EXAMPLE = "rows-cross-from-a-source-file-to-a-target-file";

    /** The property the registration path reads to find a connector jar by id. */
    private static final String CONNECTORS_DIRECTORY = "tapstate.e2e.connectors-dir";

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    private static final String UPGRADED = "e2e_upgraded";
    private static final String INSTALLED_FRESH = "e2e_installed_fresh";

    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final Duration POLL = Duration.ofMillis(250);

    /** Fields that differ between any two runs and say nothing about shape. */
    private static final Set<String> NOT_PART_OF_THE_SHAPE = Set.of("_id");

    @TempDir
    private Path workspace;

    @BeforeAll
    static void requireTheUpgradeLane() {
        // Asked for before Docker is: this witness reaches for a published release, so it belongs to
        // the lane that may fail over a stale release rather than to every pull request.
        UpgradeLaneGate.require();
        DockerGate.require();
    }

    /**
     * Where the harness's own connector is staged, and what the registration path is pointed at while
     * this runs. It is a synthetic connector built fresh rather than one of the real ones, so this
     * needs no outside directory and does not skip on a machine that has none.
     */
    private String previousConnectorsDirectory;

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDirectory == null) {
            System.clearProperty(CONNECTORS_DIRECTORY);
        } else {
            System.setProperty(CONNECTORS_DIRECTORY, previousConnectorsDirectory);
        }
    }

    @Test
    void aStoreTheReleasedBuildWroteBecomesWhatAFreshInstallOfThisBuildWouldHave() {
        String released = releasedImage();
        Path connectorStage = directory("connectors");
        Path connectorJar = E2eConnectorJar.buildInto(connectorStage);
        previousConnectorsDirectory =
                System.setProperty(CONNECTORS_DIRECTORY, connectorStage.toString());

        try (NetworkedMongo mongo = NetworkedMongo.start()) {
            Example upgraded = new Example("upgraded");
            try (PublishedImageServer older = PublishedImageServer.start(
                    released, mongo.network(), mongo.uriForContainers(UPGRADED), connectorJar, workspace)) {
                upgraded.driveThrough(older);
            }

            try (MongoClient client = MongoClients.create(mongo.uriForThisHost(UPGRADED))) {
                MongoDatabase store = client.getDatabase(UPGRADED);

                // The premise, asserted rather than assumed: what the released build left really is the
                // older shape. Without this the whole case still passes if that build already wrote what
                // this one writes, having migrated nothing and proven nothing.
                assertThat(new SystemMetaStore(store).installedVersion())
                        .as("what the released build recorded, before this build opens the store")
                        .isEmpty();
                assertThat(storedResourcesHoldingText(store))
                        .as("resources the released build wrote as text")
                        .isPositive();

                try (ServerHandle upgrading = RealProcessServer.start(mongo.uriForThisHost(UPGRADED))) {
                    // The account the released build created still opens this one. Worth asserting for
                    // its own sake, and it is also what makes the comparison at the end sound: several
                    // collections are created the first time something asks for them rather than at
                    // startup, so two stores differ on which of those exist unless the same paths have
                    // been walked on both. This is the path the fresh install below walks.
                    new ControlPlane(upgrading.baseUrl()).login(USER, PASSWORD);

                    assertThat(new SystemMetaStore(store).installedVersion())
                            .as("the store after this build opened it")
                            .contains(MigrationRunner.SUPPORTED_VERSION);
                    assertThat(storedResourcesHoldingText(store))
                            .as("resources still holding text once the upgrade finished")
                            .isZero();

                    // The pipeline the released build left behind is still running, and still reading.
                    // A row put in after the upgrade is the only thing that separates a pipeline that
                    // resumed from one that merely still has a row count from before it.
                    upgraded.rowInsertedAfterTheUpgradeCrosses(upgrading);
                }
            }

            Example fresh = new Example("fresh");
            try (ServerHandle installing = RealProcessServer.start(mongo.uriForThisHost(INSTALLED_FRESH))) {
                fresh.driveThrough(installing);
            }

            try (MongoClient client = MongoClients.create(mongo.uriForThisHost(UPGRADED));
                    MongoClient other = MongoClients.create(mongo.uriForThisHost(INSTALLED_FRESH))) {
                MongoDatabase wasUpgraded = client.getDatabase(UPGRADED);
                MongoDatabase wasInstalled = other.getDatabase(INSTALLED_FRESH);

                assertThat(new SystemMetaStore(wasUpgraded).installedVersion())
                        .as("the upgraded store against the fresh one")
                        .isEqualTo(new SystemMetaStore(wasInstalled).installedVersion());
                assertThat(collectionsOf(wasUpgraded))
                        .as("which collections each store holds")
                        .isEqualTo(collectionsOf(wasInstalled));
                assertThat(indexesOf(wasUpgraded))
                        .as("the indexes on each collection")
                        .isEqualTo(indexesOf(wasInstalled));
                assertThat(shapeOfConvertedResources(wasUpgraded))
                        .as("the fields a stored resource carries")
                        .isEqualTo(shapeOfConvertedResources(wasInstalled));
            }
        }
    }

    /** One run of the published example, with directories of its own so two runs never share a file. */
    private final class Example {

        private final Path sourceDirectory;
        private final Path targetDirectory;

        private Example(String name) {
            this.sourceDirectory = directory(name + "-source");
            this.targetDirectory = directory(name + "-target");
        }

        private void driveThrough(ServerHandle server) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            // The first administrator is created over loopback or not at all. For a server in a
            // container this host is not loopback, so the container makes the call on its own behalf;
            // for one in a process here, this is loopback already.
            if (server instanceof PublishedImageServer containerised) {
                containerised.bootstrapFirstAdmin(USER, PASSWORD);
                control.login(USER, PASSWORD);
            } else {
                control.bootstrapAndLogin(USER, PASSWORD);
            }
            new E2eExecutor(binding(control), new FilePipelineLoader(workspace), TIMEOUT, POLL)
                    .execute(EnvelopeParser.parse(specification()));
        }

        /**
         * Puts one more row in after the upgrade and waits for it to arrive. The count asked for is one
         * above where the example settles, so a pipeline that stopped at the upgrade cannot satisfy it
         * by standing still.
         */
        private void rowInsertedAfterTheUpgradeCrosses(ServerHandle server) {
            FileEndpoints files = new FileEndpoints();
            EndpointAddress target = EndpointAddress.uri(targetDirectory.toString());
            long before = files.count(target, "orders");
            files.cdc(EndpointAddress.uri(sourceDirectory.toString()), "orders", CdcOp.INSERT, 2);
            Await.until("a row written after the upgrade to reach the target", TIMEOUT,
                    () -> files.count(target, "orders") > before,
                    () -> "the target still holds " + files.count(target, "orders")
                            + ", as it did before the row was inserted");
        }

        private HttpTierBinding binding(ControlPlane control) {
            Map<String, String> environment = new LinkedHashMap<>();
            environment.put("SRC_DIR", sourceDirectory.toString());
            environment.put("TGT_DIR", targetDirectory.toString());
            UnaryOperator<String> env = environment::get;
            return new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.CONNECTOR_ID, new FileEndpoints()), env);
        }
    }

    /** The example's own files, copied where the loader and the executor both look for them. */
    private String specification() {
        Path source = Path.of("examples", EXAMPLE);
        try (Stream<Path> files = Files.list(source)) {
            for (Path file : files.toList()) {
                Files.copy(file, workspace.resolve(file.getFileName()),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            return Files.readString(source.resolve("spec.e2e.yml"));
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the example at " + source, e);
        }
    }

    private String releasedImage() {
        try {
            Matcher found = IMAGE.matcher(Files.readString(SHIPPED_STACK));
            if (!found.find()) {
                throw new AssertionError(
                        "the shipped stack at " + SHIPPED_STACK + " names no published image, so there is "
                                + "no released build to upgrade from");
            }
            return found.group(1);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the shipped stack at " + SHIPPED_STACK, e);
        }
    }

    private Path directory(String name) {
        try {
            return Files.createDirectories(workspace.resolve(name));
        } catch (IOException e) {
            throw new UncheckedIOException("could not make " + name + " under " + workspace, e);
        }
    }

    private static long storedResourcesHoldingText(MongoDatabase store) {
        return SystemCollections.ARTIFACTS.on(store)
                .countDocuments(new Document("canonical", new Document("$exists", true)));
    }

    private static Set<String> collectionsOf(MongoDatabase store) {
        return new TreeSet<>(store.listCollectionNames().into(new ArrayList<>()));
    }

    private static Map<String, Set<String>> indexesOf(MongoDatabase store) {
        Map<String, Set<String>> byCollection = new LinkedHashMap<>();
        for (String collection : collectionsOf(store)) {
            byCollection.put(collection, store.getCollection(collection).listIndexes().into(new ArrayList<>())
                    .stream().map(index -> index.getString("name")).collect(Collectors.toCollection(TreeSet::new)));
        }
        return byCollection;
    }

    /**
     * The field paths a stored resource carries.
     *
     * <p>Shape rather than content: two runs write different ids, timestamps and directory paths, none
     * of which says anything about whether a changeset ran. What does say it is which fields exist --
     * a resource left unconverted carries the text field and not the structured one, and no amount of
     * differing content hides that.
     *
     * <p>Only the resources, and that boundary is the point rather than a convenience. This asks
     * whether the changesets ran, and a store that has been upgraded is not otherwise field-for-field
     * identical to one installed today: an older build wrote fields that newer builds stopped writing,
     * and nothing removes them because nothing has ever said they should be removed. Measured here
     * rather than reasoned about -- the released build this upgrades from writes a provenance field on
     * catalog entries that the build after it dropped, and it is still in the upgraded store
     * afterwards. Widening this to every collection would fail on that, which is a decision about
     * whether upgrades should strip retired fields and not a question about the migration.
     *
     * <p>What the other collections are held to is one level up: the set of collections, and the
     * indexes on each. A changeset that never ran shows there.
     */
    private static Set<String> shapeOfConvertedResources(MongoDatabase store) {
        Set<String> fields = new TreeSet<>();
        for (Document document : SystemCollections.ARTIFACTS.on(store).find()) {
            collectFieldPaths("", document, fields);
        }
        return fields;
    }

    private static void collectFieldPaths(String prefix, Document document, Set<String> into) {
        for (Map.Entry<String, Object> field : document.entrySet()) {
            if (prefix.isEmpty() && NOT_PART_OF_THE_SHAPE.contains(field.getKey())) {
                continue;
            }
            String path = prefix.isEmpty() ? field.getKey() : prefix + "." + field.getKey();
            into.add(path);
            if (field.getValue() instanceof Document nested) {
                collectFieldPaths(path, nested, into);
            }
        }
    }

}
