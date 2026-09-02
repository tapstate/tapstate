package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Removing a pipeline does not reach into the layer its nests keep their state in - neither a
 * neighbour's, nor its own.
 *
 * <p>A removal reclaims the three documents that describe one pipeline and detaches its cursor from the
 * chains it read. What it must not do is tidy away the state layer as well. That layer has a lifetime of
 * its own and a different owner: it is dropped when a pipeline is <em>stopped</em>, by name, one namespace
 * at a time. A removal that also dropped state would be reaching past its own contract - and on a shared
 * arrangement it would be reaching into namespaces belonging to pipelines nobody asked to remove.
 *
 * <p><b>Which is why the state is seeded after the stop rather than before it.</b> Stopping is what clears
 * a pipeline's own namespaces, so state seeded before the stop would already be gone by the time the
 * removal ran, and every assertion below would pass against an implementation that dropped everything it
 * could reach. Seeded afterwards, the namespaces are demonstrably present at the moment the removal is
 * called, and each one that survives is a place the removal did not go. The departing pipeline's own
 * namespace is the sharper of the two: a cleanup written as "take this pipeline's state with it" is
 * invisible to any assertion made only about the survivor.
 *
 * <p>Seeding directly rather than by running a nest is deliberate and follows the same reasoning the stop
 * path's own witness gives: what is under test is where a removal goes, and a real assembly would only be
 * a slower way of putting entries where these put them. It also keeps the case honest about the one thing
 * it does not claim - that a nest was ever built here.
 *
 * <p>The survivor is then asked to keep working, on its own behaviour rather than on what the removal
 * returned: it must carry a further change through to its target. Without that, a removal that emptied
 * the state layer and left the documents readable would still pass.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class ArtifactDeleteLeavesNestStateStandingIT {

    private static final String SOURCE_ID = "shared_src";
    private static final String DEPARTING_BASE = "departing_pipeline";
    private static final String SURVIVING_BASE = "surviving_pipeline";
    private static final String DEPARTING_TARGET = "departing_tgt";
    private static final String SURVIVING_TARGET = "surviving_tgt";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 4;

    /** What a state key holds here: bytes nobody reads, so any difference is presence, not content. */
    private static final byte[] HELD = "a-document-mid-assembly".getBytes(StandardCharsets.UTF_8);
    private static final String KEY = "root-1";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void removingOnePipelineLeavesEveryNestNamespaceWhereItWas(Tiers tier, @TempDir Path directory)
            throws Exception {
        String storeUri = storeUri("delete_keeps_nest_state", tier);
        // The tier rides on the pipeline ids rather than on the namespaces, and that placement is
        // load-bearing twice over. The state database has a fixed name, so on one Mongo the two tiers
        // would otherwise seed over each other. And a namespace is built from the pipeline id, so a
        // cleanup keyed off that id - the shape a careless one takes - has to be able to match what is
        // seeded here. A tier suffix bolted on after the id would leave this case passing against it.
        String departingId = DEPARTING_BASE + "_" + tier.name().toLowerCase(Locale.ROOT);
        String survivingId = SURVIVING_BASE + "_" + tier.name().toLowerCase(Locale.ROOT);
        String departingNamespace = namespace(departingId);
        String survivingNamespace = namespace(survivingId);

        try (ServerHandle server = tier.launch(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            ControlPlane control = connected(server, directory);
            Path sourceDirectory = Files.createDirectories(directory.resolve(SOURCE_ID));
            FileEndpoints files = new FileEndpoints();
            files.seed(EndpointAddress.uri(sourceDirectory.toString()), TABLE, SeedRows.generated(SEEDED_ROWS));

            control.discoverSchema(
                    SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));
            control.apply(workspace(directory, sourceDirectory, departingId, survivingId));
            control.lifecycle(departingId, LifecycleVerb.START);
            control.lifecycle(survivingId, LifecycleVerb.START);
            awaitRunning(control, departingId);
            awaitRunning(control, survivingId);

            // Both on one chain before anything is removed, or this is a removal witness over an
            // arrangement it was never about.
            Await.until(
                    "both pipelines to hold a cursor on one shared mining chain",
                    () -> documents.miningChainIds().size() == 1
                            && documents.consumersOf(onlyChain(documents)).containsAll(
                                    Set.of(departingId, survivingId)),
                    () -> "chains=" + documents.miningChainIds() + " consumers="
                            + documents.miningChainIds().stream().map(documents::consumersOf).toList());

            control.stop(departingId, true);
            Await.until(
                    departingId + " to reach " + PipelineState.STOPPED,
                    () -> control.state(departingId).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(departingId)));

            try (MongoConnection connection = new MongoConnection(
                    new MongoConnectionSettings(storeUri, null, Duration.ofSeconds(5)))) {
                connection.verify();
                KeyedStateStore nestState = new MongoStorePort(connection).keyedState();

                // After the stop, so nothing that has already run can be what leaves these standing.
                nestState.save(departingNamespace, KEY, HELD);
                nestState.save(survivingNamespace, KEY, HELD);
                assertThat(nestState.load(departingNamespace, KEY))
                        .as("the seeding took, so what is asserted after the removal is a difference the "
                                + "removal made rather than state that was never there")
                        .isPresent();
                assertThat(nestState.load(survivingNamespace, KEY)).isPresent();

                control.deleteArtifact(departingId, control.contentHash(departingId));

                assertThat(control.artifact(departingId))
                        .as("the removal went through, so the assertions below are about what it did")
                        .isEmpty();
                assertThat(nestState.load(survivingNamespace, KEY))
                        .as("a pipeline nobody removed keeps the state its nests were holding; a removal "
                                + "that swept the state layer would take a working pipeline's documents "
                                + "with it and report success")
                        .contains(HELD);
                assertThat(nestState.load(departingNamespace, KEY))
                        .as("and the removed pipeline's own state is left standing too: clearing it "
                                + "belongs to the stop, by namespace, and a removal that did it as well "
                                + "would be reaching into a layer with a lifetime of its own")
                        .contains(HELD);
            }

            // Everything above is still satisfied by a removal that emptied the state layer and left these
            // rows readable. What separates that from a working arrangement is the survivor still moving.
            long rowsBefore = files.count(EndpointAddress.uri(directory.resolve(SURVIVING_TARGET).toString()), TABLE);
            files.cdc(EndpointAddress.uri(sourceDirectory.toString()), TABLE, CdcOp.INSERT, 3);
            Await.until(
                    "the surviving pipeline to carry further changes after its neighbour was removed",
                    () -> files.count(EndpointAddress.uri(directory.resolve(SURVIVING_TARGET).toString()), TABLE) > rowsBefore,
                    () -> "rows at target = "
                            + files.count(EndpointAddress.uri(directory.resolve(SURVIVING_TARGET).toString()), TABLE)
                            + ", was " + rowsBefore);
        }
    }

    /**
     * A namespace shaped the way a nest builds one - the pipeline it belongs to, then the step and the
     * path within it. Only the pipeline half is load-bearing here: it is what a removal would key a
     * cleanup off.
     */
    private static String namespace(String pipelineId) {
        return "nest." + pipelineId + ".order_doc.items";
    }

    /** The single chain both pipelines read through; asserted to be single before this is ever called. */
    private static String onlyChain(StoreDocuments documents) {
        return documents.miningChainIds().iterator().next();
    }

    private static void awaitRunning(ControlPlane control, String pipelineId) {
        Await.until(
                pipelineId + " to reach " + PipelineState.RUNNING,
                () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static ControlPlane connected(ServerHandle server, Path directory) throws Exception {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector(
                E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
        return control;
    }

    private static String storeUri(String name, Tiers tier) {
        return SharedMongo.replicaSetUrl(name + "_" + tier.name().toLowerCase(Locale.ROOT));
    }

    /** One source, two targets, two pipelines - the sharing arrangement, applied as one closure. */
    private static Map<String, String> workspace(Path directory, Path sourceDirectory,
            String departingId, String survivingId) throws Exception {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, sourceDirectory));
        resources.put(DEPARTING_TARGET + ".tap.yml", Workspaces.targetYaml(
                DEPARTING_TARGET, Files.createDirectories(directory.resolve(DEPARTING_TARGET))));
        resources.put(SURVIVING_TARGET + ".tap.yml", Workspaces.targetYaml(
                SURVIVING_TARGET, Files.createDirectories(directory.resolve(SURVIVING_TARGET))));
        resources.put(departingId + ".tap.yml",
                Workspaces.pipelineYaml(departingId, SOURCE_ID, DEPARTING_TARGET, TABLE));
        resources.put(survivingId + ".tap.yml",
                Workspaces.pipelineYaml(survivingId, SOURCE_ID, SURVIVING_TARGET, TABLE));
        return resources;
    }
}
