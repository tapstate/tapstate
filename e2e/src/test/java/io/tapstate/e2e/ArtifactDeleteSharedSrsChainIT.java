package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that removing one of two pipelines sharing a mining chain leaves the other one working -
 * and that "working" is measured on the surviving pipeline's own behaviour, not on what the removal
 * returned.
 *
 * <p>This is the case that has to discriminate two opposite mistakes, and neither of them is visible from
 * the removal call:
 *
 * <ul>
 *   <li><b>Taking the chain down with the pipeline.</b> Sharing a capture is the encouraged arrangement,
 *       so a removal that tidied away the chain would cut the surviving pipeline's supply. That failure at
 *       least announces itself - the target stops moving straight away.</li>
 *   <li><b>Removing the pipeline without detaching its cursor.</b> This one announces nothing. The chain is
 *       there, the survivor is still {@code RUNNING}, no error is raised anywhere - and the departed
 *       consumer's frozen cursor is folded into two independent minimums that pin the chain's durable
 *       frontier and its cdc write headroom permanently. The pipeline looks alive and never advances
 *       again.</li>
 * </ul>
 *
 * <p>So the assertions are deliberately placed after the removal and on the survivor: rows keep arriving at
 * its target, and the chain's durable read offset keeps moving. A case that stopped at "the removal
 * succeeded and the chain document is still there" would pass against the second mistake, which is the more
 * expensive of the two precisely because nothing reports it.
 *
 * <p>The consumer set is asserted exactly rather than by absence alone: the departed cursor has to be gone
 * <em>and</em> the survivor's has to remain, and a detach that cleared both would satisfy either half on
 * its own while breaking the chain for everyone left on it.
 *
 * <p>The departed pipeline's own target is checked too, and it is checked against rows it was first shown
 * to have landed. What a removal may reclaim is bookkeeping; delivered rows are the user's data and
 * outlive the artifact that produced them. Measuring this on a target that never received anything would
 * pass against every implementation, which is why the case waits for the rows before taking them away.
 *
 * <p>One source feeds both pipelines, which is what puts them on one chain - chain identity is the physical
 * source coordinate, so two pipelines reading one source are two consumers of one capture by construction
 * rather than by an assertion this case would have to make about internals.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class ArtifactDeleteSharedSrsChainIT {

    private static final String SOURCE_ID = "shared_src";
    private static final String DEPARTING_ID = "departing_pipeline";
    private static final String SURVIVING_ID = "surviving_pipeline";
    private static final String DEPARTING_TARGET = "departing_tgt";
    private static final String SURVIVING_TARGET = "surviving_tgt";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 4;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void removingOnePipelineDetachesOnlyItsCursorAndLeavesTheSharedChainCarryingTheOther(
            Tiers tier, @TempDir Path directory) throws Exception {
        String storeUri = storeUri("delete_shared_chain", tier);
        try (ServerHandle server = tier.launch(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            ControlPlane control = connected(server, directory);
            Path sourceDirectory = Files.createDirectories(directory.resolve(SOURCE_ID));
            FileEndpoints files = new FileEndpoints();
            files.seed(EndpointAddress.uri(sourceDirectory.toString()), TABLE, SeedRows.generated(SEEDED_ROWS));

            control.discoverSchema(
                    SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));
            control.apply(workspace(directory, sourceDirectory));
            control.lifecycle(DEPARTING_ID, LifecycleVerb.START);
            control.lifecycle(SURVIVING_ID, LifecycleVerb.START);
            awaitRunning(control, DEPARTING_ID);
            awaitRunning(control, SURVIVING_ID);

            // Both pipelines have to be on one chain before anything is removed, or the case is a removal
            // witness over an arrangement it was never about.
            Await.until(
                    "both pipelines to hold a cursor on one shared mining chain",
                    () -> documents.miningChainIds().size() == 1
                            && documents.consumersOf(onlyChain(documents)).containsAll(
                                    Set.of(DEPARTING_ID, SURVIVING_ID)),
                    () -> "chains=" + documents.miningChainIds() + " consumers="
                            + documents.miningChainIds().stream().map(documents::consumersOf).toList());
            String chainId = onlyChain(documents);

            // The departing pipeline has to have landed something before it goes, or "its target still
            // holds what it wrote" is a claim about an empty file and passes no matter what is reclaimed.
            Await.until(
                    DEPARTING_ID + " to land the seeded rows, so what outlives its removal is data that "
                            + "was demonstrably there",
                    () -> files.count(EndpointAddress.uri(directory.resolve(DEPARTING_TARGET).toString()), TABLE) == SEEDED_ROWS,
                    () -> "rows at " + DEPARTING_TARGET + " = "
                            + files.count(EndpointAddress.uri(directory.resolve(DEPARTING_TARGET).toString()), TABLE));

            // The departing pipeline comes to rest first; the surviving one is left running throughout,
            // because what this case measures is whether it keeps going.
            control.stop(DEPARTING_ID, true);
            Await.until(
                    DEPARTING_ID + " to reach " + PipelineState.STOPPED,
                    () -> control.state(DEPARTING_ID).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(DEPARTING_ID)));

            long departingTargetRows = files.count(EndpointAddress.uri(directory.resolve(DEPARTING_TARGET).toString()), TABLE);

            control.deleteArtifact(DEPARTING_ID, control.contentHash(DEPARTING_ID));

            assertThat(control.artifact(DEPARTING_ID))
                    .as("the removal went through: sharing a chain is not a reason to refuse one")
                    .isEmpty();
            assertThat(documents.holds(MongoStorePort.SRS_META, chainId))
                    .as("the chain record itself, which belongs to everyone reading through it and is "
                            + "never the departing pipeline's to take with it")
                    .isTrue();
            assertThat(documents.consumersOf(chainId))
                    .as("exactly who holds a cursor now - the departed one gone, the survivor's kept; a "
                            + "detach that cleared both would break the chain for everyone left on it")
                    .containsExactly(SURVIVING_ID);
            assertThat(files.count(EndpointAddress.uri(directory.resolve(DEPARTING_TARGET).toString()), TABLE))
                    .as("the rows the departed pipeline had already landed, which are the user's data and "
                            + "not bookkeeping the removal is entitled to reclaim; a removal that tidied "
                            + "away 'its' target would destroy delivered data and report success")
                    .isEqualTo(departingTargetRows)
                    .isEqualTo(SEEDED_ROWS);

            // Everything above is still satisfied by an implementation that detached nothing but removed the
            // chain document too, and by one that detached correctly. What follows is what separates a
            // working chain from one that is quietly pinned forever.
            long readSeqAfterRemoval = documents.consumerReadSeq(chainId, SURVIVING_ID, TABLE);
            long rowsAfterRemoval = files.count(EndpointAddress.uri(directory.resolve(SURVIVING_TARGET).toString()), TABLE);
            files.cdc(EndpointAddress.uri(sourceDirectory.toString()), TABLE, CdcOp.INSERT, 3);

            Await.until(
                    "the surviving pipeline to carry further changes after its neighbour was removed",
                    () -> files.count(EndpointAddress.uri(directory.resolve(SURVIVING_TARGET).toString()), TABLE) > rowsAfterRemoval,
                    () -> "rows at target = "
                            + files.count(EndpointAddress.uri(directory.resolve(SURVIVING_TARGET).toString()), TABLE)
                            + ", was " + rowsAfterRemoval);
            // The cursor the cdc write gate takes its headroom from. A departed consumer left attached
            // freezes the minimum this is folded into, the gate closes, and the survivor stops advancing
            // while still reporting itself RUNNING - the failure that reports nothing.
            Await.until(
                    "the surviving consumer's cursor on the shared chain to keep advancing",
                    () -> documents.consumerReadSeq(chainId, SURVIVING_ID, TABLE) > readSeqAfterRemoval,
                    () -> "read seq = " + documents.consumerReadSeq(chainId, SURVIVING_ID, TABLE)
                            + ", was " + readSeqAfterRemoval + ", chain = " + documents.chain(chainId));
        }
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
    private static Map<String, String> workspace(Path directory, Path sourceDirectory) throws Exception {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, sourceDirectory));
        resources.put(DEPARTING_TARGET + ".tap.yml", Workspaces.targetYaml(
                DEPARTING_TARGET, Files.createDirectories(directory.resolve(DEPARTING_TARGET))));
        resources.put(SURVIVING_TARGET + ".tap.yml", Workspaces.targetYaml(
                SURVIVING_TARGET, Files.createDirectories(directory.resolve(SURVIVING_TARGET))));
        resources.put(DEPARTING_ID + ".tap.yml",
                Workspaces.pipelineYaml(DEPARTING_ID, SOURCE_ID, DEPARTING_TARGET, TABLE));
        resources.put(SURVIVING_ID + ".tap.yml",
                Workspaces.pipelineYaml(SURVIVING_ID, SOURCE_ID, SURVIVING_TARGET, TABLE));
        return resources;
    }
}
