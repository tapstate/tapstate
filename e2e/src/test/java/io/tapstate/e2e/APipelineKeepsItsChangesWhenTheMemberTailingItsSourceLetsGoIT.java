package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.jdi.ObjectReference;
import com.sun.jdi.StringReference;
import com.sun.jdi.VMDisconnectedException;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.EventRequest;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Remote consumers keep a shared capture alive through local clears and take it over after its owner exits.
 *
 * <p>One member tails a source for every pipeline reading it, wherever those pipelines are driven. That
 * member keeps the independent capture while another member still needs it. Clearing its own pipeline
 * must not leave that consumer without a tail. Once the actual capture-owning process exits, the
 * surviving consumer's member takes the tail over from its durable position.
 *
 * <p>The capture holder's claim calls are held for two designated pipelines until the other
 * member has their claims. The first joiner is stopped with its state cleared before the holder's pipeline is stopped
 * the same way. Changes before and after the owner's exit have to reach the one left, and taking the tail over must not
 * attach the stopped first joiner to the chain again.
 * The source and the targets are real Mongo: the harness's own connector replays its whole table through
 * the ring on every run, which a pipeline left without a tail would receive anyway.
 */
class APipelineKeepsItsChangesWhenTheMemberTailingItsSourceLetsGoIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;
    private static final int PIPELINES = 3;

    private static final String SOURCE_ID = "let_go_src";

    /**
     * Bound on the handover: the member still reading asks for the claim once per claim renewal, and the
     * tail it opens starts from the durable record. Both are product timings; this decides only how long
     * a broken handover takes to say so.
     */
    private static final Duration HANDOVER = Duration.ofMinutes(2);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void startingTheTakeoverWitnessOnTheOtherMemberPlacesItsDriverThere() throws Exception {
        runTakeoverWitness();
    }

    @Test
    void thePipelineLeftReadingACaptureNobodyElseNeedsTakesItsTailOver() throws Exception {
        runTakeoverWitness();
    }

    private void runTakeoverWitness() throws Exception {
        String run = UUID.randomUUID().toString().replace("-", "");
        String sourceUri = SharedMongo.replicaSetUrl("e2e_let_go_src_" + run);

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    TwoMemberCluster cluster = TwoMemberCluster.startPreparedWithClaimBreakpoints(storage);
                    StoreDocuments documents = StoreDocuments.nativeSrsAt(storage.cacheUri());
                    MongoConfigurationDomain configuration = MongoConfigurationDomain.openExisting(
                            new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)));
                    MongoCacheDomain cache = MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                            Duration.ofSeconds(5), "placement-witness");
                    var firstClaims = new ClaimBreakpoint(cluster.processCarrying(TwoMemberCluster.NODE_A));
                    var secondClaims = new ClaimBreakpoint(cluster.processCarrying(TwoMemberCluster.NODE_B))) {
                var claims = cache.workloadClaims(configuration.bindings().cachePreparation().orElseThrow().businessScope());
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema(SOURCE_ID, CONNECTOR, Map.of("uri", sourceUri));

                String first = pipelineId(1);
                EndpointAddress firstTarget = startWithContestedClaim(control, TwoMemberCluster.NODE_A, firstClaims, secondClaims,
                        1, sourceUri, run);
                Await.until("the first pipeline's load to cross",
                        () -> mongo.count(firstTarget, TABLE) >= SEEDED_ROWS,
                        () -> "rows at the first target = " + mongo.count(firstTarget, TABLE));
                String holder = Await.answered("the cluster to name the member reading the source",
                        () -> control.captureOwnersOf(first).values().stream().findFirst());
                assertThat(control.pipelineControllerOf(first)).as("the first pipeline established the capture locally")
                        .contains(holder);
                ControlPlane remote = cluster.memberOtherThan(holder);
                remote.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                String remoteMember = holder.equals(TwoMemberCluster.NODE_A)
                        ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;

                CaptureTakeoverCandidates candidates = new CaptureTakeoverCandidates(holder, remoteMember, PIPELINES);
                Placement firstPlacement = tracePlacement(configuration, claims, control, cluster, storage,
                        first, holder, TwoMemberCluster.NODE_A);
                candidates.add(first, firstPlacement.driver(), firstPlacement.trace());
                ClaimBreakpoint holderClaims = holder.equals(TwoMemberCluster.NODE_A) ? firstClaims : secondClaims;
                ClaimBreakpoint remoteClaims = holderClaims == firstClaims ? secondClaims : firstClaims;
                var remoteTargets = new LinkedHashMap<String, EndpointAddress>();
                for (int n = 2; n <= PIPELINES; n++) {
                    remoteTargets.put(pipelineId(n),
                            startWithContestedClaim(remote, remoteMember, remoteClaims, holderClaims, n, sourceUri, run));
                }
                for (var remoteTarget : remoteTargets.entrySet()) {
                    String pipeline = remoteTarget.getKey();
                    EndpointAddress target = remoteTarget.getValue();
                    Placement placement = tracePlacement(configuration, claims, remote, cluster, storage,
                            pipeline, holder, remoteMember);
                    assertThat(placement.driver()).isEqualTo(remoteMember);
                    assertThat(candidates.add(pipeline, placement.driver(), placement.trace())).isTrue();
                    String driver = placement.driver();
                    Await.until(pipeline + " to receive its load",
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + " of " + SEEDED_ROWS);
                    if (!candidates.ready()) continue;

                    String stoppedJoiner = candidates.firstJoiner();
                    String remainingPipeline = candidates.remainingPipeline();
                    String stoppedConsumer = SrsConsumerId.of(stoppedJoiner, SOURCE_ID).value();
                    String remainingConsumer = SrsConsumerId.of(remainingPipeline, SOURCE_ID).value();
                    String chain = Await.answered("both joiners to hold cursors on the same chain",
                            () -> documents.miningChainIds().stream()
                                    .filter(id -> documents.consumersOf(id)
                                            .containsAll(List.of(stoppedConsumer, remainingConsumer)))
                                    .findFirst());
                    requireController(remote, claims, storage, stoppedJoiner, remoteMember);
                    requireController(remote, claims, storage, remainingPipeline, remoteMember);
                    assertThat(remote.captureOwnersOf(remainingPipeline).values()).containsOnly(holder);
                    var stoppedScope = NativePipelineScopes.current(storage, stoppedJoiner);
                    remote.stop(stoppedJoiner, true);
                    NativePipelineScopes.awaitCleared(remote, storage, stoppedScope, Duration.ofSeconds(60));
                    Await.until("the stopped first joiner's cursor to be cleared before takeover",
                            () -> !documents.consumersOf(chain).contains(stoppedConsumer),
                            () -> "consumers on " + chain + " = " + documents.consumersOf(chain));

                    // A shared capture remains needed even after its owner's own pipelines are cleared.
                    for (String stopping : candidates.onTheHolder()) {
                        requireController(remote, claims, storage, stopping, holder);
                        requireController(remote, claims, storage, remainingPipeline, remoteMember);
                        assertThat(remote.captureOwnersOf(remainingPipeline).values()).containsOnly(holder);
                        var original = NativePipelineScopes.current(storage, stopping);
                        remote.stop(stopping, true);
                        NativePipelineScopes.awaitCleared(remote, storage, original, Duration.ofSeconds(60));
                    }

                    assertThat(remote.captureOwnersOf(remainingPipeline).values())
                            .as("clearing local pipelines does not retire another consumer's capture").containsOnly(holder);
                    long beforeExit = mongo.count(target, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("the original shared tail to remain live after its local pipelines clear", HANDOVER,
                            () -> mongo.count(target, TABLE) > beforeExit,
                            () -> "rows=" + mongo.count(target, TABLE) + ", owner=" + remote.captureOwnersOf(remainingPipeline));
                    assertThat(remote.captureOwnersOf(remainingPipeline).values())
                            .as("the pre-exit change crossed through the original holder without early takeover")
                            .containsOnly(holder);

                    requireController(remote, claims, storage, remainingPipeline, remoteMember);
                    cluster.processCarrying(holder).kill();
                    Await.until("the remaining joiner's member to take the capture over", HANDOVER,
                            () -> remote.captureOwnersOf(remainingPipeline).containsValue(driver),
                            () -> "capture owners = " + remote.captureOwnersOf(remainingPipeline));

                    long rowsBefore = mongo.count(target, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + holder + " let the capture go to reach " + remainingPipeline,
                            HANDOVER,
                            () -> mongo.count(target, TABLE) > rowsBefore,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + ", was " + rowsBefore
                                    + "; it is " + remote.state(remainingPipeline) + " and its capture is owned by "
                                    + remote.captureOwnersOf(remainingPipeline).values());
                    assertThat(remote.state(remainingPipeline))
                            .describedAs("it ran throughout, and nobody asked it to do anything")
                            .contains(PipelineState.RUNNING);
                    assertThat(remote.captureOwnersOf(remainingPipeline).values())
                            .describedAs("its source is now tailed by the member driving it")
                            .containsOnly(driver);
                    assertThat(documents.consumersOf(chain))
                            .describedAs("taking the tail over must not reattach the cleared first joiner %s",
                                    stoppedJoiner)
                            .doesNotContain(stoppedConsumer);
                    return;
                }
                candidates.requireReady();
            }
        }
    }

    private static void awaitController(ControlPlane control, String pipeline, String node) {
        Await.until("the selected member to own " + pipeline,
                () -> control.pipelineControllerOf(pipeline).filter(node::equals).isPresent(),
                () -> "controller=" + control.pipelineControllerOf(pipeline));
    }

    private static EndpointAddress startWithContestedClaim(ControlPlane control, String owner,
            ClaimBreakpoint ownerClaim, ClaimBreakpoint contenderClaim, int number, String sourceUri, String run) throws Exception {
        String pipeline = pipelineId(number);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            contenderClaim.arm();
            ownerClaim.arm();
            var result = workers.submit(() -> {
                var target = startPipelineInto(control, number, sourceUri, run);
                awaitController(control, pipeline, owner);
                return target;
            });
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            try {
                while (!result.isDone() && System.nanoTime() < deadline) {
                    contenderClaim.observeAndHold(pipeline);
                    ownerClaim.observeAndHold(pipeline);
                    // Give the selected member a turn only while its competitor is suspended.
                    if (contenderClaim.observed) ownerClaim.releaseHeld();
                }
                if (result.isDone()) {
                    EndpointAddress target = result.get();
                    assertThat(contenderClaim.observed).as("a competing Mongo claim for %s was held", pipeline).isTrue();
                    assertThat(ownerClaim.observed).as("the selected Mongo claim for %s was observed", pipeline).isTrue();
                    return target;
                }
                throw new AssertionError("Claim scheduling timed out for " + pipeline
                        + "; competing attempt observed=" + contenderClaim.observed
                        + "; selected attempt observed=" + ownerClaim.observed);
            } finally {
                result.cancel(true);
                try {
                    ownerClaim.disarm();
                } finally {
                    contenderClaim.disarm();
                }
            }
        }
    }

    private static final class ClaimBreakpoint implements AutoCloseable {
        private final VirtualMachine vm;
        private final BreakpointRequest request;
        private final List<EventSet> held = new ArrayList<>();
        private boolean observed;

        ClaimBreakpoint(RealProcessServer process) throws Exception {
            vm = process.attachDebugger();
            try {
                var store = vm.classesByName("io.tapstate.adapters.mongostore.MongoCacheWorkloadClaimStore").getFirst();
                var method = store.methodsByName("acquirePipeline").getFirst();
                request = vm.eventRequestManager().createBreakpointRequest(method.location());
                request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            } catch (Exception | Error failure) {
                vm.dispose();
                throw failure;
            }
        }

        void arm() {
            observed = false;
            request.enable();
        }

        void observeAndHold(String pipeline) throws InterruptedException {
            var events = vm.eventQueue().remove(50);
            if (events == null) return;
            if (events.stream().anyMatch(event -> event instanceof BreakpointEvent hit
                    && request.equals(hit.request()) && pipeline.equals(pipelineOf(hit)))) {
                observed = true;
                held.add(events);
            } else {
                events.resume();
            }
        }

        void releaseHeld() {
            held.forEach(EventSet::resume);
            held.clear();
        }

        void disarm() {
            request.disable();
            releaseHeld();
        }

        @Override public void close() {
            try {
                vm.dispose();
            } catch (VMDisconnectedException closed) {
                // This witness intentionally kills the capture holder before the debugger scope ends.
            }
        }

        private static String pipelineOf(BreakpointEvent hit) {
            try {
                var scope = (ObjectReference) hit.thread().frame(0).getArgumentValues().getFirst();
                return ((StringReference) scope.getValue(scope.referenceType().fieldByName("pipelineId"))).value();
            } catch (com.sun.jdi.IncompatibleThreadStateException failure) {
                throw new AssertionError("The claim breakpoint did not suspend its worker", failure);
            }
        }
    }

    private record Placement(String driver, String trace) {}

    private static Placement tracePlacement(MongoConfigurationDomain configuration, WorkloadClaimStore claims,
            ControlPlane control, TwoMemberCluster cluster, PreparedNativeStore storage, String pipeline,
            String holder, String requestedMember) {
        String driver = Await.answered("the cluster to name the member driving " + pipeline,
                () -> control.pipelineControllerOf(pipeline));
        var preparation = configuration.preparations().read(pipeline).orElseThrow();
        var claim = claims.read(new WorkloadClaimKey(
                storage.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline)).orElseThrow();
        String request = cluster.processCarrying(requestedMember).baseUrl()
                .resolve("/api/pipelines/" + pipeline + ":start").toString();
        // The retained issuer and claim generations distinguish initial placement from a later takeover.
        String trace = "POST " + request + "; capture holder=" + holder
                + "; initial preparation=" + preparation + "; actuation=" + claim + "; API controller=" + driver;
        System.out.println(trace);
        assertThat(claim.leased()).as("the actuation claim is live; %s", trace).isTrue();
        assertThat(claim.claim().owner().nodeId()).as("the native claim agrees with the API; %s", trace).isEqualTo(driver);
        return new Placement(driver, trace);
    }

    private static void requireController(ControlPlane control, WorkloadClaimStore claims,
            PreparedNativeStore storage, String pipeline, String driver) {
        var claim = claims.read(new WorkloadClaimKey(
                storage.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline)).orElseThrow();
        assertThat(claim.leased()).as("%s still has a live controller before changing the fixture", pipeline).isTrue();
        assertThat(claim.claim().owner().nodeId()).as("%s retained its selected controller", pipeline).isEqualTo(driver);
        assertThat(control.pipelineControllerOf(pipeline)).as("%s has the same controller in the API", pipeline).contains(driver);
    }

    private static String pipelineId(int n) {
        return "let_go_pipe_" + n;
    }

    private static EndpointAddress startPipelineInto(ControlPlane control, int n, String sourceUri, String run) {
        String targetUri = SharedMongo.replicaSetUrl("e2e_let_go_tgt_" + n + "_" + run);
        String targetId = "let_go_tgt_" + n;
        String pipeline = pipelineId(n);
        Map<String, String> resources = new LinkedHashMap<>();
        // Sent every time, unchanged: an apply resolves references within what it is given.
        resources.put(SOURCE_ID + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, CONNECTOR, sourceUri, TABLE));
        resources.put(targetId + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(targetId, CONNECTOR, targetUri));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [%s], type: filter, expr: "op == 'r' || op == 'i'" }
                serve:
                  from: rows_through
                  sync:
                    - source: %s
                """.formatted(pipeline, SOURCE_ID, TABLE, targetId));
        control.apply(resources);
        control.lifecycle(pipeline, LifecycleVerb.START);
        Await.until(pipeline + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
        return EndpointAddress.uri(targetUri);
    }
}
