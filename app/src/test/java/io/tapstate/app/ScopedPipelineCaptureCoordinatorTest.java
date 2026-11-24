package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.core.lifecycle.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.runtime.srs.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ScopedPipelineCaptureCoordinatorTest {
    @Test void aFirstSharedSnapshotUsesTheExactPostArrivalConsumerReceipt() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var old = f.streamingProgress.get();
            var cursor = old.cursor();
            var unpinned = new ConsumerOffset(cursor.pipelineId(), cursor.perTableSeq(), cursor.sinkAcked(),
                    List.of(), null, 0, cursor.sinkAckedByTable(), cursor.progressKind());
            f.streamingProgress.set(new SrsConsumerProgress(old.context(), old.membershipRevision(), old.revision(),
                    unpinned, null, old.writerRun(), old.ringDoneThrough()));
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            var supplied = org.mockito.ArgumentCaptor.forClass(SrsConsumerProgress.class);
            verify(f.runtime).resumeSnapshot(any(), any(), anyLong(), any(), any(), any(), supplied.capture());
            assertThat(supplied.getValue()).isEqualTo(f.streamingProgress.get());
            assertThat(supplied.getValue().snapshot()).isNull();
            verify(f.runtime, never()).beginSnapshot(any(), any(), anyLong(), any(), any(), any());
        }
    }

    @Test void sharedTakeoverKeepsOneOpenerWhenAnotherPipelineAddsDemand() throws Exception {
        try (var f = new Fixture(ReadMode.CDC_ONLY)) {
            var maintenance = (ScheduledExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(f.coordinator, "maintenance");
            maintenance.shutdownNow();
            assertThat(maintenance.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            f.checkPortIdentity.set(true);
            var authored = (PipelineResource) f.memory.artifacts().get("flow").orElseThrow();
            f.memory.artifacts().create(new PipelineResource("peer", null, authored.sources(), authored.transforms(),
                    authored.view(), authored.serve(), authored.settings(), authored.experimental()));
            var peerScope = new PipelineIntentScope("cluster", 1, "cache", "peer", 11);
            var peerClaim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "peer"), OWNER, 5, 6, 1);
            var peer = new ExecutionFence(peerScope, peerClaim);
            when(f.authorization.require(peer)).thenReturn(peerClaim);
            when(f.authorization.authorized(peer)).thenReturn(true);
            var stores = f.sources.srs(BUSINESS, () -> {});
            var consumers = stores.consumers();
            var peerContext = new SrsConsumerContext(new SrsConsumerKey("mck-key", "peer", "input"), 11);
            var first = new SrsConsumerMembership(f.streamingContext, 1, false, Set.of("orders"), PIPELINE);
            var second = new SrsConsumerMembership(peerContext, 1, false, Set.of("orders"), peerClaim);
            var members = new ArrayList<>(List.of(first, second));
            when(consumers.find(peerContext.key())).thenReturn(Optional.of(second));
            for (var member : members) {
                var cursor = new ConsumerOffset(member.context().key().consumerId(), Map.of("orders", 0L), null,
                        List.of(), null, 0, Map.of(), ConsumerProgressKind.SRS);
                when(consumers.progress(member.context())).thenReturn(Optional.of(new SrsConsumerProgress(
                        member.context(), 1, 1, cursor, null, null, Map.of("orders", 0L))));
            }
            when(consumers.attached("mck-key")).thenAnswer(call -> new SrsConsumerTopology(BUSINESS, "mck-key", members.size(), List.copyOf(members)));
            var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 0), "retained"),
                    Map.of("orders", new SrsTableCheckpoint(new SrsModelKey("mck-key", "orders", "cache", 1, 0), 0)));
            var chain = new AtomicReference<>(new SrsChainState("mck-key", "mck-key", "1h", 1,
                    Set.of("orders"), Set.of("orders"), 1, checkpoint, Instant.EPOCH, 2, 2));
            when(stores.chains().load("mck-key")).thenAnswer(call -> Optional.of(chain.get()));
            when(stores.chains().requestTables(eq("mck-key"), anySet(), any())).thenAnswer(call -> {
                var old = chain.get();
                chain.set(new SrsChainState(old.miningChainId(), old.captureId(), old.retention(), old.epoch(),
                        call.getArgument(1), old.servingTables(), 1, checkpoint, Instant.EPOCH, members.size(), members.size()));
                return chain.get();
            });
            when(stores.chains().openEpoch(eq("mck-key"), anyLong(), any())).thenAnswer(call -> {
                var old = chain.get();
                chain.set(new SrsChainState(old.miningChainId(), old.captureId(), old.retention(), old.epoch() + 1,
                        old.requestedTables(), old.requestedTables(), 1, checkpoint, Instant.EPOCH, members.size(), members.size()));
                return OptionalLong.of(chain.get().epoch());
            });
            when(stores.chains().publishServingTables(anyString(), anyLong(), anySet(), any())).thenReturn(true);
            var tail = mock(NativeSrsCaptureRunUnit.Tail.class);
            when(tail.health()).thenReturn(new CaptureHealth());
            doAnswer(call -> {
                java.util.function.Function<SrsCaptureWriter, CapturePort> open = call.getArgument(5);
                open.apply(mock(SrsCaptureWriter.class));
                return tail;
            }).when(f.runtime).startTail(any(), any(), any(), any(), any(), any(), any(), anyBoolean());
            doNothing().when(f.runtime).retain(any(), anyString(), any(), any());
            var flow = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            var other = f.coordinator.forRun(peerScope, peerClaim, peer);
            flow.startCapture("flow", f.snapshot(), f.models());
            other.startCapture("peer", f.snapshot(), SourceModelSnapshot.capture("peer", f.definitions, f.memory.schemas()));
            var local = new WorkloadClaim(f.capture.key(), OWNER, 2, 0, 1, Instant.now().plusSeconds(60));
            var prepared = ((CaptureCreationResult.AlreadyIssued) f.configuration.issueFirstCreate(null)).preparation();
            when(f.cache.load(prepared)).thenAnswer(call -> new CaptureCacheState(prepared, WorkloadClaimFence.from(local), Optional.of(chain.get())));
            org.mockito.Mockito.doAnswer(call -> f.ownership.acquire(call.getArgument(0))).when(f.ownership).acquireInitial(any());
            when(f.ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true, local));
            doAnswer(call -> {
                var expected = call.getArgument(1, WorkloadClaimFence.class);
                if (expected.owner().nodeId().equals("remote")) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
                return expected;
            }).when(f.authorization).requireCapture(any(), any());
            org.mockito.Mockito.clearInvocations(f.ownership);
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintenanceTick");
            verify(f.ownership,never()).acquireInitial(any());verify(f.ownership,atLeastOnce()).acquire(any());
            assertThat(flow.captureFailure("flow")).as("the first takeover opens its original tail").isEmpty();
            assertThat(other.captureFailure("peer")).as("the follower keeps the same capture").isEmpty();
            awaitOwnerOperations(f);
            assertThat(f.checkedConfigurations).hasSize(1);
            var opener = f.checkedConfigurations.getFirst();
            assertThat(opener.node()).isEqualTo(new PipelineNode("flow", "input"));
            assertThat(opener.original()).isEqualTo(ORIGINAL);
            assertThat(opener.primary().owner().namespace()).isEqualTo("pdk.chain.mck-key");
            var extraContext = new SrsConsumerContext(new SrsConsumerKey("mck-key", "expander", "input"), 1);
            var extraClaim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "expander"), OWNER, 5, 6, 1);
            members.add(new SrsConsumerMembership(extraContext, 1, false, Set.of("customers"), extraClaim));
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintenanceTick");
            assertThat(flow.captureFailure("flow")).isEmpty();
            assertThat(other.captureFailure("peer")).isEmpty();
            awaitOwnerOperations(f);
            assertThat(f.checkedConfigurations).hasSize(2).allSatisfy(opened -> {
                assertThat(opened.node()).isEqualTo(opener.node());
                assertThat(opened.primary()).isEqualTo(opener.primary());
                assertThat(opened.artifactHash()).isEqualTo(opener.artifactHash());
            });
            verify(f.runtime, times(2)).startTail(eq(BUSINESS), any(), eq(WorkloadClaimFence.from(local)),
                    any(), any(), any(), any(), anyBoolean());
        }
    }

    @Test void combinedBoundedMeasurementsKeepStoredPositionsAndRetainedCountsSeparate() throws Exception {
        try (var f = new Fixture()) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            verify(f.snapshots, timeout(3000)).recordCount("input", "orders", 1, 1);
            when(f.snapshots.count("input", "orders", 1)).thenReturn(OptionalLong.of(20));
            when(f.boundedLoads.complete(ORIGINAL)).thenReturn(true);
            clearInvocations(f.boundedLoads);
            var at = Instant.now();
            var sample = run.measurements("flow", new ExecutionPlan("flow", 2L, 3L, 1L, List.of(), List.of(), at), at);
            assertThat(sample.positions()).isEmpty();
            assertThat(sample.snapshot().byTable().get("orders").rowsDone()).isEqualTo(20);
            assertThat(sample.runSnapshot().byTable().get("orders").rowsDone()).isOne();
            assertThat(sample.initialLoad().tables().get("orders").rowsRead()).isOne();
            assertThat(sample.initialLoad().tables().get("orders").stage()).isEqualTo(InitialLoad.Stage.LANDED);
            verify(f.boundedLoads, times(1)).complete(ORIGINAL);
        }
    }

    @Test void combinedCdcOnlyMeasurementsPublishOnlyTheirOwnDurableTargetPositions() {
        try (var f = new Fixture(ReadMode.CDC_ONLY)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            var progress = f.streamingProgress.get();
            var ack = new ChainPosition(new SourceOrder(1, 3), "cdc-target-confirmed");
            var cursor = new ConsumerOffset(progress.cursor().pipelineId(), Map.of("orders", 3L), ack,
                    List.of(), null, 0L, Map.of("orders", ack), ConsumerProgressKind.SRS);
            f.streamingProgress.set(new SrsConsumerProgress(progress.context(), 1, 1, cursor, null));
            var at = Instant.now();
            var sample = run.measurements("flow", new ExecutionPlan("flow", 2L, 3L, 1L, List.of(), List.of(), at), at);
            assertThat(sample.snapshot()).isEqualTo(SnapshotReading.NONE);
            assertThat(sample.runSnapshot()).isEqualTo(SnapshotReading.NONE);
            assertThat(sample.initialLoad().tables()).isEmpty();
            assertThat(sample.positions()).contains(Map.of("orders", "cdc-target-confirmed"));
        }
    }
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void healthyMaintenanceNeverChecksRemoteAuthorizationUnderTheCoordinatorMonitor(boolean localOwner) throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var maintenance = (ScheduledExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(f.coordinator, "maintenance");
            maintenance.shutdownNow();
            assertThat(maintenance.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            if (localOwner) ownedShared(f);
            else f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL).startCapture("flow", f.snapshot(), f.models());
            var underMonitor = new AtomicBoolean();
            var calls = new AtomicInteger();
            doAnswer(call -> {
                calls.incrementAndGet();
                if (Thread.holdsLock(f.coordinator)) underMonitor.set(true);
                return call.getArgument(1);
            }).when(f.authorization).requireCapture(any(), any());

            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintenanceTick");

            awaitOwnerOperations(f);
            assertThat(calls).hasPositiveValue();
            assertThat(underMonitor).as("a healthy remote fence read must not block unrelated start planning").isFalse();
        }
    }
    @Test void healthyMaintenanceReusesTheChainValidatedWithAttachedConsumers() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var maintenance = (ScheduledExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(f.coordinator, "maintenance");
            maintenance.shutdownNow();
            assertThat(maintenance.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            var held = ownedShared(f);
            var stores = f.sources.srs(BUSINESS, () -> {});
            var topology = stores.consumers().attached(held.chain());
            var chain = stores.chains().load(held.chain()).orElseThrow();
            when(stores.consumers().attachedWithChain(eq(held.chain()), any(SrsChainStore.class)))
                    .thenReturn(new SrsConsumerStore.AttachedChain(topology, chain));
            clearInvocations(stores.chains(), stores.consumers());
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintenanceTick");
            awaitOwnerOperations(f);
            verify(stores.chains(), never()).load(held.chain());
            verify(stores.consumers()).attachedWithChain(eq(held.chain()), any(SrsChainStore.class));
        }
    }

    @Test void onePublicationSharesOneOriginalCursorAcrossAllCaptureProjections() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            var progress = f.progress(true, 1, 1);
            var ack = new ChainPosition(new SourceOrder(1, 3), "target-confirmed");
            var cursor = new ConsumerOffset(progress.cursor().pipelineId(), Map.of("orders", 3L), ack,
                    List.of("orders"), "seam", 1L, Map.of("orders", ack), ConsumerProgressKind.SRS);
            f.streamingProgress.set(new SrsConsumerProgress(progress.context(), 1, 1, cursor, progress.snapshot()));
            var consumers = f.sources.srs(BUSINESS, () -> {}).consumers();
            clearInvocations(consumers);
            var observations = new InMemoryStorePort();
            observations.state().create("flow", StateJson.of(PipelineState.RUNNING), Instant.now());
            var plan = new ExecutionPlan("flow", 2L, 3L, 1L, List.of("node"), List.of(), Instant.now());
            var publisher = new RuntimeConvergenceConfiguration().observationPublisher(observations,
                    mock(io.tapstate.runtime.engine.Engine.class), run, ids -> Map.of("flow", plan));

            var observed = publisher.publish("flow").orElseThrow();

            verify(consumers, times(1)).progress(f.streamingContext);
            assertThat(observed.positions()).containsOnly(entry("orders", "target-confirmed"));
            assertThat(observed.snapshot().get("orders").landed()).isTrue();
            assertThat(observed.initialLoad().tables().get("orders").stage()).isEqualTo(InitialLoad.Stage.LANDED);
            f.streamingProgress.set(f.progress(false, 1, 1));
            var next = publisher.publish("flow").orElseThrow();
            assertThat(next.positions()).isEmpty();
            assertThat(next.snapshot().get("orders").landed()).isFalse();
            assertThat(next.initialLoad().tables().get("orders").stage()).isNotEqualTo(InitialLoad.Stage.LANDED);
            verify(consumers, times(2)).progress(f.streamingContext);
        }
    }
    @org.junit.jupiter.api.RepeatedTest(2)
    void stopCanRetireItsOriginalSharedCaptureWhenHandoffStartsDuringTheFinalDemandRead() {
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofHours(1))) {
            var node = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"), OWNER, 1, 0, 1);
            when(f.ownership.parentNode()).thenReturn(node);
            var held = ownedShared(f);
            var owner = ownerOf(f, held);
            var lease = (CaptureClaimLease) org.springframework.test.util.ReflectionTestUtils.getField(owner, "lease");
            var stores = f.sources.srs(BUSINESS, () -> {});
            var guarded = mock(SrsConsumerStore.class);
            var guard = new AtomicReference<Runnable>();
            doAnswer(call -> {
                guard.set(call.getArgument(1));
                return new SrsStores(stores.chains(), stores.models(), stores.log(), guarded);
            }).when(f.sources).srs(eq(BUSINESS), any());
            doAnswer(call -> {
                // The checkout was already in flight when the original capture lost its proof.
                doThrow(new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null))
                        .when(f.authorization).requireCapture(eq(BUSINESS), eq(WorkloadClaimFence.from(held.claim())));
                lease.renew();
                guard.get().run();
                throw new AssertionError("the ended capture still authorized a consumer read");
            }).when(guarded).attached(held.chain());
            doAnswer(call -> {
                guard.get().run();
                throw new AssertionError("the ended capture still authorized recovery inspection");
            }).when(guarded).inspectSharedRecovery(held.preparation());

            held.run().stopCapture("flow", false);
            verify(guarded).attached(held.chain());
            verify(held.tail()).close();
            verify(f.ownership).release(held.claim());
        }
    }

    private static Object ownerOf(Fixture fixture, OwnedShared held) {
        synchronized (fixture.coordinator) {
            var owners = (Map<?, ?>) org.springframework.test.util.ReflectionTestUtils.getField(fixture.coordinator, "owners");
            return java.util.Objects.requireNonNull(owners.get(held.preparation().captureId()));
        }
    }

    private static void awaitOwnerOperations(Fixture fixture) throws Exception {
        var attempts = new ArrayList<CompletableFuture<?>>();
        synchronized (fixture.coordinator) {
            var owners = (Map<?, ?>) org.springframework.test.util.ReflectionTestUtils.getField(fixture.coordinator, "owners");
            for (Object owner : owners.values()) {
                var attempt = (CompletableFuture<?>) org.springframework.test.util.ReflectionTestUtils.getField(owner, "operationAttempt");
                if (attempt != null) attempts.add(attempt);
            }
        }
        for (var attempt : attempts) attempt.get(3, TimeUnit.SECONDS);
    }

    private static CaptureRun healthyCaptureRun() {
        var run = mock(CaptureRun.class);
        when(run.health()).thenReturn(new io.tapstate.runtime.srs.CaptureHealth());
        return run;
    }

    @Test void validationRouteUsesTheOriginalAliasMembershipAndCurrentRemoteCapture() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)){
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var route=run.validationRoute("input","orders");
            assertThat(route.execution()).isEqualTo(ORIGINAL);assertThat(route.alias()).isEqualTo("input");
            assertThat(route.capture()).isEqualTo(WorkloadClaimFence.from(f.capture));assertThat(route.epoch()).isOne();
            assertThat(route.membershipRevision()).isOne();assertThat(route.preparation().birth().captureId()).isEqualTo(f.capture.key().resourceId());
            var opening=new CaptureObservation.Context(route.preparation().captureId(),UUID.randomUUID().toString(),route.channel(),
                    "another-alias","another-pipeline","physical-consumer",route.epoch(),0,route.capture(),List.of("orders"));
            route.requireOpening(opening);
            assertThatThrownBy(()->run.validationRoute("wrong","orders")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->run.validationRoute("input","wrong")).isInstanceOf(TapstateException.class);
            var old=route.capture();var replaced=new WorkloadClaimFence(old.key(),old.owner(),old.claimGeneration()+1,old.executionGeneration(),old.topologyRevision());
            when(f.cache.load(route.preparation())).thenReturn(new CaptureCacheState(route.preparation(),replaced,Optional.of(Fixture.streamingChain())));
            assertThatThrownBy(()->run.validationRoute("input","orders")).isInstanceOf(TapstateException.class);
        }
    }
    @Test void aRetiredOrSnapshotOnlySessionCannotSupplyACdcValidationRoute(){
        try(var f=new Fixture()){
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            assertThatThrownBy(()->run.validationRoute("input","orders")).isInstanceOf(TapstateException.class);
            run.stopCapture("flow",false);
            assertThatThrownBy(()->run.validationRoute("input","orders")).isInstanceOf(TapstateException.class);
        }
    }
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final WorkloadOwner OWNER=new WorkloadOwner("node","boot");
    private static final WorkloadClaimFence PIPELINE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,2,3,1);
    private static final ExecutionFence ORIGINAL=new ExecutionFence(SCOPE,PIPELINE);
    @Test void aLostCaptureReleasesItsOriginalClaimOnlyAfterItsPhysicalReaderClosed() {
        try(var f=new Fixture(List.of("input"),1,ReadMode.SNAPSHOT_ONLY,Duration.ofMillis(100))) {
            f.rowCount.set(32);var unavailable=new TapstateException(IoError.SRS_MODEL_UNAVAILABLE,
                    Map.of("chain","mck-key","table","orders"),null);
            var failRenewal=new AtomicBoolean();
            doAnswer(call -> {
                if(failRenewal.get())throw unavailable;
                return Optional.of(call.getArgument(0));
            }).when(f.ownership).renew(any());
            doAnswer(call -> {assertThat(f.closedPorts).hasPositiveValue();return null;}).when(f.ownership).release(f.capture);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            failRenewal.set(true);
            verify(f.ownership,timeout(3000)).release(f.capture);
            assertThat(f.closedPorts).hasPositiveValue();
        }
    }
    @Test void aLostReadersFailedCloseKeepsTheOriginalReleaseForTheExplicitRetry() throws Exception {
        try(var f=new Fixture(List.of("input"),1,ReadMode.SNAPSHOT_ONLY,Duration.ofMillis(100));var paused=f.pauseSecondRead()) {
            var originalCloseFailure=new IllegalStateException("original reader close pending");
            f.rowCount.set(32);f.closeFailure.set(originalCloseFailure);
            var failRenewal=new AtomicBoolean();var firstCloseReturned=new CountDownLatch(1);
            var observedCloseFailure=new AtomicReference<Throwable>();
            doAnswer(call -> {
                try {return call.callRealMethod();}
                catch(RuntimeException|Error failed){observedCloseFailure.set(failed);throw failed;}
                finally {firstCloseReturned.countDown();}
            }).when(f.runtime).stopSnapshot(any());
            doAnswer(call -> {
                if(failRenewal.get())throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
                return Optional.of(call.getArgument(0));
            }).when(f.ownership).renew(any());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            paused.awaitEntered();failRenewal.set(true);assertThat(firstCloseReturned.await(3,TimeUnit.SECONDS)).isTrue();
            assertThat(observedCloseFailure.get()).isSameAs(originalCloseFailure);
            verify(f.ownership,never()).release(f.capture);
            f.closeFailure.set(null);f.coordinator.retireScope(SCOPE,PIPELINE);
            verify(f.ownership).release(f.capture);
        }
    }
    @Test void anAbsentLocalOwnerDoesNotProveItsExactCaptureTenureEnded() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var properties=new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            var gate=new ClusterMembershipGate(properties);gate.observeNodeSession(() -> true);gate.install(new ClusterMembership("cluster",1,Set.of("node","node-b","node-c")));
            gate.canCommit(Set.of("node","node-b"));var raw=new InMemoryWorkloadClaimStore();
            var ownership=new CaptureOwnership("cluster",OWNER,gate,new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(60));
            var permit=ownership.acquire(new CaptureId("mck-key"));
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"mck-key","mck-key",
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString());
            var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),OWNER,1,0,1);
            try(var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(BUSINESS,f.pipelines,f.sources,
                    f.configuration,f.cache,f.notes),new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(node),f.authorization,
                    ownership,Duration.ofSeconds(10),() -> {},Clock.systemUTC()),f.runtime,f.buffer,f.connectors,
                    (original,config,guard,preparation,stream) -> {throw new AssertionError("no capture may open");})) {
                coordinator.stopSharedLocally(prepared,permit.fence());
                assertThatThrownBy(() -> coordinator.retireShared(prepared,permit.fence())).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> coordinator.requireSharedRetired(prepared,permit.fence())).isInstanceOf(TapstateException.class);
                assertThat(raw.read(permit.claim().key()).orElseThrow().leased()).isTrue();
                assertThat(raw.release(permit.claim())).isTrue();
                coordinator.retireShared(prepared,permit.fence());coordinator.requireSharedRetired(prepared,permit.fence());
                assertThat(raw.read(permit.claim().key()).orElseThrow().claim().claimGeneration()).isEqualTo(permit.claim().claimGeneration());
            }
        }
    }
    @Test void failedOriginalCaptureProofReleasesOnlyTheLeaseTakenByThisStart() {
        try(var f=new Fixture()) {
            when(f.authorization.requireCapture(any(),any())).thenThrow(new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null));
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOf(TapstateException.class);
            verify(f.ownership).release(f.capture);
            verify(f.snapshots,never()).advanceOrder(anyLong(),anyLong(),anyMap());
            assertThat(run.isCapturing("flow")).isFalse();
        }
    }
    @Test void originalBoundedStartUsesPreparedFactsAndStopKeepsDurableStateAndNotes() throws Exception {
        try(var f=new Fixture()) {
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings(); assertThat(bindings.snapshots().get("input").epoch()).isOne();
            verify(f.snapshots,timeout(3000)).recordCount("input","orders",1,1);
            var rows=f.buffer.drain(bindings.bufferKey("input"),SrsRingbuffer.ringName("mck-key","orders"));
            assertThat(rows).singleElement().satisfies(row -> {
                assertThat(row.after()).containsEntry("source","input"); assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(1));
            });
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();
            while(run.snapshotProgress("flow").byTable().isEmpty() && System.nanoTime()<deadline)Thread.sleep(5);
            assertThat(run.snapshotProgress("flow").byTable().get("orders").rowsDone()).isOne();
            assertThat(run.loadDelivered("flow")).isFalse();
            run.startCapture("flow",f.snapshot(),f.models()); assertThat(f.opened).hasValue(1);
            verify(f.configuration).issueFirstCreate(any()); verify(f.snapshots).advanceOrder(eq(0L),eq(0L),anyMap());
            verify(f.notes).open(RequiredNoteOwner.forNode(1,new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,7));
            run.stopCapture("flow",false); run.stopCapture("flow",false);
            verify(f.ownership).release(f.capture); verify(f.notes,never()).clear(any(),anyBoolean());
            assertThat(f.closedPorts).hasValue(1); assertThat(run.isCapturing("flow")).isFalse();
            assertThat(run.snapshotProgress("flow")).isEqualTo(SnapshotReading.NONE);
            assertThat(run.capturedRows("flow")).isEqualTo(CaptureReading.NONE);
        }
    }

    @Test void nativeInitialLoadUsesCurrentRunRowsAndTheOriginalLandingProof() throws Exception {
        try (var f = new Fixture()) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            verify(f.snapshots, timeout(3000)).recordCount("input", "orders", 1, 1);
            var at = Instant.now();
            var plan = new ExecutionPlan("flow", 2L, 3L, 1L, List.of("node"), List.of(), at);
            var pending = run.initialLoad("flow", plan, at);
            assertThat(pending).isNotNull();
            assertThat(pending.runKey()).isEqualTo(InitialLoad.executionKey(plan));
            assertThat(pending.tables().get("orders").rowsRead()).isOne();
            assertThat(pending.tables().get("orders").stage()).isNotEqualTo(InitialLoad.Stage.LANDED);
            when(f.snapshots.count("input", "orders", 1)).thenReturn(OptionalLong.of(20));
            when(f.boundedLoads.complete(ORIGINAL)).thenReturn(true);
            var completed = run.initialLoad("flow", plan, at);
            assertThat(completed.tables().get("orders")).isEqualTo(new InitialLoad.Table(InitialLoad.Stage.LANDED, 1L, null));
            assertThat(completed.phase(PipelineState.COMPLETED)).isEqualTo(InitialLoad.Phase.COMPLETED);
            assertThat(run.initialLoad("flow", new ExecutionPlan("flow", 2L, 4L, 1L, List.of(), List.of(), at), at)).isNull();
            run.stopCapture("flow", false);
            assertThat(run.initialLoad("flow", plan, at)).isNull();
        }
    }

    @Test void streamingSnapshotProgressRequiresItsOwnDurableCompletionMarker() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            assertThat(run.loadDelivered("flow")).isFalse();
            assertThat(run.snapshotProgress("flow").byTable().get("orders"))
                    .isEqualTo(new TableSnapshot(0, null, null, false));
            var at = Instant.now();
            var plan = new ExecutionPlan("flow", 2L, 3L, 1L, List.of("node"), List.of(), at);
            assertThat(run.initialLoad("flow", plan, at).tables().get("orders").stage()).isNotEqualTo(InitialLoad.Stage.LANDED);
            f.streamingProgress.set(f.progress(true, 1, 1));
            assertThat(run.loadDelivered("flow")).isTrue();
            var load = run.initialLoad("flow", plan, at);
            assertThat(load.readMode()).isEqualTo("snapshot_and_cdc");
            assertThat(load.tables().get("orders").stage()).isEqualTo(InitialLoad.Stage.LANDED);
            assertThat(load.phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.STREAMING);
            assertThat(run.snapshotProgress("flow").byTable().get("orders"))
                    .isEqualTo(new TableSnapshot(0, 0L, 100, true));
            assertThat(run.runSnapshotProgress("flow").byTable().get("orders"))
                    .isEqualTo(new TableSnapshot(0, 0L, 100, true));
        }
    }

    @Test void cdcOnlyLoadReportsNoInventedSnapshotTables() {
        try (var f = new Fixture(ReadMode.CDC_ONLY)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            var at = Instant.now();
            var load = run.initialLoad("flow", new ExecutionPlan("flow", 2L, 3L, 1L, List.of("node"), List.of(), at), at);
            assertThat(load.readMode()).isEqualTo("cdc_only");
            assertThat(load.tables()).isEmpty();
            assertThat(load.phase(PipelineState.RUNNING)).isEqualTo(InitialLoad.Phase.STREAMING);
        }
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "membership", "snapshot"})
    void streamingSnapshotReadCannotBorrowAnotherProgressIdentity(String damage) throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            f.streamingProgress.set("missing".equals(damage) ? null
                    : f.progress(true, "membership".equals(damage) ? 2 : 1, "snapshot".equals(damage) ? 2 : 1));
            assertThatThrownBy(() -> run.snapshotProgress("flow")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> run.runSnapshotProgress("flow")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> run.measurements("flow", null, Instant.now())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> run.initialLoad("flow",
                    new ExecutionPlan("flow", 2L, 3L, 1L, List.of("node"), List.of(), Instant.now()), Instant.now()))
                    .isInstanceOf(TapstateException.class);
        }
    }

    @Test void currentRunCountsDoNotAdoptTheRetainedSnapshotTotal() throws Exception {
        try(var f=new Fixture()) {
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models());
            verify(f.snapshots,timeout(3000)).recordCount("input","orders",1,1);
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();
            while(run.snapshotProgress("flow").byTable().isEmpty() && System.nanoTime()<deadline)Thread.sleep(5);
            assertThat(run.snapshotProgress("flow").byTable()).containsKey("orders");
            when(f.snapshots.count("input","orders",1)).thenReturn(OptionalLong.of(20));
            assertThat(run.runSnapshotProgress("flow").byTable().get("orders").rowsDone()).isOne();
            assertThat(run.snapshotProgress("flow").byTable().get("orders").rowsDone()).isEqualTo(20);
        }
    }
    @Test void twoSourceNodesSharingOneCaptureKeepSeparatePrivateRowsAndOneLease() throws Exception {
        try(var f=new Fixture(List.of("left","right"))) {
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models()); var bindings=run.captureBindings();
            verify(f.snapshots,timeout(3000)).recordCount("left","orders",1,1);
            verify(f.snapshots,timeout(3000)).recordCount("right","orders",1,1);
            String ring=SrsRingbuffer.ringName("mck-key","orders");
            assertThat(f.buffer.drain(bindings.bufferKey("left"),ring)).singleElement().satisfies(row -> assertThat(row.after()).containsEntry("source","left"));
            assertThat(f.buffer.drain(bindings.bufferKey("right"),ring)).singleElement().satisfies(row -> assertThat(row.after()).containsEntry("source","right"));
            verify(f.ownership).acquireInitial(any());verify(f.ownership).acquire(any()); assertThat(f.opened).hasValue(2);
            run.stopCapture("flow",false); verify(f.ownership).release(f.capture); assertThat(f.closedPorts).hasValue(2);
        }
    }
    @Test void anotherCommandCannotStartTheOriginalRunOrReadArtifactsOnItsBehalf() {
        try(var f=new Fixture()) {
            var other=new PipelineIntentScope("cluster",1,"cache","flow",8);
            var run=f.coordinator.forRun(other,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOf(TapstateException.class);
            verifyNoInteractions(f.configuration,f.snapshots,f.ownership);
        }
    }
    @Test void anOldCallbackResumedAfterReleaseCannotRecreateItsHandoff() throws Exception {
        try(var f=new Fixture()) {
            var output=new AtomicReference<CaptureHandoff>();
            doAnswer(call -> { output.set(call.getArgument(3));return healthyCaptureRun(); }).when(f.runtime).beginSnapshotOnly(any(),anyLong(),any(),any(),any());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL); run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings(); var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
            when(f.authorization.requireCapture(any(),any())).thenAnswer(call -> {
                if(Thread.currentThread().getName().equals("old-source-callback")) { entered.countDown();if(!resume.await(3,TimeUnit.SECONDS))throw new AssertionError("resume not released"); }
                return call.getArgument(1);
            });
            try(var executor=Executors.newSingleThreadExecutor(task -> new Thread(task,"old-source-callback"))) {
                var pending=executor.submit(() -> output.get().accept(Envelope.read(1,"orders",Map.of("id",1),null)));
                try {
                    assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
                    run.stopCapture("flow", false);
                    resume.countDown();
                    assertThatThrownBy(() -> pending.get(3, TimeUnit.SECONDS))
                            .isInstanceOfSatisfying(java.util.concurrent.ExecutionException.class, failure ->
                                    assertThat(failure.getCause()).isInstanceOfSatisfying(TapstateException.class,
                                            refused -> assertThat(refused.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED)));
                    assertThat(f.buffer.drain(bindings.bufferKey("input"),SrsRingbuffer.ringName("mck-key","orders"))).isEmpty();
                } finally { resume.countDown(); }
            }
        }
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void aSourceCloseFailureRetainsTheOriginalSessionAndLeaseForRetry(boolean error) throws Exception {
        try(var f=new Fixture(List.of("input"),1);var paused=f.pauseSecondRead()) {
            Throwable failure=error?new AssertionError("close invariant"):new IllegalStateException("close invariant");
            f.closeFailure.set(failure);f.rowCount.set(3);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());var bindings=run.captureBindings();
            String key=bindings.bufferKey("input"),ring=SrsRingbuffer.ringName("mck-key","orders");
            try {
                paused.awaitEntered();
                assertThat(f.readRows).hasValue(2);
                assertThatThrownBy(() -> run.stopCapture("flow",false)).isSameAs(failure);
                assertThat(paused.closer.get()).isSameAs(Thread.currentThread());
                verify(f.ownership,never()).release(f.capture);assertThat(run.isCapturing("flow")).isTrue();
                run.stopCapture("flow",false);verify(f.ownership).release(f.capture);assertThat(run.isCapturing("flow")).isFalse();
                assertThat(f.buffer.snapshotState(key,ring).declared()).isFalse();
            } finally { f.buffer.release(key); }
        }
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void aReaderCloseFailureAlreadyCleanedUpDoesNotNeedToBeThrownAgainByStop(boolean error) throws Exception {
        try(var f=new Fixture(List.of("input"),1)) {
            Throwable failure=error?new AssertionError("reader close invariant"):new IllegalStateException("reader close invariant");
            f.closeFailure.set(failure);f.rowCount.set(3);
            var physical=new AtomicReference<CaptureRun>();
            doAnswer(call -> {
                var started=(CaptureRun)call.callRealMethod();physical.set(started);return started;
            }).when(f.runtime).beginSnapshotOnly(any(),anyLong(),any(),any(),any());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings();String key=bindings.bufferKey("input"),ring=SrsRingbuffer.ringName("mck-key","orders");
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos();
            while(f.readRows.get()<2 && System.nanoTime()<deadline)Thread.sleep(5);
            assertThat(f.readRows).hasValue(2);
            // Release the handoff before stopping, so the reader performs the first close and final cleanup.
            f.buffer.release(key);
            assertThat(physical.get().awaitLoaded(Duration.ofSeconds(3))).isTrue();
            assertThat(physical.get().failure()).contains(failure);
            verify(f.ownership,never()).release(f.capture);assertThat(run.isCapturing("flow")).isTrue();
            doAnswer(call -> {
                assertThat(physical.get().awaitLoaded(Duration.ZERO)).isTrue();
                assertThat(f.closedPorts).hasValue(1);return null;
            }).when(f.ownership).release(f.capture);
            assertThatCode(() -> run.stopCapture("flow",false)).doesNotThrowAnyException();
            verify(f.ownership).release(f.capture);assertThat(run.isCapturing("flow")).isFalse();
            assertThat(f.buffer.snapshotState(key,ring).declared()).isFalse();
        }
    }
    @Test void wholeCoordinatorCloseRetainsUnclosedOriginalSessionsForRetry() throws Exception {
        try(var f=new Fixture(List.of("input"),1);var paused=f.pauseSecondRead()) {
            f.rowCount.set(3);var failure=new IllegalStateException("original reader close pending");f.closeFailure.set(failure);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            paused.awaitEntered();
            assertThatThrownBy(f.coordinator::close).isSameAs(failure);verify(f.ownership,never()).release(f.capture);
            f.coordinator.close();verify(f.ownership).release(f.capture);
        }
    }

    @Test void aSupersededLocalExecutionCannotRetainItsPhysicalReader() throws Exception {
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofMillis(100))) {
            var authorized = new AtomicBoolean(true);
            when(f.authorization.authorized(ORIGINAL)).thenAnswer(call -> authorized.get());
            when(f.authorization.require(ORIGINAL)).thenAnswer(call -> {
                if (!authorized.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
                return PIPELINE;
            });
            var held = ownedShared(f);
            var retentionEntered = new CountDownLatch(1);
            var resumeRetention = new CountDownLatch(1);
            var readerClosed = new CountDownLatch(1);
            doAnswer(call -> { readerClosed.countDown(); return null; }).when(held.tail()).close();
            f.coordinator.runOnlyWhileDriving(id -> {
                assertThat(id).isEqualTo("flow");
                retentionEntered.countDown();
                try {
                    resumeRetention.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return true;
            });
            try {
                assertThat(retentionEntered.await(3, TimeUnit.SECONDS))
                        .as("the live local capture reaches its retention decision").isTrue();
                verify(held.tail(), never()).close();
                authorized.set(false);
                assertThat(f.authorization.authorized(ORIGINAL)).isFalse();
                resumeRetention.countDown();
                assertThat(readerClosed.await(3, TimeUnit.SECONDS))
                        .as("the superseded original execution releases its physical reader despite a locally driving same-name pipeline")
                        .isTrue();
                verify(f.ownership, timeout(3000).atLeastOnce()).release(held.claim());
                verify(f.notes, never()).clear(any(), anyBoolean());
            } finally {
                resumeRetention.countDown();
            }
        }
    }

    @Test void aBlockedMaintenanceChainReadDoesNotDelayCaptureBindings() throws Exception {
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofMillis(100));
                var callers = Executors.newSingleThreadExecutor()) {
            var held = ownedShared(f);
            var chains = f.sources.srs(BUSINESS, () -> {}).chains();
            var original = chains.load(held.chain()).orElseThrow();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            doAnswer(call -> {
                if (Thread.currentThread().getName().startsWith("tapstate-capture-owner-")) {
                    entered.countDown();
                    release.await();
                }
                return Optional.of(original);
            }).when(chains).load(held.chain());
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).as("maintenance reaches the chain read").isTrue();
                var bindings = callers.submit(held.run()::captureBindings);
                assertThat(bindings.get(1, TimeUnit.SECONDS)).isNotNull();
                assertThat(release.getCount()).as("the store read is still withheld").isEqualTo(1);
            } finally { release.countDown(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void aFailedMaintenanceReadCannotRetireAReplacementOwner(boolean fatal) throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC); var callers = Executors.newSingleThreadExecutor()) {
            var held = ownedShared(f);
            var maintenance = (ScheduledExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(f.coordinator, "maintenance");
            maintenance.shutdownNow();
            assertThat(maintenance.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            awaitOwnerOperations(f);
            var owner = ownerOf(f, held);
            var chains = f.sources.srs(BUSINESS, () -> {}).chains();
            var original = chains.load(held.chain()).orElseThrow();
            var reading = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            Throwable failure = fatal ? new AssertionError("old owner read failed") : new IllegalStateException("old owner read failed");
            doAnswer(call -> {
                reading.countDown();
                assertThat(release.await(3, TimeUnit.SECONDS)).isTrue();
                if (failure instanceof Error error) throw error;
                throw (RuntimeException) failure;
            }).when(chains).load(held.chain());
            try {
                org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "scheduleOwnerOperation", owner,
                        (Runnable) () -> org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintainOwner", owner));
                assertThat(reading.await(3, TimeUnit.SECONDS)).isTrue();
                var stopping = callers.submit(() -> held.run().stopCapture("flow", false));
                assertThatThrownBy(() -> stopping.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                release.countDown();
                stopping.get(3, TimeUnit.SECONDS);
                doReturn(Optional.of(original)).when(chains).load(held.chain());
                var replacement = ownedShared(f);
                org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintainOwner", owner);
                assertThat(ownerOf(f, replacement)).isNotSameAs(owner);
                assertThat(replacement.run().isCapturing("flow")).isTrue();
                assertThat(replacement.run().captureFailure("flow")).isEmpty();
                verify(replacement.tail(), never()).close();
            } finally { release.countDown(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void aFailedMaintenanceReadStillFailsItsOriginalOwner(boolean fatal) throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var maintenance = (ScheduledExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(f.coordinator, "maintenance");
            maintenance.shutdownNow();
            assertThat(maintenance.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            Throwable failure = fatal ? new AssertionError("original owner read failed") : new IllegalStateException("original owner read failed");
            var chains = f.sources.srs(BUSINESS, () -> {}).chains();
            doAnswer(call -> { if (failure instanceof Error error) throw error; throw (RuntimeException) failure; })
                    .when(chains).load(held.chain());
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintainOwner", ownerOf(f, held));
            assertThat(held.run().captureFailure("flow")).contains(failure);
            verify(held.tail()).close();
        }
    }

    @Test void anAlreadyFailedTailIsHandledBeforeAnyServingStateRead() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var maintenance = (ScheduledExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(f.coordinator, "maintenance");
            maintenance.shutdownNow();
            assertThat(maintenance.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            var failure = new IllegalStateException("physical tail failed");
            held.tail().health().recording(mock(CaptureListener.class)).onError(failure);
            var chains = f.sources.srs(BUSINESS, () -> {}).chains();
            clearInvocations(chains);
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.coordinator, "maintenanceTick");
            awaitOwnerOperations(f);
            assertThat(held.run().captureFailure("flow")).contains(failure);
            verify(held.tail()).close();
            verify(chains, never()).load(anyString());
        }
    }

    @Test void aBlockedDemandReadDoesNotDelayCaptureRenewal() throws Exception {
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofMillis(100))) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var renewedDuringRead = new CountDownLatch(1);
            var held = ownedShared(f);
            when(f.authorization.authorized(ORIGINAL)).thenAnswer(call -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("demand read was not released");
                return true;
            });
            doAnswer(call -> {
                if (entered.getCount() == 0 && release.getCount() != 0) renewedDuringRead.countDown();
                return Optional.of(call.getArgument(0));
            }).when(f.ownership).renew(held.claim());
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                assertThat(renewedDuringRead.await(2, TimeUnit.SECONDS)).isTrue();
                verify(f.ownership, never()).release(held.claim());
            } finally {
                release.countDown();
            }
        }
    }

    @Test void anAuthorizedLocalExecutionRetainsItsPhysicalReader() throws Exception {
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofMillis(100))) {
            var held = ownedShared(f);
            var renewed = new CountDownLatch(2);
            doAnswer(call -> {
                renewed.countDown();
                return Optional.of(call.getArgument(0));
            }).when(f.ownership).renew(held.claim());
            assertThat(renewed.await(3, TimeUnit.SECONDS)).isTrue();
            verify(f.authorization, timeout(3000).atLeast(2)).authorized(ORIGINAL);
            verify(held.tail(), never()).close();
            verify(f.ownership, never()).release(held.claim());
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = PipelineState.class, names = {"RUNNING", "PAUSED"})
    void anAuthorizedPeerRetainsThePhysicalReaderAfterTheLocalExecutionIsSuperseded(PipelineState state) throws Exception {
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofMillis(100))) {
            var held = ownedShared(f);
            var peerScope = new PipelineIntentScope("cluster", 1, "cache", "peer", 11);
            var peerClaim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "peer"),
                    new WorkloadOwner("peer-node", "peer-boot"), 5, 6, 1);
            var peer = new ExecutionFence(peerScope, peerClaim);
            var context = new SrsConsumerContext(new SrsConsumerKey(held.chain(), "peer", "input"), 11);
            var membership = new SrsConsumerMembership(context, 1, false, Set.of("orders"), peerClaim);
            var consumers = f.sources.srs(BUSINESS, () -> {}).consumers();
            var members = new ArrayList<>(consumers.attached(held.chain()).consumers());
            members.add(membership);
            when(consumers.attached(held.chain())).thenReturn(new SrsConsumerTopology(BUSINESS, held.chain(), 2, members));
            var states = new InMemoryStorePort().state();
            states.create("peer", StateJson.of(state), Instant.EPOCH);
            when(f.pipelines.pipelineStates(peerScope)).thenReturn(states);
            var peerAuthorized = new AtomicBoolean(true);
            var peerChecked = new AtomicBoolean();
            when(f.authorization.authorized(peer)).thenAnswer(call -> {
                peerChecked.set(true);
                return peerAuthorized.get();
            });
            var renewed = new CountDownLatch(2);
            doAnswer(call -> {
                if (peerChecked.get()) renewed.countDown();
                return Optional.of(call.getArgument(0));
            }).when(f.ownership).renew(held.claim());
            var readerClosed = new CountDownLatch(1);
            doAnswer(call -> { readerClosed.countDown(); return null; }).when(held.tail()).close();
            when(f.authorization.authorized(ORIGINAL)).thenReturn(false);
            assertThat(renewed.await(3, TimeUnit.SECONDS)).isTrue();
            verify(f.authorization, timeout(3000).atLeast(2)).authorized(peer);
            verify(f.pipelines, timeout(3000).atLeast(2)).pipelineStates(peerScope);
            verify(held.tail(), never()).close();
            verify(f.ownership, never()).release(held.claim());
            peerAuthorized.set(false);
            assertThat(readerClosed.await(3, TimeUnit.SECONDS)).isTrue();
            verify(f.ownership, timeout(3000).atLeastOnce()).release(held.claim());
            verify(f.notes, never()).clear(any(), anyBoolean());
        }
    }

    @Test void slowRetentionDoesNotHoldTheMonitorNeededByStartPlanning() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var retaining = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var planned = new CountDownLatch(1);
            doAnswer(call -> {
                retaining.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("retention was not released");
                return null;
            }).when(f.runtime).retain(any(), anyString(), any(), any());
            var pass = mock(PipelineConvergencePass.class);
            when(pass.scope()).thenReturn(SCOPE);
            try (var worker = Executors.newSingleThreadExecutor()) {
                java.util.concurrent.Future<?> planning = null;
                try {
                    assertThat(retaining.await(3, TimeUnit.SECONDS)).isTrue();
                    planning = worker.submit(() -> {
                        f.coordinator.retainedPlanningModels(pass, f.snapshot(), f.models(), guard -> { guard.run(); return true; });
                        planned.countDown();
                    });
                    assertThat(planned.await(1, TimeUnit.SECONDS))
                            .as("start planning must not wait for unrelated SRS retention I/O")
                            .isTrue();
                } finally {
                    release.countDown();
                    if (planning != null) planning.get(3, TimeUnit.SECONDS);
                }
            }
            assertThat(held.run().captureFailure("flow")).isEmpty();
        }
    }

    @Test void aRetiredSharedTailKeepsItsOriginalSafeFailureFact() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report = pendingTailFailure(f, null, true);
            var fact = report.held().tail().health().failureFact().orElseThrow();
            assertThat(fact.publicFailure()).isNotNull();
            assertThat(report.held().run().captureFailureFact("flow")).get().isSameAs(fact);
        }
    }

    @Test void steadySharedMaintenanceUsesOneRetentionSnapshotForTheTailCheck() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var stores = f.sources.srs(BUSINESS, () -> {});
            var chain = stores.chains().load(held.chain()).orElseThrow();
            var topology = stores.consumers().attached(held.chain());
            var snapshot = new SrsConsumerStore.RetentionSnapshot(chain, topology.consumers().stream()
                    .map(member -> new SrsConsumerStore.RetentionConsumer(member, Map.of())).toList());
            when(stores.consumers().supportsControlSnapshot()).thenReturn(true);
            when(stores.consumers().controlSnapshot(held.chain()))
                    .thenReturn(Optional.of(new SrsConsumerStore.ControlSnapshot(topology, chain, List.of())));
            when(stores.consumers().retentionSnapshot(held.chain())).thenReturn(Optional.of(snapshot));

            verify(f.runtime, timeout(4000)).retain(eq(BUSINESS), eq(held.chain()),
                    eq(WorkloadClaimFence.from(held.claim())), any(), same(snapshot));
            verify(stores.consumers(), never()).controlSnapshot(held.chain());
            verify(f.runtime, times(1)).startTail(any(), any(), any(), any(), any(), any(), any(), anyBoolean());
        }
    }

    @Test void openingANewSharedEpochDiscardsTheMaintenanceRetentionSnapshot() throws InterruptedException {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var fence = WorkloadClaimFence.from(held.claim());
            var stores = f.sources.srs(BUSINESS, () -> {});
            var currentMember = stores.consumers().attached(held.chain()).consumers().getFirst();
            var peerClaim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "peer"),
                    OWNER, 1, 1, 1);
            var peer = new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey(held.chain(), "peer", "input"), 1),
                    1, false, Set.of("lines"), peerClaim);
            var tables = Set.of("orders", "lines");
            var model = new SrsModelKey(held.chain(), "orders", "cache", 1, 0);
            var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 0), "cut"),
                    Map.of("orders", new SrsTableCheckpoint(model, 0)));
            var old = new SrsChainState(held.chain(), held.preparation().captureId(), "1h", 1, tables, Set.of("orders"),
                    1, checkpoint, Instant.EPOCH, 2, 2);
            var opened = new SrsChainState(held.chain(), held.preparation().captureId(), "1h", 2, tables, tables,
                    1, checkpoint, Instant.EPOCH, 2, 2);
            var chain = new AtomicReference<>(old);
            when(stores.chains().load(held.chain())).thenAnswer(call -> Optional.of(chain.get()));
            when(stores.chains().requestTables(held.chain(), tables, fence)).thenReturn(old);
            when(stores.chains().openEpoch(held.chain(), 1, fence)).thenAnswer(call -> {
                chain.set(opened);
                return OptionalLong.of(2);
            });
            when(stores.chains().publishServingTables(held.chain(), 2, tables, fence)).thenReturn(true);
            var members = List.of(new SrsConsumerStore.RetentionConsumer(currentMember, Map.of("orders", 0L)),
                    new SrsConsumerStore.RetentionConsumer(peer, Map.of()));
            var oldSnapshot = new SrsConsumerStore.RetentionSnapshot(old, members);
            var newSnapshot = new SrsConsumerStore.RetentionSnapshot(opened, members);
            var refreshed = new CountDownLatch(1);
            doAnswer(call -> {
                if (chain.get() == opened) refreshed.countDown();
                return null;
            }).when(f.runtime).retain(eq(BUSINESS), eq(held.chain()), eq(fence), any());
            when(stores.consumers().retentionSnapshot(held.chain()))
                    .thenAnswer(call -> Optional.of(chain.get() == old ? oldSnapshot : newSnapshot));

            assertThat(refreshed.await(5, TimeUnit.SECONDS)).isTrue();
            verify(f.runtime).startTail(eq(BUSINESS), eq(opened), eq(fence), any(), any(), any(), any(), anyBoolean());
            verify(f.runtime, never()).retain(eq(BUSINESS), eq(held.chain()), eq(fence), any(), same(oldSnapshot));
        }
    }

    @Test void unavailableRetentionSnapshotKeepsTheHealthyTailAndRetriesCleanup() throws InterruptedException {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var stores = f.sources.srs(BUSINESS, () -> {});
            var failures = new AtomicInteger();
            var retained = new CountDownLatch(2);
            doAnswer(call -> {
                if (failures.get() > 0) retained.countDown();
                return null;
            }).when(f.runtime).retain(eq(BUSINESS), eq(held.chain()),
                    eq(WorkloadClaimFence.from(held.claim())), any());
            when(stores.consumers().retentionSnapshot(held.chain())).thenAnswer(call -> {
                failures.incrementAndGet();
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "snapshot unavailable"), null);
            });

            assertThat(retained.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failures).hasValueGreaterThanOrEqualTo(2);
            assertThat(held.run().captureFailure("flow")).isEmpty();
            verify(f.ownership, never()).release(held.claim());
            verify(held.tail(), never()).close();
            verify(f.runtime, times(1)).startTail(any(), any(), any(), any(), any(), any(), any(), anyBoolean());
        }
    }

    @Test void unavailableRetentionSnapshotStillRejectsALostCaptureOwner() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var stores = f.sources.srs(BUSINESS, () -> {});
            var fenced = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            when(stores.consumers().retentionSnapshot(held.chain())).thenAnswer(call -> {
                when(f.authorization.requireCapture(any(), any())).thenThrow(fenced);
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "snapshot unavailable"), null);
            });

            verify(f.ownership, timeout(3000)).release(held.claim());
            assertThat(held.run().captureFailure("flow")).contains(fenced);
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = IoError.class,
            names = {"WORKLOAD_CLAIM_FENCED", "CACHE_BUSINESS_FENCED", "DOCUMENT_UNREADABLE"})
    void retentionSnapshotFencesAndDamagedFactsStillEndTheCapture(IoError code) {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var stores = f.sources.srs(BUSINESS, () -> {});
            var failure = new TapstateException(code, Map.of(), null);
            when(stores.consumers().retentionSnapshot(held.chain())).thenThrow(failure);

            verify(f.ownership, timeout(3000)).release(held.claim());
            assertThat(held.run().captureFailure("flow")).contains(failure);
        }
    }

    @Test void unavailableRetentionRetriesWithoutEndingTheOriginalCapture() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var temporary = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "controlled conflict"), null);
            doThrow(temporary).doNothing().when(f.runtime).retain(any(), anyString(), any(), any());
            verify(f.runtime, timeout(4000).atLeast(2)).retain(eq(BUSINESS), eq(held.chain()), eq(WorkloadClaimFence.from(held.claim())), any());
            assertThat(held.run().captureFailure("flow")).isEmpty();
            verify(f.ownership, never()).release(held.claim());
            verify(held.tail(), never()).close();
            verify(f.runtime, times(1)).startTail(any(), any(), any(), any(), any(), any(), any(), anyBoolean());
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = IoError.class,
            names = {"WORKLOAD_CLAIM_FENCED", "CACHE_BUSINESS_FENCED", "DOCUMENT_UNREADABLE"})
    void retentionFencesAndCorruptStateStillEndTheCapture(IoError code) {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var failure = new TapstateException(code, Map.of(), null);
            doThrow(failure).when(f.runtime).retain(any(), anyString(), any(), any());
            verify(f.ownership, timeout(3000)).release(held.claim());
            assertThat(held.run().captureFailure("flow")).contains(failure);
            verify(held.tail()).close();
        }
    }

    @Test void unavailableRetentionCannotHideOwnershipLostDuringTheAttempt() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var held = ownedShared(f);
            var fenced = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            doAnswer(call -> {
                when(f.authorization.requireCapture(any(), any())).thenThrow(fenced);
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "controlled conflict"), null);
            }).when(f.runtime).retain(any(), anyString(), any(), any());
            verify(f.ownership, timeout(3000)).release(held.claim());
            assertThat(held.run().captureFailure("flow")).contains(fenced);
        }
    }

    private record OwnedShared(PipelineCaptureCoordinator run, WorkloadClaim claim,
            NativeSrsCaptureRunUnit.Tail tail, String chain,CapturePreparation preparation) { }

    private record PendingReport(OwnedShared held,Runnable original,Runnable local,Runnable settled,Throwable failure) { }
    private static PendingReport pendingTailFailure(Fixture f) {
        return pendingTailFailure(f,null);
    }
    private static PendingReport pendingTailFailure(Fixture f,CountDownLatch allowClose) {
        return pendingTailFailure(f, allowClose, false);
    }
    private static PendingReport pendingTailFailure(Fixture f,CountDownLatch allowClose,boolean typedFact) {
    var port=mock(PdkCapturePort.class,RETURNS_SELF);f.portOverride.set(port);
    var runtime=mock(PipelineAlertRuntime.class);var storage=mock(StorageBinding.class);
    when(storage.defaultSourceId()).thenReturn("other");
    var signals=new BuiltinResourceSignals(()->runtime,()->storage,
            new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.systemUTC());
    var source=new SourceBindingSnapshot(f.source,io.tapstate.core.model.canonical.CanonicalHash.of(f.source),
            new SourceBindingIdentity("input","e6e8d616-985e-49cc-a0f4-9802897abf95",1));
    var observers=new ConcurrentHashMap<String,SourceOperationObserver>();
    f.coordinator.withSourceOperations(id->Optional.of(source),(identity,domain,guard,local)->{
        var observer=signals.source(identity,domain,guard,local);observers.put(domain,observer);return observer;
    });
    var held=ownedShared(f,true);
    var observer=observers.entrySet().stream().filter(e->e.getKey().endsWith(":stream")).findFirst().orElseThrow().getValue();
    var drive=observer.fork("stream");
    drive.completed("orders",SourceOperationObserver.Capability.STREAM_READ,new java.net.ConnectException("connection failed"));
    var original=org.mockito.ArgumentCaptor.forClass(Runnable.class);
    var local=org.mockito.ArgumentCaptor.forClass(Runnable.class);
    var settled=org.mockito.ArgumentCaptor.forClass(Runnable.class);
    verify(runtime).resourceInput(any(),eq("source.connection-failed"),eq(1L),anyString(),original.capture(),local.capture(),
            argThat(v->v instanceof io.tapstate.core.alerting.AlertInput.Operational op&&!op.succeeded()),settled.capture());
    var ioClosed=new CountDownLatch(1);
    doAnswer(call->{observer.close();drive.close();ioClosed.countDown();if(allowClose!=null)allowClose.await();return null;}).when(held.tail()).close();
    var failure=new TapstateException(ConnectorError.CAPTURE_FAILED,Map.of("connector","fixture","detail","connection failed"),null);
    var health=held.tail().health();
    if (typedFact) health.recording(mock(CaptureListener.class)).onFailure(new io.tapstate.core.common.FailureFact(
            failure, Map.of("code", failure.code().code(), "rawMessage", "redacted original"),
            new io.tapstate.core.common.FailureFact.PublicFailure(failure.code().code(),
                    Map.of("connector", "fixture", "detail", "connection failed"))));
    else health.recording(mock(CaptureListener.class)).onError(failure);
    assertThat(health.failure()).contains(failure);
    try {assertThat(ioClosed.await(3,TimeUnit.SECONDS)).isTrue();}
    catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
    if(allowClose==null)verify(held.tail()).close();
        return new PendingReport(held,original.getValue(),local.getValue(),settled.getValue(),failure);
    }
    @Test void tailFailureClosesIoButKeepsCapturedReportProofUntilSettlement() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report=pendingTailFailure(f);
            assertThat(report.held().run().captureFailure("flow")).contains(report.failure());
            assertThatThrownBy(()->f.tailGuard.get().run()).isInstanceOf(TapstateException.class);
            assertThatCode(report.original()::run).doesNotThrowAnyException();
            assertThatCode(report.local()::run).doesNotThrowAnyException();
            verify(f.ownership,never()).release(report.held().claim());
            report.held().run().stopCapture("flow",false);
            assertThatCode(report.local()::run).doesNotThrowAnyException();
            report.settled().run();
            verify(f.ownership,timeout(3000)).release(report.held().claim());
            assertThatThrownBy(report.original()::run).isInstanceOf(TapstateException.class);
        }
    }
    @Test void aReportDrainDeadlineCannotBeExtendedByAnotherMaintenancePass() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report=pendingTailFailure(f);assertThatCode(report.local()::run).doesNotThrowAnyException();
            f.reportNanos.addAndGet(Duration.ofSeconds(16).toNanos());
            assertThatThrownBy(report.original()::run).isInstanceOf(TapstateException.class);
            assertThatThrownBy(report.local()::run).isInstanceOf(TapstateException.class);
            verify(f.ownership,timeout(3000)).release(report.held().claim());report.settled().run();
        }
    }
    @Test void independentLeaseLossStillInvalidatesCapturedReportsImmediately() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report=pendingTailFailure(f);var lost=new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            when(f.authorization.requireCapture(any(),any())).thenThrow(lost);
            doThrow(lost).when(f.authorization).requireCaptureLocal(any(),any());
            assertThatThrownBy(report.original()::run).isSameAs(lost);assertThatThrownBy(report.local()::run).isSameAs(lost);
            verify(f.ownership,timeout(3000)).release(report.held().claim());report.settled().run();
        }
    }
    @Test void hostCloseDoesNotKeepReportAuthorityAliveForItsDrainWindow() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report=pendingTailFailure(f);f.coordinator.close();
            assertThatThrownBy(report.original()::run).isInstanceOf(TapstateException.class);
            assertThatThrownBy(report.local()::run).isInstanceOf(TapstateException.class);
            verify(f.ownership).release(report.held().claim());report.settled().run();
        }
    }
    @Test void aDrainingCaptureCannotReserveAnotherAttachment() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report=pendingTailFailure(f);report.held().run().stopCapture("flow",false);
            clearInvocations(f.ownership);
            assertThatThrownBy(()->report.held().run().startCapture("flow",f.snapshot(),f.models()))
                    .isInstanceOf(RingNotOpenYet.class);
            verify(f.ownership,never()).acquire(any());
            assertThatCode(report.local()::run).doesNotThrowAnyException();report.settled().run();
            verify(f.ownership,timeout(3000)).release(report.held().claim());
        }
    }
    @Test void pendingReportsKeepTheOriginalClaimNeededAcrossSessionCleanupAndRenewal() {
        try(var f=new Fixture(List.of("input"),8,ReadMode.SNAPSHOT_AND_CDC,Duration.ofMillis(100))) {
            var renewals=new AtomicInteger();
            doAnswer(call->{renewals.incrementAndGet();return Optional.of(call.getArgument(0));}).when(f.ownership).renew(any());
            var report=pendingTailFailure(f);report.held().run().stopCapture("flow",false);int before=renewals.get();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->renewals.get()>=before+2);
            assertThatCode(report.original()::run).doesNotThrowAnyException();
            assertThatCode(report.local()::run).doesNotThrowAnyException();verify(f.ownership,never()).release(report.held().claim());
            report.settled().run();verify(f.ownership,timeout(3000).atLeastOnce()).release(report.held().claim());
        }
    }
    @Test void explicitRetirementDoesNotWaitForAnUnsettledReport() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report=pendingTailFailure(f);report.held().run().stopCapture("flow",false);
            f.coordinator.retireShared(report.held().preparation(),WorkloadClaimFence.from(report.held().claim()));
            assertThatThrownBy(report.original()::run).isInstanceOf(TapstateException.class);
            assertThatThrownBy(report.local()::run).isInstanceOf(TapstateException.class);
            verify(f.ownership).release(report.held().claim());report.settled().run();
        }
    }
    @Test void anExpiredReportDrainCannotReleaseBeforeBlockedPhysicalCleanupExits() {
        var allowClose=new CountDownLatch(1);
        try(var f=new Fixture(List.of("input"),8,ReadMode.SNAPSHOT_AND_CDC,Duration.ofMillis(100))) {
            try {
                var renewals=new AtomicInteger();var released=new AtomicBoolean();
                doAnswer(call->{renewals.incrementAndGet();return Optional.of(call.getArgument(0));}).when(f.ownership).renew(any());
                doAnswer(call->{released.set(true);return null;}).when(f.ownership).release(any());
                var report=pendingTailFailure(f,allowClose);int before=renewals.get();
                report.settled().run();f.reportNanos.addAndGet(Duration.ofSeconds(16).toNanos());
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->released.get()||renewals.get()>=before+2);
                assertThat(released).as("the original claim remains held while physical cleanup is unproved").isFalse();
            }finally{allowClose.countDown();}
        }
    }

    private static SrsConsumerStore consumerStore() {
        var consumers = mock(SrsConsumerStore.class);
        when(consumers.attachedWithChain(anyString(), any(SrsChainStore.class))).thenCallRealMethod();
        return consumers;
    }

    @Test void aMaintenanceCloseDoesNotBlockAccessToAnIndependentPipeline() throws Exception {
        var allowClose = new CountDownLatch(1);
        try (var f = new Fixture(List.of("input"), 8, ReadMode.SNAPSHOT_AND_CDC, Duration.ofMillis(100));
                var calls = Executors.newSingleThreadExecutor()) {
            try {
                pendingTailFailure(f, allowClose);
                var other = new PipelineIntentScope("cluster", BUSINESS.bindingVersion(), BUSINESS.cacheIncarnation(), "independent", 1);
                var checked = calls.submit(() -> f.coordinator.hasScope(other));
                assertThat(checked.get(1, TimeUnit.SECONDS)).isFalse();
            } finally { allowClose.countDown(); }
        }
    }

    @Test void failedStartCleanupLeavesTheSharedCaptureRegistryAvailable() throws Exception {
        var closeEntered = new CountDownLatch(1);
        var allowClose = new CountDownLatch(1);
        var refused = new IllegalStateException("original second source refused its opening");
        var captured = healthyCaptureRun();
        try (var f = new Fixture(List.of("left", "right"), 2); var calls = Executors.newFixedThreadPool(2)) {
            doReturn(captured).doThrow(refused).when(f.runtime).beginSnapshotOnly(any(), anyLong(), any(), any(), any());
            doAnswer(call -> {
                closeEntered.countDown();
                assertThat(allowClose.await(5, TimeUnit.SECONDS)).isTrue();
                return null;
            }).when(f.runtime).stopSnapshot(same(captured));
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            var original = calls.submit(() -> run.startCapture("flow", f.snapshot(), f.models()));
            try {
                assertThat(closeEntered.await(5, TimeUnit.SECONDS)).isTrue();
                var other = new PipelineIntentScope("cluster", BUSINESS.bindingVersion(), BUSINESS.cacheIncarnation(), "independent", 1);
                assertThat(calls.submit(() -> f.coordinator.hasScope(other)).get(1, TimeUnit.SECONDS)).isFalse();
                assertThat(original).isNotDone();
            } finally { allowClose.countDown(); }
            assertThatThrownBy(() -> original.get(5, TimeUnit.SECONDS)).hasCause(refused);
            assertThat(run.isCapturing("flow")).isFalse();
        }
    }

    @Test void aRetryCannotReportAnOriginalSessionWhileFailedStartCleanupIsStillRunning() throws Exception {
        var closeEntered = new CountDownLatch(1);
        var allowClose = new CountDownLatch(1);
        var refused = new IllegalStateException("original second source refused its opening");
        var captured = healthyCaptureRun();
        try (var f = new Fixture(List.of("left", "right"), 2);
                var calls = Executors.newFixedThreadPool(2)) {
            doReturn(captured).doThrow(refused).when(f.runtime).beginSnapshotOnly(any(), anyLong(), any(), any(), any());
            doAnswer(call -> {
                closeEntered.countDown();
                assertThat(allowClose.await(10, TimeUnit.SECONDS)).isTrue();
                return null;
            }).when(f.runtime).stopSnapshot(same(captured));
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            var artifacts = f.snapshot();
            var models = f.models();
            var original = calls.submit(() -> run.startCapture("flow", artifacts, models));
            try {
                assertThat(closeEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(run.isCapturing("flow")).as("the original session is still registered for cleanup").isTrue();
                var retry = calls.submit(() -> run.startCapture("flow", artifacts, models));
                assertThatThrownBy(() -> retry.get(1, TimeUnit.SECONDS))
                        .as("the same-fence start cannot report an already ended session as ready")
                        .hasCauseInstanceOf(TapstateException.class);
            } finally { allowClose.countDown(); }
            assertThatThrownBy(() -> original.get(5, TimeUnit.SECONDS)).hasCause(refused);
            assertThat(run.isCapturing("flow")).isFalse();
        }
    }

    @Test void baselineMismatchCleanupLeavesTheSharedCaptureRegistryAvailable() throws Exception {
        var closeEntered = new CountDownLatch(1);
        var allowClose = new CountDownLatch(1);
        var nativeStores = new SrsStores(mock(SrsChainStore.class, RETURNS_SELF),
                mock(SrsModelStore.class, RETURNS_SELF), mock(SrsLogStore.class),
                mock(SrsConsumerStore.class, RETURNS_SELF));
        try (var f = new Fixture(List.of("input"), 8, ReadMode.CDC_ONLY, Duration.ofSeconds(10), "fixture", nativeStores);
                var calls = Executors.newFixedThreadPool(2)) {
            OwnedShared held = ownedShared(f, false, ignored -> {}, () -> {}, true);
            var constructor = SrsBaselineModelMismatch.class.getDeclaredConstructor(String.class, String.class);
            constructor.setAccessible(true);
            var mismatch = constructor.newInstance(held.chain(), "orders");
            class MismatchedPort implements CapturePort, AutoCloseable {
                @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                    throw mismatch;
                }
                @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("unexpected snapshot"); }
                @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError("unexpected discovery"); }
                @Override public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError("unexpected probe"); }
                @Override public void close() {
                    closeEntered.countDown();
                    try { assertThat(allowClose.await(10, TimeUnit.SECONDS)).isTrue(); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                }
            }
            f.portOverride.set(new MismatchedPort());
            var starting = calls.submit(() -> held.run().startCapture("flow", f.snapshot(), f.models()));
            try {
                assertThat(closeEntered.await(5, TimeUnit.SECONDS)).isTrue();
                var other = new PipelineIntentScope("cluster", BUSINESS.bindingVersion(), BUSINESS.cacheIncarnation(), "independent", 1);
                var checked = calls.submit(() -> f.coordinator.hasScope(other));
                assertThat(checked.get(1, TimeUnit.SECONDS)).isFalse();
            } finally { allowClose.countDown(); }
            assertThatThrownBy(() -> starting.get(5, TimeUnit.SECONDS)).hasCause(mismatch);
        }
    }

    @Test void maintenanceRetriesOriginalRetirementAfterBothReleaseAttemptsFail() throws Exception {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var report = pendingTailFailure(f);
            WorkloadClaim original = report.held().claim();
            report.held().run().stopCapture("flow", false);
            var attempted = new CountDownLatch(2);
            var released = new CountDownLatch(1);
            var calls = new AtomicInteger();
            var unavailable = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "temporary release failure"), null);
            doAnswer(call -> {
                if (calls.incrementAndGet() <= 2) {
                    attempted.countDown();
                    throw unavailable;
                }
                released.countDown();
                return null;
            }).when(f.ownership).release(original);
            report.settled().run();
            assertThat(attempted.await(5, TimeUnit.SECONDS)).as("both initial release attempts failed").isTrue();
            assertThat(released.await(3, TimeUnit.SECONDS))
                    .as("subsequent maintenance retries the retained original owner after storage recovers")
                    .isTrue();
            verify(f.ownership, times(3)).release(original);
        }
    }

    @Test void shutdownWaitsForExplicitRetirementStartedAfterItsOwnerSnapshot() throws Exception {
        var ownerWorkerEntered = new CountDownLatch(1);
        var releaseOwnerWorker = new CountDownLatch(1);
        var endingTenure = new CountDownLatch(1);
        var releaseTenure = new CountDownLatch(1);
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC); var calls = Executors.newFixedThreadPool(2)) {
            var report = pendingTailFailure(f);
            var held = report.held();
            held.run().stopCapture("flow", false);
            WorkloadClaimFence fence = WorkloadClaimFence.from(held.claim());
            doAnswer(call -> {
                endingTenure.countDown();
                assertThat(releaseTenure.await(10, TimeUnit.SECONDS)).isTrue();
                return null;
            }).when(f.ownership).endTenure(fence);
            var field = ScopedPipelineCaptureCoordinator.class.getDeclaredField("ownerOperations");
            field.setAccessible(true);
            var worker = (ExecutorService) field.get(f.coordinator);
            worker.submit(() -> {
                ownerWorkerEntered.countDown();
                return releaseOwnerWorker.await(10, TimeUnit.SECONDS);
            });
            assertThat(ownerWorkerEntered.await(5, TimeUnit.SECONDS)).isTrue();
            var shutdown = calls.submit(f.coordinator::close);
            try {
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(worker::isShutdown);
                var retirement = calls.submit(() -> f.coordinator.retireShared(held.preparation(), fence));
                assertThat(endingTenure.await(5, TimeUnit.SECONDS)).isTrue();
                releaseOwnerWorker.countDown();
                assertThatThrownBy(() -> shutdown.get(1, TimeUnit.SECONDS))
                        .as("the original explicit retirement must finish before host close returns")
                        .isInstanceOf(TimeoutException.class);
                releaseTenure.countDown();
                retirement.get(5, TimeUnit.SECONDS);
                shutdown.get(5, TimeUnit.SECONDS);
                verify(f.ownership).release(held.claim());
                verify(f.ownership).endTenure(fence);
            } finally {
                releaseOwnerWorker.countDown();
                releaseTenure.countDown();
                report.settled().run();
            }
        }
    }

    @Test void anExpiredLocalProofFallsBackWhileFullAuthorityStillStands() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var run = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            run.startCapture("flow", f.snapshot(), f.models());
            assertThat(f.coordinator.observationSelection(ORIGINAL)).isPresent();
            var pass = mock(PipelineConvergencePass.class);
            when(pass.scope()).thenReturn(SCOPE);
            when(pass.claim()).thenReturn(PIPELINE);
            var state = mock(StateStore.class);
            var observations = mock(ObservationStore.class);
            when(pass.state()).thenReturn(state);
            when(pass.observationSample()).thenReturn(observations);
            var result = mock(io.tapstate.runtime.scheduler.ConvergeResult.class);
            when(result.checkpoint()).thenReturn(Optional.of(CheckpointDoc.initial("flow", "RUNNING", Instant.now())));
            var observation = mock(Observation.class);
            when(observation.state()).thenReturn(PipelineState.RUNNING);
            var publisher = mock(io.tapstate.runtime.scheduler.ObservationPublisher.class);
            when(publisher.publishScoped(SCOPE, state, observations, null)).thenAnswer(call -> {
                f.authorization.require(ORIGINAL);
                return Optional.of(observation);
            });
            var cache = mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class);
            var sampling = new NativeObservationSampling(
                    mock(io.tapstate.adapters.mongostore.MongoConfigurationDomain.class), cache, f.coordinator,
                    id -> Optional.of(ORIGINAL), id -> null, "views", Clock.systemUTC());
            var config = new PipelineIntentStore.ActuationReading(null, OptionalLong.of(1));
            assertThat(sampling.publish(publisher, pass, result, config, null)).contains(observation);
            when(f.authorization.requireLocal(ORIGINAL)).thenThrow(new TapstateException(
                    io.tapstate.runtime.engine.EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", "flow"), null));
            assertThat(f.authorization.require(ORIGINAL)).isEqualTo(PIPELINE);
            clearInvocations(f.authorization);
            assertThat(sampling.publish(publisher, pass, result, config, null)).contains(observation);
            verify(f.authorization).requireLocal(ORIGINAL);
            verify(publisher, times(2)).publishScoped(SCOPE, state, observations, null);
            verifyNoInteractions(cache);
            assertThat(f.coordinator.observationSelection(ORIGINAL)).isEmpty();
            var fenced = new TapstateException(io.tapstate.runtime.engine.EngineError.EXECUTION_NOT_AUTHORIZED,
                    Map.of("pipeline", "flow"), null);
            when(f.authorization.require(ORIGINAL)).thenThrow(fenced);
            assertThatThrownBy(() -> sampling.publish(publisher, pass, result, config, null)).isSameAs(fenced);
            assertThatThrownBy(() -> run.snapshotProgress("flow")).isSameAs(fenced);
        }
    }
    @Test void retryReopensFromThePersistedCutWhenThePendingCutCannotBeFlushed() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            f.coordinator.withReadRetries(operation -> new io.tapstate.core.common.RetryPolicy(
                    true, Duration.ofMillis(1), Duration.ofSeconds(30)));
            var port = mock(PdkCapturePort.class, RETURNS_SELF);
            var interrupted = new AtomicReference<java.util.function.Consumer<io.tapstate.core.common.FailureFact>>();
            doAnswer(call -> { interrupted.set(call.getArgument(0)); return port; })
                    .when(port).withStreamRecovery(any(), any(), any());
            f.portOverride.set(port);
            var held = ownedShared(f, true);
            var persistedToken = "proved-durable-cut";
            var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 7), persistedToken),
                    Map.of("orders", new SrsTableCheckpoint(new SrsModelKey(held.chain(), "orders", "cache", 1, 0), 0)));
            String capture = held.preparation().captureId();
            var old = new SrsChainState(held.chain(), capture, "1h", 1, Set.of("orders"), Set.of("orders"),
                    1, checkpoint, Instant.EPOCH, 1, 1);
            var next = new SrsChainState(held.chain(), capture, "1h", 2, Set.of("orders"), Set.of("orders"),
                    1, checkpoint, Instant.EPOCH, 1, 1);
            var stored = new AtomicReference<>(old);
            var chains = f.sources.srs(BUSINESS, () -> {}).chains();
            var fence = WorkloadClaimFence.from(held.claim());
            when(chains.load(held.chain())).thenAnswer(call -> Optional.of(stored.get()));
            when(chains.requestTables(held.chain(), Set.of("orders"), fence)).thenAnswer(call -> stored.get());
            when(chains.openEpoch(held.chain(), 1, fence)).thenAnswer(call -> {
                stored.set(next);
                return OptionalLong.of(2);
            });
            when(chains.publishServingTables(held.chain(), 2, Set.of("orders"), fence)).thenReturn(true);
            var replacement = mock(NativeSrsCaptureRunUnit.Tail.class);
            when(replacement.health()).thenReturn(new CaptureHealth());
            var resume = new AtomicReference<CaptureStart>();
            doAnswer(call -> {
                resume.set(call.getArgument(4));
                return replacement;
            }).when(f.runtime).startTail(eq(BUSINESS), eq(next), eq(fence), any(), any(), any(), any(), anyBoolean());
            var blockedFlush = new TapstateException(IoError.SRS_MODEL_UNAVAILABLE,
                    Map.of("chain", held.chain(), "table", "orders", "version", "0"), null);
            doThrow(blockedFlush).when(held.tail()).finishCheckpointing();

            interrupted.get().accept(new io.tapstate.core.common.FailureFact(new java.io.IOException("source link failed")));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                    resume.get() != null || held.run().captureFailure("flow").isPresent());
            assertThat(held.run().captureFailure("flow")).isEmpty();
            assertThat(resume.get()).isEqualTo(CaptureStart.resume(new SourcePosition(persistedToken)));
            verify(held.tail()).finishCheckpointing();
            verify(held.tail()).close();
        }
    }

    private static OwnedShared ownedShared(Fixture f) {
        return ownedShared(f,false);
    }
    private static OwnedShared ownedShared(Fixture f,boolean openPort) {
        return ownedShared(f, openPort, run -> run.startCapture("flow", f.snapshot(), f.models()), () -> {});
    }
    private static OwnedShared ownedShared(Fixture f, boolean openPort,
            java.util.function.Consumer<PipelineCaptureCoordinator> start, Runnable afterPort) {
        return ownedShared(f, openPort, start, afterPort, false);
    }
    private static OwnedShared ownedShared(Fixture f, boolean openPort,
            java.util.function.Consumer<PipelineCaptureCoordinator> start, Runnable afterPort, boolean realTail) {
        String canonical="mck-key",physical="srs-recovery-"+"b".repeat(64),operation=UUID.randomUUID().toString();
        String captureId=SourceCaptureIdentity.sharedCapture(canonical);
        var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,physical,captureId,
                SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),operation,canonical,UUID.randomUUID().toString());
        var capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,captureId),OWNER,4,2,1,Instant.now().plusSeconds(60));
        var context=new SrsConsumerContext(new SrsConsumerKey(physical,"flow","input"),7);
        var membership=new SrsConsumerMembership(context,1,false,Set.of("orders"),PIPELINE);
        var chains=mock(SrsChainStore.class);var models=mock(SrsModelStore.class);var consumers=consumerStore();
        var chain=new AtomicReference<>(new SrsChainState(physical,captureId,"1h",0,Set.of("orders"),Set.of(),0,null,null,1,1));
        var cursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",-1L),null,List.of(),null,0,Map.of(),ConsumerProgressKind.SRS);
        var progress=new AtomicReference<>(new SrsConsumerProgress(context,1,0,cursor,null));
        when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
        when(f.configuration.inspectSharedHistory(eq(prepared),anyString(),anyString(),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,true));
        when(f.cache.load(prepared)).thenReturn(new CaptureCacheState(prepared,WorkloadClaimFence.from(capture),Optional.of(chain.get())));
        org.mockito.Mockito.doAnswer(call -> f.ownership.acquire(call.getArgument(0))).when(f.ownership).acquireInitial(any());
        when(f.ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
        when(consumers.find(context.key())).thenReturn(Optional.of(membership));
        when(consumers.progress(context)).thenAnswer(ignored -> Optional.of(progress.get()));
        when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,1,List.of(membership)));
        when(consumers.startReadingAfter(eq(context),eq(1L),eq(0L),anyMap(),eq(PIPELINE))).thenAnswer(call -> {
            progress.set(new SrsConsumerProgress(context,1,1,cursor,null,null,call.getArgument(3)));return true;
        });
        when(chains.load(physical)).thenAnswer(ignored -> Optional.of(chain.get()));
        when(chains.requestTables(eq(physical),anySet(),any())).thenAnswer(ignored -> chain.get());
        when(chains.openEpoch(eq(physical),eq(0L),any())).thenAnswer(ignored -> {
            chain.set(new SrsChainState(physical,captureId,"1h",1,Set.of("orders"),Set.of("orders"),0,null,null,1,1));
            return OptionalLong.of(1);
        });
        when(chains.publishServingTables(eq(physical),eq(1L),anySet(),any())).thenReturn(true);
        var model=new SrsModel(new SrsModelKey(physical,"orders","cache",1,0),SrsRingbuffer.ringName(physical,"orders"),
                new SourceTable("orders",List.of(),List.of(),List.of()),0);
        when(models.publishedForNewSnapshot(anyString(),anyMap())).thenReturn(Optional.of(Map.of("orders",model)));
        when(f.sources.srs(any(),any())).thenReturn(new SrsStores(chains,models,mock(SrsLogStore.class),consumers));
        doReturn(-1L).when(f.runtime).initialReadAfter(any(),anyString(),anyString(),any(),anyString(),any());
        doNothing().when(f.runtime).retain(any(),anyString(),any(),any());
        var tail=mock(NativeSrsCaptureRunUnit.Tail.class);when(tail.health()).thenReturn(new CaptureHealth());
        if (!realTail) doAnswer(call->{
            if(openPort){f.tailGuard.set(call.getArgument(6));java.util.function.Function<SrsCaptureWriter,CapturePort> factory=call.getArgument(5);factory.apply(mock(SrsCaptureWriter.class));afterPort.run();}
            return tail;
        }).when(f.runtime).startTail(any(),any(),any(),any(),any(),any(),any(),anyBoolean());
        var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);start.accept(run);
        return new OwnedShared(run, capture, tail, physical,prepared);
    }

    @Test void aSharedTailOpeningFailureBelongsOnlyToTheRunThatOpenedIt() {
        try (var f = new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var failure = new TapstateException(ConnectorError.CAPTURE_FAILED,
                    Map.of("connector", "fixture", "detail", "refused"), null);
            var fact = io.tapstate.core.common.FailureFact.unavailable(failure);
            var receiver = new AtomicReference<java.util.function.Consumer<io.tapstate.core.common.FailureFact>>();
            var port = mock(PdkCapturePort.class, RETURNS_SELF);
            doAnswer(call -> { receiver.set(call.getArgument(0)); return port; }).when(port).withOpeningFailure(any());
            f.portOverride.set(port);
            var held = ownedShared(f, true,
                    run -> assertThatThrownBy(() -> run.startCapture("flow", f.snapshot(), f.models())).isSameAs(failure),
                    () -> { receiver.get().accept(fact); throw failure; });
            var other = f.coordinator.forRun(SCOPE, PIPELINE, ORIGINAL);
            assertThat(other.openingFailureFact("flow", failure).diagnostic()).isEmpty();
            assertThat(held.run().openingFailureFact("flow", failure)).isSameAs(fact);
            assertThat(held.run().openingFailureFact("flow", failure).diagnostic()).isEmpty();
        }
    }

    @Test void aTrustedReplacementUsesPhysicalRingsWhileKeepingCanonicalCaptureNotesAndHistory() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            String canonical="mck-key",physical="srs-recovery-"+"b".repeat(64),operation=UUID.randomUUID().toString();
            String captureId=SourceCaptureIdentity.sharedCapture(canonical);
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,physical,captureId,
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),operation,canonical,UUID.randomUUID().toString());
            var capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,captureId),OWNER,4,2,1,Instant.now().plusSeconds(60));
            var context=new SrsConsumerContext(new SrsConsumerKey(physical,"flow","input"),7);
            var membership=new SrsConsumerMembership(context,1,false,Set.of("orders"),PIPELINE);
            var chains=mock(SrsChainStore.class);var models=mock(SrsModelStore.class);var consumers=consumerStore();
            var chain=new AtomicReference<>(new SrsChainState(physical,captureId,"1h",0,Set.of("orders"),Set.of(),0,null,null,1,1));
            var cursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",-1L),null,List.of(),null,0,Map.of(),ConsumerProgressKind.SRS);
            var progress=new AtomicReference<>(new SrsConsumerProgress(context,1,0,cursor,null));
            when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(f.configuration.inspectSharedHistory(eq(prepared),anyString(),anyString(),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,true));
            when(f.cache.load(prepared)).thenReturn(new CaptureCacheState(prepared,WorkloadClaimFence.from(capture),Optional.of(chain.get())));
            org.mockito.Mockito.doAnswer(call -> f.ownership.acquire(call.getArgument(0))).when(f.ownership).acquireInitial(any());
            when(f.ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
            when(consumers.find(context.key())).thenReturn(Optional.of(membership));
            when(consumers.progress(context)).thenAnswer(ignored -> Optional.of(progress.get()));
            when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,1,List.of(membership)));
            when(consumers.startReadingAfter(eq(context),eq(1L),eq(0L),anyMap(),eq(PIPELINE))).thenAnswer(call -> {
                progress.set(new SrsConsumerProgress(context,1,1,cursor,null,null,call.getArgument(3)));return true;
            });
            when(chains.load(physical)).thenAnswer(ignored -> Optional.of(chain.get()));
            when(chains.requestTables(eq(physical),anySet(),any())).thenAnswer(ignored -> chain.get());
            when(chains.openEpoch(eq(physical),eq(0L),any())).thenAnswer(ignored -> {
                chain.set(new SrsChainState(physical,captureId,"1h",1,Set.of("orders"),Set.of("orders"),0,null,null,1,1));
                return OptionalLong.of(1);
            });
            when(chains.publishServingTables(eq(physical),eq(1L),anySet(),any())).thenReturn(true);
            var model=new SrsModel(new SrsModelKey(physical,"orders","cache",1,0),SrsRingbuffer.ringName(physical,"orders"),
                    new SourceTable("orders",List.of(),List.of(),List.of()),0);
            when(models.publishedForNewSnapshot(anyString(),anyMap())).thenReturn(Optional.of(Map.of("orders",model)));
            when(f.sources.srs(any(),any())).thenReturn(new SrsStores(chains,models,mock(SrsLogStore.class),consumers));
            doReturn(-1L).when(f.runtime).initialReadAfter(any(),anyString(),anyString(),any(),anyString(),any());
            doNothing().when(f.runtime).retain(any(),anyString(),any(),any());
            var tail=mock(NativeSrsCaptureRunUnit.Tail.class);when(tail.health()).thenReturn(new CaptureHealth());
            doReturn(tail).when(f.runtime).startTail(any(),any(),any(),any(),any(),any(),any(),anyBoolean());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var bindings=run.captureBindings();String ring=SrsRingbuffer.ringName(physical,"orders");
            assertThat(bindings.logged().get("input").run().miningChainId()).isEqualTo(physical);
            verify(models).publishedForNewSnapshot(physical,Map.of("orders",ring));
            verify(f.runtime).initialReadAfter(eq(BUSINESS),eq(physical),eq("orders"),any(),eq("1h"),any());
            assertThat(f.buffer.snapshotState(bindings.bufferKey("input"),ring).declared()).isTrue();
            assertThat(f.buffer.snapshotState(bindings.bufferKey("input"),SrsRingbuffer.ringName(canonical,"orders")).declared()).isFalse();
            verify(f.notes,atLeastOnce()).open(new RequiredNoteOwner(1,"pdk.chain."+canonical,RequiredNoteOwner.Kind.CHAIN,canonical,0));
            verify(f.notes,never()).open(new RequiredNoteOwner(1,"pdk.chain."+physical,RequiredNoteOwner.Kind.CHAIN,physical,0));
            verify(f.ownership).acquire(new CaptureId(captureId));
            verify(f.runtime).startTail(eq(BUSINESS),any(),eq(WorkloadClaimFence.from(capture)),any(),any(),any(),any(),eq(true));
            verify(f.configuration).inspectSharedHistory(eq(prepared),eq("input"),anyString(),eq("flow"),anyString());
        }
    }

    @Test void aReplacementFreshFullCallbackKeepsItsCanonicalIdentityInThePublishedCaptureBinding() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            String canonical="mck-key",physical="srs-recovery-"+"c".repeat(64),captureId=SourceCaptureIdentity.sharedCapture(canonical);
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,physical,captureId,
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString(),canonical,UUID.randomUUID().toString());
            var capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,captureId),OWNER,4,2,1,Instant.now().plusSeconds(60));
            var context=new SrsConsumerContext(new SrsConsumerKey(physical,"flow","input"),7);
            var membership=new SrsConsumerMembership(context,1,false,Set.of("orders"),PIPELINE);
            var oldModel=new SrsModelKey(physical,"orders","cache",1,0);
            var checkpoint=new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,13),"fixed-cut"),Map.of("orders",new SrsTableCheckpoint(oldModel,13)));
            var originalChain=new SrsChainState(physical,captureId,"1h",1,Set.of("orders"),Set.of("orders"),1,checkpoint,Instant.EPOCH,1,1);
            var openedChain=new SrsChainState(physical,captureId,"1h",2,Set.of("orders"),Set.of("orders"),1,checkpoint,Instant.EPOCH,1,1);
            var cursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",13L),null,List.of(),null,0,Map.of(),ConsumerProgressKind.SRS);
            var retained=new SrsConsumerProgress(context,1,1,cursor,null);
            var consumers=consumerStore();var chains=mock(SrsChainStore.class);
            when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(f.configuration.inspectSharedHistory(eq(prepared),anyString(),anyString(),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,true));
            when(f.cache.load(prepared)).thenReturn(new CaptureCacheState(prepared,WorkloadClaimFence.from(capture),Optional.of(originalChain)));
            org.mockito.Mockito.doAnswer(call -> f.ownership.acquire(call.getArgument(0))).when(f.ownership).acquireInitial(any());
            when(f.ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
            when(consumers.find(context.key())).thenReturn(Optional.of(membership));
            when(consumers.progress(context)).thenReturn(Optional.of(retained));
            when(consumers.attached(physical)).thenReturn(new SrsConsumerTopology(BUSINESS,physical,1,List.of(membership)));
            var current=new AtomicReference<>(originalChain);
            when(chains.load(physical)).thenAnswer(ignored -> Optional.of(current.get()));
            when(chains.requestTables(eq(physical),anySet(),any())).thenAnswer(ignored -> current.get());
            when(chains.openEpoch(eq(physical),eq(1L),any())).thenAnswer(ignored -> {current.set(openedChain);return OptionalLong.of(2);});
            when(chains.publishServingTables(eq(physical),eq(2L),anySet(),any())).thenReturn(true);
            when(f.sources.srs(any(),any())).thenReturn(new SrsStores(chains,mock(SrsModelStore.class),mock(SrsLogStore.class),consumers));
            var completed=mock(CompletedPipelineFull.class);
            when(f.completedFull.find(eq(SCOPE),any(),anyMap())).thenReturn(Optional.of(completed));
            var full=mock(SrsFreshFullPreparation.class);when(full.originalChain()).thenReturn(openedChain);
            when(consumers.inspectFreshFull(eq(completed),any(),eq(1L),any(),eq(WorkloadClaimFence.from(capture)))).thenReturn(full);
            var origin=new SrsFreshFullOrigin("clear-full",SCOPE,"full-preparation","a".repeat(64),"input");
            var nextModel=new SrsModelKey(physical,"orders","cache",2,0);
            var pending=new SrsFreshFull(origin,2,Map.of("orders",nextModel));
            var published=new SrsChainState(physical,captureId,"1h",2,Set.of("orders"),Set.of("orders"),1,checkpoint,Instant.EPOCH,1,1,
                    new SourceResumeOverride("fixed-cut",1,Instant.EPOCH),pending);
            var nextCursor=new ConsumerOffset(context.key().consumerId(),Map.of("orders",13L),null,List.of(),"fixed-cut",2,Map.of(),ConsumerProgressKind.SRS);
            var progress=new SrsConsumerProgress(context,1,2,nextCursor,new SrsSnapshotPin(2,"fixed-cut",Map.of("orders",nextModel)),null,Map.of("orders",13L));
            var receipt=new SrsFreshFullReceipt(published,progress);
            var mismatch=mock(SrsBaselineModelMismatch.class);when(mismatch.chainId()).thenReturn(physical);when(mismatch.getSuppressed()).thenReturn(new Throwable[0]);
            var tail=mock(NativeSrsCaptureRunUnit.Tail.class);when(tail.health()).thenReturn(new CaptureHealth());
            doThrow(mismatch).doReturn(tail).when(f.runtime).startTail(any(),any(),any(),any(),any(),any(),any(),anyBoolean());
            doNothing().when(f.runtime).retain(any(),anyString(),any(),any());
            doAnswer(call -> {
                Runnable guard=call.getArgument(5);guard.run();
                java.util.function.Consumer<SrsFreshFullReceipt> callback=call.getArgument(8);callback.accept(receipt);
                return healthyCaptureRun();
            }).when(f.runtime).beginSnapshot(any(),any(),eq(2L),any(),any(),any(),eq(full),any(),any());
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var binding=run.captureBindings().logged().get("input");
            assertThat(binding.canonicalChainId()).isEqualTo(canonical);
            assertThat(binding.run().miningChainId()).isEqualTo(physical);assertThat(binding.run().scope()).isEqualTo(SCOPE);
            assertThat(binding.snapshotEpoch()).isEqualTo(2);assertThat(binding.recoveryAfter()).containsExactly(Map.entry("orders",13L));
            verify(f.runtime,times(2)).startTail(eq(BUSINESS),any(),eq(WorkloadClaimFence.from(capture)),any(),any(),any(),any(),eq(true));
        }
    }

    @Test void aReservedTrustedMappingRefusesBeforeTakingCaptureOwnershipOrOpeningPorts() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            when(f.configuration.issueFirstCreate(any())).thenThrow(new TapstateException(IoError.CAPTURE_STATE_REBUILD_REQUIRED,Map.of("capture","canonical"),null));
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CAPTURE_STATE_REBUILD_REQUIRED));
            verifyNoInteractions(f.ownership);assertThat(f.opened).hasValue(0);verify(f.cache,never()).load(any());
            assertThat(run.isCapturing("flow")).isFalse();
        }
    }

    @Test void aPreparedReplacementForAnotherCanonicalSourceRefusesBeforeCaptureOwnership() {
        try(var f=new Fixture(ReadMode.SNAPSHOT_AND_CDC)) {
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"srs-recovery-"+"b".repeat(64),"foreign-capture",
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString(),"foreign-canonical",UUID.randomUUID().toString());
            var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,prepared.captureId()),OWNER,1,0,1);
            var chain=new SrsChainState(prepared.chainId(),prepared.captureId(),"1h",1,Set.of("orders"),Set.of("orders"),0,null,null,1,1);
            var cached=new CaptureCacheState(prepared,claim,Optional.of(chain));
            when(f.configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            when(f.cache.load(prepared)).thenReturn(cached);
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);
            assertThatThrownBy(() -> run.startCapture("flow",f.snapshot(),f.models())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
            verifyNoInteractions(f.ownership);assertThat(f.opened).hasValue(0);assertThat(run.isCapturing("flow")).isFalse();
        }
    }

    /** Holds the reader inside next() until the explicit closer has consumed the one-shot failure. */
    private static final class PausedRead implements AutoCloseable {
        final CountDownLatch entered=new CountDownLatch(1),resume=new CountDownLatch(1);
        final AtomicReference<Thread> closer=new AtomicReference<>();
        void beforeRow(int row) {
            if(row!=2)return;
            entered.countDown();
            try {if(!resume.await(3,TimeUnit.SECONDS))throw new AssertionError("reader pause was not released");}
            catch(InterruptedException interrupted) {Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
        }
        void awaitEntered() throws InterruptedException {assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();}
        void closed() {closer.compareAndSet(null,Thread.currentThread());close();}
        @Override public void close() {resume.countDown();}
    }
    private static final class Port implements CapturePort,SnapshotOnlyCapture {
        final String source; final Runnable guard; final AtomicInteger closed,read; final int count; final AtomicReference<Throwable> closeFailure;
        final PausedRead pausedRead;
        Port(String source,Runnable guard,AtomicInteger closed,AtomicInteger read,int count,AtomicReference<Throwable> closeFailure,PausedRead pausedRead) {
            this.source=source;this.guard=guard;this.closed=closed;this.read=read;this.count=count;this.closeFailure=closeFailure;this.pausedRead=pausedRead;
        }
        public CaptureBatch snapshotOnly(CaptureConfig config) {
            guard.run(); var rows=java.util.stream.IntStream.range(0,count).mapToObj(i -> Envelope.read(1,"orders",Map.of("source",source,"id",i),null)).iterator(); var ended=new AtomicBoolean();
            return new CaptureBatch() {
                public boolean hasNext() { guard.run(); return rows.hasNext(); }
                public Envelope next() { guard.run();int row=read.incrementAndGet();if(pausedRead!=null)pausedRead.beforeRow(row);return rows.next(); }
                public Optional<SourcePosition> seam() { throw new AssertionError("bounded reads have no seam"); }
                public void close() { if(ended.compareAndSet(false,true)) {
                    try {closed.incrementAndGet();var failure=closeFailure.get();if(failure instanceof RuntimeException runtime)throw runtime;if(failure instanceof Error error)throw error;}
                    finally {if(pausedRead!=null)pausedRead.closed();}
                } }
            };
        }
        public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError(); }
        public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) { throw new AssertionError(); }
        public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
        public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void snapshotCallbacksUseTheVerifiedSourcePolicyWithoutChangingTheFrozenModel(boolean sampled) {
        try(var f=new Fixture(List.of("input"),8,ReadMode.SNAPSHOT_AND_CDC,Duration.ofSeconds(10),sampled?"mongodb":"fixture")) {
            var frozen=new SourceTable("orders",List.of(new SourceField("old-sample","STRING(100)")),List.of(),List.of());
            f.memory.schemas().save(new DiscoveredSourceModel("input",f.source.connector(),1L,new SourceModel(List.of(frozen))));
            var run=f.coordinator.forRun(SCOPE,PIPELINE,ORIGINAL);run.startCapture("flow",f.snapshot(),f.models());
            var request=f.preparation.get().request();var seam=new SourcePosition("current-sample");
            var observed=Map.of("orders",new SourceTable("orders",List.of(new SourceField("new-sample","INT32")),List.of(),List.of()));
            if(sampled) {
                f.snapshotCallback.get().prepare(request,seam,observed);
                verify(f.preparation.get()).prepare(request,seam,observed,SourceModelComparison.SAMPLED_DOCUMENT);
            } else {
                assertThatThrownBy(() -> f.snapshotCallback.get().prepare(request,seam,observed))
                        .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.SRS_FULL_PREPARATION_UNPROVEN));
                verify(f.preparation.get(),never()).prepare(any(),any(),anyMap(),any());
            }
            assertThat(f.models().model(f.source).tables()).containsExactly(frozen);
        }
    }

    @Test void retainedPlanningUsesOnlyThisConsumersExactPinAndNeverReplacesItsTargetInputsWithNewSamples() {
        try(var f=new Fixture(List.of("input"),8,ReadMode.SNAPSHOT_AND_CDC,Duration.ofSeconds(10),"mongodb")) {
            var observed=new SourceTable("orders",List.of(new SourceField("new-sample","STRING(20480)")),List.of(),List.of());
            f.memory.schemas().save(new DiscoveredSourceModel("input","mongodb",1,new SourceModel(List.of(observed))));
            String canonical="mck-key";String captureId=SourceCaptureIdentity.sharedCapture(canonical);
            var prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,canonical,captureId,
                    SourceCaptureIdentity.physical(f.source.connector(),f.source.config()),UUID.randomUUID().toString());
            when(f.configuration.read(BUSINESS,captureId)).thenReturn(Optional.of(prepared));
            var origin=ConnectorNoteOrigin.capture("mongodb","a".repeat(64),f.source.config());
            when(f.configuration.inspectSharedOrigin(eq(BUSINESS),eq("input"),anyString(),anyString(),anyString())).thenReturn(Optional.of(origin));
            when(f.connectors.resolveRetained(any(),any(),eq(origin))).thenCallRealMethod();
            var key=f.streamingProgress.get().snapshot().models().get("orders");
            var pinned=new SourceTable("orders",List.of(new SourceField("old-sample","STRING(100)")),List.of(),List.of());
            var srs=f.sources.srs(BUSINESS,() -> {});
            when(srs.models().require(key)).thenReturn(new SrsModel(key,SrsRingbuffer.ringName(canonical,"orders"),pinned,0));
            var pass=mock(PipelineConvergencePass.class);when(pass.scope()).thenReturn(SCOPE);
            var captured=f.models();
            var planning=f.coordinator.retainedPlanningModels(pass, f.snapshot(), captured, guard -> false);
            assertThat(planning.modelForBinding("input", f.source).tables()).containsExactly(pinned);
            assertThat(captured.model(f.source).tables()).containsExactly(observed);
            assertThat(f.models().model(f.source).tables()).containsExactly(observed);
            verify(srs.models(),never()).publishedForNewSnapshot(anyString(),anyMap());
            var missing=new TapstateException(IoError.SRS_MODEL_UNAVAILABLE,Map.of("chain",canonical,"table","orders","version","0"),null);
            when(srs.models().require(key)).thenThrow(missing);
            assertThatThrownBy(() -> f.coordinator.retainedPlanningModels(pass, f.snapshot(), captured, guard -> false)).isSameAs(missing);
        }
    }

    private static final class Fixture implements AutoCloseable {
        final InMemoryStorePort memory=new InMemoryStorePort();
        final PipelineStoreViews pipelines=mock(PipelineStoreViews.class);
        final SrsStoreViews sources=mock(SrsStoreViews.class);
        final CaptureStatePreparations configuration=mock(CaptureStatePreparations.class);
        final CaptureCachePreparations cache=mock(CaptureCachePreparations.class);
        final RequiredNoteStore notes=mock(RequiredNoteStore.class);
        final SnapshotStateStore snapshots=mock(SnapshotStateStore.class);
        final ExecutionAuthorization authorization=mock(ExecutionAuthorization.class);
        final CaptureOwnership ownership=mock(CaptureOwnership.class);
        final ConnectorProvisioner connectors=mock(ConnectorProvisioner.class);
        final AtomicReference<io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer> snapshotCallback=new AtomicReference<>();
        final AtomicReference<SrsSnapshotPreparation> preparation=new AtomicReference<>();
        final ScopedPipelineCaptureCoordinator.BoundedLoads boundedLoads=mock(ScopedPipelineCaptureCoordinator.BoundedLoads.class);
        final ScopedPipelineCaptureCoordinator.CompletedFullLookup completedFull=mock(ScopedPipelineCaptureCoordinator.CompletedFullLookup.class);
        final SnapshotBuffer buffer;
        final NativeSrsCaptureRunUnit runtime;
        final ScopedPipelineCaptureCoordinator coordinator;
        final SourceResource source;
        final List<SourceResource> definitions=new ArrayList<>();
        final AtomicInteger opened=new AtomicInteger(),closedPorts=new AtomicInteger(),readRows=new AtomicInteger(),rowCount=new AtomicInteger(1);
        final AtomicReference<Throwable> closeFailure=new AtomicReference<>();
        final AtomicReference<CapturePort> portOverride=new AtomicReference<>();
        final AtomicBoolean checkPortIdentity = new AtomicBoolean();
        final List<ConfiguredCaptureRun> checkedConfigurations = new CopyOnWriteArrayList<>();
        final AtomicReference<Runnable> tailGuard=new AtomicReference<>();
        final AtomicLong reportNanos=new AtomicLong();
        PausedRead pausedRead;
        PausedRead pauseSecondRead() {pausedRead=new PausedRead();return pausedRead;}
        final WorkloadClaim capture;
        final SrsConsumerContext streamingContext = new SrsConsumerContext(new SrsConsumerKey("mck-key", "flow", "input"), 7);
        final AtomicReference<SrsConsumerProgress> streamingProgress = new AtomicReference<>();
        Fixture(ReadMode mode) { this(List.of("input"), 8, mode); }
        Fixture() { this(List.of("input")); }
        Fixture(List<String> ids) { this(ids,8); }
        Fixture(List<String> ids,int capacity) { this(ids, capacity, ReadMode.SNAPSHOT_ONLY); }
        Fixture(List<String> ids,int capacity,ReadMode mode) {this(ids,capacity,mode,Duration.ofSeconds(10));}
        Fixture(List<String> ids,int capacity,ReadMode mode,Duration renewal) {
            this(ids,capacity,mode,renewal,"fixture");
        }
        Fixture(List<String> ids,int capacity,ReadMode mode,Duration renewal,String connector) {
            this(ids, capacity, mode, renewal, connector, new SrsStores(mock(SrsChainStore.class),
                    mock(SrsModelStore.class), mock(SrsLogStore.class), mock(SrsConsumerStore.class)));
        }
        Fixture(List<String> ids,int capacity,ReadMode mode,Duration renewal,String connector,SrsStores nativeStores) {
            buffer=new SnapshotBuffer(capacity);
            for(String id:ids) {
                var definition=new SourceResource(id,null,connector,Map.of("database","x"),SourceMode.CDC,List.of(TableRef.literal("orders")),new Srs("key",null,null,null,true),null);
                definitions.add(definition); memory.artifacts().create(definition);
            }
            source=definitions.getFirst();
            var pipeline=new PipelineResource("flow",null,ids.stream().<SourceRef>map(id -> SourceRef.spec(id,true)).toList(),null,null,null,
                    new Settings(null,null,null,null,mode,"latest"),null);
            memory.artifacts().create(pipeline);
            boolean bounded = mode == ReadMode.SNAPSHOT_ONLY;
            String id = bounded ? SourceCaptureIdentity.snapshotCapture("mck-key", Set.of("orders")) : "mck-key";
            var prepared=new CapturePreparation(BUSINESS,bounded?CapturePreparation.Mode.SNAPSHOT_ONLY:CapturePreparation.Mode.SHARED_SRS,"mck-key",id,SourceCaptureIdentity.physical(source.connector(),source.config()),UUID.randomUUID().toString());
            capture=new WorkloadClaim(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,id),bounded?OWNER:new WorkloadOwner("remote","remote-boot"),1,0,1,Instant.now().plusSeconds(60));
            when(configuration.issueFirstCreate(any())).thenReturn(new CaptureCreationResult.AlreadyIssued(prepared));
            if (!bounded) when(configuration.inspectSharedHistory(eq(prepared),anyString(),anyString(),anyString(),anyString())).thenReturn(new CaptureSharedHistory(prepared,false));
            var cached = new CaptureCacheState(prepared, WorkloadClaimFence.from(capture),
                    bounded ? Optional.empty() : Optional.of(streamingChain()));
            when(cache.load(prepared)).thenReturn(cached);
            when(cache.control(any(),anyString())).thenAnswer(call->{var held=cache.load(call.getArgument(0));return new CaptureCachePreparations.Control(held.preparation(),held.claim(),held.chain().map(SrsChainState::epoch).orElse(0L),held.chain().map(value->value.servingTables().contains(call.getArgument(1,String.class))).orElse(false));});
            when(pipelines.snapshotStates(any(),any(),any())).thenReturn(snapshots);
            when(snapshots.advanceOrder(anyLong(),anyLong(),anyMap())).thenReturn(1L);
            when(snapshots.count(anyString(),anyString(),anyLong())).thenReturn(OptionalLong.of(1));
            when(authorization.require(any())).thenReturn(PIPELINE);
            when(authorization.authorized(ORIGINAL)).thenReturn(true);
            when(authorization.requireCapture(any(),any())).thenAnswer(call -> call.getArgument(1));
            org.mockito.Mockito.doAnswer(call -> ownership.acquire(call.getArgument(0))).when(ownership).acquireInitial(any());
            when(ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(true,capture));
            when(ownership.renew(any())).thenAnswer(call -> Optional.of(call.getArgument(0)));
            when(ownership.ttl()).thenReturn(Duration.ofSeconds(60));
            when(notes.guardedBy(any())).thenReturn(notes);
            when(pipelines.sinkPreparations(any(),any(),any())).thenReturn(mock(SinkPreparationStore.class));
            when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),1));
            var noteOrigins=new java.util.HashMap<RequiredNoteScope,RequiredNoteOrigin>();
            when(notes.prepareOrigin(any(),any())).thenAnswer(call -> {
                RequiredNoteOrigin origin=call.getArgument(1);
                noteOrigins.put(call.getArgument(0),origin);return origin;
            });
            when(notes.origin(any())).thenAnswer(call -> noteOrigins.get(call.getArgument(0)));
            var artifact=new ConnectorRef(List.of(),connector.equals("mongodb")
                    ?"io.tapdata.mongodb.MongodbConnector":"not.loaded","2.0.8",null,null,"a".repeat(64),false);
            doCallRealMethod().when(connectors).withArtifactRetention(any());
            when(connectors.resolve(connector)).thenReturn(artifact);
            when(connectors.resolve(connector,"a".repeat(64))).thenReturn(artifact);
            runtime=spy(new NativeSrsCaptureRunUnit(BUSINESS,nativeStores,mock(HazelcastInstance.class)));
            var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),OWNER,1,0,1);
            coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(BUSINESS,pipelines,sources,configuration,cache,notes,ignored -> false,
                            boundedLoads,completedFull),
                    new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(node),authorization,ownership,renewal,() -> {},Clock.systemUTC()),
                    runtime,buffer,connectors,(original,config,guard,preparation,stream) -> {
                        guard.run();if(preparation!=null)snapshotCallback.set(preparation);opened.incrementAndGet();
                        if (checkPortIdentity.get()) {
                            original.port(config, notes, connectors, guard);
                            checkedConfigurations.add(original);
                        }
                        if(portOverride.get()!=null)return portOverride.get();
                        return new Port(config.node().nodeId(),guard,closedPorts,readRows,rowCount.get(),closeFailure,pausedRead);
                    },reportNanos::get);
            if (!bounded) streaming();
        }
        private SrsConsumerProgress progress(boolean complete, long membership, long epoch) {
            var key = new SrsModelKey("mck-key", "orders", "cache", 1, 0);
            var cursor = new ConsumerOffset(streamingContext.key().consumerId(), Map.of("orders", 0L), null,
                    complete ? List.of("orders") : List.of(), "seam", epoch, Map.of(), ConsumerProgressKind.SRS);
            return new SrsConsumerProgress(streamingContext, membership, 1, cursor,
                    new SrsSnapshotPin(epoch, "seam", Map.of("orders", key)), null, Map.of("orders", 0L));
        }

        private static SrsChainState streamingChain() {
            return new SrsChainState("mck-key", "mck-key", "1h", 1, Set.of("orders"), Set.of("orders"),
                    0, null, null, 1, 1);
        }

        private void streaming() {
            org.mockito.Mockito.doAnswer(call -> ownership.acquire(call.getArgument(0))).when(ownership).acquireInitial(any());
            when(ownership.acquire(any())).thenReturn(new CaptureOwnership.Permit(false, capture));
            when(snapshots.count(anyString(), anyString(), anyLong())).thenReturn(OptionalLong.of(0));
            var consumers = consumerStore();
            when(consumers.attached(any(),anyString())).thenAnswer(call->{var run=call.getArgument(0,SrsConsumerRun.class);var member=consumers.find(run.context().key()).orElse(null);return member!=null&&!member.detached()&&member.context().equals(run.context())&&member.revision()==run.membershipRevision()&&member.commandFence().equals(run.originalClaim())&&member.tables().contains(call.getArgument(1,String.class));});
            streamingProgress.set(progress(false, 1, 1));
            when(consumers.find(streamingContext.key())).thenReturn(Optional.of(
                    new SrsConsumerMembership(streamingContext, 1, false, Set.of("orders"), PIPELINE)));
            when(consumers.progress(streamingContext)).thenAnswer(ignored -> Optional.ofNullable(streamingProgress.get()));
            var chains = mock(SrsChainStore.class);
            when(chains.load("mck-key")).thenReturn(Optional.of(streamingChain()));
            var models = mock(SrsModelStore.class);
            var key = new SrsModelKey("mck-key", "orders", "cache", 1, 0);
            when(models.publishedForNewSnapshot(anyString(), anyMap())).thenReturn(Optional.of(Map.of("orders",
                    new SrsModel(key, SrsRingbuffer.ringName("mck-key", "orders"), new SourceTable("orders", List.of(), List.of(), List.of()), 0))));
            when(sources.srs(any(), any())).thenReturn(new SrsStores(chains, models, mock(SrsLogStore.class), consumers));
            org.mockito.stubbing.Answer<CaptureRun> snapshot = call -> {
                var preparation = mock(SrsSnapshotPreparation.class);
                this.preparation.set(preparation);
                when(preparation.request()).thenReturn(new SnapshotPreparation(1, new SourcePosition("seam")));
                when(preparation.stillOwed()).thenReturn(List.of("orders"));
                java.util.function.Function<SrsSnapshotPreparation, CapturePort> ports = call.getArgument(3);
                ports.apply(preparation);
                CaptureHandoff output = call.getArgument(4); output.loaded("orders");
                return healthyCaptureRun();
            };
            doAnswer(snapshot).when(runtime).beginSnapshot(any(), any(), anyLong(), any(), any(), any());
            doAnswer(snapshot).when(runtime).resumeSnapshot(any(), any(), anyLong(), any(), any(), any(), any());
        }

        ArtifactStore snapshot() { return ReadOnlyArtifactSnapshot.capture(memory.artifacts()); }
        SourceModelSnapshot models() { return SourceModelSnapshot.capture("flow",definitions,memory.schemas()); }
        @Override public void close() { try { coordinator.close(); } finally { runtime.close(); } }
    }
}
