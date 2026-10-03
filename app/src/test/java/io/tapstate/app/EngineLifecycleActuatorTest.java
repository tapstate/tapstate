package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.PipelineIncarnationService;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SnapshotCapacityUnavailable;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.SuccessorEnd;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StateStore;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Function;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The assembly-layer binding from the lifecycle actuator seam to the Jet engine and the capture coordinator,
 * driven against a real embedded member. It runs a pipeline through start -> pause -> resume -> stop over an
 * idle stand-in topology and proves the verb composition: start fills the capture before submitting the job
 * that reads it; pause and resume are engine-only (the capture keeps running); stop cancels the job before
 * stopping the capture behind it. The same actuator drives the store-backed topology production runs, by
 * pipeline id alone.
 */
class EngineLifecycleActuatorTest {

    @Test
    void anAllocatedBuildFailurePublishesItsActualScopeWithoutPriorLatest() throws Exception {
        requireAllocatedBuildFailurePublication(false, false);
    }

    @Test
    void anAllocatedBuildFailureReplacesOnlyItsActualNewPriorLatest() throws Exception {
        requireAllocatedBuildFailurePublication(true, false);
    }

    @Test
    void aLostOwnerCannotPublishAnOldAllocatedBuildFailure() throws Exception {
        requireAllocatedBuildFailurePublication(false, true);
    }

    @Test
    void anAllocatedBuildFailureKeepsItsCauseAcrossAOneShotOwnerRetry() throws Exception {
        requireAllocatedBuildFailurePublication(false, false, true);
    }

    private void requireAllocatedBuildFailurePublication(boolean publishNewBeforeRefusal, boolean loseOwner)
            throws Exception {
        requireAllocatedBuildFailurePublication(publishNewBeforeRefusal, loseOwner, false);
    }

    @Test
    void anAllocatedBuildFailureKeepsItsCauseWhileContinuationOwnershipStaysBusy() throws Exception {
        requireAllocatedBuildFailurePublication(false, false, false, true);
    }

    private void requireAllocatedBuildFailurePublication(boolean publishNewBeforeRefusal, boolean loseOwner,
            boolean retryOnce) throws Exception {
        requireAllocatedBuildFailurePublication(publishNewBeforeRefusal, loseOwner, retryOnce, false);
    }

    private void requireAllocatedBuildFailurePublication(boolean publishNewBeforeRefusal, boolean loseOwner,
            boolean retryOnce, boolean continuationRetry) throws Exception {
        var artifacts = new InMemoryArtifactStore();
        String sourceId = "admitted_source";
        artifacts.save(new SourceResource(sourceId, null, "mysql", Map.of("host", "controlled"), SourceMode.CDC,
                List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null));
        var join = io.tapstate.core.model.Step.inline("widen", io.tapstate.core.model.FromClause.aliases(
                Map.of("o", FromRef.literal("orders"), "c", FromRef.literal("customers"))),
                new io.tapstate.core.model.TransformBody.Join(io.tapstate.core.model.JoinEngine.BUILTIN,
                        "SELECT o.id AS order_id, o.no_such_column AS missing, c.seq AS customer_seq "
                                + "FROM o LEFT JOIN c ON o.seq = c.id"), null);
        artifacts.save(new PipelineResource(PIPE, null, List.of(SourceRef.spec(sourceId, true)),
                List.of(join), new io.tapstate.core.model.ViewBlock.Inline(
                        "order_state", FromRef.literal("widen"), "order_id", null), null,
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        var store = new InMemoryStorePort(artifacts);
        List<io.tapstate.spi.store.SourceField> columns = List.of(
                new io.tapstate.spi.store.SourceField("id", "bigint", io.tapstate.core.common.TapstateType.INT64),
                new io.tapstate.spi.store.SourceField("seq", "bigint", io.tapstate.core.common.TapstateType.INT64));
        store.schemas().save(new io.tapstate.spi.store.DiscoveredSourceModel(sourceId, "mysql", 1L,
                new io.tapstate.spi.store.SourceModel(List.of(
                        new io.tapstate.spi.store.SourceTable("orders", columns, List.of("id"), List.of()),
                        new io.tapstate.spi.store.SourceTable("customers", columns, List.of("id"), List.of())))));
        var identity = new AtomicReference<String>();
        ArtifactStore identityArtifacts = new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> rows) { artifacts.saveAll(rows); }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) { return artifacts.get(id); }
            @Override public List<io.tapstate.core.model.Resource> list() { return artifacts.list(); }
            @Override public Optional<String> pipelineIncarnationId(String id) {
                return PIPE.equals(id) ? Optional.ofNullable(identity.get()) : Optional.empty();
            }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                if (!PIPE.equals(id) || artifacts.get(id).isEmpty()) { return Optional.empty(); }
                identity.compareAndSet(null, candidate);
                return Optional.of(identity.get());
            }
        };
        var generations = new InMemoryWorkloadClaimStore();
        var owner = org.mockito.Mockito.spy(PipelineActuationOwnership.single("single", generations));
        var eligible = new java.util.concurrent.atomic.AtomicBoolean(true);
        var ownerEntered = new java.util.concurrent.CountDownLatch(1);
        var releaseOwner = new java.util.concurrent.CountDownLatch(1);
        var retryReturned = new java.util.concurrent.CountDownLatch(1);
        var workerOwnerChecks = new java.util.concurrent.atomic.AtomicInteger();
        var holdOwnerRetry = new java.util.concurrent.atomic.AtomicBoolean(continuationRetry);
        if (loseOwner || retryOnce || continuationRetry) {
            org.mockito.Mockito.doAnswer(invocation -> {
                if (Thread.currentThread().getName().startsWith("tapstate-telemetry-latest-")) {
                    int check = workerOwnerChecks.incrementAndGet();
                    if ((retryOnce && check == 2) || (continuationRetry && check >= 2 && holdOwnerRetry.get())) {
                        // Busy ownership can end between probes or persist through handoff admission.
                        retryReturned.countDown();
                        return PipelineActuationOwnership.Permit.busy();
                    }
                    if (loseOwner) {
                        ownerEntered.countDown();
                        if (!releaseOwner.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("old telemetry owner check was not released");
                        }
                    }
                }
                return invocation.callRealMethod();
            }).when(owner).permit(org.mockito.ArgumentMatchers.eq(PIPE));
        }
        var scopes = new ObservationScopeRegistry();
        var latest = new RetainedObservations(null);
        ObservationPublisher publisher = new ObservationPublisher(store.state(), latest);
        StoreBackedDagSource actualBuilder = new StoreBackedDagSource(store);
        var admittedScope = new AtomicReference<ObservationStore.Scope>();
        var buildRefusal = new AtomicReference<TapstateException>();
        var buildCalls = new java.util.concurrent.atomic.AtomicInteger();
        DagSource rejecting = new DagSource() {
            @Override public DAG dagFor(String id) { return actualBuilder.dagFor(id); }
            @Override public List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(String id) {
                return actualBuilder.stateHeldBy(id);
            }
            @Override public NestCapacity capacityOf(String id) { return actualBuilder.capacityOf(id); }
            @Override public StartPreparation prepareStart(String id, String database) {
                StartPreparation actual = actualBuilder.prepareStart(id, database);
                return new StartPreparation(actual.capacity(), actual.stateLocations(), actual.artifactSnapshot(),
                        actual.cursorWriterToken(), fence -> {
                            buildCalls.incrementAndGet();
                            assertThat(fence).isNotNull();
                            assertThat(generations.currentGeneration("single", PIPE)).hasValue(fence.executionGeneration());
                            admittedScope.set(scopes.current(PIPE).orElseThrow());
                            assertThat(admittedScope.get().executionGeneration()).isEqualTo(fence.executionGeneration());
                            assertThat(StateJson.parse(store.state().read(PIPE).orElseThrow().stateJson()))
                                    .isEqualTo(PipelineState.NEW);
                            if (publishNewBeforeRefusal) {
                                publisher.publishScoped(PIPE, null, admittedScope.get()).orElseThrow();
                                assertThat(latest.readStored(PIPE).orElseThrow().observation().state())
                                        .isEqualTo(PipelineState.NEW);
                            }
                            try { return actual.dagBuilder().apply(fence); }
                            catch (TapstateException refusal) { buildRefusal.set(refusal); throw refusal; }
                        });
            }
        };
        var calls = new CopyOnWriteArrayList<String>();
        var engine = new Engine(member);
        var capture = new RecordingCaptureCoordinator(calls);
        var actuator = new EngineLifecycleActuator(engine, rejecting, capture, teardown(), owner,
                new PipelineIncarnationService(identityArtifacts), scopes);
        var loop = new PipelineConverger(store.desired(), store.state(), actuator, Clock.systemUTC());
        var recovery = new ObservationScopeRecovery(identityArtifacts, generations, latest, store.state(), "single");
        var sink = new io.tapstate.core.logging.RingBufferLogSink(8, 8);
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ConvergenceDriver.class);
        var appender = new PipelineLogAppender(sink, new io.tapstate.core.logging.SecretRedactor());
        appender.setContext(logger.getLoggerContext()); appender.start(); logger.addAppender(appender);
        PipelineLogContext context = PipelineLogContext.capture();
        store.desired().save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        var handoffs = continuationRetry ? org.mockito.Mockito.spy(new ObservationContinuationRecovery(scopes, latest,
                store.state(), store.desired(), identityArtifacts, actuator, engine)) : null;
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(publisher, null, MetricsExport.none(),
                scopes, null, recovery, handoffs, 1, 4, Duration.ofSeconds(5))) {
            var driver = new ConvergenceDriver(loop, store.desired(), publisher, null, MetricsExport.none(),
                    eligible::get, owner, LifecycleWorkDispatcher.inline(), scopes, telemetry);
            driver.reconcile();
            CheckpointDoc failed = store.state().read(PIPE).orElseThrow();
            assertThat(StateJson.parse(failed.stateJson())).isEqualTo(PipelineState.FAILED);
            assertThat(buildRefusal.get()).isNotNull();
            assertThat(buildRefusal.get().code()).isEqualTo(ActuationError.JOIN_SQL_INVALID);
            assertThat(buildCalls.get()).isEqualTo(1);
            assertThat(admittedScope.get().pipelineIncarnationId()).isEqualTo(identity.get());
            assertThat(generations.currentGeneration("single", PIPE)).hasValue(admittedScope.get().executionGeneration());
            assertThat(engine.executionJob(PIPE)).as("a real admitted generation is not a fabricated native Job").isEmpty();
            assertThat(member.getJet().getJob(PIPE)).isNull();
            if (loseOwner) {
                assertThat(ownerEntered.await(5, TimeUnit.SECONDS)).isTrue();
                eligible.set(false); releaseOwner.countDown();
            }
            if (retryOnce || continuationRetry) {
                assertThat(retryReturned.await(5, TimeUnit.SECONDS)).isTrue();
                if (continuationRetry) {
                    org.mockito.Mockito.verify(handoffs, org.mockito.Mockito.atLeastOnce()).prepareHandoff(
                            org.mockito.ArgumentMatchers.eq(PIPE), org.mockito.ArgumentMatchers.eq(admittedScope.get()),
                            org.mockito.ArgumentMatchers.any(java.util.function.BooleanSupplier.class));
                }
                long idleUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (System.nanoTime() - idleUntil < 0
                        && telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() != 0) {
                    TimeUnit.MILLISECONDS.sleep(5);
                }
                var parked = telemetry.health().get(TelemetryDispatcher.Sink.LATEST);
                assertThat(parked.inFlight()).isZero();
                assertThat(parked.queueDepth()).as("a transient owner retry retains the original bounded failure request")
                        .isEqualTo(1);
                assertThat(latest.readStored(PIPE)).isEmpty();
                holdOwnerRetry.set(false);
                driver.reconcile();
            }
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() - until < 0) {
                var saved = latest.readStored(PIPE);
                boolean done = loseOwner ? telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0
                        : saved.filter(row -> row.scope().filter(admittedScope.get()::equals).isPresent()
                                && row.observation().state() == PipelineState.FAILED
                                && row.observation().failure() != null).isPresent();
                if (done) { break; }
                TimeUnit.MILLISECONDS.sleep(5);
            }
            var logScope = new io.tapstate.core.logging.LogSink.Scope(admittedScope.get().pipelineIncarnationId(),
                    admittedScope.get().executionGeneration());
            if (loseOwner) {
                assertThat(latest.readStored(PIPE)).as("the released owner cannot publish its old failure").isEmpty();
                assertThat(sink.tail(PIPE, logScope)).isEmpty();
            } else {
                var saved = latest.readStored(PIPE);
                assertThat(saved).as("the actual failed admission publishes without relying on previous nonNEW telemetry")
                        .isPresent();
                Observation observation = saved.orElseThrow().observation();
                assertThat(observation.failure()).isNotNull();
                assertThat(observation.state()).isEqualTo(PipelineState.FAILED);
                assertThat(saved.orElseThrow().scope()).contains(admittedScope.get());
                assertThat(observation.failure().code()).isEqualTo(ActuationError.JOIN_SQL_INVALID.code());
                assertThat(observation.failure().params()).containsEntry("step", "widen");
                assertThat(observation.metrics()).containsEntry("errors." + ActuationError.JOIN_SQL_INVALID.code(), 1L);
                assertThat(observation.facts()).extracting(MetricFact::name)
                        .doesNotContain("tapstate.pipeline.records", "tapstate.pipeline.bytes");
                assertThat(sink.tail(PIPE, logScope).stream().filter(line -> "WARN".equals(line.level())
                        && line.message().contains("entered FAILED"))).hasSize(1);
                driver.reconcile(); driver.reconcile();
                while (System.nanoTime() - until < 0) {
                    var health = telemetry.health().get(TelemetryDispatcher.Sink.LATEST);
                    if (health.queueDepth() == 0 && health.inFlight() == 0) { break; }
                    TimeUnit.MILLISECONDS.sleep(5);
                }
                var health = telemetry.health().get(TelemetryDispatcher.Sink.LATEST);
                assertThat(health.queueDepth()).isZero();
                assertThat(health.inFlight()).isZero();
                assertThat(latest.readStored(PIPE).orElseThrow().observation().metrics())
                        .containsEntry("errors." + ActuationError.JOIN_SQL_INVALID.code(), 1L);
                assertThat(sink.tail(PIPE, logScope).stream().filter(line -> "WARN".equals(line.level())
                        && line.message().contains("entered FAILED"))).hasSize(1);
                assertThat(store.state().read(PIPE)).contains(failed);
                assertThat(buildCalls.get()).isEqualTo(1);
                assertThat(generations.currentGeneration("single", PIPE)).hasValue(admittedScope.get().executionGeneration());
            }
            assertThat(PipelineLogContext.capture()).isEqualTo(context);
        } finally {
            releaseOwner.countDown(); context.restore(); logger.detachAppender(appender); appender.stop();
        }
    }

    @Test
    void anAutomaticallyReplacedFailedExecutionCarriesItsRealEventBoundary() {
        var calls = new CopyOnWriteArrayList<String>();
        var generations = new InMemoryWorkloadClaimStore();
        var ownership = PipelineActuationOwnership.single("single", generations);
        var incarnations = coldStopIncarnations();
        var scopes = new ObservationScopeRegistry();
        var desired = new InMemoryDesiredStore();
        var state = new RestartableStopStateStore();
        state.enableStops(desired, id -> ownership.stopAuthority(id).orElse(null), generations);
        var capture = new RecordingCaptureCoordinator(calls);
        var engine = new Engine(member);
        var actuator = new EngineLifecycleActuator(engine, new RecordingDagSource(calls), capture, teardown(),
                ownership, incarnations, scopes);
        var allowance = new java.util.concurrent.atomic.AtomicBoolean(true);
        var loop = new PipelineConverger(desired, state, actuator, Clock.systemUTC(),
                id -> allowance.getAndSet(false));
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        assertThat(loop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        Job original = member.getJet().getJob(PIPE);
        awaitStatus(original, JobStatus.RUNNING);
        var source = engine.executionJob(PIPE).orElseThrow();

        capture.captureFailure = new TapstateException(io.tapstate.runtime.engine.EngineError.JOB_FAILED,
                Map.of("pipeline", PIPE, "cause", "controlled capture reader failure"), null);
        var failed = loop.converge(PIPE);
        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson())).isEqualTo(PipelineState.FAILED);
        var failureEvents = PipelineStateEvents.of(PIPE, source.scope(), failed,
                PipelineFailures.of(PIPE, failed.failure().orElseThrow()));
        assertThat(failureEvents).extracting(io.tapstate.core.lifecycle.PipelineEvent::kind)
                .contains(io.tapstate.core.lifecycle.PipelineEvent.Kind.FAILURE);

        capture.captureFailure = null;
        var recovered = loop.converge(PIPE);
        Job replacement = member.getJet().getJob(PIPE);
        awaitStatus(replacement, JobStatus.RUNNING);
        var target = engine.executionJob(PIPE).orElseThrow();
        assertThat(target.job().jobId()).isNotEqualTo(source.job().jobId());
        assertThat(target.scope().pipelineIncarnationId()).isEqualTo(source.scope().pipelineIncarnationId());
        assertThat(target.scope().executionGeneration()).isEqualTo(source.scope().executionGeneration() + 1);
        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson())).isEqualTo(PipelineState.RUNNING);
        assertThat(state.stopReservations()).isEqualTo(1);
        assertThat(state.readStopReservation(PIPE)).isEmpty();
        var recoveryEvents = PipelineStateEvents.of(PIPE, target.scope(), recovered, null, true);
        assertThat(recoveryEvents).extracting(io.tapstate.core.lifecycle.PipelineEvent::kind).containsExactly(
                io.tapstate.core.lifecycle.PipelineEvent.Kind.STATE_CHANGED,
                io.tapstate.core.lifecycle.PipelineEvent.Kind.EXECUTION_RECOVERED,
                io.tapstate.core.lifecycle.PipelineEvent.Kind.EXECUTION_RESTARTED);
        assertThat(recoveryEvents).extracting(io.tapstate.core.lifecycle.PipelineEvent::executionGeneration)
                .containsOnly(target.scope().executionGeneration());
        assertThat(PipelineStateEvents.of(PIPE, target.scope(), recovered, null, true)).containsExactlyElementsOf(recoveryEvents);
        assertThat(PipelineStateEvents.of(PIPE, target.scope(), loop.converge(PIPE), null, true)).isEmpty();
    }

    @Test
    void aMissingRunningJobReplacementEmitsOnlyItsRealExecutionBoundary() {
        var calls = new CopyOnWriteArrayList<String>();
        var generations = new InMemoryWorkloadClaimStore();
        var ownership = PipelineActuationOwnership.single("single", generations);
        var scopes = new ObservationScopeRegistry();
        var desired = new InMemoryDesiredStore();
        var state = new RestartableStopStateStore();
        state.enableStops(desired, id -> ownership.stopAuthority(id).orElse(null), generations);
        var engine = new Engine(member);
        var actuator = new EngineLifecycleActuator(engine, new RecordingDagSource(calls),
                new RecordingCaptureCoordinator(calls), teardown(), ownership, coldStopIncarnations(), scopes);
        var loop = new PipelineConverger(desired, state, actuator, Clock.systemUTC());
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        loop.converge(PIPE);
        Job original = member.getJet().getJob(PIPE);
        awaitStatus(original, JobStatus.RUNNING);
        var source = engine.executionJob(PIPE).orElseThrow();
        var before = state.read(PIPE).orElseThrow();

        original.cancel();
        awaitStatus(original, JobStatus.FAILED);
        assertThat(actuator.failure(PIPE)).isEmpty();
        var replaced = loop.converge(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        var target = engine.executionJob(PIPE).orElseThrow();
        assertThat(target.job().jobId()).isNotEqualTo(source.job().jobId());
        assertThat(target.scope().pipelineIncarnationId()).isEqualTo(source.scope().pipelineIncarnationId());
        assertThat(target.scope().executionGeneration()).isEqualTo(source.scope().executionGeneration() + 1);
        assertThat(state.read(PIPE)).contains(before);
        assertThat(replaced.checkpoint()).contains(before);
        var events = PipelineStateEvents.of(PIPE, target.scope(), replaced, null);
        assertThat(events).extracting(io.tapstate.core.lifecycle.PipelineEvent::kind)
                .containsExactly(io.tapstate.core.lifecycle.PipelineEvent.Kind.EXECUTION_RESTARTED);
        assertThat(events).extracting(io.tapstate.core.lifecycle.PipelineEvent::executionGeneration)
                .containsOnly(target.scope().executionGeneration());
        assertThat(PipelineStateEvents.of(PIPE, target.scope(), replaced, null)).containsExactlyElementsOf(events);
        assertThat(PipelineStateEvents.of(PIPE, source.scope(), replaced, null)).isEmpty();
        assertThat(PipelineStateEvents.of(PIPE, target.scope(), loop.converge(PIPE), null)).isEmpty();
    }

    @Test
    void anUnscopedCompatibilityStartIsNotFailedByAnUnavailableEventReceipt() {
        var calls = new CopyOnWriteArrayList<String>();
        var state = new InMemoryStateStore();
        state.create(PIPE, StateJson.of(PipelineState.RUNNING), Instant.now());
        var before = state.read(PIPE).orElseThrow();
        var desired = new InMemoryDesiredStore();
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        var actuator = new EngineLifecycleActuator(new Engine(member), new RecordingDagSource(calls),
                new RecordingCaptureCoordinator(calls), teardown(),
                PipelineActuationOwnership.single("single", new InMemoryWorkloadClaimStore()));

        var restored = new PipelineConverger(desired, state, actuator, Clock.systemUTC()).converge(PIPE);

        assertThat(restored.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(restored.checkpoint()).contains(before);
        assertThat(state.read(PIPE)).contains(before);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        assertThat(restored.executionBoundary()).isEmpty();
        assertThat(calls).contains("startCapture:" + PIPE, "buildDag:" + PIPE)
                .doesNotContain("stopCapture:" + PIPE + "[keep][jobLive]");
    }

    @Test
    void pausingFreezesKnownCountersBeforeTheNativeProducerIsSuspended() {
        freezeAtLifecycleBoundary(true);
    }

    @Test
    void stoppingFreezesKnownCountersBeforeTheNativeProducerIsReleased() {
        freezeAtLifecycleBoundary(false);
    }

    private void freezeAtLifecycleBoundary(boolean pause) {
        List<String> events = new CopyOnWriteArrayList<>();
        var scopes = new ObservationScopeRegistry();
        var state = new InMemoryStateStore();
        state.create(PIPE, StateJson.of(PipelineState.RUNNING), Instant.now());
        var store = new InMemoryObservationStore();
        Instant since = Instant.now();
        var snapshot = new AtomicReference<>(new SnapshotReading(Map.of("orders",
                new TableSnapshot(7, null, null)), since));
        var publisher = new ObservationPublisher(state, store, id -> OptionalLong.empty(), id -> Map.of(),
                id -> {
                    events.add("sample:" + member.getJet().getJob(PIPE).getStatus());
                    return snapshot.get();
                });
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> resources) {
                throw new UnsupportedOperationException();
            }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) { return Optional.empty(); }
            @Override public List<io.tapstate.core.model.Resource> list() { return List.of(); }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                return Optional.of("inc-a");
            }
        };
        var actuator = new EngineLifecycleActuator(new Engine(member), new RecordingDagSource(events),
                new RecordingCaptureCoordinator(events), teardown(),
                PipelineActuationOwnership.single("single", new InMemoryWorkloadClaimStore()),
                new PipelineIncarnationService(artifacts), scopes, publisher, store);
        actuator.start(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        var scope = scopes.current(PIPE).orElseThrow();
        if (pause) { actuator.pause(PIPE); }
        else { actuator.stop(PIPE, false); }
        assertThat(events).contains("sample:RUNNING");

        if (pause) {
            awaitStatus(member.getJet().getJob(PIPE), JobStatus.SUSPENDED);
            actuator.resume(PIPE);
            awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
            snapshot.set(new SnapshotReading(Map.of("orders", new TableSnapshot(2, null, null)), Instant.now()));
        } else {
            state.create(PIPE, StateJson.of(PipelineState.STOPPED), Instant.now());
            snapshot.set(SnapshotReading.NONE);
        }
        var frame = scopes.continueFrame(publisher.prepareScoped(PIPE, null, scope).orElseThrow(), scope);
        MetricPoint point = point(frame.observation(), "tapstate.pipeline.snapshot.rows");
        assertThat(point.value()).isEqualTo(pause ? 9 : 7);
        assertThat(point.startTime()).isEqualTo(since);
        assertThat(scopes.current(PIPE)).contains(scope);
    }

    @Test
    void rebuildingResumeCarriesKnownCountersAndHistogramsButRemeasuresGauges() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> resources) {
                throw new UnsupportedOperationException();
            }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) { return Optional.empty(); }
            @Override public List<io.tapstate.core.model.Resource> list() { return List.of(); }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                return Optional.of("inc-a");
            }
        };
        AtomicReference<ObservationStore.Stored> latest = new AtomicReference<>();
        ObservationStore observations = new ObservationStore() {
            @Override public void save(Observation observation) { throw new AssertionError("unscoped write"); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                latest.set(new Stored(observation, Optional.of(scope)));
                return true;
            }
            @Override public Optional<Observation> read(String id) {
                return Optional.ofNullable(latest.get()).map(Stored::observation);
            }
            @Override public Optional<Stored> readStored(String id) { return Optional.ofNullable(latest.get()); }
            @Override public void delete(String id) { latest.set(null); }
        };
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member),
                new RecordingDagSource(events), coordinator, teardown(),
                PipelineActuationOwnership.single("single", new InMemoryWorkloadClaimStore()),
                new PipelineIncarnationService(artifacts), scopes);
        Instant started = Instant.parse("2026-09-27T00:00:00Z");
        Instant rebuiltAt = started.plusSeconds(30);

        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), observations), null,
                MetricsExport.none(), scopes, 1, 4)) {
            actuator.start(PIPE);
            ObservationStore.Scope oldScope = scopes.current(PIPE).orElseThrow();
            awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
            telemetry.offer(metricFrame(started.plusSeconds(10), started, 7, 3, 11), oldScope);
            awaitObservation(latest, oldScope, started.plusSeconds(10));

            actuator.pause(PIPE);
            awaitStatus(member.getJet().getJob(PIPE), JobStatus.SUSPENDED);
            coordinator.loadDelivered = false;
            actuator.resume(PIPE);
            ObservationStore.Scope newScope = scopes.current(PIPE).orElseThrow();
            assertThat(newScope.executionGeneration()).isEqualTo(oldScope.executionGeneration() + 1);
            telemetry.offer(metricFrame(rebuiltAt, rebuiltAt, 2, 1, 22), newScope);
            Observation continued = awaitObservation(latest, newScope, rebuiltAt);

            MetricPoint rows = point(continued, "tapstate.pipeline.records");
            assertThat(rows.value()).isEqualTo(9);
            assertThat(rows.startTime()).isEqualTo(started);
            assertThat(continued.metrics()).containsEntry("records.out", 9L);
            MetricPoint duration = point(continued, "tapstate.pipeline.record.delivery.duration");
            assertThat(duration.histogram().count()).isEqualTo(4);
            assertThat(duration.histogram().sum()).isEqualTo(4.0);
            assertThat(duration.histogram().bucketCounts().getFirst()).isEqualTo(4);
            assertThat(duration.startTime()).isEqualTo(started);
            assertThat(point(continued, "tapstate.pipeline.lag").value()).isEqualTo(22);
        }
    }

    @Test
    void aTerminalOldJobKeepsColdRebuildCountersWithoutInstallingItsScope() {
        List<String> events = new CopyOnWriteArrayList<>();
        var generations = new InMemoryWorkloadClaimStore();
        var ownership = PipelineActuationOwnership.single("single", generations);
        var incarnations = coldStopIncarnations();
        var coordinator = new RecordingCaptureCoordinator(events);
        var dags = new RecordingDagSource(events);
        var warmEngine = new Engine(member);
        var warmScopes = new ObservationScopeRegistry();
        var warm = new EngineLifecycleActuator(warmEngine, dags, coordinator, teardown(),
                ownership, incarnations, warmScopes);
        warm.start(PIPE);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        warm.pause(PIPE);
        awaitStatus(held, JobStatus.SUSPENDED);
        coordinator.loadDelivered = false;
        Engine.ExecutionJob old = warmEngine.executionJob(PIPE).orElseThrow();
        Instant started = Instant.parse("2026-09-27T00:00:00Z");
        var stored = new ObservationStore.Stored(metricFrame(started.plusSeconds(10), started,
                7, 3, 11).observation(), Optional.of(old.scope()));
        warmEngine.cancelExact(PIPE, old);
        assertThat(warmEngine.awaitTerminalExact(PIPE, old, Duration.ofSeconds(15))).isTrue();

        var recoveredEngine = new Engine(member);
        var coldScopes = new ObservationScopeRegistry();
        var cold = new EngineLifecycleActuator(recoveredEngine, dags, coordinator, teardown(),
                ownership, incarnations, coldScopes, null, retainedObservation(stored));
        assertThat(recoveredEngine.awaitTerminalExact(PIPE, old, Duration.ofSeconds(15))).isTrue();
        assertThat(recoveredEngine.noUnfinishedJob(PIPE)).isTrue();
        assertThat(recoveredEngine.executionJob(PIPE)).contains(old);
        assertThat(coldScopes.current(PIPE)).isEmpty();
        assertThat(cold.needsRebuildOnResume(PIPE)).isTrue();
        StopAuthority authority = cold.stopAuthority(PIPE).orElseThrow();
        assertThat(authority.executionGeneration()).isEqualTo(old.scope().executionGeneration());
        assertThat(incarnations.current(PIPE)).contains(old.scope().pipelineIncarnationId());
        StopReservation.Subject subject = cold.stopSubject(PIPE).orElseThrow();
        var reservation = new StopReservation(PIPE, "terminal-cold-rebuild", 10, 11,
                new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"), subject);

        assertThat(cold.finishStop(reservation, true, true, false,
                () -> cold.stopAuthority(PIPE).filter(authority::equals).isPresent())).isTrue();
        assertThat(coldScopes.current(PIPE))
                .as("a stored cumulative floor must not install the old observation scope").isEmpty();
        assertThat(coordinator.hasActiveCapture(PIPE)).isFalse();
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(1);

        cold.start(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        ObservationStore.Scope next = coldScopes.current(PIPE).orElseThrow();
        assertThat(next.pipelineIncarnationId()).isEqualTo(old.scope().pipelineIncarnationId());
        assertThat(next.executionGeneration()).isEqualTo(old.scope().executionGeneration() + 1);
        Instant rebuiltAt = started.plusSeconds(30);
        Observation continued = coldScopes.continueFrame(
                metricFrame(rebuiltAt, rebuiltAt, 2, 1, 22), next).observation();

        assertThat(point(continued, "tapstate.pipeline.records").value())
                .as("the terminal predecessor's retained seven records continue with two new records")
                .isEqualTo(9);
        assertThat(point(continued, "tapstate.pipeline.records").startTime()).isEqualTo(started);
        assertThat(continued.metrics()).containsEntry("records.out", 9L);
        assertThat(subject).isEqualTo(new StopReservation.ExistingJob(
                old.scope().pipelineIncarnationId(), old.scope().executionGeneration(), old.job(), authority));
        assertThat(dags.fences).extracting(ExecutionFence::executionGeneration).containsExactly(1L, 2L);
    }

    @Test
    void aCancelledAdmittedStartCanStopWhileItsTerminalPredecessorHasTheOlderGeneration() {
        List<String> events = new CopyOnWriteArrayList<>();
        var generations = new InMemoryWorkloadClaimStore();
        var ownership = PipelineActuationOwnership.single("single", generations);
        var coordinator = new RecordingCaptureCoordinator(events);
        var dags = new RecordingDagSource(events);
        var engine = new Engine(member);
        var scopes = new ObservationScopeRegistry();
        var actuator = new EngineLifecycleActuator(engine, dags, coordinator, teardown(),
                ownership, coldStopIncarnations(), scopes);
        actuator.start(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        Engine.ExecutionJob old = engine.executionJob(PIPE).orElseThrow();
        engine.cancelExact(PIPE, old);
        assertThat(engine.awaitTerminalExact(PIPE, old, Duration.ofSeconds(15))).isTrue();

        // The second admission advances the durable authority, then loses its submission permission.
        try (LifecycleActuator.PreparedStart abandoned = actuator.prepareStart(PIPE)) {
            assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(2);
            assertThat(scopes.current(PIPE)).contains(new ObservationStore.Scope(
                    old.scope().pipelineIncarnationId(), 2));
            assertThat(coordinator.hasActiveCapture(PIPE)).isTrue();
        }
        assertThat(scopes.current(PIPE)).isEmpty();
        assertThat(coordinator.hasActiveCapture(PIPE)).isFalse();
        assertThat(engine.executionJob(PIPE)).contains(old);
        assertThat(engine.noUnfinishedJob(PIPE)).isTrue();
        StopAuthority authority = actuator.stopAuthority(PIPE).orElseThrow();
        assertThat(authority.executionGeneration()).isEqualTo(2);
        StopReservation.Subject subject = actuator.stopSubject(PIPE).orElseThrow();
        assertThat(subject).isEqualTo(new StopReservation.NoJob("single", authority));
        var reservation = new StopReservation(PIPE, "cancelled-admission-stop", 10, 11,
                new DesiredState(PIPE, PipelineState.STOPPED, "rev-2"), subject);

        assertThat(actuator.finishStop(reservation, false, true, false,
                () -> actuator.stopAuthority(PIPE).filter(authority::equals).isPresent())).isTrue();
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(2);
        assertThat(coordinator.hasActiveCapture(PIPE)).isFalse();
        assertThat(engine.executionJob(PIPE)).contains(old);
        assertThat(dags.fences).extracting(ExecutionFence::executionGeneration).containsExactly(1L, 2L);
    }

    @Test
    void aCapacityDeferredColdRebuildKeepsItsDurableFloorAfterTheProcessViewRestarts() {
        Instant started = Instant.parse("2026-10-01T00:00:00Z");
        Clock clock = Clock.fixed(started.plusSeconds(20), java.time.ZoneOffset.UTC);
        List<String> events = new CopyOnWriteArrayList<>();
        var generations = new InMemoryWorkloadClaimStore();
        var ownership = PipelineActuationOwnership.single("single", generations);
        var incarnations = coldStopIncarnations();
        var desired = new InMemoryDesiredStore();
        var state = new RestartableStopStateStore();
        state.enableStops(desired, id -> ownership.stopAuthority(id).orElse(null), generations);
        var coordinator = new RecordingCaptureCoordinator(events);
        var dags = new RecordingDagSource(events);
        var originalEngine = new Engine(member);
        var originalScopes = new ObservationScopeRegistry();
        var original = new EngineLifecycleActuator(originalEngine, dags, coordinator, teardown(),
                ownership, incarnations, originalScopes);
        var originalLoop = new PipelineConverger(desired, state, original, clock);

        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        assertThat(originalLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        Engine.ExecutionJob old = originalEngine.executionJob(PIPE).orElseThrow();
        desired.save(new DesiredState(PIPE, PipelineState.PAUSED, "rev-1"));
        assertThat(originalLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        awaitStatus(held, JobStatus.SUSPENDED);
        var paused = state.read(PIPE).orElseThrow();
        assertThat(StateJson.parse(paused.stateJson())).isEqualTo(PipelineState.PAUSED);
        var stored = new ObservationStore.Stored(metricFrame(started.plusSeconds(10), started,
                7, 3, 11).observation(), Optional.of(old.scope()));
        ObservationStore observations = retainedObservation(stored);

        // Only stores and the native member survive. This actor has no producer epoch or local floor.
        var stoppingEngine = new Engine(member);
        var stoppingScopes = new ObservationScopeRegistry();
        var stopping = new EngineLifecycleActuator(stoppingEngine, dags, coordinator, teardown(),
                ownership, incarnations, stoppingScopes, null, observations);
        var stoppingLoop = new PipelineConverger(desired, state, stopping, clock);
        coordinator.loadDelivered = false;
        coordinator.snapshotCapacityUnavailable = true;
        DesiredState resume = new DesiredState(PIPE, PipelineState.RUNNING, "rev-1", false,
                null, false, null);
        desired.save(resume);

        assertThat(stoppingLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.START_CAPACITY);
        assertThat(stoppingEngine.awaitTerminalExact(PIPE, old, Duration.ofSeconds(15))).isTrue();
        assertThat(coordinator.hasActiveCapture(PIPE)).isFalse();
        assertThat(stoppingScopes.current(PIPE)).isEmpty();
        assertThat(generations.executionGeneration(standaloneKey()))
                .as("capacity refusal precedes replacement generation admission")
                .isEqualTo(old.scope().executionGeneration());
        assertThat(observations.readStored(PIPE)).contains(stored);
        assertThat(dags.fences).extracting(ExecutionFence::executionGeneration)
                .containsExactly(old.scope().executionGeneration());
        assertThat(state.readStopReservation(PIPE).orElseThrow().phase())
                .isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
        assertThat(continuationRecovery(stoppingScopes, observations, state, desired, incarnations,
                stopping, stoppingEngine).prepareHandoff(PIPE, null, () -> true)).isTrue();
        assertThat(observations.readContinuation(PIPE).orElseThrow().continuation().knownBaseline()).isTrue();

        // Discard the stopped actor, registry, native-future cache and capture coordinator.
        // The exact desired intent, checkpoint and seven-record observation remain durable.
        var restartedOwnership = PipelineActuationOwnership.single("single", generations);
        state.enableStops(desired, id -> restartedOwnership.stopAuthority(id).orElse(null), generations);
        var restartedEngine = new Engine(member);
        var restartedScopes = new ObservationScopeRegistry();
        var restartedCapture = new RecordingCaptureCoordinator(events);
        restartedCapture.loadDelivered = false;
        var restartedDags = new RecordingDagSource(events);
        var restarted = new EngineLifecycleActuator(restartedEngine, restartedDags, restartedCapture,
                teardown(), restartedOwnership, incarnations, restartedScopes, null, observations);
        var restartedLoop = new PipelineConverger(desired, state, restarted, clock);
        assertThat(restartedScopes.current(PIPE)).isEmpty();
        assertThat(desired.read(PIPE)).contains(resume);

        assertThat(restartedLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        Job replacement = member.getJet().getJob(PIPE);
        awaitStatus(replacement, JobStatus.RUNNING);
        assertThat(replacement.getId()).isNotEqualTo(held.getId());
        restartedLoop.converge(PIPE);
        ObservationStore.Scope next = restartedScopes.current(PIPE).orElseThrow();
        assertThat(next.pipelineIncarnationId()).isEqualTo(old.scope().pipelineIncarnationId());
        assertThat(next.executionGeneration()).isEqualTo(old.scope().executionGeneration() + 1);
        assertThat(restartedDags.fences).extracting(ExecutionFence::executionGeneration)
                .containsExactly(next.executionGeneration());
        Instant measuredAt = started.plusSeconds(30);
        assertThat(continuationRecovery(restartedScopes, observations, state, desired, incarnations,
                restarted, restartedEngine).prepareHandoff(PIPE, next, () -> true)).isTrue();
        Observation continued = commitContinuationFrame(restartedScopes, observations,
                metricFrame(measuredAt, measuredAt, 2, 1, 22), restartedEngine.executionJob(PIPE).orElseThrow());
        assertThat(restartedLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);

        assertThat(point(continued, "tapstate.pipeline.records").value())
                .as("a deferred rebuilding resume retains the durable seven-record floor across restart")
                .isEqualTo(9);
        assertThat(point(continued, "tapstate.pipeline.records").startTime()).isEqualTo(started);
        assertThat(continued.metrics()).containsEntry("records.out", 9L);
        assertThat(point(continued, "tapstate.pipeline.record.delivery.duration").histogram().count())
                .isEqualTo(4);
        assertThat(point(continued, "tapstate.pipeline.lag").value()).isEqualTo(22);
        assertThat(state.readStopReservation(PIPE)).isEmpty();
        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson())).isEqualTo(PipelineState.RUNNING);

        // Rebuild the service view again after the marker is gone while retaining this same real Job.
        var sameJobEngine = new Engine(member);
        var sameJobScopes = new ObservationScopeRegistry();
        var sameJobActor = new EngineLifecycleActuator(sameJobEngine, restartedDags, restartedCapture, teardown(),
                restartedOwnership, incarnations, sameJobScopes, null, observations);
        var sameJobRecovery = continuationRecovery(sameJobScopes, observations, state, desired, incarnations,
                sameJobActor, sameJobEngine);
        var resolved = sameJobRecovery.resolveExisting(PIPE, () -> true).orElseThrow();
        assertThat(sameJobRecovery.adoptExisting(resolved)).isTrue();
        Engine.ExecutionJob samePhysical = sameJobEngine.executionJob(PIPE).orElseThrow();
        assertThat(samePhysical.job().jobId()).isEqualTo(replacement.getId());
        Observation reread = commitContinuationFrame(sameJobScopes, observations,
                metricFrame(started.plusSeconds(40), measuredAt, 2, 1, 22), samePhysical);
        assertThat(point(reread, "tapstate.pipeline.records").value())
                .as("the public nine is not added again to the same native two") .isEqualTo(9);
        assertThat(point(reread, "tapstate.pipeline.records").startTime()).isEqualTo(started);
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(next.executionGeneration());

        Instant nativeRestart = started.plusSeconds(50);
        Observation nextEpoch = commitContinuationFrame(sameJobScopes, observations,
                metricFrame(nativeRestart, nativeRestart, 3, 2, 23), samePhysical);
        assertThat(point(nextEpoch, "tapstate.pipeline.records").value())
                .as("a real new native producer epoch adds three to the known logical nine") .isEqualTo(12);
        assertThat(point(nextEpoch, "tapstate.pipeline.record.delivery.duration").histogram().count()).isEqualTo(6);
        Observation lateOldEpoch = commitContinuationFrame(sameJobScopes, observations,
                metricFrame(started.plusSeconds(60), measuredAt, 2, 1, 24), samePhysical);
        assertThat(point(lateOldEpoch, "tapstate.pipeline.records").value())
                .as("a late old native epoch cannot lower or re-add the current logical twelve") .isEqualTo(12);
        assertThat(point(lateOldEpoch, "tapstate.pipeline.records").startTime()).isEqualTo(started);
        assertThat(sameJobEngine.executionJob(PIPE)).contains(samePhysical);
        assertThat(restartedDags.fences).hasSize(1);
    }

    @Test
    void aStampedStartAfterStopFromPausedStartsFreshCounters() {
        assertColdReplacementCounterPolicy(true);
    }

    @Test
    void anUnstampedPartialSnapshotResumeKeepsOldCountersWhenItRebuilds() {
        assertColdReplacementCounterPolicy(false);
    }

    private void assertColdReplacementCounterPolicy(boolean startAfterStop) {
        Instant started = Instant.parse("2026-10-01T00:00:00Z");
        Clock clock = Clock.fixed(started.plusSeconds(20), java.time.ZoneOffset.UTC);
        List<String> events = new CopyOnWriteArrayList<>();
        var generations = new InMemoryWorkloadClaimStore();
        var ownership = PipelineActuationOwnership.single("single", generations);
        var incarnations = coldStopIncarnations();
        var desired = new InMemoryDesiredStore();
        var state = new RestartableStopStateStore();
        state.enableStops(desired, id -> ownership.stopAuthority(id).orElse(null), generations);
        var coordinator = new RecordingCaptureCoordinator(events);
        var dags = new RecordingDagSource(events);
        var originalEngine = new Engine(member);
        var original = new EngineLifecycleActuator(originalEngine, dags, coordinator, teardown(),
                ownership, incarnations, new ObservationScopeRegistry());
        var originalLoop = new PipelineConverger(desired, state, original, clock);

        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        assertThat(originalLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        Engine.ExecutionJob old = originalEngine.executionJob(PIPE).orElseThrow();
        desired.save(new DesiredState(PIPE, PipelineState.PAUSED, "rev-1"));
        assertThat(originalLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        awaitStatus(held, JobStatus.SUSPENDED);
        var paused = state.read(PIPE).orElseThrow();
        assertThat(StateJson.parse(paused.stateJson())).isEqualTo(PipelineState.PAUSED);
        var stored = new ObservationStore.Stored(metricFrame(started.plusSeconds(10), started,
                7, 3, 11).observation(), Optional.of(old.scope()));
        ObservationStore observations = retainedObservation(stored);

        // No local producer epoch or pending floor survives into the actor that rebuilds the job.
        var replacementEngine = new Engine(member);
        var replacementScopes = new ObservationScopeRegistry();
        var replacement = new EngineLifecycleActuator(replacementEngine, dags, coordinator, teardown(),
                ownership, incarnations, replacementScopes, null, observations);
        var replacementLoop = new PipelineConverger(desired, state, replacement, clock);
        coordinator.loadDelivered = false;
        assertThat(coordinator.snapshotCapacityUnavailable).isFalse();
        DesiredState nextIntent;
        if (startAfterStop) {
            // STOP is overwritten by START before convergence reads it, while actual remains PAUSED.
            desired.save(new DesiredState(PIPE, PipelineState.STOPPED, "rev-1", false));
            nextIntent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-1", false,
                    null, true, paused.epoch());
        } else {
            // A genuine RESUME has no restart stamp; the unfinished load supplies the rebuild reason.
            nextIntent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-1", false,
                    null, false, null);
        }
        desired.save(nextIntent);
        assertThat(replacementScopes.current(PIPE)).isEmpty();

        assertThat(replacementLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        Job newJob = member.getJet().getJob(PIPE);
        awaitStatus(newJob, JobStatus.RUNNING);
        assertThat(newJob.getId()).isNotEqualTo(held.getId());
        assertThat(replacementEngine.awaitTerminalExact(PIPE, old, Duration.ofSeconds(15))).isTrue();
        replacementLoop.converge(PIPE);
        ObservationStore.Scope next = replacementScopes.current(PIPE).orElseThrow();
        assertThat(next.pipelineIncarnationId()).isEqualTo(old.scope().pipelineIncarnationId());
        assertThat(next.executionGeneration()).isEqualTo(old.scope().executionGeneration() + 1);
        assertThat(dags.fences).extracting(ExecutionFence::executionGeneration)
                .containsExactly(old.scope().executionGeneration(), next.executionGeneration());
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(next.executionGeneration());
        assertThat(coordinator.hasActiveCapture(PIPE)).isTrue();
        if (startAfterStop) {
            assertThat(state.readStopReservation(PIPE)).isEmpty();
        } else {
            assertThat(state.readStopReservation(PIPE).orElseThrow().phase())
                    .isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
        }
        assertThat(desired.read(PIPE)).contains(nextIntent);
        assertThat(observations.readStored(PIPE)).contains(stored);

        if (!startAfterStop) {
            assertThat(continuationRecovery(replacementScopes, observations, state, desired, incarnations,
                    replacement, replacementEngine).prepareHandoff(PIPE, next, () -> true)).isTrue();
        }
        Instant measuredAt = started.plusSeconds(30);
        Observation measured = startAfterStop ? replacementScopes.continueFrame(
                metricFrame(measuredAt, measuredAt, 2, 1, 22), next).observation()
                : commitContinuationFrame(replacementScopes, observations,
                        metricFrame(measuredAt, measuredAt, 2, 1, 22), replacementEngine.executionJob(PIPE).orElseThrow());
        assertThat(replacementLoop.converge(PIPE).status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.readStopReservation(PIPE)).isEmpty();
        long expectedRecords = startAfterStop ? 2 : 9;
        long expectedDurationCount = startAfterStop ? 1 : 4;
        Instant expectedStart = startAfterStop ? measuredAt : started;
        assertThat(point(measured, "tapstate.pipeline.records").value())
                .as(startAfterStop
                        ? "stamped START-after-STOP resets the seven-record predecessor even from PAUSED"
                        : "a genuine partial-snapshot RESUME carries seven records into two new records")
                .isEqualTo(expectedRecords);
        assertThat(point(measured, "tapstate.pipeline.records").startTime()).isEqualTo(expectedStart);
        assertThat(measured.metrics()).containsEntry("records.out", expectedRecords);
        MetricPoint duration = point(measured, "tapstate.pipeline.record.delivery.duration");
        assertThat(duration.histogram().count()).isEqualTo(expectedDurationCount);
        assertThat(duration.histogram().sum()).isEqualTo((double) expectedDurationCount);
        assertThat(duration.histogram().bucketCounts().getFirst()).isEqualTo(expectedDurationCount);
        assertThat(duration.startTime()).isEqualTo(expectedStart);
        assertThat(point(measured, "tapstate.pipeline.lag").value()).isEqualTo(22);
        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson())).isEqualTo(PipelineState.RUNNING);
    }

    private static PipelineIncarnationService coldStopIncarnations() {
        return new PipelineIncarnationService(new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> resources) {
                throw new UnsupportedOperationException();
            }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) { return Optional.empty(); }
            @Override public List<io.tapstate.core.model.Resource> list() { return List.of(); }
            @Override public Optional<String> pipelineIncarnationId(String id) {
                return PIPE.equals(id) ? Optional.of("inc-a") : Optional.empty();
            }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                return pipelineIncarnationId(id);
            }
        });
    }

    private static ObservationStore retainedObservation(ObservationStore.Stored stored) {
        return new RetainedObservations(stored);
    }

    /** Retains only typed latest/continuation values; the real Mongo transaction proof lives in integration tests. */
    private static final class RetainedObservations implements ObservationStore {
        private Stored current;
        private StoredContinuation continuation;
        private long revision;
        private RetainedObservations(Stored initial) { current = initial; }
        @Override public synchronized void save(Observation observation) { throw new AssertionError("unexpected legacy write"); }
        @Override public synchronized Optional<Observation> read(String id) { return readStored(id).map(Stored::observation); }
        @Override public synchronized Optional<Stored> readStored(String id) {
            return current != null && current.observation().pipelineId().equals(id) ? Optional.of(current) : Optional.empty();
        }
        @Override public synchronized Optional<StoredContinuation> readContinuation(String id) {
            return continuation == null || !continuation.receipt().pipelineId().equals(id)
                    ? Optional.empty() : Optional.of(continuation);
        }
        @Override public synchronized Optional<ContinuationReceipt> saveContinuation(String id, StopReservation marker,
                Optional<ContinuationReceipt> expected, ObservationContinuation next) {
            if (!id.equals(marker.pipelineId()) || !marker.token().equals(next.token())
                    || !Objects.equals(marker.source().scope(), next.sourceScope()) || !expected.equals(receipt())) {
                return Optional.empty();
            }
            continuation = storedContinuation(id, next);
            return receipt();
        }
        @Override public synchronized boolean saveScoped(Observation observation, Scope scope) {
            return saveScoped(observation, scope, ContinuationWrite.keep()).committed();
        }
        @Override public synchronized PublicationResult saveScoped(Observation observation, Scope scope, ContinuationWrite write) {
            if (current != null && current.scope().isPresent()) {
                Scope before = current.scope().orElseThrow();
                if (before.executionGeneration() > scope.executionGeneration()
                        || before.executionGeneration() == scope.executionGeneration()
                                && (!before.equals(scope) || !observation.observedAt().isAfter(current.observation().observedAt()))) {
                    return new PublicationResult(false, Optional.empty());
                }
            }
            if (write instanceof ContinuationWrite.Store store) {
                if (!store.expectedReceipt().equals(receipt())
                        || store.next().target().filter(target -> target.scope().equals(scope)).isEmpty()) {
                    return new PublicationResult(false, Optional.empty());
                }
                continuation = storedContinuation(observation.pipelineId(), store.next());
            }
            current = new Stored(observation, Optional.of(scope));
            return new PublicationResult(true, write instanceof ContinuationWrite.Store ? receipt() : Optional.empty());
        }
        private Optional<ContinuationReceipt> receipt() {
            return continuation == null ? Optional.empty() : Optional.of(continuation.receipt());
        }
        private StoredContinuation storedContinuation(String id, ObservationContinuation next) {
            try {
                String digest = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                        .digest(next.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                return new StoredContinuation(next, new ContinuationReceipt(id, "typed-test-" + ++revision,
                        digest, 3, next.token(), next.sourceScope(), next.target(), next.baselineOrigin(), next.knownBaseline()));
            } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        }
        @Override public void delete(String id) { throw new AssertionError("unexpected delete"); }
    }

    private ObservationContinuationRecovery continuationRecovery(ObservationScopeRegistry scopes, ObservationStore observations,
            StateStore state, DesiredStore desired, PipelineIncarnationService incarnations,
            LifecycleActuator actor, Engine engine) {
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> resources) { throw new UnsupportedOperationException(); }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) { return Optional.empty(); }
            @Override public List<io.tapstate.core.model.Resource> list() { return List.of(); }
            @Override public Optional<String> pipelineIncarnationId(String id) { return incarnations.current(id); }
        };
        return new ObservationContinuationRecovery(scopes, observations, state, desired, artifacts, actor, engine);
    }

    private static Observation commitContinuationFrame(ObservationScopeRegistry scopes, ObservationStore observations,
            ObservationPublisher.Prepared raw, Engine.ExecutionJob job) {
        var packet = scopes.prepareContinuationPublication(raw,
                new ObservationScopeRegistry.ActualTarget(job.scope(), job.job()), () -> true).orElseThrow();
        var result = observations.saveScoped(packet.projected().observation(), job.scope(),
                ObservationStore.ContinuationWrite.store(packet.snapshot().orElseThrow(), packet.expectedReceipt()));
        assertThat(result.committed()).isTrue();
        assertThat(scopes.publicationAccepted(packet.ticket(), result.continuationReceipt().orElseThrow())).isTrue();
        return packet.projected().observation();
    }

    private static ObservationPublisher.Prepared metricFrame(Instant at, Instant since,
            long rows, long durationCount, long lag) {
        Map<String, String> table = Map.of(MetricAttributes.PIPELINE_ID, PIPE,
                MetricAttributes.TABLE_ID, "orders");
        Map<String, String> delivered = Map.of(MetricAttributes.PIPELINE_ID, PIPE,
                MetricAttributes.TABLE_ID, "orders", MetricAttributes.DIRECTION, "out",
                MetricAttributes.OP, "insert");
        List<Long> buckets = new ArrayList<>(java.util.Collections.nCopies(
                HistogramBounds.RECORD_DELIVERY_DURATION.buckets(), 0L));
        buckets.set(0, durationCount);
        List<MetricFact> facts = List.of(
                MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                        MetricPoint.accumulated(delivered, since, at, rows)),
                MetricFact.single("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM,
                        HistogramBounds.UNIT, MetricPoint.distribution(table, since, at,
                                HistogramBounds.RECORD_DELIVERY_DURATION.value(
                                        durationCount, durationCount, buckets))),
                MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                        MetricPoint.reading(table, at, lag)));
        Observation observation = new Observation(PIPE, PipelineState.RUNNING,
                Map.of("records.out", rows, "lag.orders", lag), Map.of(), Map.of(), null, at, facts);
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static MetricPoint point(Observation observation, String name) {
        return observation.facts().stream().filter(fact -> fact.name().equals(name))
                .findFirst().orElseThrow().points().getFirst();
    }

    private static Observation awaitObservation(AtomicReference<ObservationStore.Stored> latest,
            ObservationStore.Scope scope, Instant at) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            ObservationStore.Stored stored = latest.get();
            if (stored != null && stored.scope().filter(scope::equals).isPresent()
                    && at.equals(stored.observation().observedAt())) {
                return stored.observation();
            }
            TimeUnit.MILLISECONDS.sleep(5);
        }
        throw new AssertionError("observation did not reach its scoped latest slot");
    }

    @Test
    void startBindsTheArtifactIncarnationToItsDurableExecutionGenerationForObservations() {
        List<String> events = new CopyOnWriteArrayList<>();
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> resources) {
                throw new UnsupportedOperationException();
            }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) {
                return Optional.empty();
            }
            @Override public List<io.tapstate.core.model.Resource> list() { return List.of(); }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                assertThat(id).isEqualTo(PIPE);
                return Optional.of("inc-a");
            }
        };
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member),
                new RecordingDagSource(events), new RecordingCaptureCoordinator(events), teardown(),
                PipelineActuationOwnership.single("single", new InMemoryWorkloadClaimStore()),
                new PipelineIncarnationService(artifacts), scopes);

        actuator.start(PIPE);

        assertThat(scopes.current(PIPE)).contains(new io.tapstate.spi.store.ObservationStore.Scope("inc-a", 1));
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
    }

    private static final String PIPE = "orders-pipe";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("start captures then submits, pause/resume touch only the engine, stop cancels then stops capture")
    void composesCaptureAndEngineAcrossTheFullLifecycle() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(new Engine(member), dagSource,
                coordinator, teardown(), PipelineActuationOwnership.single("single", generations));
        // At stop the coordinator awaits the pipeline's job going terminal; that only happens if the cancel ran
        // before the capture stop, so it discriminates the stop ordering rather than racing it.
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));
        coordinator.jobAbsentProbe = () -> member.getJet().getJob(PIPE) == null;

        actuator.start(PIPE);
        // Capture opens and fills the ring before the DAG is built and submitted against that generation.
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(coordinator.jobWasAbsentAtStart).isTrue();
        assertThat(dagSource.fences).as("even a standalone submission has a durable run identity")
                .singleElement().isNotNull();
        assertThat(dagSource.fences.getFirst().claimGeneration()).isZero();
        assertThat(dagSource.fences.getFirst().executionGeneration()).isEqualTo(1);
        Job job = member.getJet().getJob(PIPE);
        assertThat(job).as("start submits a job named by the pipeline id").isNotNull();
        awaitStatus(job, JobStatus.RUNNING);

        actuator.pause(PIPE);
        awaitStatus(job, JobStatus.SUSPENDED);
        actuator.resume(PIPE);
        awaitStatus(job, JobStatus.RUNNING);
        // Pause and resume are engine-only: the capture keeps running, so the coordinator is never touched.
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(1);

        actuator.stop(PIPE, true);
        awaitStatus(job, JobStatus.FAILED); // Jet reports a cancelled job as FAILED
        // Stop cancels the job, then stops the capture behind it: the job was already terminal when capture stopped.
        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "buildDag:" + PIPE,
                "stopCapture:" + PIPE + "[purge][jobTerminal]");
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(1);
    }

    @Test
    void aJoblessRestartClosesAnOldCaptureBeforeOpeningTheNewReader() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        coordinator.activeCapture = true;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(events), coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly(
                "stopCapture:" + PIPE + "[keep][jobLive]",
                "startCapture:" + PIPE,
                "buildDag:" + PIPE);
        assertThat(coordinator.activeCapture).isTrue();
    }

    @Test
    void aDuplicateStartWithALiveJobKeepsItsCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(events), coordinator, teardown(),
                PipelineActuationOwnership.single("single", generations));

        actuator.start(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        actuator.start(PIPE);

        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(generations.executionGeneration(standaloneKey()))
                .as("the live job was not submitted a second time").isEqualTo(1);
    }

    @Test
    void anUnconfirmedStandaloneGenerationClosesAdmittedCaptureWithoutJobSubmission() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        ExecutionGenerationStore unavailable = new ExecutionGenerationStore() {
            @Override
            public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long revision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OptionalLong advanceStandalone(String clusterId, String pipelineId) {
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "unreachable"), null);
            }
        };
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown(),
                PipelineActuationOwnership.single("single", unavailable));

        assertThatThrownBy(() -> actuator.start(PIPE))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(ActuationError.EXECUTION_GENERATION_UNAVAILABLE));
        assertThat(events).containsExactly("startCapture:" + PIPE,
                "stopCapture:" + PIPE + "[keep][jobLive]");
        assertThat(coordinator.activeCapture).isFalse();
        assertThat(member.getJet().getJob(PIPE)).isNull();
    }

    @Test
    void aCancelledStartClosesItsCaptureWithoutSubmittingOrPurging() {
        for (boolean cancelDuringBuild : List.of(false, true)) {
            List<String> events = new CopyOnWriteArrayList<>();
            RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
            RecordingDagSource dagSource = new RecordingDagSource(events);
            coordinator.interruptAfterStart = !cancelDuringBuild;
            dagSource.interruptDuringBuild = cancelDuringBuild;
            LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                    new Engine(member), dagSource, coordinator, teardown());

            try {
                actuator.start(PIPE);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally {
                Thread.interrupted();
            }
            assertThat(coordinator.activeCapture).isFalse();
            assertThat(member.getJet().getJob(PIPE)).isNull();
            assertThat(events).contains("startCapture:" + PIPE,
                    "stopCapture:" + PIPE + "[keep][jobLive]");
            if (cancelDuringBuild) {
                assertThat(events).contains("buildDag:" + PIPE);
            } else {
                assertThat(events).doesNotContain("buildDag:" + PIPE);
            }
        }
    }

    @Test
    void aPreparedStartPassesTheSameCursorTokenToCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        dagSource.artifactSnapshot = new InMemoryArtifactStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(dagSource.preparedTokens).hasSize(1);
        assertThat(coordinator.captureTokens).containsExactly(dagSource.preparedTokens.getFirst());
    }

    /**
     * A start the capture side gives back submits nothing and throws nothing. The pipeline is left carrying
     * no job, which is what the next pass starts again -- rather than a job over a ring nobody opened, or a
     * failure recorded for a capture another member is still opening.
     */
    @Test
    void aStartTheCaptureGivesBackSubmitsNothing() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        coordinator.givesTheStartBack = true;
        RecordingDagSource dagSource = new RecordingDagSource(events);
        LifecycleActuator actuator =
                TestEngineLifecycleActuators.create(new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly("startCapture:" + PIPE);
        assertThat(member.getJet().getJob(PIPE)).as("no job was submitted").isNull();
        assertThat(actuator.isCarryingAJob(PIPE)).isFalse();
    }

    @Test
    void aPreparedStartAbandonedByACasLossClosesCaptureBeforeAnyJobIsSubmitted() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(events), coordinator, teardown());

        try (LifecycleActuator.PreparedStart prepared = actuator.prepareStart(PIPE)) {
            assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
            assertThat(coordinator.activeCapture).isTrue();
            assertThat(member.getJet().getJob(PIPE)).isNull();
        }

        assertThat(coordinator.activeCapture).isFalse();
        assertThat(member.getJet().getJob(PIPE)).isNull();
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE,
                "stopCapture:" + PIPE + "[keep][jobLive]");
    }

    @Test
    void aRingThatIsNotReadyDoesNotRecordRunningBeforeAJobExists() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.givesTheStartBack = true;
        assertDeferredStartKeepsActualNew(coordinator);
    }

    @Test
    void anExhaustedSnapshotPoolDoesNotRecordRunningBeforeAJobExists() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.snapshotCapacityUnavailable = true;
        assertDeferredStartKeepsActualNew(coordinator);
    }

    @Test
    void capacityRetriesDoNotAdvanceExecutionGenerationBeforeSubmission() {
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.snapshotCapacityUnavailable = true;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(new CopyOnWriteArrayList<>()), coordinator,
                teardown(), PipelineActuationOwnership.single("single", generations));
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        PipelineConverger loop = new PipelineConverger(desired, state, actuator, Clock.systemUTC());
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));

        loop.converge(PIPE);
        loop.converge(PIPE);
        assertThat(generations.executionGeneration(standaloneKey()))
                .as("a capacity retry has not created a data-plane execution").isZero();

        coordinator.snapshotCapacityUnavailable = false;
        loop.converge(PIPE);
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(1);
        assertThat(actuator.isCarryingAJob(PIPE)).isTrue();
    }

    @Test
    void aClaimLostAfterCaptureAdmissionClosesCaptureAndSubmitsNoJob() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
        PipelineActuationOwnership ownership = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate,
                new ClusterWorkloadClaims(raw, gate), Duration.ofSeconds(30),
                Duration.ofSeconds(10), () -> 0L);
        assertThat(ownership.permit(PIPE).granted()).isTrue();
        WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, PIPE);
        WorkloadClaim held = raw.read(key).orElseThrow().claim();
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        coordinator.afterStart = () -> assertThat(raw.release(held)).isTrue();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(events), coordinator, teardown(), ownership);
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));

        new PipelineConverger(desired, state, actuator, Clock.systemUTC()).converge(PIPE);

        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson())).isEqualTo(PipelineState.NEW);
        assertThat(events).containsExactly("startCapture:" + PIPE,
                "stopCapture:" + PIPE + "[keep][jobLive]");
        assertThat(coordinator.activeCapture).isFalse();
        assertThat(member.getJet().getJob(PIPE)).isNull();
        assertThat(raw.executionGeneration(key)).isZero();
    }

    private void assertDeferredStartKeepsActualNew(RecordingCaptureCoordinator coordinator) {
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        InMemoryStateStore state = new InMemoryStateStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(new CopyOnWriteArrayList<>()),
                coordinator, teardown());

        new PipelineConverger(desired, state, actuator, Clock.systemUTC()).converge(PIPE);

        assertThat(state.read(PIPE)).hasValueSatisfying(checkpoint ->
                assertThat(StateJson.parse(checkpoint.stateJson()))
                        .as("capacity waits must not become a running actual state").isEqualTo(PipelineState.NEW));
        assertThat(actuator.isCarryingAJob(PIPE)).isFalse();
    }

    @Test
    void surfacesACaptureFailureThroughTheSeamWhenTheEngineJobReportsNone() {
        RuntimeException boom = new RuntimeException("cdc tail died");
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.captureFailure = boom;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        // No job was submitted, so the engine reports no failure; the cdc capture's death still surfaces through
        // the seam the converge loop reads, which is what drives a pipeline whose tail died into FAILED even
        // though its Jet job keeps running over a ring gone quiet.
        assertThat(actuator.failure(PIPE)).contains(boom);
    }

    @Test
    void validatesBeforeCaptureOrSubmission() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        TapstateException refused = new TapstateException(
                ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED, Map.of("source", "orders_src"), null);
        dagSource.validation = () -> {
            events.add("validate:" + PIPE);
            throw refused;
        };
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown());

        assertThatThrownBy(() -> actuator.start(PIPE)).isSameAs(refused);
        assertThat(events).containsExactly("validate:" + PIPE);
    }

    @Test
    void passesThePreparedArtifactSnapshotToCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        ArtifactStore snapshot = ReadOnlyArtifactSnapshot.capture(new InMemoryArtifactStore());
        dagSource.artifactSnapshot = snapshot;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown());
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        try {
            assertThat(coordinator.artifactSnapshot).isSameAs(snapshot);
        } finally {
            actuator.stop(PIPE, true);
        }
    }

    @Test
    void theTopologyAStartBuildsIsHeldToThatStartsOwnRun() {
        // The run's generation is taken before the first side effect; the topology is built afterwards, once
        // placement and any outstanding teardown are settled. Everything else a start does sits between the
        // two, and nothing local ties them together -- so a build handed no generation is the quiet failure
        // here: it reaches every external effect unfenced, and a run nothing fences is a run nothing can
        // later stop from writing beside the run that replaced it.
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        PipelineActuationOwnership ownership = clusteredOwnership();
        assertThat(ownership.permit(PIPE).granted())
                .as("the member has to hold the pipeline before a start of it can be fenced at all")
                .isTrue();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown(), ownership);
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        try {
            assertThat(dagSource.fences).as("the topology was built exactly once").hasSize(1);
            ExecutionFence fence = dagSource.fences.get(0);
            assertThat(fence).as("the build was given a run to be held to, not none").isNotNull();
            assertThat(fence.pipelineId()).isEqualTo(PIPE);
            assertThat(fence.executionGeneration())
                    .as("and it is this start's own generation, the one begun a moment earlier")
                    .isEqualTo(1);
        } finally {
            actuator.stop(PIPE, true);
        }
    }

    @Test
    void reportsNoFailureWhenNeitherTheJobNorTheCaptureHasFailed() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        assertThat(actuator.failure(PIPE)).isEmpty();
    }

    /**
     * A hold that landed before the load reached the target cannot be carried on from in place, so this
     * resume rebuilds: a stop that keeps, then a start.
     *
     * <p>The rows a load has read but not delivered reach the source vertex through a member-local hand-off
     * that vertex consumes once, and a resume restarts the job under a guarantee that keeps no execution
     * state -- so the vertex that comes back finds the hand-off empty over a capture that has moved on, and
     * reads nothing at all while reporting healthy. A start is what fills it again.
     *
     * <p>Asserted on the verbs and their order, which is the whole of what this seam decides. That the stop
     * is the keeping one is asserted with them and is load-bearing: a purging stop would throw away the
     * position of a pipeline nobody asked to clear.
     */
    @Test
    void aResumeOverALoadThatHasNotReachedTheTargetRebuildsInsteadOfCarryingOn() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(new Engine(member), dagSource,
                coordinator, teardown(), PipelineActuationOwnership.single("single", generations));
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        actuator.pause(PIPE);
        awaitStatus(held, JobStatus.SUSPENDED);

        coordinator.loadDelivered = false;
        actuator.resume(PIPE);

        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "buildDag:" + PIPE,
                "stopCapture:" + PIPE + "[keep][jobTerminal]",
                "startCapture:" + PIPE, "buildDag:" + PIPE);
        Job rebuilt = member.getJet().getJob(PIPE);
        assertThat(rebuilt).as("the rebuild submits a job of its own").isNotSameAs(held);
        awaitStatus(rebuilt, JobStatus.RUNNING);
        assertThat(dagSource.fences).extracting(ExecutionFence::executionGeneration).containsExactly(1L, 2L);
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(2);
    }

    @Test
    void anUndeliveredPausedLoadWaitingForCapacityDoesNotRecordRunning() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(new CopyOnWriteArrayList<>()),
                coordinator, teardown());
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        PipelineConverger loop = new PipelineConverger(desired, state, actuator, Clock.systemUTC());
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        loop.converge(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        desired.save(new DesiredState(PIPE, PipelineState.PAUSED, "rev-1"));
        loop.converge(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.SUSPENDED);

        coordinator.loadDelivered = false;
        coordinator.snapshotCapacityUnavailable = true;
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        loop.converge(PIPE);

        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson()))
                .as("rebuilding resume must not report a running job when admission deferred")
                .isEqualTo(PipelineState.STOPPED);
        assertThat(actuator.isCarryingAJob(PIPE)).isFalse();
    }

    @Test
    void aRestartWaitingForCapacityStopsOnceAndRetriesTheSameIntent() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(new CopyOnWriteArrayList<>()),
                coordinator, teardown());
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        PipelineConverger loop = new PipelineConverger(desired, state, actuator, Clock.systemUTC());
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-1"));
        loop.converge(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        long previousEpoch = state.read(PIPE).orElseThrow().epoch();

        coordinator.snapshotCapacityUnavailable = true;
        desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-2", false,
                null, true, previousEpoch));
        loop.converge(PIPE);
        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson()))
                .isEqualTo(PipelineState.STOPPED);
        assertThat(actuator.isCarryingAJob(PIPE)).isFalse();

        coordinator.snapshotCapacityUnavailable = false;
        loop.converge(PIPE);
        assertThat(StateJson.parse(state.read(PIPE).orElseThrow().stateJson()))
                .isEqualTo(PipelineState.RUNNING);
        assertThat(actuator.isCarryingAJob(PIPE)).isTrue();
    }

    /**
     * The same hold over a pipeline that reads its source once and opens no tail. It rebuilds too, and for
     * the same reason: what a resume cannot carry on from is the load, and a load is no less unfinished for
     * having no tail behind it.
     *
     * <p>Its rows travel exactly as any other load's do -- appended to the member-local hand-off by the
     * capture, taken from it by the source vertex, which consumes what is there once. A resume re-runs the
     * topology under a guarantee that keeps no execution state, so the vertex that comes back finds the
     * hand-off empty and there is nothing behind it to fill it again: no tail, and a capture that has
     * already returned. It emits nothing at all, for ever, with the job running, the pipeline reporting
     * healthy and nothing thrown -- and every row the hold caught in flight is absent from the target for
     * good.
     *
     * <p>Driven through the store-backed coordinator rather than the stand-in above, because the stand-in
     * is told the answer and this case is about how that answer is reached. A read that opens no tail opens
     * no chain either, so nothing durable records what its load delivered, and this is exactly the shape
     * where the question has to be answered without a record to answer it from.
     *
     * <p>Asserted on the capture being run a second time, which is the whole of what a rebuild is for here:
     * the run that refills the hand-off the resumed vertex is about to read.
     */
    @Test
    void aResumeOverASnapshotOnlyLoadThatHasNotReachedTheTargetRebuildsAsWell() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null));
        artifacts.save(new PipelineResource(PIPE, null, List.of(SourceRef.spec("orders_src", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_src", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_ONLY, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        int[] captures = {0};
        // What a snapshot-only run really hands back: rows read, and no chain, because it opens no tail.
        CaptureStarter starter = (spec, passthrough) -> {
            captures[0]++;
            return new CaptureRun(Optional.empty(), false, 2L, Map.of("orders", 2L),
                    Optional.empty(), Optional.of(() -> { }), new CaptureHealth());
        };
        PipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, starter, new SrsCoordinator(store.meta()), new SnapshotBuffer());
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        actuator.start(PIPE);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        actuator.pause(PIPE);
        awaitStatus(held, JobStatus.SUSPENDED);

        actuator.resume(PIPE);

        assertThat(captures[0])
                .as("the resumed vertex reads the hand-off, and only a second capture run refills it")
                .isEqualTo(2);
    }

    /**
     * The state teardown these cases run against: the stand-in topology keeps no state, so what this drops
     * is nothing. It is here because the actuator will not be built without one, which is the point.
     */
    /** A member that holds its cluster's pipelines, so a start of one is granted a run to be fenced to. */
    private PipelineActuationOwnership clusteredOwnership() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(new InMemoryWorkloadClaimStore(), gate);
        return new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate, claims,
                Duration.ofSeconds(30), Duration.ofSeconds(10), () -> 0L);
    }

    private NestStateTeardown teardown() {
        return new NestStateTeardown(member, new InMemoryKeyedStateStore(), new InMemoryNestDeadLetterStore());
    }

    private static WorkloadClaimKey standaloneKey() {
        return new WorkloadClaimKey("single", WorkloadClaimType.PIPELINE_ACTUATION, PIPE);
    }

    private static void awaitStatus(Job job, JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = job.getStatus();
            if (last == expected) {
                return;
            }
            sleep();
        }
        throw new AssertionError("job did not reach " + expected + " within budget; last status was " + last);
    }

    /** Polls for the job going terminal within a budget; true if it does, false on timeout. */
    private static boolean awaitTerminal(Job job) {
        if (job == null) {
            return false;
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (job.getStatus().isTerminal()) {
                return true;
            }
            sleep();
        }
        return false;
    }

    private static void sleep() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Records each capture verb into a shared event log; at stop it records whether the job was already terminal. */
    private static final class RecordingCaptureCoordinator implements PipelineCaptureCoordinator {

        private final List<String> events;
        private Supplier<Boolean> jobTerminalProbe = () -> false;
        private Throwable captureFailure;
        private boolean loadDelivered = true;
        private ArtifactStore artifactSnapshot;
        private Supplier<Boolean> jobAbsentProbe = () -> true;
        private boolean jobWasAbsentAtStart;
        private boolean givesTheStartBack;
        private boolean snapshotCapacityUnavailable;
        private Runnable afterStart = () -> { };
        private boolean interruptAfterStart;
        private boolean activeCapture;
        private final List<String> captureTokens = new CopyOnWriteArrayList<>();

        RecordingCaptureCoordinator(List<String> events) {
            this.events = events;
        }

        @Override
        public void startCapture(String pipelineId) {
            jobWasAbsentAtStart = jobAbsentProbe.get();
            events.add("startCapture:" + pipelineId);
            if (givesTheStartBack) {
                throw new RingNotOpenYet(CaptureId.of(
                        new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders")), null));
            }
            if (snapshotCapacityUnavailable) {
                throw new SnapshotCapacityUnavailable();
            }
            activeCapture = true;
            afterStart.run();
            if (interruptAfterStart) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void startCapture(String pipelineId, ArtifactStore artifactSnapshot) {
            this.artifactSnapshot = artifactSnapshot;
            startCapture(pipelineId);
        }

        @Override
        public void startCapture(String pipelineId, ArtifactStore artifactSnapshot, String cursorWriterToken) {
            captureTokens.add(cursorWriterToken);
            startCapture(pipelineId, artifactSnapshot);
        }

        @Override
        public void stopCapture(String pipelineId, boolean purgeState) {
            events.add("stopCapture:" + pipelineId + (purgeState ? "[purge]" : "[keep]")
                    + (jobTerminalProbe.get() ? "[jobTerminal]" : "[jobLive]"));
            activeCapture = false;
        }

        @Override
        public boolean hasActiveCapture(String pipelineId) {
            return activeCapture;
        }

        @Override
        public Optional<Throwable> captureFailure(String pipelineId) {
            return Optional.ofNullable(captureFailure);
        }

        @Override
        public boolean loadDelivered(String pipelineId) {
            return loadDelivered;
        }
    }

    /** Records each topology request into the shared log and returns the idle stand-in topology. */
    private static final class RecordingDagSource implements DagSource {
        private final List<String> events;
        private final IdleDagSource idle = new IdleDagSource();
        private final List<ExecutionFence> fences = new CopyOnWriteArrayList<>();
        private Runnable validation = () -> {
        };
        private ArtifactStore artifactSnapshot;
        private boolean interruptDuringBuild;
        private final List<String> preparedTokens = new CopyOnWriteArrayList<>();

        RecordingDagSource(List<String> events) {
            this.events = events;
        }

        @Override
        public void validateStart(String pipelineId) {
            validation.run();
        }

        @Override
        public StartPreparation prepareStart(String pipelineId, String defaultDatabase) {
            if (artifactSnapshot == null) {
                StartPreparation prepared = DagSource.super.prepareStart(pipelineId, defaultDatabase);
                preparedTokens.add(prepared.cursorWriterToken());
                return prepared;
            }
            validateStart(pipelineId);
            StartPreparation prepared = new StartPreparation(
                    capacityOf(pipelineId), stateLocations(pipelineId, defaultDatabase),
                    Optional.of(artifactSnapshot),
                    fence -> dagFor(pipelineId, fence));
            preparedTokens.add(prepared.cursorWriterToken());
            return prepared;
        }

        /** Keeps no state, so there is nothing for a budget to be applied to. */
        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }

        @Override
        public DAG dagFor(String pipelineId) {
            events.add("buildDag:" + pipelineId);
            if (interruptDuringBuild) {
                Thread.currentThread().interrupt();
            }
            return idle.dagFor(pipelineId);
        }

        /**
         * Kept out of the shared event log on purpose: the log is asserted verbatim by the cases about verb
         * ordering, and a run generation in it would make every one of those cases read as being about this.
         */
        @Override
        public DAG dagFor(String pipelineId, ExecutionFence fence) {
            fences.add(fence);
            return dagFor(pipelineId);
        }

        @Override
        public java.util.List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(
                String pipelineId) {
            return idle.stateHeldBy(pipelineId);
        }
    }
    /**
     * A faithful in-memory {@link StateStore} double for the converge-loop tests: it applies the pure
     * {@link EpochCas} exactly as the Mongo adapter applies it atomically, so the converger's fencing and
     * rebase behaviour is exercised against real fencing semantics rather than a mock. A {@code beforeSwap}
     * hook lets a test slip a competing writer in just before a compare-and-swap, staging the artificial
     * failover that a single node never produces on its own.
     */
    private static final class RestartableStopStateStore implements StateStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }


        private final Map<String, CheckpointDoc> docs = new HashMap<>();
        private Runnable beforeSwap = () -> {};
        private int swapAttempts = 0;
        private final Map<String, StopReservation> stops = new HashMap<>();
        private DesiredStore stopIntents;
        private ExecutionGenerationStore generations;
        private Function<String, StopAuthority> authorities;
        private Runnable beforeComplete = () -> { };
        private int reservations;

        @Override
        public Optional<CheckpointDoc> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public void create(String pipelineId, String stateJson, Instant touchTime) {
            if (docs.containsKey(pipelineId)) {
                throw new IllegalStateException("create on an already-seeded pipeline " + pipelineId);
            }
            docs.put(pipelineId, CheckpointDoc.initial(pipelineId, stateJson, touchTime));
        }

        @Override
        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            swapAttempts++;
            beforeSwap.run();
            return applySwap(pipelineId, expectedEpoch, nextStateJson, touchTime);
        }

        /** Applies the fence without running the {@code beforeSwap} hook — the seam a competitor writes through. */
        CasOutcome applySwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            CheckpointDoc current = docs.get(pipelineId);
            if (current == null) {
                throw new IllegalStateException("compareAndSwap on an unseeded pipeline " + pipelineId);
            }
            if (stops.containsKey(pipelineId)) { return new CasOutcome.Fenced(current.epoch()); }
            CasOutcome outcome = EpochCas.swap(current, expectedEpoch, nextStateJson, touchTime);
            if (outcome instanceof CasOutcome.Applied applied) {
                docs.put(pipelineId, applied.next());
            }
            return outcome;
        }

        void onBeforeSwap(Runnable hook) {
            this.beforeSwap = hook;
        }

        /** How many times the converger has called {@link #compareAndSwap} — the retry count under test. */
        int swapAttempts() {
            return swapAttempts;
        }

        void enableStops(DesiredStore intents, Function<String, StopAuthority> currentAuthority,
                ExecutionGenerationStore generations) {
            this.generations = Objects.requireNonNull(generations);
            stopIntents = Objects.requireNonNull(intents);
            authorities = Objects.requireNonNull(currentAuthority);
        }

        @Override public boolean supportsStopReservations() { return stopIntents != null; }

        @Override public synchronized Optional<StopReservation> readStopReservation(String pipelineId) {
            return Optional.ofNullable(stops.get(pipelineId));
        }

        @Override public synchronized Optional<StopReservation> reserveStop(
                CheckpointDoc expected, StopReservation proposal, Instant at) {
            if (!expected.equals(docs.get(expected.pipelineId())) || stops.containsKey(expected.pipelineId())
                    || expected.epoch() != proposal.sourceEpoch() || proposal.reservedEpoch() != expected.epoch() + 1
                    || !intent(proposal.originalDesired()) || !authority(proposal.pipelineId(), proposal.authorityOrNull())) {
                return Optional.empty();
            }
            CasOutcome.Applied admitted = (CasOutcome.Applied) EpochCas.swap(expected, expected.epoch(),
                    expected.stateJson(), at);
            docs.put(expected.pipelineId(), admitted.next());
            stops.put(expected.pipelineId(), proposal);
            reservations++;
            return Optional.of(proposal);
        }

        @Override public synchronized Optional<StopReservation> rebindStop(
                StopReservation expected, StopAuthority successor, Instant at) {
            if (!exact(expected) || !intent(expected.originalDesired()) || !authority(expected.pipelineId(), successor)) {
                return Optional.empty();
            }
            StopReservation rebound = expected.rebind(successor, Math.incrementExact(expected.reservedEpoch()));
            advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
            stops.put(expected.pipelineId(), rebound);
            return Optional.of(rebound);
        }

        @Override public synchronized Optional<StopReservation> promoteStopReservation(StopReservation expected,
                DesiredState intent, StopAuthority writer, Instant at) {
            if (!exact(expected) || !expected.legacy() || !intent(intent) || !authority(expected.pipelineId(), writer)) {
                return Optional.empty();
            }
            StopReservation promoted = move(expected, StopReservation.Phase.STOPPING,
                    StopReservation.CounterPolicy.freeze(docs.get(expected.pipelineId()), expected.originalDesired()),
                    writer, null, docs.get(expected.pipelineId()).stateJson(), at);
            return Optional.of(promoted);
        }

        @Override public synchronized Optional<StopReservation> markReplacementPending(StopReservation expected, Instant at) {
            if (!guard(expected)) { return Optional.empty(); }
            return Optional.of(move(expected, StopReservation.Phase.REPLACEMENT_PENDING, expected.counterPolicy(),
                    expected.writerAuthority(), null, StateJson.of(PipelineState.STOPPED), at));
        }

        @Override public synchronized Optional<SuccessorAdmission> admitSuccessor(StopReservation expected,
                String incarnation, String boot, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.REPLACEMENT_PENDING
                    || !StateJson.of(PipelineState.STOPPED).equals(docs.get(expected.pipelineId()).stateJson())) {
                return Optional.empty();
            }
            long generation = generations.advanceStandalone(expected.source().clusterId(), expected.pipelineId()).orElseThrow();
            var writer = StopAuthority.standalone(expected.source().clusterId(), generation);
            var slot = new StopReservation.Successor(new ObservationStore.Scope(incarnation, generation), boot, null);
            return Optional.of(new SuccessorAdmission(move(expected, StopReservation.Phase.SUCCESSOR_ADMITTED,
                    expected.counterPolicy(), writer, slot, StateJson.of(PipelineState.STOPPED), at), Optional.empty()));
        }

        @Override public synchronized Optional<StopReservation> bindSuccessor(StopReservation expected,
                ObservationStore.Scope scope, StopReservation.JobIdentity job, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.SUCCESSOR_ADMITTED
                    || !expected.successor().scope().equals(scope) || !expected.successor().submissionBootId().equals(job.bootId())) {
                return Optional.empty();
            }
            return Optional.of(move(expected, StopReservation.Phase.SUCCESSOR_BOUND, expected.counterPolicy(),
                    expected.writerAuthority(), new StopReservation.Successor(scope, job.bootId(), job),
                    StateJson.of(PipelineState.RUNNING), at));
        }

        @Override public synchronized Optional<StopReservation> retireSuccessor(StopReservation expected,
                SuccessorEnd end, Instant at) {
            if (!guard(expected)) { return Optional.empty(); }
            boolean matches = switch (end) {
                case SuccessorEnd.Absent absent -> expected.phase() == StopReservation.Phase.SUCCESSOR_ADMITTED
                        && expected.successor().scope().equals(absent.scope())
                        && expected.successor().submissionBootId().equals(absent.submissionBootId());
                case SuccessorEnd.Terminal terminal -> expected.phase() == StopReservation.Phase.SUCCESSOR_BOUND
                        && expected.successor().scope().equals(terminal.scope()) && expected.successor().job().equals(terminal.job());
            };
            return matches ? Optional.of(move(expected, StopReservation.Phase.REPLACEMENT_PENDING,
                    expected.counterPolicy(), expected.writerAuthority(), null, StateJson.of(PipelineState.STOPPED), at))
                    : Optional.empty();
        }

        @Override public synchronized Optional<CheckpointDoc> completeHandoff(StopReservation expected,
                HandoffIdentity ready, Instant at) {
            beforeComplete.run();
            if (!guard(expected) || expected.phase() != StopReservation.Phase.SUCCESSOR_BOUND
                    || !expected.handoffIdentity().equals(ready)) { return Optional.empty(); }
            CheckpointDoc completed = advance(expected, StateJson.of(PipelineState.RUNNING), at);
            stops.remove(expected.pipelineId());
            return Optional.of(completed);
        }

        private boolean guard(StopReservation expected) {
            return exact(expected) && intent(expected.originalDesired())
                    && authority(expected.pipelineId(), expected.writerAuthority());
        }

        private StopReservation move(StopReservation expected, StopReservation.Phase phase,
                StopReservation.CounterPolicy policy, StopAuthority writer, StopReservation.Successor slot,
                String actual, Instant at) {
            CheckpointDoc checkpoint = advance(expected, actual, at);
            var moved = new StopReservation(expected.pipelineId(), expected.token(), expected.sourceEpoch(),
                    checkpoint.epoch(), expected.originalDesired(), expected.source(), phase, policy, writer, slot,
                    StopReservation.CURRENT_FORMAT);
            stops.put(expected.pipelineId(), moved);
            return moved;
        }

        @Override public synchronized Optional<CheckpointDoc> completeStop(StopReservation expected, Instant at) {
            beforeComplete.run();
            if (!exact(expected) || !intent(expected.originalDesired())
                    || !authority(expected.pipelineId(), expected.authorityOrNull())) {
                return Optional.empty();
            }
            CheckpointDoc completed = advance(expected, StateJson.of(PipelineState.STOPPED), at);
            stops.remove(expected.pipelineId());
            return Optional.of(completed);
        }

        @Override public synchronized Optional<CheckpointDoc> retireStop(StopReservation expected,
                DesiredState successor, StopAuthority authority, Instant at) {
            if (!exact(expected) || expected.originalDesired().equals(successor) || !intent(successor)
                    || !authority(expected.pipelineId(), authority)) {
                return Optional.empty();
            }
            CheckpointDoc retired = advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
            stops.remove(expected.pipelineId());
            return Optional.of(retired);
        }

        private boolean exact(StopReservation marker) {
            CheckpointDoc doc = docs.get(marker.pipelineId());
            return marker.equals(stops.get(marker.pipelineId())) && doc != null && doc.epoch() == marker.reservedEpoch();
        }

        private boolean intent(DesiredState intent) {
            return stopIntents.read(intent.pipelineId()).filter(intent::equals).isPresent();
        }

        private boolean authority(String pipeline, StopAuthority expected) {
            return Objects.equals(expected, authorities.apply(pipeline));
        }

        private CheckpointDoc advance(StopReservation expected, String stateJson, Instant at) {
            CheckpointDoc current = docs.get(expected.pipelineId());
            CasOutcome.Applied applied = (CasOutcome.Applied) EpochCas.swap(current, expected.reservedEpoch(), stateJson, at);
            docs.put(expected.pipelineId(), applied.next());
            return applied.next();
        }

        void onBeforeComplete(Runnable action) { beforeComplete = action; }
        int stopReservations() { return reservations; }
    }

}
