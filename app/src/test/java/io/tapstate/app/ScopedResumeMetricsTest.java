package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.metrics.Metrics;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ScopedResumeMetricsTest {
    private static final String ID = "orders";
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 3, "cache-a", ID, 1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, ID);
    private static final Clock CLOCK = Clock.systemUTC();
    private static final Instant FIRST_START = Instant.parse("2026-11-25T00:00:00Z");
    private static final Instant NEXT_START = FIRST_START.plusSeconds(60);

    enum Scenario { WAIT_AND_RECONSTRUCT, IMMEDIATE_RESUME, STAMPED_START, IN_PLACE_RESUME, PREFLIGHT_REFUSAL }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Scenario.class)
    void preservesResumeMetricsAndStartSemantics(Scenario scenario) {
        Config config = new Config().setClusterName("resume-probe-" + java.util.UUID.randomUUID());
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        var member = Hazelcast.newHazelcastInstance(config);
        try {
            var fixture = new Fixture(member);
            fixture.accept(fixture.lifecycle.start("operator", ID));
            var first = fixture.process();
            first.driver().reconcile();
            Job old = member.getJet().getJob(ID);
            assertThat(old).isNotNull();
            awaitNativeReadings(first.engine(), 7L, 3L, FIRST_START);
            first.driver().reconcile();
            var before = fixture.observation().orElseThrow();
            var oldKey = fixture.currentKey();
            assertThat(point(before).startTime()).isEqualTo(FIRST_START);
            assertThat(before.metrics()).containsEntry("records.out", 7L);
            assertThat(tablePoint(before, "tapstate.pipeline.record.delivery.duration").histogram().count()).isEqualTo(3);
            fixture.accept(fixture.lifecycle.pause("operator", ID));
            first.driver().reconcile();
            await().atMost(Duration.ofSeconds(10)).until(() -> old.getStatus() == JobStatus.SUSPENDED);
            assertThat(fixture.actual()).isEqualTo(PipelineState.PAUSED);
            var paused = fixture.observation().orElseThrow();
            System.out.printf("Scoped resume metrics %s pausedEpoch=%s pausedOut=%s pausedStart=%s%n",
                    scenario, fixture.checkpoint().epoch(), paused.metrics().get("records.out"),
                    paused.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                            .flatMap(fact -> fact.points().stream()).map(MetricPoint::startTime).toList());

            DesiredState intent;
            if (scenario == Scenario.STAMPED_START) {
                fixture.accept(fixture.lifecycle.stop("operator", ID, false));
                intent = fixture.lifecycle.start("operator", ID);
                assertThat(intent.rebuiltAtStateEpoch()).isEqualTo(fixture.checkpoint().epoch());
                assertThat(intent.reassemble()).isTrue();
            } else {
                intent = fixture.lifecycle.resume("operator", ID);
                assertThat(intent.targetState()).isEqualTo(PipelineState.RUNNING);
                assertThat(intent.reassemble()).isFalse();
                assertThat(intent.rebuiltAtStateEpoch()).isNull();
            }
            fixture.accept(intent);
            fixture.loaded = scenario == Scenario.IN_PLACE_RESUME;
            fixture.waitForRing = scenario == Scenario.WAIT_AND_RECONSTRUCT;
            fixture.refusePreparation = scenario == Scenario.PREFLIGHT_REFUSAL;
            first.driver().reconcile();
            var intermediate = fixture.observation();
            System.out.printf("Scoped resume metrics %s original=%s oldJob=%s oldStatus=%s checkpoint=%s generation=%s live=%s out=%s%n",
                    scenario, oldKey, old.getId(), old.getStatus(), fixture.checkpoint(), fixture.claim().executionGeneration(),
                    liveJobs(member), intermediate.map(Observation::metrics).map(metrics -> metrics.get("records.out")));

            if (scenario == Scenario.PREFLIGHT_REFUSAL) {
                assertThat(fixture.claim()).isEqualTo(oldKey.claim());
                assertThat(fixture.actual()).isEqualTo(PipelineState.FAILED);
                assertThat(old.getStatus().isTerminal()).isTrue();
                assertThat(liveJobs(member)).isEmpty();
                assertThat(fixture.observations.get(oldKey)).isEqualTo(before);
                assertThat(intermediate.orElseThrow().failure().code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED.code());
                return;
            }
            if (scenario == Scenario.IN_PLACE_RESUME) {
                await().atMost(Duration.ofSeconds(10)).until(() -> old.getStatus() == JobStatus.RUNNING);
                assertThat(liveJobs(member)).containsExactly(old.getId());
                assertThat(fixture.claim()).isEqualTo(oldKey.claim());
                return;
            }

            assertThat(old.getStatus().isTerminal()).isTrue();
            var process = first;
            long expectedGeneration = oldKey.claim().executionGeneration() + 1;
            if (scenario == Scenario.WAIT_AND_RECONSTRUCT) {
                assertThat(fixture.actual()).isEqualTo(PipelineState.RUNNING);
                assertThat(fixture.claim().executionGeneration()).isEqualTo(expectedGeneration);
                assertThat(liveJobs(member)).isEmpty();
                assertThat(intermediate.orElseThrow().metrics()).doesNotContainKey("records.out");
                assertThat(fixture.observations.get(oldKey)).isEqualTo(before);
                fixture.waitForRing = false;
                process = fixture.process();
                process.driver().reconcile();
                expectedGeneration++;
            }
            var running = process;
            awaitNativeReadings(running.engine(), 2L, 1L, NEXT_START);
            var replacement = running.engine().captureRun(ID).orElseThrow();
            assertThat(replacement.jobId()).isNotEqualTo(old.getId());
            assertThat(liveJobs(member)).containsExactly(replacement.jobId());
            assertThat(fixture.claim().executionGeneration()).isEqualTo(expectedGeneration);
            assertThat(replacement.operatorManifest().executionIdentity()).isEqualTo(new ExecutionFence(SCOPE, fixture.claim()));
            int priorReads = fixture.observationReads.size();
            running.driver().reconcile();
            var actual = fixture.observation().orElseThrow();
            running.driver().reconcile();
            var repeated = fixture.observation().orElseThrow();
            System.out.printf("Scoped resume metrics %s originalOut=%s originalStart=%s oldStillStored=%s replacementJob=%s current=%s nativeRows=%s out=%s start=%s repeated=%s publicationReads=%s%n",
                    scenario, before.metrics().get("records.out"), point(before).startTime(), fixture.observations.containsKey(oldKey),
                    replacement.jobId(), fixture.currentKey(), running.engine().recordsDelivered(ID), actual.metrics().get("records.out"),
                    point(actual).startTime(), repeated.metrics().get("records.out"),
                    fixture.observationReads.subList(priorReads, fixture.observationReads.size()));
            long expectedCount = scenario == Scenario.STAMPED_START ? 2L : 9L;
            Instant expectedStart = scenario == Scenario.STAMPED_START ? NEXT_START : FIRST_START;
            long expectedDurationCount = scenario == Scenario.STAMPED_START ? 1 : 4;
            long eventTime = running.engine().newestDeliveredEventTime(ID).get("orders");
            assertSoftly(soft -> {
                soft.assertThat(actual.metrics().get("records.out")).as("published cumulative output").isEqualTo(expectedCount);
                soft.assertThat(point(actual).startTime()).as("published counter start").isEqualTo(expectedStart);
                soft.assertThat(repeated.metrics().get("records.out")).as("repeated publication").isEqualTo(expectedCount);
                for (Observation sample : List.of(actual, repeated)) {
                    MetricPoint duration = tablePoint(sample, "tapstate.pipeline.record.delivery.duration");
                    soft.assertThat(duration.startTime()).as("duration counter start").isEqualTo(expectedStart);
                    soft.assertThat(duration.histogram().count()).as("duration count").isEqualTo(expectedDurationCount);
                    soft.assertThat(duration.histogram().bucketCounts().getFirst()).as("first duration bucket")
                            .isEqualTo(expectedDurationCount);
                    MetricPoint lag = tablePoint(sample, "tapstate.pipeline.lag");
                    soft.assertThat(lag.value()).as("lag from the live job")
                            .isEqualTo(Math.max(0L, (lag.observedAt().toEpochMilli() - eventTime) / 1000));
                }
            });
        } finally {
            member.shutdown();
        }
    }

    private static MetricPoint point(Observation observation) {
        return observation.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                .flatMap(fact -> fact.points().stream()).filter(point -> "out".equals(point.attributes().get("direction")))
                .findFirst().orElseThrow();
    }

    private static MetricPoint tablePoint(Observation observation, String name) {
        return observation.facts().stream().filter(fact -> fact.name().equals(name))
                .flatMap(fact -> fact.points().stream()).filter(point -> "orders".equals(point.attributes().get(MetricAttributes.TABLE_ID)))
                .findFirst().orElseThrow();
    }

    private static void awaitNativeReadings(Engine engine, long rows, long durationSamples, Instant start) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(engine.recordsDelivered(ID)).containsEntry("orders", Map.of("insert", rows));
            assertThat(engine.deliveryDurations(ID)).hasEntrySatisfying("orders", h -> assertThat(h.count()).isEqualTo(durationSamples));
            assertThat(engine.newestDeliveredEventTime(ID)).containsEntry("orders", start.toEpochMilli());
        });
    }

    private static List<Long> liveJobs(HazelcastInstance member) {
        return member.getJet().getJobs().stream().filter(job -> ID.equals(job.getName()) && !job.getStatus().isTerminal())
                .map(Job::getId).toList();
    }

    private record ObservationKey(long epoch, WorkloadClaimFence claim) {}
    private record Process(Engine engine, ConvergenceDriver driver) {}

    private static final class Fixture {
        final HazelcastInstance member;
        final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        final InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        final InMemoryStateStore state = new InMemoryStateStore();
        final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        final PipelineLifecycleService lifecycle = new PipelineLifecycleService(new ArtifactQueryService(artifacts),
                desired, new AuditGate(record -> {}, CLOCK), state::read);
        final Map<ObservationKey, Observation> observations = new HashMap<>();
        final List<ObservationKey> observationReads = new java.util.ArrayList<>();
        PipelineIntentSnapshot intent;
        long revision;
        boolean loaded, waitForRing, refusePreparation;

        Fixture(HazelcastInstance member) {
            this.member = member;
            artifacts.save(new SourceResource("orders_src", null, "mysql", Map.of("host", "unused"),
                    SourceMode.CDC, List.of(TableRef.literal("orders")), null, null));
            artifacts.save(new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: orders
                    source: orders_src
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: /.*/
                      sync:
                        - id: sink
                          source: orders_dest
                          write_mode: upsert
                          ddl: apply
                    """));
            state.create(ID, StateJson.of(PipelineState.NEW), CLOCK.instant());
        }

        void accept(DesiredState next) { intent = new PipelineIntentSnapshot(SCOPE, ++revision, next); }
        CheckpointDoc checkpoint() { return state.read(ID).orElseThrow(); }
        PipelineState actual() { return StateJson.parse(checkpoint().stateJson()); }
        WorkloadClaimFence claim() { return WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()); }
        ObservationKey currentKey() { return new ObservationKey(checkpoint().epoch(), claim()); }
        Optional<Observation> observation() { return Optional.ofNullable(observations.get(currentKey())); }

        Process process() {
            var membership = mock(ClusterMembershipGate.class);
            when(membership.businessEligible()).thenReturn(true);
            when(membership.committed()).thenReturn(new ClusterMembership("cluster", 7, Set.of("node")));
            var scopedClaims = mock(WorkloadClaimStore.class);
            when(scopedClaims.acquirePipeline(eq(SCOPE), any(), anyLong(), any()))
                    .thenAnswer(call -> claims.acquire(KEY, call.getArgument(1), call.getArgument(2), call.getArgument(3)));
            when(scopedClaims.readPipeline(eq(SCOPE))).thenAnswer(call -> claims.read(KEY));
            when(scopedClaims.advanceExecution(any(), anyLong(), any()))
                    .thenAnswer(call -> claims.advanceExecution(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
            var ownership = PipelineActuationOwnership.scoped(new CacheBusinessScope("cluster", 3, "cache-a"), OWNER,
                    membership, new ClusterWorkloadClaims(scopedClaims, membership), Duration.ofMinutes(1),
                    Duration.ofSeconds(10), System::nanoTime);
            var root = mock(ScopedPipelineCaptureCoordinator.class);
            when(root.forRun(eq(SCOPE), any(), any())).thenAnswer(call -> capture(call.getArgument(2)));
            var engine = new Engine(member);
            var dags = mock(DagSource.class);
            var coordinates = OperatorCoordinatePlan.compile(SCOPE, (PipelineResource) artifacts.get(ID).orElseThrow(),
                    "cache", step -> null, Map.of(), NestSettings.defaults());
            when(dags.prepareStart(any(PipelineConvergencePass.class), eq("cache"))).thenReturn(
                    new DagSource.StartPreparation(DagSource.NestCapacity.none(), Set.of(), Optional.of(artifacts),
                            () -> new DagSource.PlannedTopology(List.of("node"), fence -> {
                                boolean initial = fence.executionGeneration() == 1;
                                return new DagSource.PlannedDag(dag(initial ? 7 : 2, initial ? FIRST_START : NEXT_START),
                                        ExecutionShape.totalOne(), List.of("node"), Map.of(), Map.of());
                            }), Map.of(), Optional.of(SourceModelSnapshot.capture(ID, List.of(), mock(SchemaStore.class))),
                            Optional.of(coordinates)));
            ConnectorReadiness readiness = (pipeline, connectors) -> {
                if (refusePreparation) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
                return Set.of();
            };
            var ports = new ScopedEngineLifecycleActuator.Ports((manifest, capacities, members, guard) -> guard.run(),
                    manifest -> {}, (pass, manifest) -> { throw new AssertionError("unexpected clearing rebuild"); },
                    (fence, claim) -> ExecutionPlanRecorder.NONE);
            var submissions = new ScopedEngineLifecycleActuator.Submissions();
            var views = mock(PipelineStoreViews.class);
            when(views.pipelineStates(eq(SCOPE))).thenReturn(state);
            when(views.pipelineStates(eq(SCOPE), any(), any())).thenAnswer(call -> stateView(call.getArgument(1), call.getArgument(2)));
            when(views.pipelineObservations(eq(SCOPE), any(), any())).thenAnswer(call -> observationView(call.getArgument(1), call.getArgument(2)));
            var intents = mock(PipelineIntentStore.class);
            when(intents.pipelineIds()).thenReturn(List.of(ID));
            when(intents.loadForActuation(ID)).thenAnswer(call -> Optional.of(intent));
            when(intents.readForActuation(ID)).thenAnswer(call -> Optional.of(new PipelineIntentStore.ActuationReading(intent, java.util.OptionalLong.empty())));
            var store = new InMemoryStorePort(artifacts);
            var readings = new RunBoundCaptureReadings(root,
                    pipeline -> engine.captureRun(pipeline).map(run -> (ExecutionFence) run.operatorManifest().executionIdentity()),
                    engine::capturedRows);
            var publisher = new RuntimeConvergenceConfiguration().observationPublisher(store, engine, readings);
            var sampling = new NativeObservationSampling(mock(MongoConfigurationDomain.class), mock(MongoCacheDomain.class), root,
                    pipeline -> engine.captureRun(pipeline).map(run -> (ExecutionFence) run.operatorManifest().executionIdentity()),
                    pipeline -> null, "cache", CLOCK);
            var converger = new PipelineConverger(desired, state, mock(LifecycleActuator.class), CLOCK);
            var driver = ConvergenceDriver.scoped(converger, publisher, null, MetricsExport.none(), () -> true, ownership,
                    new ConvergenceDriver.ScopedPorts(intents, views, () -> {},
                            pass -> new ScopedEngineLifecycleActuator(pass, engine, dags, root, ownership, "cache",
                                    readiness, CLOCK, ports, submissions), pass -> RebuildAdmission.never(), ids -> {},
                            PipelineAlertFeed.none(), null, null, sampling));
            return new Process(engine, driver);
        }

        private PipelineCaptureCoordinator capture(ExecutionFence execution) {
            var bound = mock(PipelineCaptureCoordinator.class);
            when(bound.loadDelivered(ID)).thenAnswer(call -> loaded);
            when(bound.snapshotProgress(ID)).thenReturn(SnapshotReading.NONE);
            when(bound.runSnapshotProgress(ID)).thenReturn(SnapshotReading.NONE);
            when(bound.captureBindings()).thenReturn(new PipelineCaptureBindings(execution, Map.of(), false));
            doAnswer(call -> {
                if (waitForRing) throw new RingNotOpenYet(new CaptureId("capture"));
                return null;
            }).when(bound).startCapture(eq(ID), any(ArtifactStore.class), any(SourceModelSnapshot.class));
            return bound;
        }

        private StateStore stateView(WorkloadClaimFence original, Runnable active) {
            return new StateStore() {
                public Optional<CheckpointDoc> read(String id) { return state.read(id); }
                public CasOutcome compareAndSwap(String id, long epoch, String json, Instant at) {
                    active.run(); assertThat(original).isEqualTo(claim());
                    return state.compareAndSwap(id, epoch, json, at);
                }
                public void create(String id, String json, Instant at) { throw new AssertionError("unexpected create"); }
                public void delete(String id) { throw new AssertionError("unexpected delete"); }
            };
        }

        private ObservationStore observationView(WorkloadClaimFence original, Runnable active) {
            return new ObservationStore() {
                public Optional<Observation> read(String id) {
                    observationReads.add(currentKey());
                    return observation();
                }
                public void save(Observation value) { throw new AssertionError("missing epoch"); }
                public void save(Observation value, long epoch) {
                    active.run(); assertThat(original).isEqualTo(claim());
                    assertThat(epoch).isEqualTo(checkpoint().epoch());
                    observations.put(new ObservationKey(epoch, original), value);
                }
                public void delete(String id) { throw new AssertionError("unexpected delete"); }
            };
        }
    }

    private static DAG dag(int records, Instant counterStart) {
        var dag = new DAG();
        var source = dag.newVertex("source", ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                () -> new AbstractProcessor() {
                    private int emitted;
                    public boolean tryProcess() {
                        while (emitted < records && tryEmit(emitted)) emitted++;
                        return false;
                    }
                    public boolean complete() { return tryProcess(); }
                })));
        var sink = dag.newVertex("sink", ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                () -> new AbstractProcessor() {
                    private long count;
                    protected boolean tryProcess(int ordinal, Object item) {
                        Metrics.metric("outCountingSince").set(counterStart.toEpochMilli());
                        Metrics.metric("recordsOut.insert.orders").set(++count);
                        Metrics.metric("outEventTime.orders").set(counterStart.toEpochMilli());
                        // Take one zero-duration sample per pair of delivered records.
                        long samples = count / 2;
                        Metrics.metric("outDeliveryCount.orders").set(samples);
                        Metrics.metric("outDeliverySumMillis.orders").set(0);
                        for (int bucket = 0; bucket < HistogramBounds.RECORD_DELIVERY_DURATION.buckets(); bucket++) {
                            Metrics.metric("outDeliveryBucket." + bucket + ".orders").set(bucket == 0 ? samples : 0);
                        }
                        return true;
                    }
                })));
        return dag.edge(Edge.between(source, sink));
    }
}
