package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SinkBatchReading;
import io.tapstate.core.lifecycle.QueueReading;
import io.tapstate.core.lifecycle.StageWorkReading;
import io.tapstate.core.lifecycle.StageQueueReading;
import io.tapstate.core.lifecycle.StageRuntimeReading;
import io.tapstate.core.lifecycle.StageOutputReading;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The observation publisher built by the assembly factory projects the metric ports it binds. This pins the
 * factory wiring deterministically, off a seeded store and with no live Jet job: the per-table durable
 * sink-acked position the store holds reaches the observation, and recordCount stays absent (present-only)
 * when the engine reports no live job. The recordCount value off a real live job is witnessed separately in
 * {@link LifecycleVerbsOnRealChainE2ETest}; the real sink-ack advance the position rides is witnessed in
 * {@link CaptureToSinkAckFrontierTest}.
 */
class AssemblyObservationPublisherTest {

    private static final String PIPELINE = "orders-pipe";
    private static final String TABLE = "orders";
    private static final Instant T0 = Instant.parse("2026-07-19T00:00:00Z");

    @Test
    void bindsOneCompleteDeliveryReadingAndKeepsAnUnavailableReadingAbsent() {
        InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);
        Engine engine = metricEngine();
        DeliveryReading measured = new DeliveryReading(Map.of(TABLE, Map.of("i", 4L)),
                Map.of(TABLE, 40L), Map.of(TABLE, T0.toEpochMilli()), T0, Map.of());
        when(engine.deliveryReading(PIPELINE)).thenReturn(measured, DeliveryReading.NONE);
        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());

        publisher.publish(PIPELINE);

        Observation present = store.observations().read(PIPELINE).orElseThrow();
        assertThat(present.metrics()).containsEntry("records.out", 4L).containsEntry("bytes.out", 40L);
        assertThat(present.facts()).filteredOn(fact -> fact.name().equals("tapstate.pipeline.records"))
                .singleElement().satisfies(fact ->
                        assertThat(fact.points().getFirst().startTime()).isEqualTo(T0));
        org.mockito.Mockito.verify(engine).deliveryReading(PIPELINE);
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.never()).countingSince(PIPELINE);
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.never()).recordsDelivered(PIPELINE);
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.never()).bytesDelivered(PIPELINE);
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.never()).newestDeliveredEventTime(PIPELINE);
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.never()).deliveryDurations(PIPELINE);

        publisher.publish(PIPELINE);

        Observation quiet = store.observations().read(PIPELINE).orElseThrow();
        assertThat(quiet.metrics()).doesNotContainKeys("records.out", "bytes.out");
        assertThat(quiet.facts()).noneMatch(fact -> fact.name().equals("tapstate.pipeline.records"));
    }

    @Test
    void bindsCompleteActiveBusinessWorkWithoutRedatingTheCollectedSample() {
        InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);
        Engine engine = metricEngine();
        when(engine.stageRuntimeReading(PIPELINE)).thenReturn(new StageRuntimeReading(
                new StageWorkReading(Map.of("transform", 1L), T0), StageQueueReading.NONE));
        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());
        publisher.publish(PIPELINE);
        assertThat(store.observations().read(PIPELINE).orElseThrow().facts())
                .filteredOn(fact -> fact.name().equals("tapstate.pipeline.work.active"))
                .singleElement().satisfies(fact -> {
                    assertThat(fact.points().getFirst().value()).isEqualTo(1);
                    assertThat(fact.points().getFirst().observedAt()).isEqualTo(T0);
                });
        when(engine.stageRuntimeReading(PIPELINE)).thenReturn(StageRuntimeReading.NONE);
        publisher.publish(PIPELINE);
        assertThat(store.observations().read(PIPELINE).orElseThrow().facts())
                .noneMatch(fact -> fact.name().equals("tapstate.pipeline.work.active"));
    }

    @Test
    void bindsCompleteOutputRetryFactsAndRetainsOtherStageFactsWhenOutputBecomesUnavailable() {
        InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);
        Engine engine = metricEngine();
        StageWorkReading work = new StageWorkReading(Map.of("transform", 1L), T0);
        StageQueueReading queues = new StageQueueReading(Map.of("transform", new StageQueueReading.Sample(
                new QueueReading(2, 16, 4), T0)));
        var buckets = new java.util.ArrayList<>(java.util.Collections.nCopies(
                HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.buckets(), 0L));
        buckets.set(0, 1L);
        Instant since = T0.minusSeconds(30);
        StageOutputReading output = new StageOutputReading(Map.of("transform", new StageOutputReading.Sample(
                3, HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.value(1, 0.0001, buckets), since, T0)));
        when(engine.stageRuntimeReading(PIPELINE)).thenReturn(new StageRuntimeReading(work, queues, output));
        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());
        publisher.publish(PIPELINE);
        assertThat(store.observations().read(PIPELINE).orElseThrow().facts())
                .filteredOn(fact -> fact.name().startsWith("tapstate.pipeline.stage.output."))
                .hasSize(2).allSatisfy(fact -> {
                    assertThat(fact.points().getFirst().startTime()).isEqualTo(since);
                    assertThat(fact.points().getFirst().observedAt()).isEqualTo(T0);
                });
        org.mockito.Mockito.verify(engine).stageRuntimeReading(PIPELINE);
        when(engine.stageRuntimeReading(PIPELINE)).thenReturn(new StageRuntimeReading(work, queues));
        publisher.publish(PIPELINE);
        var facts = store.observations().read(PIPELINE).orElseThrow().facts();
        assertThat(facts).noneMatch(fact -> fact.name().startsWith("tapstate.pipeline.stage.output."));
        assertThat(facts).extracting(fact -> fact.name()).contains("tapstate.pipeline.work.active",
                "tapstate.pipeline.stage.queue.depth", "tapstate.pipeline.stage.queue.high_water");
    }

    @Test
    void projectsThePerTableSinkAckedPositionAndKeepsRecordCountAbsentWithNoLiveJob() {
        SourceResource source = new SourceResource("orders_src", null, "fake", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal(TABLE)), null, null);
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source);
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec("orders_src", true)), null, null, null, null, null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chain = SourceCaptureResolution.of(source).chainId().value();
        store.meta().create(chain, null);
        store.meta().advanceSinkAcked(chain, PIPELINE, new ChainPosition(new SourceOrder(1, 7), "w7"));
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);

        // An engine whose member reports no live job, so recordCount resolves absent.
        HazelcastInstance member = mock(HazelcastInstance.class);
        JetService jet = mock(JetService.class);
        when(member.getJet()).thenReturn(jet);
        com.hazelcast.cluster.Cluster cluster = mock(com.hazelcast.cluster.Cluster.class);
        when(member.getCluster()).thenReturn(cluster);
        when(cluster.getMembers()).thenReturn(java.util.Set.of());
        when(jet.getJob(anyString())).thenReturn(null);
        ObservationPublisher publisher =
                new RuntimeConvergenceConfiguration()
                        .observationPublisher(store, new Engine(member), new NoOpCaptureCoordinator());

        publisher.publish(PIPELINE);

        Observation observed = store.observations().read(PIPELINE).orElseThrow();
        assertThat(observed.positions())
                .as("the factory binds the position port and the publisher projects it, keyed by table")
                .containsExactly(entry(TABLE, "w7"));
        assertThat(observed.metrics())
                .as("recordCount is absent with no live job (present-only), and so is any failure count")
                .doesNotContainKey("errorCount")
                .doesNotContainKey("recordCount");
    }

    @Test
    void projectsTheFrontierReadingsTheEnginePortReports() {
        InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);

        // The engine stands in for a live run here; that a real run publishes these is witnessed against a
        // real nest job. What is pinned is that the factory binds the port at all - a publisher built
        // without it goes on projecting every other statistic, and the one reading that tells a stalled
        // frontier's two causes apart is simply never there to be missed.
        Engine engine = metricEngine();
        when(engine.frontierGaps(PIPELINE)).thenReturn(Map.of(TABLE, 480L));
        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());

        publisher.publish(PIPELINE);

        assertThat(store.observations().read(PIPELINE).orElseThrow().metrics())
                .as("the factory binds the frontier port and the publisher names each reading by its chain")
                .containsEntry("frontierGap." + TABLE, 480L);
    }

    @Test
    void projectsOnlyMeasuredSinkBatchesFromTheEnginePort() {
        InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);
        Engine engine = metricEngine();
        when(engine.sinkBatchReading(PIPELINE)).thenReturn(
                new SinkBatchReading(1, 2, 2, 1, 1, null, null, null, T0));

        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());
        publisher.publish(PIPELINE);

        Observation observed = store.observations().read(PIPELINE).orElseThrow();
        assertThat(observed.facts()).anySatisfy(fact -> {
            assertThat(fact.name()).isEqualTo("tapstate.pipeline.sink.batch.pending");
            assertThat(fact.points().getFirst().value()).isEqualTo(1L);
        });
    }

    @Test
    void projectsOnlyMeasuredInputQueuePressureFromTheEnginePort() {
        InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);
        Engine engine = metricEngine();
        when(engine.queueReading(PIPELINE)).thenReturn(Optional.of(new QueueReading(64, 64, 64)));

        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());
        publisher.publish(PIPELINE);

        Observation observed = store.observations().read(PIPELINE).orElseThrow();
        assertThat(observed.facts().stream().filter(fact -> fact.name().startsWith("tapstate.pipeline.queue.")))
                .extracting(fact -> fact.name()).containsExactlyInAnyOrder(
                        "tapstate.pipeline.queue.depth", "tapstate.pipeline.queue.capacity",
                        "tapstate.pipeline.queue.high_water");
        assertThat(observed.facts().stream().filter(fact -> fact.name().startsWith("tapstate.pipeline.queue.")))
                .allSatisfy(fact -> {
                    assertThat(fact.unit()).isEqualTo("{item}");
                    assertThat(fact.points()).singleElement().satisfies(point -> {
                        assertThat(point.attributes()).isEqualTo(Map.of("tapstate.pipeline.id", PIPELINE));
                        assertThat(point.value()).isEqualTo(64L);
                    });
                });
    }
    private static Engine metricEngine() {
        Engine engine = mock(Engine.class);
        // Native identity sampling is an explicit boundary double; the assembly wiring remains real.
        when(engine.openObservationMetrics(anyString())).thenAnswer(ignored -> {
            var session = mock(Engine.ObservationMetricsSession.class);
            when(session.current()).thenReturn(true);
            return session;
        });
        return engine;
    }
}
