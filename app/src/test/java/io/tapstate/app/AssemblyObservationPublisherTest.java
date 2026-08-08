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
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
    void projectsThePerTableSinkAckedPositionAndKeepsRecordCountAbsentWithNoLiveJob() {
        SourceResource source = new SourceResource("orders_src", null, "fake", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal(TABLE)), null, null, null);
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source);
        artifacts.save(new PipelineResource(PIPELINE, null, List.of("orders_src"), null, null, null, null, null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chain = SourceCaptureResolution.of(source).chainId().value();
        store.meta().create(chain, null);
        store.meta().advanceSinkAcked(chain, PIPELINE, new ChainPosition(new SourceOrder(1, 7), "w7"));
        store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING), T0);

        // An engine whose member reports no live job, so recordCount resolves absent.
        HazelcastInstance member = mock(HazelcastInstance.class);
        JetService jet = mock(JetService.class);
        when(member.getJet()).thenReturn(jet);
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
                .as("recordCount is absent with no live job (present-only); errorCount stays present at 0")
                .containsEntry("errorCount", 0L)
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
        Engine engine = mock(Engine.class);
        when(engine.frontierGaps(PIPELINE)).thenReturn(Map.of(TABLE, 480L));
        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, new NoOpCaptureCoordinator());

        publisher.publish(PIPELINE);

        assertThat(store.observations().read(PIPELINE).orElseThrow().metrics())
                .as("the factory binds the frontier port and the publisher names each reading by its chain")
                .containsEntry("frontierGap." + TABLE, 480L);
    }
}
