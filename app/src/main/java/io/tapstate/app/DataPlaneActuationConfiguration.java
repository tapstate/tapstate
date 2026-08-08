package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StorePort;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

/**
 * Wires the data-plane actuation binding into the assembly root: the Jet {@link Engine} over the embedded
 * member, the topology source, the source-side capture plane (the mining-chain coordinator, the PDK capture
 * port, and the run unit that assembles a capture), and the {@link LifecycleActuator} that joins the converge
 * loop to both the engine and the capture. Split from the convergence loop's wiring so the loop can be
 * brought up in isolation over a stand-in actuator, and gated on the same store switch as the convergence it
 * serves - a run with no store drives no pipeline, so it needs neither the loop, the engine binding, nor the
 * capture plane. The topology source and the capture plane read pipelines and sources from the same store, so
 * they are gated with the rest of the binding.
 */
@Configuration
@ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
class DataPlaneActuationConfiguration {

    @Bean
    Engine engine(HazelcastInstance hazelcastMember, @Nullable KeyedStateStore nestStateStore) {
        return new Engine(hazelcastMember, nestStateStore);
    }

    @Bean
    DagSource dagSource(StorePort storePort, NestSettings nestSettings) {
        return new StoreBackedDagSource(storePort, nestSettings);
    }

    @Bean
    SrsCoordinator srsCoordinator(SrsMetaStore srsMetaStore) {
        return new SrsCoordinator(srsMetaStore);
    }

    @Bean
    CapturePort capturePort(ConnectorProvisioner connectorProvisioner) {
        return new PdkCapturePort(connectorProvisioner);
    }

    @Bean
    SnapshotBuffer snapshotBuffer() {
        // The one member-local buffer that carries snapshot rows from the capture coordinator (the writer) to
        // the source vertices (the readers). It is bound into the member user context so a source resolves it
        // member-side, and injected into the coordinator so the snapshot pass-through fills it -- the same
        // instance on both sides, so what the coordinator writes is exactly what a source drains.
        return new SnapshotBuffer();
    }

    @Bean
    CaptureRunUnit captureRunUnit(CapturePort capturePort, SrsCoordinator srsCoordinator,
            SrsMetaStore srsMetaStore, HazelcastInstance hazelcastMember) {
        return new CaptureRunUnit(capturePort, srsCoordinator, srsMetaStore, hazelcastMember);
    }

    @Bean
    PipelineCaptureCoordinator pipelineCaptureCoordinator(
            StorePort storePort, CaptureRunUnit captureRunUnit, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer) {
        return new StoreBackedPipelineCaptureCoordinator(
                storePort, captureRunUnit::start, srsCoordinator, snapshotBuffer);
    }

    @Bean
    NestStateTeardown nestStateTeardown(HazelcastInstance hazelcastMember, StorePort storePort) {
        return new NestStateTeardown(hazelcastMember, storePort.keyedState());
    }

    @Bean
    LifecycleActuator lifecycleActuator(Engine engine, DagSource dagSource,
            PipelineCaptureCoordinator pipelineCaptureCoordinator, NestStateTeardown nestStateTeardown) {
        return new EngineLifecycleActuator(engine, dagSource, pipelineCaptureCoordinator, nestStateTeardown);
    }
}
