package io.tapstate.app;

import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.FrontierStallWatch;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.NestColdLayerWatch;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.StorePort;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

/**
 * Wires the runtime convergence loop into the assembly root — the first runtime-ring module the server
 * runs. The framework-free {@link PipelineConverger} (runtime ring, no Spring) is constructed here over
 * the driver-free store port, and a scheduled {@link ConvergenceDriver} (this assembly layer, where the
 * scheduler lives — the framework is banned in the runtime ring) ticks it so actual state tracks desired
 * intent. Control writes desired intent; this side writes actual state; the two meet only at the store.
 *
 * <p>Gated, like the store it reads and writes, on {@code tapstate.store.mongo.enabled}: a run with no store
 * brings up neither the store nor the convergence loop.
 */
@Configuration
@ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
@EnableScheduling
class RuntimeConvergenceConfiguration {

    @Bean
    PipelineConverger pipelineConverger(StorePort storePort, LifecycleActuator lifecycleActuator, Clock clock) {
        return new PipelineConverger(storePort.desired(), storePort.state(), lifecycleActuator, clock);
    }

    @Bean
    ObservationPublisher observationPublisher(
            StorePort storePort, Engine engine, PipelineCaptureCoordinator captureCoordinator) {
        // The publisher's four run-statistic sources: recordCount and the per-chain frontier readings ride
        // from the engine's live Jet job, the per-table sink-acked positions from the store, and the
        // per-table initial load from the capture coordinator. All are ports, so the scheduler stays clear
        // of the engine, the store and the capture side that back them; a stopped pipeline, an unacked
        // table, an unloaded one or a chain with no reading reports absence, not zero.
        // The readings also go to a watch that reports a namespace which has stopped being served from
        // memory. Nothing else about such a pipeline moves -- per-key state fills no edge queue, so the
        // queues, the lag and the throughput all go on reading normal -- so without this it is not
        // observable at all except by someone who already suspected it and went looking at two ratios.
        return new ObservationPublisher(storePort.state(), storePort.observations(),
                engine::recordCount, new StoreBackedSinkPositions(storePort),
                captureCoordinator::snapshotProgress, engine::frontierGaps, engine::nestStateReadings,
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, new LoggingNestColdLayerAlert()),
                engine::frontierStalls,
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, new LoggingFrontierStallAlert()),
                // The one number about a nest that nothing else can stand in for: a pipeline discarding
                // every row it reads and one discarding none produce the same documents and the same
                // statistics everywhere else, because the rows counted here were never going to appear in
                // any document. Without it on this face, "is anything being thrown away" is answerable
                // only by reading logs on whichever member happened to run the vertex.
                engine::nestDeadLetters,
                // What a large rebuild is doing while it does it. A single dimension row edited can owe a
                // million rows of writing, and for as long as that takes the target holds half the old
                // value and half the new one while every other reading on this face says healthy: the job
                // runs, the queues drain, the error count is zero. Without these two an operator cannot
                // tell that from a pipeline that has finished, and so cannot tell whether to wait.
                engine::joinRecomputeDone, engine::joinRecomputeExpected);
    }

    @Bean
    ConvergenceDriver convergenceDriver(
            PipelineConverger pipelineConverger, StorePort storePort, ObservationPublisher observationPublisher) {
        return new ConvergenceDriver(pipelineConverger, storePort.desired(), observationPublisher);
    }
}
