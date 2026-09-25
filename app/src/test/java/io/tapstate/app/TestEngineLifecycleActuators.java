package io.tapstate.app;

import io.tapstate.runtime.engine.Engine;

/** Test assembly with the same durable-generation requirement as production submission. */
final class TestEngineLifecycleActuators {

    private TestEngineLifecycleActuators() {
    }

    static EngineLifecycleActuator create(Engine engine, DagSource dagSource,
            PipelineCaptureCoordinator captureCoordinator, NestStateTeardown stateTeardown) {
        return create(engine, dagSource, captureCoordinator, stateTeardown,
                PipelineActuationOwnership.single("single", new InMemoryWorkloadClaimStore()));
    }

    static EngineLifecycleActuator create(Engine engine, DagSource dagSource,
            PipelineCaptureCoordinator captureCoordinator, NestStateTeardown stateTeardown,
            PipelineActuationOwnership ownership) {
        return new EngineLifecycleActuator(engine, dagSource, captureCoordinator, stateTeardown, ownership);
    }
}
