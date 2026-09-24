package io.tapstate.app;

/**
 * A capture coordinator that does nothing, for the lifecycle tests that exercise only the engine side of the
 * actuator over a store-free idle topology: they drive the Jet job lifecycle and do not want a capture plane.
 */
final class NoOpCaptureCoordinator implements PipelineCaptureCoordinator {

    @Override
    public void startCapture(String pipelineId) {
    }

    @Override
    public void stopCapture(String pipelineId, boolean purgeState) {
    }
}
