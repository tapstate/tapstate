package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.WorkloadClaimFence;

/** Rechecks an already processed local Stop; a successful read never grants execution authority. */
@FunctionalInterface
interface StoppedTerminalRecheck {
    enum Tenure { HELD, ENDED }
    boolean unchanged(PipelineIntentSnapshot processed, CheckpointDoc checkpoint,
            WorkloadClaimFence original, Tenure tenure);
}
