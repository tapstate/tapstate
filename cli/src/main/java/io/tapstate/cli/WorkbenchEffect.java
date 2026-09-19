package io.tapstate.cli;

/** A typed operation emitted by reduction and interpreted by the render-thread runtime. */
sealed interface WorkbenchEffect permits WorkbenchEffect.Render {

    /** Requests a frame after a state transition. */
    enum Render implements WorkbenchEffect {
        INSTANCE
    }
}
