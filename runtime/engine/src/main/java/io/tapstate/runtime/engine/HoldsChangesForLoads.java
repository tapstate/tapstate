package io.tapstate.runtime.engine;

/**
 * A source processor that can hold its table's changes back until the loads they could overtake have landed.
 */
public interface HoldsChangesForLoads {

    /**
     * Holds every change this source reads - and every bound past its own table's load - until each load
     * {@code gate} awaits has landed. Given before the processor is initialised, once per processor.
     */
    void holdChangesUntil(LoadGate gate);
}
