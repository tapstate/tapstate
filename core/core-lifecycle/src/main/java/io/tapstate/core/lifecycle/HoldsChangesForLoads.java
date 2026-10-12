package io.tapstate.core.lifecycle;

import java.util.List;

/**
 * A source processor that can hold its table's changes back until the loads they could overtake have landed.
 */
public interface HoldsChangesForLoads {

    /**
     * Holds every change this source reads - and every bound past its own table's load - until {@code landings}
     * says none of {@code awaited} is still landing. Given before the processor is initialised, once per
     * processor.
     */
    void holdChangesUntil(List<AwaitedLoad> awaited, LoadLandings landings);
}
