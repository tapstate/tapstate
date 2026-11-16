package io.tapstate.app;

import io.tapstate.spi.store.AlertStore;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class AlertNotificationPlannerReadBudgetTest {
    @Test void anIdleTickReadsOneFreshSnapshotAndDoesNotRetainItAcrossTicks() {
        var store = mock(AlertStore.class);
        when(store.planning()).thenReturn(new AlertStore.Planning(new AlertStore.PlanVersion(1, 1),
                mock(AlertStore.Host.class), List.of(), List.of(), List.of(), Map.of(), Map.of(), Map.of(), Set.of()));
        var planner = new AlertNotificationPlanner(store, Clock.systemUTC(), "planner", null, () -> {});
        for (int tick = 1; tick <= 3; tick++) {
            planner.tick();
            verify(store, times(tick)).planning();
        }
        verify(store, never()).commit(any());
    }
}
