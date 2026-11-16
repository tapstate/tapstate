package io.tapstate.app;

import io.tapstate.spi.store.AlertStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import io.tapstate.core.alerting.AlertOwnership;
import io.tapstate.core.alerting.AlertSchedule;
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
    @Test void stableRootsShareTheTickSnapshotAndTheNextTickStillReadsFresh() {
        var now = Instant.parse("2026-01-01T00:00:00Z");
        var store = mock(AlertStore.class);
        var roots = new java.util.ArrayList<AlertStore.Stored<AlertSchedule.Record>>();
        for (int i = 0; i < 35; i++) {
            var row = mock(AlertSchedule.Record.class);
            when(row.key()).thenReturn(mock(AlertSchedule.Key.class));
            when(row.owner()).thenReturn(new AlertOwnership(1, "other-worker", now.plusSeconds(30)));
            roots.add(new AlertStore.Stored<>(1, row));
        }
        when(store.planning()).thenReturn(new AlertStore.Planning(new AlertStore.PlanVersion(1, 1),
                mock(AlertStore.Host.class), List.of(), List.of(), roots, Map.of(), Map.of(), Map.of(), Set.of()));
        var planner = new AlertNotificationPlanner(store, Clock.fixed(now, ZoneOffset.UTC), "planner", null, () -> {});
        for (int tick = 1; tick <= 2; tick++) {
            planner.tick();
            verify(store, times(tick)).planning();
        }
        verify(store, never()).commit(any());
    }
}
