package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.validation.ValidationSchedule;
import io.tapstate.spi.store.ValidationTaskStore;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.*;

class NativeValidationEmptyPollingTest {
    @Test void aFinishingScanCannotHaveItsNewIdleDeadlineOverwrittenByAnAdmittingTick() {
        var configuration = mock(MongoConfigurationDomain.class);
        var member = mock(ValidationMemberResources.Member.class);
        var work = mock(ValidationMemberResources.Control.class);
        when(member.admitMaintenanceControl()).thenReturn(work);
        var admission = mock(java.util.concurrent.atomic.AtomicBoolean.class);
        try (var scheduler = new NativeValidationScheduler(configuration, member, mock(ValidationMemberResources.class),
                () -> mock(NativeValidationStartCoordinator.class), mock(MemoryValidationSchedules.class),
                Clock.systemUTC(), mock(ValidationSchedule.Calculator.class), () -> {}, () -> 1L)) {
            ReflectionTestUtils.setField(scheduler, "discovering", admission);
            when(admission.compareAndSet(false, true)).thenAnswer(call -> {
                // The previous worker publishes the new deadline and releases admission
                // after this timer has read the old deadline, just before its CAS wins.
                ReflectionTestUtils.setField(scheduler, "nextDiscovery", TimeUnit.SECONDS.toNanos(1));
                return true;
            });
            ReflectionTestUtils.invokeMethod(scheduler, "dispatch");
            verify(member, never()).admitMaintenanceControl();
            verify(admission).set(false);
            verifyNoInteractions(configuration);
        }
    }

    @Test void emptyInventoryPollsOncePerSecondAndThenDiscoversChangedConfiguration() {
        var configuration = mock(MongoConfigurationDomain.class);
        var tasks = mock(ValidationTaskStore.class);
        var member = mock(ValidationMemberResources.Member.class);
        var work = mock(ValidationMemberResources.Control.class);
        when(member.admitMaintenanceControl()).thenReturn(work);
        doAnswer(call -> { ((Runnable) call.getArgument(0)).run(); return null; }).when(work).execute(any());
        when(configuration.configurationRevision()).thenReturn(7L);
        when(configuration.pollingValidationTasks(any())).thenReturn(tasks);
        when(tasks.pipelines(null, 1)).thenReturn(List.of());
        var now = new AtomicLong(1);
        try (var scheduler = new NativeValidationScheduler(configuration, member, mock(ValidationMemberResources.class),
                () -> mock(NativeValidationStartCoordinator.class), mock(MemoryValidationSchedules.class),
                Clock.systemUTC(), mock(ValidationSchedule.Calculator.class), () -> {}, now::get)) {
            for (int tick = 0; tick < 10; tick++) {
                ReflectionTestUtils.invokeMethod(scheduler, "dispatch");
                now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
            }
            verify(configuration, times(2)).configurationRevision();
            verify(tasks).pipelines(null, 1);
            when(configuration.configurationRevision()).thenReturn(8L);
            ReflectionTestUtils.invokeMethod(scheduler, "dispatch");
            verify(tasks, times(2)).pipelines(null, 1);
            verify(configuration, times(4)).configurationRevision();
        }
    }
}
