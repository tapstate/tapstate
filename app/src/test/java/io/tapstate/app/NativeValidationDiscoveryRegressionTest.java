package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.probe.ValidationOperation;
import io.tapstate.spi.store.ValidationTaskStore;
import io.tapstate.spi.store.ValidationReportAdmission;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeValidationDiscoveryRegressionTest {
    @Test void aRegistryFailureCannotCertifyTheUnchangedRevision() throws Exception {
        var ids = new TreeMap<String, ValidationTaskStore.Snapshot>();
        ids.put("first", task("first", true));
        var starts = new AtomicInteger();
        try (var scheduler = scheduler(ids, () -> {
            if (starts.getAndIncrement() == 0) throw new IllegalStateException("transient registry setup");
        })) {
            assertThatThrownBy(() -> scan(scheduler)).isInstanceOf(IllegalStateException.class);
            scan(scheduler);
            scan(scheduler);
            assertThat(field(scheduler, "completedRevision")).isNull();
            for (int i = 0; i < 5; i++) scan(scheduler);
            assertThat(entryCount(scheduler)).isOne();
        }
    }

    @Test void aDeferredScheduleMustUseTheSlotFreedInTheSameRevision() throws Exception {
        var ids = new TreeMap<String, ValidationTaskStore.Snapshot>();
        for (int i = 0; i < 128; i++) {
            String id = "task-%03d".formatted(i);
            ids.put(id, task(id, true));
        }
        try (var scheduler = scheduler(ids, () -> {})) {
            for (int i = 0; i < 130; i++) scan(scheduler);
            assertThat(entryCount(scheduler)).isEqualTo(128);
            assertThat(field(scheduler, "completedRevision")).isEqualTo(1L);
            clearInvocations(configOf(scheduler));
            scan(scheduler);
            verify(configOf(scheduler), never()).pollingValidationTasks(any());
            var config = (MongoConfigurationDomain) field(scheduler, "configuration");
            when(config.configurationRevision()).thenReturn(2L);
            ids.put("first", task("first", true));
            ids.put("task-127", task("task-127", false));
            for (int i = 0; i < 262; i++) scan(scheduler);

            assertThat(entryCount(scheduler)).isEqualTo(128);
        }
    }

    private static NativeValidationScheduler scheduler(TreeMap<String, ValidationTaskStore.Snapshot> ids, Runnable beforeStart) {
        var config = mock(MongoConfigurationDomain.class);
        var tasks = mock(ValidationTaskStore.class);
        when(config.configurationRevision()).thenReturn(1L);
        when(config.pollingValidationTasks(any())).thenReturn(tasks);
        when(tasks.pipelines(any(), eq(1))).thenAnswer(call -> call.getArgument(0) == null ? List.of("flow") : List.of());
        when(tasks.taskIds(eq("flow"), any(), eq(1))).thenAnswer(call -> {
            String after = call.getArgument(1);
            var next = after == null ? ids.firstKey() : ids.higherKey(after);
            return next == null ? List.of() : List.of(next);
        });
        when(tasks.read(eq("flow"), any())).thenAnswer(call -> Optional.ofNullable(ids.get(call.getArgument(1))));
        var resources = mock(ValidationMemberResources.Member.class);
        var reserved = mock(ValidationMemberResources.Unbound.class);
        when(resources.reserve(any(), eq(0))).thenReturn(reserved);
        var coordinator = mock(NativeValidationStartCoordinator.class);
        when(coordinator.context()).thenReturn(new ValidationOperation.Context("cluster", "node", "boot", UUID.randomUUID(), UUID.randomUUID()));
        Supplier<NativeValidationStartCoordinator> coordinators = () -> {
            beforeStart.run();
            return coordinator;
        };
        return new NativeValidationScheduler(config, resources, mock(ValidationMemberResources.class), coordinators,
                new MemoryValidationSchedules(), Clock.systemUTC(), new ValidationScheduleCalculator(), () -> {});
    }

    private static ValidationTaskStore.Snapshot task(String id, boolean enabled) {
        var saved = mock(ValidationTaskStore.Snapshot.class);
        when(saved.task()).thenReturn(new ValidationReportAdmission.Task("cluster", "flow", id, UUID.randomUUID()));
        when(saved.revision()).thenReturn(1L);
        var schedule = new ValidationSchedule.Interval(1, ValidationSchedule.Unit.MINUTES, Instant.parse("2026-11-19T00:00:00Z"), "UTC");
        var settings = new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT, 1, schedule, enabled, 10),
                new ValidationTaskStore.Incremental(0, 10));
        when(saved.settings()).thenReturn(settings);
        when(saved.full()).thenReturn(new ValidationTaskStore.Mode(1, null, new ValidationTaskStore.ScheduledAdmission(1, null, null, null)));
        return saved;
    }

    private static void scan(NativeValidationScheduler scheduler) throws Exception {
        var method = NativeValidationScheduler.class.getDeclaredMethod("discover");
        method.setAccessible(true);
        try { method.invoke(scheduler); }
        catch (java.lang.reflect.InvocationTargetException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            throw failure;
        }
    }

    private static MongoConfigurationDomain configOf(NativeValidationScheduler scheduler) throws Exception {
        return (MongoConfigurationDomain) field(scheduler, "configuration");
    }

    private static Object field(NativeValidationScheduler scheduler, String name) throws Exception {
        var field = NativeValidationScheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(scheduler);
    }

    private static int entryCount(NativeValidationScheduler scheduler) throws Exception {
        return ((Map<?, ?>) field(scheduler, "entries")).size();
    }
}
