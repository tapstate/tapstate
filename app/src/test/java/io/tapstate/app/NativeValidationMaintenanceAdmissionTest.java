package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationSchedule;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ValidationTaskStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NativeValidationMaintenanceAdmissionTest {
    @Test void schedulerDiscoveryLeavesTheFinalTwoFramesForForegroundAndRetriesAfterRelease() throws Exception {
        var release = new CountDownLatch(1);
        var reads = new AtomicInteger();
        var configuration = mock(MongoConfigurationDomain.class);
        var tasks = mock(ValidationTaskStore.class);
        when(tasks.pipelines(null, 1)).thenReturn(List.of());
        when(configuration.validationTasks(any())).thenAnswer(call -> {
            reads.incrementAndGet();
            assertThat(release.await(2, TimeUnit.SECONDS)).isTrue();
            return tasks;
        });
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 4, () -> {});
            try (var scheduler = new NativeValidationScheduler(configuration, member, process,
                    () -> mock(NativeValidationStartCoordinator.class), mock(MemoryValidationSchedules.class),
                    Clock.systemUTC(), mock(ValidationSchedule.Calculator.class), () -> {})) {
                try (var retainedRun = member.admitMetadata(); var retainedQuota = member.admitMetadata()) {
                    try {
                        ReflectionTestUtils.invokeMethod(scheduler, "dispatch");
                        assertThat(member.snapshot().metadataFrames()).isEqualTo(2);
                        assertThat(reads).hasValue(0);
                        try (var read = member.admitMetadata(); var probe = member.admitMetadata()) {
                            assertThat(member.snapshot().metadataFrames()).isEqualTo(4);
                        }
                    } finally {
                        release.countDown();
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2))
                                .until(() -> member.snapshot().metadataFrames() == 2);
                    }
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                    ReflectionTestUtils.invokeMethod(scheduler, "dispatch");
                    assertThat(reads.get()).isGreaterThan(0);
                });
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2))
                        .until(() -> member.snapshot().metadataFrames() == 0);
                verify(tasks, atLeastOnce()).pipelines(null, 1);
            }
        } finally {
            release.countDown();
        }
    }

    @Test void historyDefersBeforeReadingOrAdvancingItsCursorAndAllowsTheNextScanAfterRelease() throws Exception {
        var tickEntered = new CountDownLatch(1);
        var releaseTick = new CountDownLatch(1);
        var configuration = mock(MongoConfigurationDomain.class);
        var tasks = mock(ValidationTaskStore.class);
        when(configuration.validationTasks(any())).thenReturn(tasks);
        when(tasks.pipelines(null, 1)).thenReturn(List.of());
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 4, () -> {});
            var heldTick = process.controlTicks(() -> {
                tickEntered.countDown();
                try { assertThat(releaseTick.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
            });
            try {
                assertThat(tickEntered.await(2, TimeUnit.SECONDS)).isTrue();
                try (var history = new NativeValidationReportHistory(configuration, mock(MongoCacheDomain.class),
                        new CacheBusinessScope("cluster", 1, "cache"), () -> null, () -> {}, member, process,
                        mock(NativeValidationOwnerReservations.class), Clock.systemUTC())) {
                    try (var retainedRun = member.admitMetadata(); var retainedQuota = member.admitMetadata()) {
                        assertThatThrownBy(history::scan).isInstanceOfSatisfying(TapstateException.class, refused -> {
                            assertThat(refused.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED);
                            assertThat(refused.args()).containsEntry("limit", "validation.metadataFrames");
                        });
                        verifyNoInteractions(tasks);
                        verify(configuration, never()).validationTasks(any());
                        try (var read = member.admitMetadata(); var probe = member.admitMetadata()) {
                            assertThat(member.snapshot().metadataFrames()).isEqualTo(4);
                        }
                    }
                    history.scan();
                    verify(tasks).pipelines(null, 1);
                    assertThat(member.snapshot().metadataFrames()).isZero();
                }
            } finally {
                releaseTick.countDown();
                heldTick.cancel(false);
            }
        } finally {
            releaseTick.countDown();
        }
    }
}
