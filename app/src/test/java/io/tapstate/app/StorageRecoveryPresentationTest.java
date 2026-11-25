package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageRecoveryCommands;
import java.time.Clock;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class StorageRecoveryPresentationTest {
    @ParameterizedTest
    @EnumSource(value = StorageRecoveryCommands.Extent.class, names = {"PIPELINE", "SHARED_SRS"})
    void anUnpublishedBusinessCannotMakeStatusWaitForItsStartup(StorageRecoveryCommands.Extent extent) throws Exception {
        var root = root();
        var service = root.recoveryService(Clock.systemUTC());
        var selected = extent == StorageRecoveryCommands.Extent.PIPELINE
                ? new StorageRecoveryCommands.Selection(extent, Set.of("pipeline"))
                : new StorageRecoveryCommands.Selection(extent, Set.of(), "canonical", "physical");
        var manifest = mock(StorageRecoveryCommands.Manifest.class);
        when(manifest.selection()).thenReturn(selected);
        var plan = mock(StorageRecoveryCommands.Plan.class);
        when(plan.manifest()).thenReturn(manifest);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        var holding = executor.submit(() -> {
            synchronized (root) {
                entered.countDown();
                try { if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("startup gate was not released"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
        });
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            executor.submit(() -> {
                if (extent == StorageRecoveryCommands.Extent.PIPELINE) {
                    var result = service.presentation(plan);
                    assertThat(result.current()).isFalse();
                    assertThat(result.targets()).isEmpty();
                    assertThat(result.executionErrorCode()).isNull();
                } else {
                    assertThatThrownBy(() -> service.presentation(plan)).isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
                }
            }).get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            holding.get(2, TimeUnit.SECONDS);
            executor.shutdown();
            assertThat(executor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test void aClosedOwnerCannotPublishAnUnavailablePreview() {
        var root = root();
        var service = root.recoveryService(Clock.systemUTC());
        ReflectionTestUtils.setField(root, "closeRequested", true);
        assertThatThrownBy(() -> service.presentation(mock(StorageRecoveryCommands.Plan.class)))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
    }

    @Test void aCloseDuringThePublicationReadCannotReturnAPreview() {
        var root = root();
        var service = root.recoveryService(Clock.systemUTC());
        var manifest = mock(StorageRecoveryCommands.Manifest.class);
        when(manifest.selection()).thenReturn(new StorageRecoveryCommands.Selection(
                StorageRecoveryCommands.Extent.PIPELINE, Set.of("pipeline")));
        var plan = mock(StorageRecoveryCommands.Plan.class);
        when(plan.manifest()).thenReturn(manifest);
        when(root.runtime().businessForLiveness()).thenAnswer(ignored -> {
            ReflectionTestUtils.setField(root, "closeRequested", true);
            return Optional.empty();
        });
        assertThatThrownBy(() -> service.presentation(plan)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
    }

    private static StorageRoot root() {
        var root = mock(StorageRoot.class, CALLS_REAL_METHODS);
        var runtime = mock(StorageRuntime.class);
        ReflectionTestUtils.setField(root, "runtime", runtime);
        ReflectionTestUtils.setField(root, "installationActive", (Runnable) () -> {});
        when(runtime.requireConfiguration()).thenReturn(mock(MongoConfigurationDomain.class, RETURNS_DEEP_STUBS));
        when(runtime.businessForLiveness()).thenReturn(Optional.empty());
        return root;
    }
}
