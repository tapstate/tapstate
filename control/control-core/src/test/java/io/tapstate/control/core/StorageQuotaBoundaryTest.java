package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageConnectionProbe;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/** Verifies real service/wrapper/dispatcher composition without granting authority or performing replay. */
class StorageQuotaBoundaryTest {
    @Test void commitProbeQuotaSurvivesItsWrapperAndGenericRetryButNotSpecificReplacement() {
        var f = new StorageSetupServiceTest.Fixture();
        f.progress = new StorageInitializationProgress(f.time);
        var failureCode = new AtomicReference<TapstateErrorCode>();
        var continued = new AtomicInteger(); f.continuation = value -> continued.incrementAndGet();
        f.probe = new StorageConnectionProbe() {
            @Override public StorageBinding.Connection normalize(StorageBinding.Connection value) { return value; }
            @Override public void test(StorageBinding.Connection value) {
                if (value.database().equals("cache") && failureCode.get() != null)
                    throw new TapstateException(failureCode.get(), Map.of(), null);
            }
        };
        var service = f.service(); var stamps = f.tested(service, StorageSetupServiceTest.CANDIDATE);
        for (var code : new TapstateErrorCode[]{IoError.STORE_QUOTA_EXCEEDED, ControlError.STORAGE_UNAVAILABLE, IoError.STORE_UNAUTHORIZED}) {
            failureCode.set(code);
            assertThatThrownBy(() -> service.commit("owner", StorageSetupServiceTest.SCOPE, () -> {}, StorageSetupServiceTest.CANDIDATE, stamps))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_TEST_FAILED));
            String expected = code == IoError.STORE_UNAUTHORIZED ? ControlError.STORAGE_TEST_FAILED.code() : IoError.STORE_QUOTA_EXCEEDED.code();
            assertThat(f.progress.snapshot(StorageSetupServiceTest.SCOPE).orElseThrow().milestones().get(0).errorCode()).isEqualTo(expected);
            assertThat(f.binding.inserts).isZero(); assertThat(f.audit).isEmpty(); assertThat(continued).hasValue(0);
        }
        failureCode.set(null);
        service.commit("owner", StorageSetupServiceTest.SCOPE, () -> {}, StorageSetupServiceTest.CANDIDATE, stamps);
        assertThat(f.binding.inserts).isEqualTo(1); assertThat(continued).hasValue(1);
        assertThat(f.progress.quotaExceeded()).isFalse();
    }

    @Test void auditQuotaAndGenericRetryNeverExecuteBindingAndSpecificFailureReplacesQuota() {
        var f = new StorageSetupServiceTest.Fixture(); f.progress = new StorageInitializationProgress(f.time);
        var continued = new AtomicInteger(); f.continuation = value -> continued.incrementAndGet();
        var service = f.service(); var stamps = f.tested(service, StorageSetupServiceTest.CANDIDATE);
        for (var code : new TapstateErrorCode[]{IoError.STORE_QUOTA_EXCEEDED, IoError.STORE_UNAVAILABLE, IoError.STORE_UNAUTHORIZED}) {
            f.onAudit = () -> { throw new TapstateException(code, Map.of("detail", "private-value-sentinel"), new IllegalArgumentException("private-cause")); };
            assertThatThrownBy(() -> service.commit("owner", StorageSetupServiceTest.SCOPE, () -> {}, StorageSetupServiceTest.CANDIDATE, stamps))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.AUDIT_BLOCKED));
            String expected = code == IoError.STORE_UNAUTHORIZED ? ControlError.AUDIT_BLOCKED.code() : IoError.STORE_QUOTA_EXCEEDED.code();
            var progress = f.progress.snapshot(StorageSetupServiceTest.SCOPE).orElseThrow();
            assertThat(progress.milestones().get(0).errorCode()).isEqualTo(expected);
            assertThat(progress.toString()).doesNotContain("private-value-sentinel", "private-cause");
            assertThat(f.binding.inserts).isZero(); assertThat(f.audit).isEmpty(); assertThat(continued).hasValue(0);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void dispatcherPublishesOnlyCoarseQuotaForBindingAndAuditFailures(boolean audit) {
        var f = new StorageControlDispatcherTest.Fixture();
        Runnable refusal = () -> { throw new TapstateException(IoError.STORE_QUOTA_EXCEEDED,
                Map.of("detail", "private-value-sentinel"), new IllegalArgumentException("private-cause")); };
        if (audit) f.setup.onAudit = refusal; else f.setup.binding.beforeCommit = refusal;
        var service = f.setup.service();
        var failure = catchThrowable(() -> dispatcher(f, service).dispatch("storage.setup.commit", f.owner(), input(f, service)));
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
            assertThat(coded.code()).isEqualTo(ControlError.STORAGE_QUOTA_EXCEEDED);
            assertThat(coded.args()).isEmpty(); assertThat(coded.getCause()).isNull();
            assertThat(coded.toString()).doesNotContain("private-value-sentinel", "private-cause");
        });
        assertThat(f.setup.binding.inserts).isZero();
    }

    @Test void continuationQuotaCrossesDispatcherWithoutRepeatingTheCommittedBinding() {
        var f = new StorageControlDispatcherTest.Fixture();
        f.setup.continuation = value -> { throw new TapstateException(IoError.STORE_QUOTA_EXCEEDED, Map.of(), null); };
        var service = f.setup.service(); var dispatcher = dispatcher(f, service); var input = input(f, service); var owner = f.owner();
        assertThatThrownBy(() -> dispatcher.dispatch("storage.setup.commit", owner, input))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_QUOTA_EXCEEDED));
        assertThatThrownBy(() -> dispatcher.dispatch("storage.setup.commit", owner, input)).isInstanceOf(TapstateException.class);
        assertThat(f.setup.binding.inserts).isEqualTo(1);
    }

    private static StorageControlDispatcher dispatcher(StorageControlDispatcherTest.Fixture f, StorageSetupService service) {
        return new StorageControlDispatcher(ControlOperations.registry(), service, f.recovery, f.imports.service, (caller, active) -> f.snapshot);
    }
    private static Map<String,Object> input(StorageControlDispatcherTest.Fixture f, StorageSetupService service) {
        var stamps = new EnumMap<StorageSetupService.Layer,String>(f.setup.tested(service, StorageSetupServiceTest.CANDIDATE));
        var candidate = StorageSetupServiceTest.CANDIDATE;
        return Map.of("operationId", StorageSetupServiceTest.SCOPE.operationId(), "generation", 1,
                "candidate", Map.of("configuration", connection(candidate.configuration()), "cache", connection(candidate.cache()), "master", connection(candidate.master())),
                "testStamps", Map.of("CONFIG", stamps.get(StorageSetupService.Layer.CONFIG), "CACHE", stamps.get(StorageSetupService.Layer.CACHE), "MASTER", stamps.get(StorageSetupService.Layer.MASTER)));
    }
    private static Map<String,Object> connection(StorageBinding.Connection value) {
        return Map.of("kind", value.kind().name(), "uri", value.uri(), "database", value.database());
    }
}
