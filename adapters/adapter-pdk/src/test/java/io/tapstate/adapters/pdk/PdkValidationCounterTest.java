package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationCounterTest {
    @Test void nativeDirectCountKeepsZeroAndLargeExactLongWithoutReadingRows() {
        var calls = new AtomicInteger();
        for (long value : List.of(0L, 9007199254740993L, Long.MAX_VALUE)) {
            assertThat(PdkValidationCounter.direct("table", () -> { calls.incrementAndGet(); return value; }, () -> { }))
                    .isEqualTo(value);
        }
        assertThat(calls).hasValue(3);
    }

    @Test void unknownNegativeDirectCountIsNotEmpty() {
        assertCode(() -> PdkValidationCounter.direct("table", () -> -1L, () -> { }),
                ValidationError.READ_PROTOCOL_VIOLATION);
    }

    @Test void countCallbacksAcceptOnlyExactNonnegativeIntegralPayloads() {
        for (Number value : List.of(0, 9007199254740993L, BigInteger.valueOf(Long.MAX_VALUE))) {
            assertThat(PdkValidationCounter.callback("table", sink -> sink.accept(result(value)), () -> { }))
                    .isEqualTo(value.longValue());
        }
        for (Object value : List.of(-1L, 1.0d, new BigDecimal("1"), "1", BigInteger.ONE.shiftLeft(63))) {
            assertCode(() -> PdkValidationCounter.callback("table", sink -> sink.accept(result(value)), () -> { }),
                    ValidationError.READ_PROTOCOL_VIOLATION);
        }
    }

    @Test void missingNullAndDuplicateCallbackResultsAreNotSuccessfulZeroCounts() {
        assertCode(() -> PdkValidationCounter.callback("table", sink -> { }, () -> { }),
                ValidationError.READ_PROTOCOL_VIOLATION);
        assertCode(() -> PdkValidationCounter.callback("table", sink -> sink.accept(null), () -> { }),
                ValidationError.READ_PROTOCOL_VIOLATION);
        assertCode(() -> PdkValidationCounter.callback("table", sink -> sink.accept(new ExecuteResult<>()), () -> { }),
                ValidationError.READ_PROTOCOL_VIOLATION);
        assertCode(() -> PdkValidationCounter.callback("table", sink -> {
            sink.accept(result(1L));
            try { sink.accept(result(1L)); } catch (TapstateException swallowed) { }
        }, () -> { }), ValidationError.READ_PROTOCOL_VIOLATION);
    }

    @Test void swallowedCallbackErrorRemainsFirstWhenTheQueryLaterThrowsSomethingElse() {
        var original = new Exception("first count error");
        var later = new Exception("later query error");
        assertThatThrownBy(() -> PdkValidationCounter.callback("table", sink -> {
            try { sink.accept(new ExecuteResult<>().error(original)); } catch (RuntimeException swallowed) { }
            throw later;
        }, () -> { })).isInstanceOfSatisfying(TapstateException.class, failed -> {
            assertThat(failed.code()).isEqualTo(ValidationError.READ_FAILED);
            assertThat(failed.getCause()).isSameAs(original);
            assertThat(failed.getSuppressed()).containsExactly(later);
        });
    }

    @Test void authorizationIsCheckedBeforeDispatchAndAfterReturn() {
        var denied = new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> PdkValidationCounter.direct("table", () -> { calls.incrementAndGet(); return 1; },
                () -> { throw denied; })).isSameAs(denied);
        assertThat(calls).hasValue(0);
        var authorized = new AtomicBoolean(true);
        assertThatThrownBy(() -> PdkValidationCounter.callback("table", sink -> {
            sink.accept(result(1L));
            authorized.set(false);
        }, () -> { if (!authorized.get()) throw denied; })).isSameAs(denied);
    }

    @Test void interruptIsCancellationEvenWhenTheCountReturnsQuietly() {
        try {
            assertCode(() -> PdkValidationCounter.direct("table", () -> {
                Thread.currentThread().interrupt(); return 0;
            }, () -> { }), ValidationError.READ_CANCELLED);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
        try {
            assertCode(() -> PdkValidationCounter.callback("table", sink -> {
                throw new InterruptedException("fixture cancelled");
            }, () -> { }), ValidationError.READ_CANCELLED);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
    }

    @Test void joinedOffThreadScalarCallbackCompletesWithoutADecoderOrRowCursor() {
        assertThat(PdkValidationCounter.callback("table", sink ->
                CompletableFuture.runAsync(() -> sink.accept(result(7L))).get(), () -> { })).isEqualTo(7L);
    }

    @Test void callbacksAfterReturnAreRefusedInsteadOfAcceptingANewCount() {
        var callback = new AtomicReference<Consumer<ExecuteResult<?>>>();
        assertThat(PdkValidationCounter.callback("table", sink -> {
            callback.set(sink); sink.accept(result(7L));
        }, () -> { })).isEqualTo(7L);
        assertCode(() -> callback.get().accept(result(8L)), ValidationError.READ_PROTOCOL_VIOLATION);
    }

    @Test void nativeFailuresAreCodedAndFatalErrorsEscape() {
        var error = new Exception("native count failed");
        assertThatThrownBy(() -> PdkValidationCounter.direct("table", () -> { throw error; }, () -> { }))
                .isInstanceOfSatisfying(TapstateException.class, failed -> assertThat(failed.getCause()).isSameAs(error));
        var fatal = new AssertionError("fixture invariant");
        assertThatThrownBy(() -> PdkValidationCounter.callback("table", sink -> { throw fatal; }, () -> { })).isSameAs(fatal);
    }

    @Test void ordinaryFailureCannotHideALaterFatalError() {
        var fatal = new AssertionError("fatal after invalid count");
        assertThatThrownBy(() -> PdkValidationCounter.callback("table", sink -> {
            try { sink.accept(result(-1)); } catch (TapstateException swallowed) { }
            throw fatal;
        }, () -> { })).isSameAs(fatal);
        assertThat(fatal.getSuppressed()).hasSize(1);
        assertThat(((TapstateException)fatal.getSuppressed()[0]).code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION);
    }

    private static ExecuteResult<?> result(Object value) { return new ExecuteResult<>().result(value); }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, ValidationError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failed -> assertThat(failed.code()).isEqualTo(code));
    }
}
