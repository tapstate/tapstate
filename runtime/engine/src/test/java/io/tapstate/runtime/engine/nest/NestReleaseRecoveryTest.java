package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.StageTimer;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestReleaseStore;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NestReleaseRecoveryTest {
    @Test void originalKeyTypesSurviveTheRecoveryRoute() throws Exception {
        var keys = List.<Object>of(List.of(100L), List.of(100), List.of(new BigDecimal("100.00")));
        var page = new ArrayList<NestReleaseStore.PendingKey>();
        for (Object key : keys) page.add(pending(key));
        var source = new NestReleaseRecovery(after -> after == null ? page : List.of());
        var out = new TestOutbox(16); source.init(out,new TestProcessorContext());
        assertThat(source.complete()).isTrue();
        var emitted = new ArrayList<Object>(); out.drainQueueAndReset(0,emitted,false);
        assertThat(emitted.stream().map(value -> ((NestReleaseRecovery.Wake)value).key()).toList())
                .containsExactlyElementsOf(keys);
        assertThat(emitted.stream().map(value -> NestStateKeys.nameOf(((NestReleaseRecovery.Wake)value).key())).toList())
                .doesNotHaveDuplicates();
    }
    @Test void backpressureDoesNotRefetchOrSkipThePendingPage() throws Exception {
        var calls = new AtomicInteger(); var page = List.of(pending(List.of("A")),pending(List.of("B")),pending(List.of("C")));
        var source = new NestReleaseRecovery(after -> {
            calls.incrementAndGet(); return after == null ? page : List.of();
        });
        var out = new TestOutbox(1); source.init(out,new TestProcessorContext());
        var timerField = NestReleaseRecovery.class.getDeclaredField("timer"); timerField.setAccessible(true);
        var timer = (StageTimer) timerField.get(source);
        var emitted = new ArrayList<Object>();
        assertThat(source.complete()).isFalse(); assertThat(calls).hasValue(1);
        assertThat(timer.value().count()).isEqualTo(1);
        out.drainQueueAndReset(0,emitted,false);
        assertThat(source.complete()).isFalse(); assertThat(calls).hasValue(1);
        assertThat(timer.value().count()).isEqualTo(2);
        out.drainQueueAndReset(0,emitted,false);
        assertThat(source.complete()).isTrue(); out.drainQueueAndReset(0,emitted,false);
        assertThat(timer.value().count()).isEqualTo(3);
        assertThat(source.complete()).isTrue(); assertThat(calls).hasValue(2);
        assertThat(timer.value().count()).isEqualTo(3);
        assertThat(emitted.stream().map(value -> ((NestReleaseRecovery.Wake)value).key()).toList())
                .containsExactly(List.of("A"),List.of("B"),List.of("C"));
    }
    @Test void unreadableOrMisaddressedRecoveryKeysAreCodedFailures() throws Exception {
        for (var key : List.of(new NestReleaseStore.PendingKey("bad",new byte[]{1}),
                new NestReleaseStore.PendingKey("wrong",pending(List.of("A")).routingKey()))) {
            var source = new NestReleaseRecovery(after -> List.of(key));
            source.init(new TestOutbox(1),new TestProcessorContext());
            assertThatThrownBy(source::complete).isInstanceOfSatisfying(TapstateException.class,error -> {
                assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
            });
        }
    }
    private static NestReleaseStore.PendingKey pending(Object key) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(key); }
        return new NestReleaseStore.PendingKey(NestStateKeys.nameOf(key),bytes.toByteArray());
    }
}
