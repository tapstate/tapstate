package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnReaderTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1_000);

    @Test void the_getter_point_is_read_during_its_actual_root_request() {
        AtomicLong root = new AtomicLong(100);
        var reader = reader(attributes(17L, 1_000L, -50L), true, () -> true, root);
        var point = reader.clockSample(9);
        assertThat(point).isEqualTo(new BenchmarkCausalClock.Sample(9, OWNER, 101, 102, -50));
    }

    @Test void missing_duplicate_and_non_long_attributes_do_not_create_clock_proof() {
        var duplicate = attributes(17L, 1_000L, -50L);
        duplicate.add(new Attribute("Pid", 17L));
        var duplicateNull = new AttributeList();
        duplicateNull.add(new Attribute("Pid", null)); duplicateNull.addAll(attributes(17L, 1_000L, -50L));
        for (var values : List.of(new AttributeList(), duplicate, duplicateNull,
                attributes(17, 1_000L, -50L))) {
            var reader = reader(values, true, () -> true, new AtomicLong());
            assertThatThrownBy(() -> reader.clockSample(0)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void pid_reuse_and_foreign_processes_are_rejected() {
        for (var values : List.of(attributes(18L, 1_000L, -50L), attributes(17L, 1_001L, -50L))) {
            assertThatThrownBy(() -> reader(values, true, () -> true, new AtomicLong()).clockSample(0))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("owned runtime identity");
        }
    }

    @Test void an_exit_before_or_during_the_read_is_not_a_clock_point() {
        var dead = reader(attributes(17L, 1_000L, -50L), true, () -> false, new AtomicLong());
        assertThatThrownBy(() -> dead.clockSample(0)).isInstanceOf(AssertionError.class).hasMessageContaining("exited");
        AtomicLong checks = new AtomicLong();
        var exits = reader(attributes(17L, 1_000L, -50L), true,
                () -> checks.incrementAndGet() == 1, new AtomicLong());
        assertThatThrownBy(() -> exits.clockSample(0)).isInstanceOf(AssertionError.class).hasMessageContaining("exited");
    }

    @Test void controls_preserve_a_typed_refusal_and_require_typed_results() {
        var refusal = reader(attributes(17L, 1_000L, -50L), false, () -> true, new AtomicLong());
        assertThat(refusal.start("measured")).isFalse(); assertThat(refusal.stop()).isFalse();
        assertThatThrownBy(() -> reader(attributes(17L, 1_000L, -50L), "true", () -> true, new AtomicLong()).stop())
                .isInstanceOf(AssertionError.class).hasMessageContaining("typed result");
    }

    @Test void receipt_pages_must_be_binary_and_within_the_frozen_frame_bound() {
        var accepted = reader(attributes(17L, 1_000L, -50L), new byte[65_536], () -> true, new AtomicLong());
        assertThat(accepted.page(0)).hasSize(65_536);
        for (Object bad : List.of("bytes", new byte[0], new byte[65_537])) {
            assertThatThrownBy(() -> reader(attributes(17L, 1_000L, -50L), bad, () -> true, new AtomicLong()).page(0))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("page");
        }
    }

    private static AttributeList attributes(Object pid, Object start, Object point) {
        var attributes = new AttributeList();
        attributes.add(new Attribute("Pid", pid)); attributes.add(new Attribute("JvmStartTimeMillis", start));
        attributes.add(new Attribute("NanoTime", point)); return attributes;
    }
    private static BenchmarkWriteReturnReader reader(AttributeList attributes, Object result,
                                                     BooleanSupplier alive, AtomicLong root) {
        var connection = (MBeanServerConnection) Proxy.newProxyInstance(
                BenchmarkWriteReturnReaderTest.class.getClassLoader(), new Class<?>[]{MBeanServerConnection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAttributes" -> attributes;
                    case "invoke" -> result;
                    default -> throw new AssertionError("unexpected management operation: " + method.getName());
                });
        return new BenchmarkWriteReturnReader(OWNER, connection, alive, root::incrementAndGet);
    }
}
