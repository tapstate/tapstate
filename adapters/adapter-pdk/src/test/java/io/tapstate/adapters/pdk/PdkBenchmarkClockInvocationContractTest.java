package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the real supplier dispatch seam; it does not claim an actual loaded JNI execution. */
class PdkBenchmarkClockInvocationContractTest {
    @TempDir Path directory;

    @Test void every_supplier_invocation_reads_native_once_even_when_values_tie_and_metadata_reads_no_counter() throws Exception {
        Path library = Files.writeString(directory.resolve("clock.dylib"), "controlled native material");
        var access = new NativeSequence(library, new long[]{7, 8, 8, 9});
        var fallback = new AtomicInteger();
        var clock = new PdkBenchmarkClock.Clock(library.toString(), access, fallback::incrementAndGet);
        assertThat(access.reads).isZero();
        assertThat(metadata(clock)).containsEntry("state", "NATIVE_RECORDED");
        assertThat(access.reads).isZero();
        for (long expected : new long[]{7, 8, 8, 9}) assertThat(clock.source().getAsLong()).isEqualTo(expected);
        assertThat(access.reads).isEqualTo(4);
        assertThat(metadata(clock)).containsEntry("state", "NATIVE_RECORDED");
        assertThat(access.reads).isEqualTo(4); assertThat(fallback).hasValue(0);
    }

    @Test void a_native_failure_stays_visible_after_the_provider_can_read_again() throws Exception {
        Path library = Files.writeString(directory.resolve("clock.dylib"), "controlled native material");
        var access = new NativeSequence(library, new long[]{7, 8, 9});
        var fallback = new AtomicInteger(100);
        var clock = new PdkBenchmarkClock.Clock(library.toString(), access, fallback::incrementAndGet);
        assertThat(clock.source().getAsLong()).isEqualTo(7);
        access.failure = new ArithmeticException("controlled native failure");
        assertThat(clock.source().getAsLong()).isEqualTo(101);
        access.failure = null;
        assertThat(clock.source().getAsLong()).isEqualTo(102);
        assertThat(access.reads).isEqualTo(2);
        assertThat(metadata(clock)).containsEntry("state", "UNKNOWN").containsEntry("reason", "NATIVE_READ_UNAVAILABLE")
                .containsEntry("mixedDomainPossible", true).containsEntry("performanceAcceptanceEligible", false);
        assertThat(access.reads).isEqualTo(2);
    }

    @Test void absent_native_selection_keeps_the_original_supplier_and_zero_native_reads() {
        var access = new NativeSequence(null, new long[]{7}); var system = new AtomicInteger();
        var clock = new PdkBenchmarkClock.Clock(null, access, system::incrementAndGet);
        assertThat(clock.source().getAsLong()).isEqualTo(1);
        assertThat(clock.source().getAsLong()).isEqualTo(2);
        assertThat(metadata(clock)).containsEntry("state", "SYSTEM_UNQUALIFIED");
        assertThat(access.reads).isZero(); assertThat(access.loads).isZero(); assertThat(access.facts).isZero();
    }

    @SuppressWarnings("unchecked") private static Map<String, Object> metadata(PdkBenchmarkClock.Clock clock) {
        return (Map<String, Object>) JsonReader.parse(clock.metadata());
    }
    private static final class NativeSequence implements PdkBenchmarkClock.NativeAccess {
        private final Path library;
        private final long[] values;
        private int reads, loads, facts;
        private RuntimeException failure;
        private NativeSequence(Path library, long[] values) { this.library = library; this.values = values.clone(); }
        @Override public void load(Path path) { loads++; }
        @Override public long read() {
            int index = reads++;
            if (failure != null) throw failure;
            return values[index];
        }
        @Override public String[] facts() {
            facts++;
            return new String[]{"00000000-0000-0000-0000-000000000000", "125", "3", library.toString(),
                    "/usr/lib/system/libsystem_kernel.dylib", "0".repeat(32), "4236", "ab".repeat(144), "3", "17", "mach_absolute_time"};
        }
    }
}
