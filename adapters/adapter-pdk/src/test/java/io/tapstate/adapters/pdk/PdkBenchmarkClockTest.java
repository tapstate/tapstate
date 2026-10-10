package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

class PdkBenchmarkClockTest {
    @Test void default_uses_the_same_supplier_without_native_calls_or_extra_counter_reads() {
        AtomicInteger reads = new AtomicInteger(); LongSupplier system = reads::incrementAndGet;
        FakeNative nativeAccess = new FakeNative(null);
        var clock = new PdkBenchmarkClock.Clock(null, nativeAccess, system);
        assertThat(clock.source()).isSameAs(system); assertThat(reads).hasValue(0);
        assertThat(metadata(clock)).containsEntry("state", "SYSTEM_UNQUALIFIED").containsEntry("configured", false);
        assertThat(reads).hasValue(0); assertThat(clock.source().getAsLong()).isEqualTo(1);
        assertThat(reads).hasValue(1); assertThat(nativeAccess.loads).isZero(); assertThat(nativeAccess.reads).isZero();
        assertThat(nativeAccess.facts).isZero();
    }

    @Test void failed_setup_returns_the_original_counter_and_retains_a_sticky_safe_reason(@TempDir Path directory) throws Exception {
        Path library = Files.writeString(directory.resolve("clock.dylib"), "fixture");
        FakeNative access = new FakeNative(library); access.loadFailure = new UnsatisfiedLinkError("private-path-or-secret");
        AtomicInteger system = new AtomicInteger();
        var clock = new PdkBenchmarkClock.Clock(library.toString(), access, system::incrementAndGet);
        assertThat(clock.source().getAsLong()).isEqualTo(1); assertThat(clock.source().getAsLong()).isEqualTo(2);
        var result = metadata(clock);
        assertThat(result).containsEntry("state", "UNKNOWN").containsEntry("reason", "NATIVE_SETUP_UNAVAILABLE")
                .containsEntry("activeProvider", "SYSTEM_NANO_TIME").containsEntry("mixedDomainPossible", true);
        assertThat(clock.metadata()).doesNotContain("private-path-or-secret"); assertThat(access.reads).isZero();
        assertThat(access.loads).isEqualTo(1); assertThat(access.facts).isZero();
    }

    @Test void a_conversion_failure_preserves_writes_but_never_resumes_the_native_domain(@TempDir Path directory) throws Exception {
        Path library = Files.writeString(directory.resolve("clock.dylib"), "fixture");
        FakeNative access = new FakeNative(library); AtomicInteger system = new AtomicInteger(100);
        var clock = new PdkBenchmarkClock.Clock(library.toString(), access, system::incrementAndGet);
        assertThat(clock.source().getAsLong()).isEqualTo(7); access.readFailure = new ArithmeticException("overflow detail");
        assertThat(clock.source().getAsLong()).isEqualTo(101); access.readFailure = null;
        assertThat(clock.source().getAsLong()).isEqualTo(102); assertThat(access.reads).isEqualTo(2);
        assertThat(metadata(clock)).containsEntry("state", "UNKNOWN").containsEntry("reason", "NATIVE_READ_UNAVAILABLE")
                .containsEntry("clockQualified", false).containsEntry("performanceAcceptanceEligible", false);
        assertThat(clock.metadata()).doesNotContain("overflow detail");
    }

    @Test void changed_loaded_code_or_native_route_refuses_the_entire_domain(@TempDir Path directory) throws Exception {
        for (boolean changeFile : new boolean[]{false, true}) {
            Path library = Files.writeString(directory.resolve(changeFile ? "file.dylib" : "route.dylib"), "fixture");
            FakeNative access = new FakeNative(library);
            var clock = new PdkBenchmarkClock.Clock(library.toString(), access, () -> 19L);
            assertThat(clock.source().getAsLong()).isEqualTo(7);
            if (changeFile) { Files.writeString(library, "changed"); } else { access.selector = "2"; }
            assertThat(metadata(clock)).containsEntry("state", "UNKNOWN").containsEntry("reason", "NATIVE_IDENTITY_CHANGED");
            assertThat(clock.source().getAsLong()).isEqualTo(19); assertThat(access.reads).isEqualTo(1);
        }
    }

    @Test void missing_binding_and_unbounded_configuration_cannot_become_recorded_native_facts(@TempDir Path directory) throws Exception {
        Path library = Files.writeString(directory.resolve("clock.dylib"), "fixture");
        for (String value : new String[]{"relative.dylib", "x".repeat(513), "bad\npath", library.toString()}) {
            FakeNative access = new FakeNative(library); access.badBinding = true;
            var clock = new PdkBenchmarkClock.Clock(value, access, () -> 23L);
            assertThat(metadata(clock)).containsEntry("state", "UNKNOWN").containsEntry("clockQualified", false);
            assertThat(clock.source().getAsLong()).isEqualTo(23); assertThat(access.reads).isZero();
        }
    }

    @Test void cold_metadata_retains_real_process_and_bounded_hashes_without_reading_a_counter(@TempDir Path directory) throws Exception {
        Path library = Files.writeString(directory.resolve("clock.dylib"), "fixture");
        FakeNative access = new FakeNative(library);
        var clock = new PdkBenchmarkClock.Clock(library.toString(), access, () -> 29L);
        String text = clock.metadata(); var result = metadata(clock);
        assertThat(result).containsEntry("state", "NATIVE_RECORDED").containsEntry("counterUnit", "nominal-ns")
                .containsEntry("siTimeQualified", false).containsEntry("utcAccuracyQualified", false)
                .containsEntry("loadingQualified", false).containsEntry("samplingCostQualified", false)
                .containsEntry("formalPerformance", false).containsEntry("costAcceptanceEligible", false);
        assertThat(((Number) result.get("pid")).longValue()).isEqualTo(ProcessHandle.current().pid());
        assertThat(((Number) result.get("jvmStartTimeMillis")).longValue()).isPositive();
        assertThat(text.getBytes(StandardCharsets.US_ASCII).length).isLessThanOrEqualTo(8192);
        assertThat(text.chars().allMatch(value -> value >= 0x20 && value <= 0x7e)).isTrue();
        var before = (Map<?, ?>) result.get("before"); var after = (Map<?, ?>) result.get("after");
        assertThat(after).isEqualTo(before); assertThat(before.get("loadedJniSha256").toString()).matches("[a-f0-9]{64}");
        assertThat(before.get("classSha256").toString()).matches("[a-f0-9]{64}");
        var classHashes = (Map<?, ?>) before.get("classHashes");
        assertThat(classHashes.keySet().stream().map(Object::toString).toList()).containsExactlyInAnyOrder("PdkBenchmarkClock", "PdkBenchmarkClock$Clock",
                "PdkBenchmarkClock$JniAccess", "PdkBenchmarkClock$NativeAccess");
        for (Object hash : classHashes.values()) { assertThat(hash.toString()).matches("[a-f0-9]{64}"); }
        assertThat(before.get("osFunctionCodePrefixSha256").toString()).matches("[a-f0-9]{64}");
        assertThat(access.reads).isZero();
    }

    @SuppressWarnings("unchecked") private static Map<String, Object> metadata(PdkBenchmarkClock.Clock clock) {
        return (Map<String, Object>) JsonReader.parse(clock.metadata());
    }

    private static final class FakeNative implements PdkBenchmarkClock.NativeAccess {
        final Path path; int loads, reads, facts; LinkageError loadFailure; RuntimeException readFailure;
        String selector = "3"; boolean badBinding;
        FakeNative(Path path) { this.path = path; }
        public void load(Path ignored) { loads++; if (loadFailure != null) { throw loadFailure; } }
        public long read() { reads++; if (readFailure != null) { throw readFailure; } return 7; }
        public String[] facts() {
            facts++;
            return new String[]{"01234567-0123-0123-0123-0123456789AB", "125", "3", path.toString(),
                    "/usr/lib/system/libsystem_kernel.dylib", "0123456789abcdef0123456789abcdef", "4236",
                    "00".repeat(144), selector, "18446743869013580446", badBinding ? "unbound" : "mach_absolute_time"};
        }
    }
}
