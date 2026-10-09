package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.ref.Reference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The external adapter reads a live child JVM and does not invent readings after it exits. */
class BenchmarkProcessProbeTest {

    @Test
    void readsChildCpuHeapRssAndGcThenMarksAnExitedChildUnavailable() throws Exception {
        Path classes = Path.of(Child.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Process child = new ProcessBuilder(java.toString(), "-cp", classes.toString(), Child.class.getName())
                .redirectErrorStream(true).start();
        try {
            BufferedReader output = new BufferedReader(
                    new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!output.ready() && child.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(output.ready()).as("child JVM reached its measurement barrier").isTrue();
            assertThat(output.readLine()).isEqualTo("READY");
            assertThat(child.isAlive()).as("child JVM remains alive before attach").isTrue();

            try (BenchmarkProcessProbe probe = BenchmarkProcessProbe.open(child.pid())) {
                assertThat(child.isAlive()).as("child JVM remains alive after attach").isTrue();
                BenchmarkProcessProbe.Snapshot sample = probe.sample();
                assertThat(sample.complete()).as("all four external readings are available: %s", sample).isTrue();
                assertThat(sample.cpuNanos().orElseThrow()).isPositive();
                assertThat(sample.heapUsedBytes().orElseThrow()).isGreaterThan(16L * 1_024 * 1_024);
                assertThat(sample.rssBytes().orElseThrow()).isGreaterThan(sample.heapUsedBytes().orElseThrow());
                assertThat(sample.gcCollectionMillis().orElseThrow()).isNotNegative();
                var compilation = probe.compilationRead();
                assertThat(compilation.ownedPid()).isEqualTo(child.pid());
                if (compilation.state() == BenchmarkProcessProbe.CompilationState.SUCCESS) {
                    assertThat(compilation.compilerName()).isNotBlank();
                    assertThat(compilation.monitoringSupported()).isTrue();
                    assertThat(compilation.totalCompilationMillis().orElseThrow()).isNotNegative();
                    var laterCompilation = probe.compilationRead();
                    assertThat(laterCompilation.state()).isEqualTo(BenchmarkProcessProbe.CompilationState.SUCCESS);
                    assertThat(laterCompilation.totalCompilationMillis().orElseThrow())
                            .isGreaterThanOrEqualTo(compilation.totalCompilationMillis().orElseThrow());
                } else {
                    assertThat(compilation.unknownReason())
                            .isEqualTo(BenchmarkProcessProbe.CompilationUnknownReason.MONITORING_UNSUPPORTED);
                    assertThat(compilation.monitoringSupported()).isFalse();
                    assertThat(compilation.totalCompilationMillis()).isEmpty();
                }
                assertThat(probe.sample().complete()).as("optional compilation read leaves resource readings available").isTrue();
                var threadReader = probe.threadPointReader();
                assertThat(threadReader.ownedPid()).isEqualTo(child.pid());
                var identity = threadReader.identity();
                assertThat(identity.pid()).isEqualTo(child.pid());
                assertThat(identity.startTimeMillis()).isPositive();
                assertThat(identity.alive()).isTrue();
                var capability = threadReader.cpuCapability();
                if (capability.supported()) { assertThat(capability.enabled()).isNotNull(); }
                else { assertThat(capability.enabled()).isNull(); }
                if (Boolean.TRUE.equals(capability.enabled())) {
                    long[] ids = threadReader.threadIds();
                    var info = threadReader.threadInfo(ids, 0);
                    assertThat(info).hasSize(ids.length);
                    var present = info.stream().filter(Objects::nonNull).findFirst().orElseThrow();
                    assertThat(present.frames()).isEmpty();
                    long[] cpu = threadReader.cpuNanos(new long[] {present.id()});
                    assertThat(cpu).hasSize(1);
                    assertThat(cpu[0]).as("dead thread is absent rather than an invented zero").isGreaterThanOrEqualTo(-1L);
                    assertThat(threadReader.threadInfo(new long[] {present.id()}, 1)).hasSize(1);
                }
            }

            child.getOutputStream().close();
            assertThat(child.waitFor(5, TimeUnit.SECONDS)).isTrue();
            try (BenchmarkProcessProbe probe = BenchmarkProcessProbe.open(child.pid())) {
                BenchmarkProcessProbe.Snapshot unavailable = probe.sample();
                assertThat(unavailable.complete()).isFalse();
                assertThat(unavailable.cpuNanos()).isEmpty();
                assertThat(unavailable.heapUsedBytes()).isEmpty();
                assertThat(unavailable.rssBytes()).isEmpty();
                assertThat(unavailable.gcCollectionMillis()).isEmpty();
                var compilation = probe.compilationRead();
                assertThat(compilation.ownedPid()).isEqualTo(child.pid());
                assertThat(compilation.state()).isEqualTo(BenchmarkProcessProbe.CompilationState.UNKNOWN);
                assertThat(compilation.unknownReason()).isEqualTo(BenchmarkProcessProbe.CompilationUnknownReason.CHILD_EXITED);
                assertThat(compilation.totalCompilationMillis()).isEmpty();
                var threadPoints = BenchmarkThreadPointDiagnostics.prepare(probe.threadPointReader(), System::nanoTime);
                threadPoints.recordAttempt(1);
                assertThat(((java.util.Map<?, ?>) threadPoints.evidence().get("preparation")).get("state"))
                        .isEqualTo("UNKNOWN");
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
    }

    public static final class Child {
        private Child() {
        }

        public static void main(String[] args) throws Exception {
            byte[] retained = new byte[24 * 1_024 * 1_024];
            for (int offset = 0; offset < retained.length; offset += 4_096) {
                retained[offset] = 1;
            }
            long until = System.nanoTime() + Duration.ofMillis(200).toNanos();
            while (System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            System.out.println("READY");
            System.in.read();
            Reference.reachabilityFence(retained);
        }
    }
}
