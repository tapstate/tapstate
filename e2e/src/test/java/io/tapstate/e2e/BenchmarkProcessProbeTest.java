package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.ref.Reference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
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
                assertThat(sample.gcPauseMillis().orElseThrow()).isNotNegative();
            }

            child.getOutputStream().close();
            assertThat(child.waitFor(5, TimeUnit.SECONDS)).isTrue();
            try (BenchmarkProcessProbe probe = BenchmarkProcessProbe.open(child.pid())) {
                BenchmarkProcessProbe.Snapshot unavailable = probe.sample();
                assertThat(unavailable.complete()).isFalse();
                assertThat(unavailable.cpuNanos()).isEmpty();
                assertThat(unavailable.heapUsedBytes()).isEmpty();
                assertThat(unavailable.rssBytes()).isEmpty();
                assertThat(unavailable.gcPauseMillis()).isEmpty();
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
