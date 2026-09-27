package io.tapstate.adapters.otel;

import jdk.jfr.FlightRecorder;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class JfrGcPausesLiveTest {

    @Test
    void aRealJvmGcPauseReachesTheProcessCounter() throws Exception {
        String classes = String.join(File.pathSeparator,
                locationOf(Child.class), locationOf(JfrGcPauses.class), locationOf(LoggerFactory.class));
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Process child = new ProcessBuilder(java.toString(), "-Xmx64m", "-cp", classes,
                Child.class.getName()).redirectErrorStream(true).start();
        try {
            boolean finished = child.waitFor(20, TimeUnit.SECONDS);
            if (!finished) {
                child.destroyForcibly();
            }
            assertThat(finished).as("GC pause probe child finished within 20 seconds").isTrue();
            String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(child.exitValue()).as(output).isZero();
            assertThat(output).contains("PAUSE_NANOS=");
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
    }

    private static String locationOf(Class<?> type) throws Exception {
        return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    }

    public static final class Child {
        private Child() {
        }

        public static void main(String[] args) throws Exception {
            if (!FlightRecorder.isAvailable()) {
                throw new IllegalStateException("JFR is unavailable in the test JVM");
            }
            try (JfrGcPauses pauses = JfrGcPauses.start()) {
                byte[][] retained = new byte[16][];
                for (int i = 0; i < 1_024 && pauses.snapshot().isEmpty(); i++) {
                    retained[i % retained.length] = new byte[128 * 1_024];
                }
                System.gc();
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (pauses.snapshot().isEmpty() && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                JfrGcPauses.Reading reading = pauses.snapshot()
                        .orElseThrow(() -> new IllegalStateException("No live GC pause was observed"));
                if (reading.durationNanos() <= 0) {
                    throw new IllegalStateException("Observed GC pause had no duration");
                }
                System.out.println("PAUSE_NANOS=" + reading.durationNanos());
            }
        }
    }
}
