package io.tapstate.e2e;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineBenchmarkReturnOwnedProcessTest {
    @TempDir Path temporary;
    @Test void timeout_stops_only_the_owned_parent_and_observed_child() throws Exception {
        Path pidFile = temporary.resolve("owned-child.pid");
        Process parent = launch(pidFile);
        long childPid = 0;
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            // The file appears before the child's pid is written into it; wait for the content, not the name.
            while (!hasPid(pidFile) && System.nanoTime() < deadline) TimeUnit.MILLISECONDS.sleep(10);
            assertThat(hasPid(pidFile)).isTrue();
            childPid = Long.parseLong(Files.readString(pidFile).trim());
            var child = ProcessHandle.of(childPid).orElseThrow();
            assertThat(child.isAlive()).isTrue();
            assertThatThrownBy(() -> PipelineBenchmarkReturnLiveRun.awaitOwned(parent, 100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("dispatch timeout");
            assertThat(parent.isAlive()).isFalse();
            child.onExit().get(5, TimeUnit.SECONDS);
            assertThat(child.isAlive()).isFalse();
        } finally {
            if (parent.isAlive()) parent.destroyForcibly();
            if (childPid > 0) ProcessHandle.of(childPid).filter(ProcessHandle::isAlive).ifPresent(ProcessHandle::destroyForcibly);
        }
    }
    private static boolean hasPid(Path file) throws java.io.IOException {
        return Files.exists(file) && !Files.readString(file).isBlank();
    }
    private static Process launch(Path pidFile) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                Child.class.getName(), pidFile.toString()).redirectErrorStream(true)
                .redirectOutput(pidFile.resolveSibling("owned-child.log").toFile()).start();
    }
    public static final class Child {
        public static void main(String[] args) throws Exception {
            if (args.length > 0) {
                String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
                Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), Child.class.getName()).start();
                Files.writeString(Path.of(args[0]), Long.toString(child.pid()));
            }
            Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        }
    }
}
