package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class SourceConfigClientProcessEnvironmentTest {
    @TempDir Path work;

    @Test
    void inheritedJvmOptionsDoNotReachTheIsolatedChild() throws Exception {
        Path output = work.resolve("jvm-properties.txt");
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-XshowSettings:properties", "-version")
                .redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().putAll(Map.of(
                "JAVA_TOOL_OPTIONS", "-Dreview.tool.probe=leaked",
                "JDK_JAVA_OPTIONS", "-Dreview.launcher.probe=leaked",
                "_JAVA_OPTIONS", "-Dreview.legacy.probe=leaked"));

        var isolate = SourceConfigClientTransportsIT.class
                .getDeclaredMethod("isolatedProcessEnvironment", ProcessBuilder.class);
        isolate.setAccessible(true);
        isolate.invoke(null, builder);

        Process child = builder.start();
        try {
            assertThat(child.waitFor(30, TimeUnit.SECONDS)).as("the JVM option probe completes").isTrue();
            String properties = Files.readString(output);
            assertThat(child.exitValue()).as("the JVM option probe starts successfully: %s", properties).isZero();
            assertSoftly(softly -> {
                softly.assertThat(builder.environment().keySet()).as("inherited JVM injection variables are removed")
                        .doesNotContain("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS");
                softly.assertThat(properties).as("inherited JVM options cannot inject child properties")
                        .doesNotContain("review.tool.probe = leaked", "review.launcher.probe = leaked",
                                "review.legacy.probe = leaked");
            });
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertThat(child.waitFor(5, TimeUnit.SECONDS)).as("the owned JVM probe is stopped").isTrue();
            }
        }
    }
}
