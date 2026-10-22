package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The actual packaged entry point must select its timezone before even rejecting arguments.
 * The declarative vocabulary cannot select a process's TZ or JVM launcher options.
 */
class TheServerChoosesItsTimezoneBeforeStartupIT {
    @TempDir
    Path work;

    static Stream<Arguments> launches() {
        return Stream.of(
                Arguments.of(List.of(), Map.of(), "UTC", "default"),
                Arguments.of(List.of("-Duser.timezone=Asia/Tokyo"), Map.of(), "Asia/Tokyo", "JVM option"),
                Arguments.of(List.of(), Map.of("JAVA_TOOL_OPTIONS", "-Duser.timezone=GMT"), "GMT", "JVM option"),
                Arguments.of(List.of(), Map.of("JDK_JAVA_OPTIONS", "-Duser.timezone=Asia/Tokyo"), "Asia/Tokyo", "JVM option"));
    }

    @ParameterizedTest
    @MethodSource("launches")
    void bootJarAppliesTheServerDefaultAndHonorsExplicitOptions(
            List<String> options, Map<String, String> environment, String zone, String source) throws Exception {
        Path jar = Path.of(System.getProperty("tapstate.e2e.boot-jar"));
        assertThat(jar).isRegularFile();
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m"));
        command.addAll(options);
        command.addAll(List.of("-jar", jar.toString(), "--role=timezone-probe-invalid"));
        Path output = work.resolve("server.log");
        ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile())
                .redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().keySet().removeIf(key -> key.equals("JAVA_TOOL_OPTIONS")
                || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS")
                || key.startsWith("SPRING_") || key.startsWith("LOGGING_"));
        builder.environment().put("TZ", "Etc/UTC");
        builder.environment().putAll(environment);
        Process process = builder.start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            String text = Files.readString(output);
            assertThat(process.exitValue()).as(text).isEqualTo(1);
            assertThat(text).contains("Server time zone: " + zone + " (" + source + ")",
                    "Unsupported --role value 'timezone-probe-invalid'.");
            assertThat(text.indexOf("Server time zone:")).isLessThan(text.indexOf("Unsupported --role"));
            assertThat(text).doesNotContain("Started Bootstrap");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
