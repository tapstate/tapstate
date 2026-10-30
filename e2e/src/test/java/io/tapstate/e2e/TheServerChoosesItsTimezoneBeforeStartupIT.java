package io.tapstate.e2e;

import io.tapstate.testsupport.JvmTestEnvironment;

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
 * The packaged entry point selects its timezone before logging and records it in both appenders.
 * The declarative vocabulary cannot select a process's TZ or JVM launcher options.
 */
class TheServerChoosesItsTimezoneBeforeStartupIT {
    @TempDir
    Path work;

    static Stream<Arguments> launches() {
        return Stream.of(
                Arguments.of(List.of(), Map.of(), "UTC", "default", "UTC"),
                Arguments.of(List.of("-Duser.timezone=Asia/Tokyo"), Map.of(), "Asia/Tokyo", "JVM option", "Asia/Tokyo"),
                Arguments.of(List.of(), Map.of("JAVA_TOOL_OPTIONS", "-Duser.timezone=GMT"), "GMT", "JVM option", "GMT"),
                Arguments.of(List.of(), Map.of("JDK_JAVA_OPTIONS", "-Duser.timezone=Asia/Tokyo"), "Asia/Tokyo", "JVM option", "Asia/Tokyo"),
                Arguments.of(List.of("-Duser.timezone=Asia/Tokio"), Map.of(), "GMT", "JVM option", "Asia/Tokio"),
                Arguments.of(List.of("-Duser.timezone=GMT+8"), Map.of(), "GMT+08:00", "JVM option", "GMT+8"));
    }

    @ParameterizedTest
    @MethodSource("launches")
    void bootJarAppliesTheServerDefaultAndHonorsExplicitOptions(
            List<String> options, Map<String, String> environment, String zone, String source, String requested) throws Exception {
        Path jar = Path.of(System.getProperty("tapstate.e2e.boot-jar"));
        assertThat(jar).isRegularFile();
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m"));
        command.addAll(options);
        command.addAll(List.of("-jar", jar.toString(), "--tapstate.store.mongo.enabled=false",
                "--tapstate.cluster.profile=not-a-profile", "--tapstate.log.dir=" + work.resolve("logs")));
        Path output = work.resolve("server.log");
        ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile())
                .redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().keySet().removeIf(key -> key.startsWith("SPRING_")
                || key.startsWith("LOGGING_") || key.startsWith("TAPSTATE_"));
        builder.environment().put("TZ", "Etc/UTC");
        JvmTestEnvironment.configure(builder, environment);
        Process process = builder.start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            String text = Files.readString(output);
            assertThat(process.exitValue()).as(text).isEqualTo(1);
            String report = "Server time zone: " + zone + " (" + source + ")";
            assertThat(text).contains(report, "not-a-profile");
            String operational = Files.readString(work.resolve("logs/tapstate-server.log"));
            assertThat(operational).contains(report, "--- [] [] []");
            assertThat(text.split("Server time zone:", -1)).hasSize(2);
            assertThat(operational.split("Server time zone:", -1)).hasSize(2);
            String offset = java.time.ZoneId.of(zone).getRules().getOffset(java.time.Instant.now()).getId();
            assertThat(operational.lines().filter(line -> line.contains(report)).findFirst().orElseThrow())
                    .contains(offset + " ");
            if (!requested.equals(zone)) {
                String resolution = "requested=\"" + requested + "\", resolved differently by the JDK";
                assertThat(text).contains(resolution);
                assertThat(operational).contains(resolution);
            }
            assertThat(text).doesNotContain("Started Bootstrap");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
