package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

/** Each case owns a JVM so cached defaults and launcher options cannot leak between cases. */
class ServerTimeZoneTest {
    @TempDir
    Path work;

    record Scenario(String name, List<String> options, Map<String, String> environment,
                    boolean warmCache, String expected) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Scenario> scenarios() {
        return Stream.of(
                new Scenario("host alias", List.of(), Map.of("TZ", "Etc/UTC"), false, "UTC"),
                new Scenario("cached host alias", List.of(), Map.of("TZ", "Etc/UTC"), true, "UTC"),
                new Scenario("non-UTC host", List.of(), Map.of("TZ", "Asia/Tokyo"), true, "UTC"),
                new Scenario("command-line override", List.of("-Duser.timezone=Asia/Tokyo"), Map.of(), true, "Asia/Tokyo"),
                new Scenario("tool-options override", List.of(), Map.of("JAVA_TOOL_OPTIONS", "-Duser.timezone=Asia/Tokyo"), true, "Asia/Tokyo"),
                new Scenario("launcher-options override", List.of(), Map.of("JDK_JAVA_OPTIONS", "-Duser.timezone=Asia/Tokyo"), true, "Asia/Tokyo"),
                new Scenario("last option wins", List.of("-Duser.timezone=GMT", "-Duser.timezone=Asia/Tokyo"), Map.of(), true, "Asia/Tokyo"),
                new Scenario("command overrides environment", List.of("-Duser.timezone=GMT"), Map.of("JAVA_TOOL_OPTIONS", "-Duser.timezone=Asia/Tokyo"), true, "GMT"),
                new Scenario("legacy JVM options override", List.of("-Duser.timezone=GMT"), Map.of("_JAVA_OPTIONS", "-Duser.timezone=Asia/Tokyo"), true, "Asia/Tokyo"),
                new Scenario("empty option", List.of("-Duser.timezone="), Map.of("TZ", "Asia/Tokyo"), true, "UTC"),
                new Scenario("bare option", List.of("-Duser.timezone"), Map.of("TZ", "Asia/Tokyo"), true, "UTC"),
                new Scenario("similar property is not an override", List.of("-Duser.timezone.extra=Asia/Tokyo"), Map.of(), true, "UTC"),
                new Scenario("explicit alias is preserved", List.of("-Duser.timezone=Etc/UTC"), Map.of(), true, "Etc/UTC"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void initializesTheServerBeforeItsLoggerWithoutOverridingJvmOptions(Scenario scenario) throws Exception {
        Path logging = work.resolve("logback.xml");
        Files.writeString(logging, """
                <configuration>
                  <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
                    <encoder><pattern>ZONE-LOG %d{XXX} %msg%n</pattern></encoder>
                  </appender>
                  <root level="INFO"><appender-ref ref="CONSOLE"/></root>
                </configuration>
                """);
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx128m", "-Dlogback.configurationFile=" + logging));
        // These children execute the assertions' product path. Retain coverage instrumentation,
        // but do not inherit the test runner's timezone or unrelated Java agents.
        ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(argument -> argument.startsWith("-javaagent:") && argument.contains("org.jacoco.agent"))
                .forEach(command::add);
        command.addAll(scenario.options());
        command.addAll(List.of("-cp", classpath, Probe.class.getName(), Boolean.toString(scenario.warmCache())));
        Path output = work.resolve("child.log");
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().keySet().removeIf(key -> key.equals("JAVA_TOOL_OPTIONS")
                || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS"));
        builder.environment().put("TZ", "Etc/UTC");
        builder.environment().putAll(scenario.environment());
        Process child = builder.start();
        try {
            assertThat(child.waitFor(30, TimeUnit.SECONDS)).as("fresh JVM completed").isTrue();
            String text = Files.readString(output);
            assertThat(child.exitValue()).as(text).isZero();
            assertThat(text).contains("effective=" + scenario.expected(), "property=" + scenario.expected());
            String offset = ZoneId.of(scenario.expected()).getRules().getOffset(Instant.now()).getId();
            assertThat(text).as("the logger must initialize after the selected default").contains("ZONE-LOG " + offset + " zone-probe");
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    public static final class Probe {
        private Probe() {}

        public static void main(String[] args) throws Exception {
            if (Boolean.parseBoolean(args[0])) {
                // This also populates user.timezone, despite there being no explicit JVM option.
                TimeZone.getDefault();
            }
            Class.forName("io.tapstate.app.Bootstrap");
            System.out.println("effective=" + TimeZone.getDefault().getID());
            System.out.println("property=" + System.getProperty("user.timezone"));
            LoggerFactory.getLogger(Probe.class).info("zone-probe");
        }
    }
}
