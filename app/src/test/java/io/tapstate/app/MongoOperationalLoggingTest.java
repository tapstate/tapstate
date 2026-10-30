package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.test.context.TestConfiguration;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class MongoOperationalLoggingTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class LoggingOnly {
    }

    @Test
    void defaultDriverNoiseCannotEvictPipelineEventsButWarningsAndErrorsRemain()
            throws Exception {
        Path logDir = scratch();
        RingBufferLogSink sink = new RingBufferLogSink(8, 200);
        try (ConsoleCapture output = new ConsoleCapture();
             ConfigurableApplicationContext ignored = boot(logDir);
             PipelineLogCapture capture = new PipelineLogCapture(
                     sink, new ControlPlaneConfiguration().secretRedactor())) {
            Logger driver = (Logger) LoggerFactory.getLogger("org.mongodb.driver.cluster");
            assertThat(driver.getEffectiveLevel()).isEqualTo(Level.WARN);
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, "orders");
            LoggerFactory.getLogger(getClass()).info("snapshot started for orders");
            for (int i = 0; i < 250; i++) {
                driver.info("No server chosen by selector {}", i);
            }
            driver.warn("driver retry warning");
            driver.error("driver connection failed");

            assertThat(new PipelineLogQueryService(sink).logs("orders").lines())
                    .extracting(LogLine::message)
                    .containsExactly("snapshot started for orders", "driver retry warning", "driver connection failed");
            assertThat(Files.readString(logDir.resolve("tapstate-server.log")))
                    .doesNotContain("No server chosen");
            assertThat(output.getAll()).doesNotContain("No server chosen");
        } finally {
            MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        }
    }

    @Test
    void explicitDriverDiagnosticsAndExceptionChainsAreSafeInEveryDestination()
            throws Exception {
        Path logDir = scratch();
        RingBufferLogSink sink = new RingBufferLogSink(8, 200);
        try (ConsoleCapture output = new ConsoleCapture();
             ConfigurableApplicationContext ignored = boot(logDir, "logging.level.org.mongodb.driver=DEBUG");
             ServerSocket endpoint = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            String uri = "mongodb://atlas-log-user:probe-log-password@127.0.0.1:" + endpoint.getLocalPort()
                    + "/private-authdb?connectTimeoutMS=100&socketTimeoutMS=100";
            // No control-plane bean or Source registry exists when this first client logs its settings.
            try (MongoClient startup = MongoClients.create(uri)) {
                assertThat(output.getAll()).contains("MongoClient with metadata").doesNotContain("atlas-log-user");
            }
            var redactor = new ControlPlaneConfiguration().secretRedactor();
            redactor.replace("source", java.util.List.of("known-password"));
            try (PipelineLogCapture capture = new PipelineLogCapture(sink, redactor)) {
                MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, "orders");
                try (MongoClient client = MongoClients.create(uri)) {
                    var failure = new IllegalStateException("operation failed known-password",
                            new IllegalArgumentException("{\"source\":\"mongodb+srv://uri-user:pa%40ss@db.example/data\","
                                    + "\"target\":\"mongodb://other-user:other-password@other.example/data\"}"));
                    failure.addSuppressed(new IllegalStateException(
                            "MongoCredential{userName='suppressed-user', mechanismProperties=<hidden>}"));
                    LoggerFactory.getLogger(getClass()).error("pipeline failed", failure);
                }
                String pipeline = new PipelineLogQueryService(sink).logs("orders").lines().stream()
                        .map(LogLine::message).collect(Collectors.joining("\n"));
                String file = Files.readString(logDir.resolve("tapstate-server.log"));
                for (String destination : java.util.List.of(pipeline, file, output.getAll())) {
                    assertThat(destination)
                            .contains("MongoClient with metadata", "127.0.0.1", "db.example",
                                    "pipeline failed", "Caused by:", "Suppressed:")
                            .doesNotContain("atlas-log-user", "probe-log-password", "private-authdb",
                                    "known-password", "uri-user", "pa%40ss", "suppressed-user",
                                    "other-user", "other-password", "%nopex", "PARSER_ERROR");
                }
            }
        } finally {
            MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        }
    }

    private static ConfigurableApplicationContext boot(Path logDir, String... extra) {
        return new SpringApplicationBuilder(LoggingOnly.class).web(WebApplicationType.NONE)
                .properties(extra).run("--tapstate.log.dir=" + logDir);
    }

    private static Path scratch() throws Exception {
        return Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "mongo-logs-");
    }

    private static final class ConsoleCapture implements AutoCloseable {
        private final PrintStream previous = System.out;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final PrintStream captured = new PrintStream(bytes, true, StandardCharsets.UTF_8);

        ConsoleCapture() {
            System.setOut(captured);
        }

        String getAll() {
            return bytes.toString(StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
            System.setOut(previous);
            var root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            if (root.getAppender("CONSOLE") instanceof ch.qos.logback.core.ConsoleAppender<?> console) {
                console.setOutputStream(previous);
            }
            captured.close();
        }
    }
}
