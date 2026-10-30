package io.tapstate.app;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
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

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class MongoDriverPipelineLogsTest {

    @Test
    void stateStoreUsernameDoesNotReachPipelineLogs() throws Exception {
        // Use an explicit parent because the JDK may cache its default before Surefire sets the property.
        Path logDir = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "pipeline-driver-logs-");
        String pipelineId = "orders-copy";
        String username = "probe-state-store-user";
        String password = "probe-state-store-password";
        RingBufferLogSink sink = new RingBufferLogSink(8, 200);
        Map<String, String> previousMdc = MDC.getCopyOfContextMap();

        try (ConfigurableApplicationContext ignored = new SpringApplicationBuilder(Bootstrap.class)
                .web(WebApplicationType.NONE)
                .properties("tapstate.store.mongo.enabled=false", "tapstate.log.dir=" + logDir)
                .run();
             PipelineLogCapture capture = new PipelineLogCapture(
                     sink, new ControlPlaneConfiguration().secretRedactor());
             ServerSocket endpoint = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            LoggerFactory.getLogger(MongoDriverPipelineLogsTest.class).info("snapshot started for orders");

            // Client construction emits the real driver's settings dump before any database operation.
            // Reserve a loopback endpoint so the probe needs neither a database nor external credentials.
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString("mongodb://" + username + ":" + password
                            + "@127.0.0.1:" + endpoint.getLocalPort()
                            + "/tapstate?connectTimeoutMS=100&socketTimeoutMS=100"))
                    .build();
            try (MongoClient client = MongoClients.create(settings)) {
                String messages = new PipelineLogQueryService(sink).logs(pipelineId).lines().stream()
                        .map(LogLine::message)
                        .collect(Collectors.joining("\n"));

                assertThat(messages).contains("snapshot started for orders");
                assertThat(messages)
                        .as("pipeline logs must not expose state-store credentials from MongoDB driver settings")
                        .doesNotContain(username, password);
            }
        } finally {
            if (previousMdc == null) {
                MDC.clear();
            } else {
                MDC.setContextMap(previousMdc);
            }
            try (var files = Files.walk(logDir)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(file);
                }
            }
        }
    }
}
