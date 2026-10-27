package io.tapstate.e2e;

import io.tapstate.testsupport.JvmTestEnvironment;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot configure startup driver logging or read process log files. */
class MongoDriverDiagnosticsHideCredentialsIT {

    @Test
    void thePackagedServerHidesCredentialsBeforeControlPlaneStartupEvenWithDriverDiagnostics() throws Exception {
        Path jar = Path.of(System.getProperty("tapstate.e2e.boot-jar")).toAbsolutePath();
        assertThat(jar).isRegularFile();
        for (boolean diagnostics : new boolean[] {false, true}) {
            Path work = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "mongo-startup-");
            try (ServerSocket endpoint = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
                String uri = "mongodb://packaged-user:packaged-password@127.0.0.1:" + endpoint.getLocalPort()
                        + "/private-auth?connectTimeoutMS=100&socketTimeoutMS=100";
                List<String> command = new ArrayList<>(List.of(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Xmx512m", "-Djava.io.tmpdir=" + work, "-jar", jar.toString(),
                        "--spring.main.web-application-type=none", "--tapstate.store.mongo.uri=" + uri,
                        "--tapstate.store.mongo.server-selection-timeout=300ms",
                        "--tapstate.log.dir=" + work.resolve("logs")));
                if (diagnostics) command.add("--logging.level.org.mongodb.driver=DEBUG");
                Path console = work.resolve("console.log");
                ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile())
                        .redirectErrorStream(true).redirectOutput(console.toFile());
                builder.environment().keySet().removeIf(key -> key.startsWith("SPRING_")
                        || key.startsWith("LOGGING_") || key.startsWith("TAPSTATE_"));
                JvmTestEnvironment.configure(builder, Map.of());
                Process process = builder.start();
                try {
                    assertThat(process.waitFor(90, TimeUnit.SECONDS)).as("bounded startup refusal").isTrue();
                    String output = Files.readString(console);
                    assertThat(process.exitValue()).as(output).isEqualTo(1);
                    assertThat(output).contains("[store.unreachable]");
                    String file = Files.readString(work.resolve("logs/tapstate-server.log"));
                    for (String destination : List.of(output, file)) {
                        assertThat(destination).doesNotContain("packaged-user", "packaged-password", "private-auth",
                                "%nopex", "PARSER_ERROR");
                        if (diagnostics) {
                            assertThat(destination).contains("MongoClient with metadata", "127.0.0.1", "<redacted>");
                        } else {
                            assertThat(destination).doesNotContain("MongoClient with metadata", "No server chosen by");
                        }
                    }
                } finally {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                        assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                    }
                }
            }
        }
    }
}
