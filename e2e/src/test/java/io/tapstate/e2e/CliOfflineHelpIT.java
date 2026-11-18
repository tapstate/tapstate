package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class CliOfflineHelpIT {

    @Test
    void shortHelpSkipsTheExplicitServerAndLogin(@TempDir Path home) throws IOException {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            String url = "http://127.0.0.1:" + server.getLocalPort();
            CliOnce.Run run = CliOnce.runWithPassword(null, List.of("-Duser.home=" + home),
                    "-c", url, "-u", "admin", "apply", "-h");

            assertThat(run.exitCode()).as("apply -h stderr: %s", run.stderr()).isZero();
            assertThat(run.stdout()).contains("Usage: tapstate apply").doesNotContain("Password:");
            assertThat(run.stderr()).isEmpty();
            server.setSoTimeout(250);
            assertThatExceptionOfType(SocketTimeoutException.class).isThrownBy(() -> server.accept().close());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"status", "status -- --help", "position pipeline -f --help"})
    void nonHelpArgumentsStillResolveTheServer(String command, @TempDir Path home) {
        CliOnce.Run run = CliOnce.runWithPassword(null, List.of("-Duser.home=" + home),
                ("--context missing " + command).split(" "));

        assertThat(run.exitCode()).isEqualTo(1);
        assertThat(run.stderr()).contains("error: cli.context-not-found");
    }
}
