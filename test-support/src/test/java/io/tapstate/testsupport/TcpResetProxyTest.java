package io.tapstate.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import org.junit.jupiter.api.Test;

class TcpResetProxyTest {
    @Test
    void aRefusedBackendConnectionDoesNotStopAccepting() throws Exception {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        int backendPort;
        try (var reservation = new ServerSocket(0, 16, loopback)) {
            backendPort = reservation.getLocalPort();
        }
        try (var proxy = new TcpResetProxy(loopback.getHostAddress(), backendPort)) {
            try (var refused = new Socket(loopback, proxy.port())) {
                refused.setSoTimeout(2000);
                try {
                    assertThat(refused.getInputStream().read()).isEqualTo(-1);
                } catch (SocketException reset) {
                    // A reset also closes the failed front connection.
                }
            }

            try (var backend = new ServerSocket(backendPort, 16, loopback);
                    var client = new Socket(loopback, proxy.port())) {
                backend.setSoTimeout(2000);
                client.setSoTimeout(2000);
                try (var forwarded = backend.accept()) {
                    forwarded.getOutputStream().write(93);
                    assertThat(client.getInputStream().read()).isEqualTo(93);
                }
            }
        }
    }
}
