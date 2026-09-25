package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * A streamed read that loses its connection ends the call, rather than attaching again to the address
 * it was given. The address is one member; which member to try next is a question only the session can
 * answer, and a transport that answers it by retrying the same address is how a watch stays pointed at
 * a node that is gone -- silently, because from the outside a stuck watch and a quiet one look alike.
 *
 * <p>Driven over a real socket that performs the websocket handshake and then closes, because that is
 * the behaviour under test: nothing else in the tree opens one of these, so a double here would be a
 * double of the thing being measured.
 */
class AStreamedReadEndsSoTheSessionCanMoveTest {

    @Test
    void aDroppedStreamEndsTheCallInsteadOfAttachingAgainToTheSameNode() throws Exception {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (ClosingWebsocketServer server = ClosingWebsocketServer.start();
                HttpControlPlaneClient client = new HttpControlPlaneClient()) {

            // The user has not stopped it: this stream ends only because the connection did.
            Future<String> call = caller.submit(() ->
                    client.watchStatus(server.baseUrl(), "tok", "pl1", (a, b, c, d) -> { }, () -> false));

            String refusal;
            try {
                refusal = call.get(20, TimeUnit.SECONDS);
            } catch (TimeoutException neverReturned) {
                call.cancel(true);
                fail("the call never ended: the transport attached again instead of handing the "
                        + "decision back, and it had attached " + server.accepted() + " times by now");
                return;
            }

            assertThat(refusal)
                    .as("a dropped connection is not a coded refusal; only the server closing with one is")
                    .isNull();
            assertThat(server.accepted())
                    .as("exactly one attach per call, so the caller chooses where the next one goes")
                    .isEqualTo(1);
        } finally {
            caller.shutdownNow();
        }
    }

    /**
     * Accepts websocket connections, completes the handshake, and closes -- a member that went away
     * mid-stream. It keeps accepting afterwards, so an attach that should not happen is counted rather
     * than refused (a refused connection would look like the server's doing, not the client's).
     */
    private static final class ClosingWebsocketServer implements AutoCloseable {

        private static final String HANDSHAKE_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        private final ServerSocket socket;
        private final Thread accepting;
        private final AtomicInteger accepted = new AtomicInteger();

        private ClosingWebsocketServer(ServerSocket socket) {
            this.socket = socket;
            this.accepting = new Thread(this::acceptUntilClosed, "closing-websocket-server");
            this.accepting.setDaemon(true);
            this.accepting.start();
        }

        static ClosingWebsocketServer start() throws IOException {
            // Bound to the loopback address itself, not to every interface: a wildcard bind does not
            // reserve the loopback address, so the port could already belong to something else there.
            return new ClosingWebsocketServer(new ServerSocket(0, 16, InetAddress.getLoopbackAddress()));
        }

        URI baseUrl() {
            return URI.create("http://127.0.0.1:" + socket.getLocalPort());
        }

        int accepted() {
            return accepted.get();
        }

        private void acceptUntilClosed() {
            while (!socket.isClosed()) {
                try (Socket connection = socket.accept()) {
                    accepted.incrementAndGet();
                    shakeHands(connection);
                } catch (IOException | RuntimeException closedOrRefused) {
                    return;
                }
            }
        }

        private static void shakeHands(Socket connection) throws IOException {
            String key = readKey(connection.getInputStream());
            if (key == null) {
                return;
            }
            OutputStream out = connection.getOutputStream();
            out.write(("HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + accept(key) + "\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            out.flush();
            // and then the member goes away, without a close frame -- which is what going away looks like
        }

        /** Reads the request head and returns the handshake key, or null if the head never arrives. */
        private static String readKey(InputStream in) throws IOException {
            StringBuilder head = new StringBuilder();
            int b;
            while (head.length() < 8192 && (b = in.read()) >= 0) {
                head.append((char) b);
                if (head.length() >= 4 && head.lastIndexOf("\r\n\r\n") == head.length() - 4) {
                    break;
                }
            }
            for (String line : head.toString().split("\r\n")) {
                if (line.toLowerCase(java.util.Locale.ROOT).startsWith("sec-websocket-key:")) {
                    return line.substring(line.indexOf(':') + 1).trim();
                }
            }
            return null;
        }

        private static String accept(String key) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                return Base64.getEncoder().encodeToString(
                        sha1.digest((key + HANDSHAKE_MAGIC).getBytes(StandardCharsets.UTF_8)));
            } catch (java.security.NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-1 is required of every JRE", impossible);
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
            accepting.interrupt();
        }
    }
}
