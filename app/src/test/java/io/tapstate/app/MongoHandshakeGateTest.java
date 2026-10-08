package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(10)
class MongoHandshakeGateTest {

    private static final int IO_TIMEOUT_MILLIS = 3_000;
    private static final Duration AWAIT_REPLY_TIMEOUT = Duration.ofSeconds(3);

    @Test
    void holdsTheFirstCompleteReplyAndThenForwardsExactBytesInBothDirections() throws Exception {
        try (ServerSocket server = listen();
                MongoHandshakeGate gate = new MongoHandshakeGate("127.0.0.1", server.getLocalPort());
                Socket client = connect(gate.port());
                Socket upstream = accept(server)) {
            byte[] request = message(1, bytes(37));
            client.getOutputStream().write(request);
            assertThat(upstream.getInputStream().readNBytes(request.length)).isEqualTo(request);

            byte[] reply = message(2, bytes(257));
            upstream.getOutputStream().write(reply, 0, 7);
            upstream.getOutputStream().write(reply, 7, reply.length - 7);
            gate.awaitFirstReply(AWAIT_REPLY_TIMEOUT);
            assertReplyIsHeld(client);

            gate.release();
            assertThat(client.getInputStream().readNBytes(reply.length)).isEqualTo(reply);

            byte[] nextRequest = message(3, bytes(8193));
            client.getOutputStream().write(nextRequest);
            assertThat(upstream.getInputStream().readNBytes(nextRequest.length)).isEqualTo(nextRequest);
            byte[] nextReply = message(4, bytes(16385));
            upstream.getOutputStream().write(nextReply);
            assertThat(client.getInputStream().readNBytes(nextReply.length)).isEqualTo(nextReply);
        }
    }

    @Test
    void everyConnectionSharesTheBarrierAndConnectionsAfterReleaseFlowNormally() throws Exception {
        try (ServerSocket server = listen();
                MongoHandshakeGate gate = new MongoHandshakeGate("127.0.0.1", server.getLocalPort());
                Socket first = connect(gate.port());
                Socket firstUpstream = accept(server)) {
            byte[] firstReply = exchangeRequest(first, firstUpstream, 11);
            gate.awaitFirstReply(AWAIT_REPLY_TIMEOUT);
            assertReplyIsHeld(first);

            try (Socket second = connect(gate.port()); Socket secondUpstream = accept(server)) {
                byte[] secondReply = exchangeRequest(second, secondUpstream, 12);
                gate.awaitFirstReplies(2, AWAIT_REPLY_TIMEOUT);
                assertReplyIsHeld(second);
                gate.release();
                assertThat(first.getInputStream().readNBytes(firstReply.length)).isEqualTo(firstReply);
                assertThat(second.getInputStream().readNBytes(secondReply.length)).isEqualTo(secondReply);

                try (Socket third = connect(gate.port()); Socket thirdUpstream = accept(server)) {
                    byte[] thirdReply = exchangeRequest(third, thirdUpstream, 13);
                    assertThat(third.getInputStream().readNBytes(thirdReply.length)).isEqualTo(thirdReply);
                }
            }
        }
    }

    @Test
    void rejectsShortAndInvalidFirstRepliesWithoutHanging() throws Exception {
        List<byte[]> invalidReplies = List.of(
                new byte[] {1, 2, 3},
                headerWithLength(15),
                headerWithLength(1024 * 1024 + 1),
                Arrays.copyOf(message(1, bytes(32)), 21));
        for (byte[] invalidReply : invalidReplies) {
            try (ServerSocket server = listen();
                    MongoHandshakeGate gate = new MongoHandshakeGate("127.0.0.1", server.getLocalPort());
                    Socket client = connect(gate.port());
                    Socket upstream = accept(server)) {
                upstream.getOutputStream().write(invalidReply);
                upstream.shutdownOutput();
                assertThat(client.getInputStream().read()).isEqualTo(-1);
                assertThatThrownBy(() -> gate.awaitFirstReply(Duration.ofMillis(1)))
                        .isInstanceOf(AssertionError.class);
            }
        }
    }

    @Test
    void closeTerminatesWorkersWaitingOnACompleteReplyAndClosesBothSocketEnds() throws Exception {
        assertCloseStopsConnection(true);
    }

    @Test
    void closeTerminatesWorkersReadingAPartialReplyAndClosesBothSocketEnds() throws Exception {
        assertCloseStopsConnection(false);
    }

    @Test
    void closeTerminatesAnIdleAcceptorAndCanBeRepeated() throws Exception {
        try (ServerSocket server = listen();
                MongoHandshakeGate gate = new MongoHandshakeGate("127.0.0.1", server.getLocalPort())) {
            int port = gate.port();
            gate.close();
            gate.close();
            assertListenerClosed(port);
            assertThatThrownBy(() -> gate.awaitFirstReply(Duration.ofMillis(1)))
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void refusesAnyUpstreamOtherThanLiteralIpv4Loopback() throws Exception {
        try (ServerSocket server = listen()) {
            assertThatThrownBy(() -> new MongoHandshakeGate("localhost", server.getLocalPort()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new MongoHandshakeGate("192.0.2.1", server.getLocalPort()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static void assertCloseStopsConnection(boolean completeReply) throws Exception {
        try (ServerSocket server = listen();
                MongoHandshakeGate gate = new MongoHandshakeGate("127.0.0.1", server.getLocalPort());
                Socket client = connect(gate.port());
                Socket upstream = accept(server)) {
            byte[] request = message(1, bytes(19));
            client.getOutputStream().write(request);
            assertThat(upstream.getInputStream().readNBytes(request.length)).isEqualTo(request);
            byte[] reply = message(2, bytes(23));
            upstream.getOutputStream().write(reply, 0, completeReply ? reply.length : 8);
            if (completeReply) {
                gate.awaitFirstReply(AWAIT_REPLY_TIMEOUT);
                assertReplyIsHeld(client);
            }
            int port = gate.port();
            // close() waits for its own executor to terminate and fails if any owned worker remains.
            gate.close();
            gate.close();
            assertPeerClosed(client);
            assertPeerClosed(upstream);
            assertListenerClosed(port);
        }
    }

    private static void assertPeerClosed(Socket socket) throws IOException {
        assertThat(socket.isClosed()).as("the observer has not closed its own socket").isFalse();
        try {
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        } catch (SocketException peerReset) {
            // Closing a peer with unread bytes may reset TCP instead of returning EOF. A read
            // timeout is not a SocketException and remains a failure, as does an unclosed worker.
            assertThat(socket.isClosed()).isFalse();
        }
    }

    private static byte[] exchangeRequest(Socket client, Socket upstream, int id) throws IOException {
        byte[] request = message(id, bytes(id));
        client.getOutputStream().write(request);
        assertThat(upstream.getInputStream().readNBytes(request.length)).isEqualTo(request);
        byte[] reply = message(id + 100, id == 13 ? new byte[0] : bytes(id + 3));
        upstream.getOutputStream().write(reply);
        return reply;
    }

    private static void assertReplyIsHeld(Socket client) throws IOException {
        // This bounded read asserts the barrier's absence of output; it is not a readiness delay.
        client.setSoTimeout(100);
        try {
            assertThatThrownBy(() -> client.getInputStream().read())
                    .isInstanceOf(SocketTimeoutException.class);
        } finally {
            client.setSoTimeout(IO_TIMEOUT_MILLIS);
        }
    }

    private static void assertListenerClosed(int port) {
        assertThatThrownBy(() -> {
            try (Socket attempted = connect(port)) {
                throw new AssertionError("The closed gate still accepts TCP connections");
            }
        }).isInstanceOf(IOException.class);
    }

    private static ServerSocket listen() throws IOException {
        ServerSocket server = new ServerSocket(0, 50, loopback());
        server.setSoTimeout(IO_TIMEOUT_MILLIS);
        return server;
    }

    private static Socket connect(int port) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(loopback(), port), IO_TIMEOUT_MILLIS);
            socket.setSoTimeout(IO_TIMEOUT_MILLIS);
            return socket;
        } catch (IOException failure) {
            socket.close();
            throw failure;
        }
    }

    private static Socket accept(ServerSocket server) throws IOException {
        Socket socket = server.accept();
        socket.setSoTimeout(IO_TIMEOUT_MILLIS);
        return socket;
    }

    private static InetAddress loopback() throws IOException {
        return InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    }

    private static byte[] headerWithLength(int length) {
        return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putInt(length).array();
    }

    private static byte[] message(int id, byte[] body) {
        return ByteBuffer.allocate(16 + body.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(16 + body.length).putInt(id).putInt(0).putInt(2013).put(body).array();
    }

    private static byte[] bytes(int length) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) bytes[index] = (byte) (index * 31);
        return bytes;
    }
}
