package io.tapstate.testsupport;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.CRC32C;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(10)
class MongoProfileRequestGateTest {
    private static final String DATABASE = "owned_store";
    private static final String COLLECTION = "cluster_execution_profiles";
    private static final String CLUSTER = "owned-cluster";
    private static final Duration BOUND = Duration.ofSeconds(3);

    @Test
    void holdsACompleteRequestBeforeAnyUpstreamByteAndThenForwardsExactBytes() throws Exception {
        try (ServerSocket server = listener();
                MongoHandshakeGate gate = gate(server);
                Socket client = connect(gate.port()); Socket upstream = server.accept()) {
            byte[] request = message(command(), 11);
            client.getOutputStream().write(request, 0, 9);
            assertNoUpstreamBytes(upstream);
            client.getOutputStream().write(request, 9, request.length - 9);
            gate.awaitProfileReservation(BOUND);
            assertNoUpstreamBytes(upstream);
            assertThat(gate.capturedProfileRequests()).isEqualTo(1);
            assertThat(gate.forwardedProfileRequests()).isZero();

            gate.release();
            assertThat(upstream.getInputStream().readNBytes(request.length)).isEqualTo(request);
            byte[] reply = message(BsonDocument.parse("{\"ok\":1}"), 12);
            upstream.getOutputStream().write(reply);
            assertThat(client.getInputStream().readNBytes(reply.length)).isEqualTo(reply);
            byte[] second = message(command(), 13);
            client.getOutputStream().write(second);
            assertThat(upstream.getInputStream().readNBytes(second.length)).isEqualTo(second);
        }
    }

    @Test
    void nonmatchingFieldsAndOtherCommandsFlowWithoutBecomingTheReservationBarrier() throws Exception {
        List<BsonDocument> controls = new ArrayList<>();
        for (String field : List.of("$db", "findAndModify", "startTransaction", "autocommit", "query", "update")) {
            BsonDocument value = command();
            value.remove(field);
            controls.add(value);
        }
        controls.add(BsonDocument.parse("{\"hello\":1,\"$db\":\"admin\"}"));
        BsonDocument otherDatabase = command(); otherDatabase.put("$db", new org.bson.BsonString("other_store")); controls.add(otherDatabase);
        BsonDocument otherCollection = command(); otherCollection.put("findAndModify", new org.bson.BsonString("other_collection")); controls.add(otherCollection);
        BsonDocument otherCluster = command(); otherCluster.getDocument("query").put("_id", new org.bson.BsonString("other-cluster")); controls.add(otherCluster);
        BsonDocument noTransaction = command(); noTransaction.put("startTransaction", org.bson.BsonBoolean.FALSE); controls.add(noTransaction);
        BsonDocument autoCommit = command(); autoCommit.put("autocommit", org.bson.BsonBoolean.TRUE); controls.add(autoCommit);
        BsonDocument wrongIncrement = command(); wrongIncrement.getDocument("update").getDocument("$inc").put("writeSerial", new org.bson.BsonInt64(2)); controls.add(wrongIncrement);
        BsonDocument wrongType = command(); wrongType.getDocument("update").getDocument("$inc").put("writeSerial", new org.bson.BsonString("1")); controls.add(wrongType);
        BsonDocument extraQuery = command(); extraQuery.getDocument("query").put("generation", new org.bson.BsonInt64(1)); controls.add(extraQuery);
        try (ServerSocket server = listener(); MongoHandshakeGate gate = gate(server);
                Socket client = connect(gate.port()); Socket upstream = server.accept()) {
            int id = 21;
            for (BsonDocument document : controls) {
                byte[] request = message(document, id++);
                client.getOutputStream().write(request);
                assertThat(upstream.getInputStream().readNBytes(request.length)).isEqualTo(request);
            }
            assertThat(gate.capturedProfileRequests()).isZero();
            assertThat(gate.forwardedProfileRequests()).isZero();
            assertThatThrownBy(() -> gate.awaitProfileReservation(Duration.ofMillis(1))).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void everySelectedConnectionWaitsAndCloseStopsTheOwnedBeforeDispatchWaiters() throws Exception {
        try (ServerSocket server = listener(); MongoHandshakeGate gate = gate(server);
                Socket first = connect(gate.port()); Socket firstUpstream = server.accept();
                Socket second = connect(gate.port()); Socket secondUpstream = server.accept()) {
            first.getOutputStream().write(message(command(), 31));
            second.getOutputStream().write(message(command(), 32));
            gate.awaitProfileReservations(2, BOUND);
            assertNoUpstreamBytes(firstUpstream);
            assertNoUpstreamBytes(secondUpstream);
            assertThat(gate.forwardedProfileRequests()).isZero();
            gate.close();
            assertThat(firstUpstream.getInputStream().read()).isEqualTo(-1);
            assertThat(secondUpstream.getInputStream().read()).isEqualTo(-1);
        }
    }

    @Test
    void checksumOnlyFlagsStillHoldAValidProfileRequestAndRejectAnInvalidChecksum() throws Exception {
        byte[] request = checksum(message(command(), 37));
        try (ServerSocket server = listener(); MongoHandshakeGate gate = gate(server);
                Socket client = connect(gate.port()); Socket upstream = server.accept()) {
            client.getOutputStream().write(request);
            gate.awaitProfileReservation(BOUND);
            assertThat(gate.forwardedProfileRequests()).isZero();
            assertNoUpstreamBytes(upstream);
            gate.release();
            assertThat(upstream.getInputStream().readNBytes(request.length)).isEqualTo(request);
        }
        request[request.length - 1] ^= 1;
        try (ServerSocket server = listener(); MongoHandshakeGate gate = gate(server);
                Socket client = connect(gate.port()); Socket upstream = server.accept()) {
            client.getOutputStream().write(request);
            assertThat(upstream.getInputStream().read()).isEqualTo(-1);
            assertThat(gate.capturedProfileRequests()).isZero();
        }
    }

    @Test
    void malformedOpMsgCannotQualifyOrReachTheBackend() throws Exception {
        byte[] request = message(command(), 41);
        request[20] = 9;
        try (ServerSocket server = listener(); MongoHandshakeGate gate = gate(server);
                Socket client = connect(gate.port()); Socket upstream = server.accept()) {
            client.getOutputStream().write(request);
            assertThat(upstream.getInputStream().read()).isEqualTo(-1);
            assertThat(gate.capturedProfileRequests()).isZero();
            assertThat(gate.forwardedProfileRequests()).isZero();
        }
    }

    private static MongoHandshakeGate gate(ServerSocket server) throws IOException {
        return MongoHandshakeGate.beforeProfileReservation("127.0.0.1", server.getLocalPort(), DATABASE, COLLECTION, CLUSTER);
    }
    private static BsonDocument command() {
        return BsonDocument.parse("{\"findAndModify\":\"cluster_execution_profiles\",\"query\":{\"_id\":\"owned-cluster\"},"
                + "\"update\":{\"$inc\":{\"writeSerial\":{\"$numberLong\":\"1\"}}},\"new\":true,"
                + "\"startTransaction\":true,\"autocommit\":false,\"$db\":\"owned_store\"}");
    }
    private static byte[] message(BsonDocument document, int id) {
        ByteBuffer body = new RawBsonDocument(document, new BsonDocumentCodec()).getByteBuffer().asNIO();
        byte[] request = new byte[21 + body.remaining()];
        ByteBuffer buffer = ByteBuffer.wrap(request).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(request.length).putInt(id).putInt(0).putInt(2013).putInt(0).put((byte) 0).put(body);
        return request;
    }
    private static byte[] checksum(byte[] original) {
        byte[] message = Arrays.copyOf(original, original.length + Integer.BYTES);
        ByteBuffer buffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0, message.length).putInt(16, 1);
        CRC32C checksum = new CRC32C();
        checksum.update(message, 0, message.length - Integer.BYTES);
        buffer.putInt(message.length - Integer.BYTES, (int) checksum.getValue());
        return message;
    }
    private static void assertNoUpstreamBytes(Socket upstream) throws IOException {
        upstream.setSoTimeout(100);
        try { assertThatThrownBy(() -> upstream.getInputStream().read()).isInstanceOf(SocketTimeoutException.class); }
        finally { upstream.setSoTimeout(3_000); }
    }
    private static ServerSocket listener() throws IOException {
        ServerSocket listener = new ServerSocket(0, 50, InetAddress.getByAddress(new byte[] {127,0,0,1}));
        listener.setSoTimeout(3_000);
        return listener;
    }
    private static Socket connect(int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127,0,0,1}), port), 3_000);
        socket.setSoTimeout(3_000);
        return socket;
    }
}
