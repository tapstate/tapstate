package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

class MongoBeforeInsertWitnessTest {
    private final ConnectionDescription connection = new ConnectionDescription(
            new ServerId(new ClusterId(), new ServerAddress("127.0.0.1", 27017)));

    @Test
    void captures_only_the_armed_insert_and_matches_its_connection_and_request() {
        MongoBeforeInsertWitness witness = new MongoBeforeInsertWitness();
        BsonTimestamp actual = new BsonTimestamp(1_792_251_694, 17);
        witness.commandStarted(started(1, "insert"));
        witness.commandSucceeded(succeeded(1, "insert", actual, connection));
        assertThat(witness.operationTime()).isNull();
        witness.capture(() -> {
            witness.commandStarted(started(2, "drop"));
            witness.commandSucceeded(succeeded(2, "drop", new BsonTimestamp(1, 1), connection));
            assertThat(witness.operationTime()).isNull();
            witness.commandStarted(started(3, "insert"));
            witness.commandSucceeded(succeeded(4, "insert", new BsonTimestamp(2, 1), connection));
            ConnectionDescription other = new ConnectionDescription(
                    new ServerId(new ClusterId(), new ServerAddress("127.0.0.1", 27018)));
            witness.commandSucceeded(succeeded(3, "insert", new BsonTimestamp(3, 1), other));
            assertThat(witness.operationTime()).isNull();
            witness.commandSucceeded(succeeded(3, "insert", actual, connection));
        });
        assertThat(witness.operationTime()).isSameAs(actual);
        witness.commandStarted(started(5, "insert"));
        witness.commandSucceeded(succeeded(5, "insert", new BsonTimestamp(4, 1), connection));
        assertThat(witness.operationTime()).isSameAs(actual);
    }

    @Test
    void failed_attempts_do_not_become_acknowledged_and_the_same_insert_failure_escapes() {
        MongoBeforeInsertWitness witness = new MongoBeforeInsertWitness();
        AssertionError failure = new AssertionError("insert failure");
        assertThatThrownBy(() -> witness.capture(() -> {
            witness.commandStarted(started(7, "insert"));
            witness.commandFailed(new CommandFailedEvent(null, 1L, 7, connection,
                    "source", "insert", 1L, failure));
            witness.commandSucceeded(succeeded(7, "insert", new BsonTimestamp(1, 1), connection));
            throw failure;
        })).isSameAs(failure);
        assertThat(witness.operationTime()).isNull();
    }

    private CommandStartedEvent started(int request, String command) {
        return new CommandStartedEvent(null, 1L, request, connection, "source", command, new BsonDocument());
    }

    private CommandSucceededEvent succeeded(int request, String command, BsonTimestamp time,
            ConnectionDescription receivedConnection) {
        return new CommandSucceededEvent(null, 1L, request, receivedConnection, "source", command,
                new BsonDocument("operationTime", time), 1L);
    }
}
