package io.tapstate.e2e;

import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonTimestamp;

/** Observes the existing synchronous insert's acknowledged operation time without another command. */
final class MongoBeforeInsertWitness implements CommandListener {
    private final AtomicBoolean armed = new AtomicBoolean();
    private final AtomicReference<Request> pending = new AtomicReference<>();
    private final AtomicReference<BsonTimestamp> acknowledged = new AtomicReference<>();

    void capture(Runnable insert) {
        acknowledged.set(null);
        armed.set(true);
        try {
            insert.run();
        } finally {
            armed.set(false);
            pending.set(null);
        }
    }

    BsonTimestamp operationTime() {
        return acknowledged.get();
    }

    @Override public void commandStarted(CommandStartedEvent event) {
        if (armed.get() && "insert".equals(event.getCommandName())) {
            pending.set(new Request(event.getRequestId(), event.getConnectionDescription().getConnectionId()));
        }
    }

    @Override public void commandSucceeded(CommandSucceededEvent event) {
        Request request = pending.get();
        if (request != null && request.matches(event.getRequestId(),
                event.getConnectionDescription().getConnectionId()) && "insert".equals(event.getCommandName())) {
            var time = event.getResponse().get("operationTime");
            if (time != null && time.isTimestamp()) acknowledged.set(time.asTimestamp());
            pending.compareAndSet(request, null);
        }
    }

    @Override public void commandFailed(CommandFailedEvent event) {
        Request request = pending.get();
        if (request != null && request.matches(event.getRequestId(),
                event.getConnectionDescription().getConnectionId())) pending.compareAndSet(request, null);
    }

    private record Request(int id, ConnectionId connection) {
        boolean matches(int otherId, ConnectionId otherConnection) {
            return id == otherId && connection.equals(otherConnection);
        }
    }
}
