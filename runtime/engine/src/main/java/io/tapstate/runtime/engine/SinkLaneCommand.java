package io.tapstate.runtime.engine;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import io.tapstate.core.event.Envelope;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** Execution-local commands that share the FIFO edge of the writer they address. */
sealed interface SinkLaneCommand {
    String lane();

    SinkLaneAcks.Ticket reply();

    /** Confirms that every row preceding this command has been accepted by the target. */
    record Fence(String lane, SinkLaneAcks.Ticket reply) implements SinkLaneCommand, Serializable {
        public Fence {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(reply, "reply");
        }
    }

    /** Applies one native key update after its predecessor fences have been confirmed. */
    final class Move implements SinkLaneCommand, DataSerializable {
        private String lane;
        private Envelope row;
        private SinkLaneAcks.Ticket reply;

        public Move() {}

        Move(String lane, Envelope row, SinkLaneAcks.Ticket reply) {
            this.lane = Objects.requireNonNull(lane, "lane");
            this.row = Objects.requireNonNull(row, "row");
            this.reply = Objects.requireNonNull(reply, "reply");
        }

        @Override public String lane() { return lane; }
        public Envelope row() { return row; }
        @Override public SinkLaneAcks.Ticket reply() { return reply; }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(lane);
            out.writeObject(row);
            out.writeObject(reply);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            lane = in.readObject();
            row = in.readObject();
            reply = in.readObject();
        }
    }
}
