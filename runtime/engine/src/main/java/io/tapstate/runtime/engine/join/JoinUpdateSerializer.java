package io.tapstate.runtime.engine.join;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.tapstate.core.event.Envelope;
import java.io.IOException;

/**
 * How a join's update crosses a member boundary: the fact key it is routed by, and the change, written by the
 * change's own serializer.
 *
 * <p>A join hands each update to the projection that shapes it, routed by the update's fact key, so on more than
 * one member most updates go to another member and are written for the crossing. Without a serializer of its own
 * an update falls back to Java's, which cannot write the change inside it, and the job dies on the first update
 * routed away - on more than one member only, and only once the placement puts the two on different members.
 *
 * <p>Like the change it carries, this is a transport form and not a stored one: updates exist on the edge between
 * the join and its projection and nowhere else.
 */
public final class JoinUpdateSerializer implements StreamSerializer<JoinUpdate> {

    /** The type id for this serializer; must be unique across the platform's Hazelcast serialization config. */
    public static final int TYPE_ID = 10003;

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public void write(ObjectDataOutput out, JoinUpdate update) throws IOException {
        out.writeString(update.factKey());
        out.writeObject(update.event());
    }

    @Override
    public JoinUpdate read(ObjectDataInput in) throws IOException {
        String factKey = in.readString();
        Envelope event = in.readObject();
        return new JoinUpdate(factKey, event);
    }
}
