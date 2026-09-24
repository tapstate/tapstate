package io.tapstate.runtime.srs;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.util.Objects;

/**
 * Which member a source vertex's one instance runs on.
 *
 * <p>A source drains a member-local hand-off: the rows of an initial load, and on a source running with the
 * shared ring switched off every change it captures. The capture fills that hand-off on the member that
 * started it, and nothing carries it anywhere else. So the one instance has to run on that member, and the
 * engine does not know which one that is: left to itself it picks a member at random, and on a cluster the
 * pick is another member as often as not. The instance it starts there finds its hand-off empty, the rows
 * wait on the other member for a reader that never comes, and the job runs on with nothing thrown and
 * nothing crossing.
 *
 * <p>{@link #on(Address)} names the member. {@link #anyMember()} leaves the pick to the engine, which is
 * only right where there is one member to pick, and is written out at each place that relies on that
 * rather than being what a caller gets for saying nothing.
 */
public final class SourcePlacement {

    private static final SourcePlacement ANY_MEMBER = new SourcePlacement(null);

    private final Address member;

    private SourcePlacement(Address member) {
        this.member = member;
    }

    /** Wherever the engine puts it: right only when there is a single member to put it on. */
    public static SourcePlacement anyMember() {
        return ANY_MEMBER;
    }

    /** On {@code member}, which must be the member whose capture fills the hand-off the source drains. */
    public static SourcePlacement on(Address member) {
        return new SourcePlacement(Objects.requireNonNull(member, "member"));
    }

    /** {@code supplier}'s one instance, held to this placement. */
    ProcessorMetaSupplier place(ProcessorSupplier supplier) {
        return member == null
                ? ProcessorMetaSupplier.forceTotalParallelismOne(supplier)
                : ProcessorMetaSupplier.forceTotalParallelismOne(supplier, member);
    }

    @Override
    public String toString() {
        return member == null ? "any member" : "on " + member;
    }
}
