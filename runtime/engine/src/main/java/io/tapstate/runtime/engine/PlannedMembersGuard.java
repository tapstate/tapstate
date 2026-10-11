package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Holds a node that runs the same number of processors on every member to the member count its width was
 * worked out for, refusing an execution that starts on any other count before a single processor exists.
 *
 * <p>The width of such a node is its per-member count times the members taking part, and the count was worked
 * out when the graph was drawn. Between the drawing and the start a member can join or leave; the engine then
 * starts the run on the members present, and the node runs a number of processors nobody worked out. Anything
 * counting on the worked-out number - above all the set of writers whose progress the durable position waits
 * on - would then be missing processors that are really writing, and a faster writer's progress could stand
 * for one nobody was waiting on. Refused here, the run never starts, and the next start works the widths out
 * again for the members present then.
 *
 * <p>Everything else is the wrapped supplier's, passed straight through.
 */
final class PlannedMembersGuard implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;

    private final ProcessorMetaSupplier delegate;
    private final int plannedMembers;

    private PlannedMembersGuard(ProcessorMetaSupplier delegate, int plannedMembers) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (plannedMembers < 1) {
            throw new IllegalArgumentException("a run is planned over at least one member, got " + plannedMembers);
        }
        this.plannedMembers = plannedMembers;
    }

    /** {@code delegate}, refused on an execution that does not start on exactly {@code plannedMembers}. */
    static ProcessorMetaSupplier of(ProcessorMetaSupplier delegate, int plannedMembers) {
        return new PlannedMembersGuard(delegate, plannedMembers);
    }

    @Override
    public void init(Context context) throws Exception {
        if (context.memberCount() != plannedMembers) {
            String pipeline = context.jobConfig().getName();
            TapstateException refused = new TapstateException(EngineError.MEMBERSHIP_CHANGED_BEFORE_START,
                    Map.of("pipeline", String.valueOf(pipeline), "planned", plannedMembers,
                            "actual", context.memberCount()),
                    null);
            // Recorded before the engine wraps it, for the reason every processor here records its own
            // failure: once the job's result is terminal the exact cause cannot be read back from it.
            if (context.hazelcastInstance() != null && pipeline != null) {
                JobFailureRegistry.of(context.hazelcastInstance()).record(pipeline, refused);
            }
            throw refused;
        }
        delegate.init(context);
    }

    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        return delegate.get(addresses);
    }

    @Override
    public int preferredLocalParallelism() {
        return delegate.preferredLocalParallelism();
    }

    @Override
    public Permission getRequiredPermission() {
        return delegate.getRequiredPermission();
    }

    @Override
    public Map<String, String> getTags() {
        return delegate.getTags();
    }

    @Override
    public boolean initIsCooperative() {
        return delegate.initIsCooperative();
    }

    @Override
    public boolean closeIsCooperative() {
        return delegate.closeIsCooperative();
    }

    @Override
    public void close(Throwable error) throws Exception {
        delegate.close(error);
    }

    @Override
    public boolean isReusable() {
        return delegate.isReusable();
    }
}
