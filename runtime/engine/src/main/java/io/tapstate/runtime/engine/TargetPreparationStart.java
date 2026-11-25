package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Prepares a sink's target tables as each execution starts, before any writer of that execution exists, and
 * is otherwise the wrapped supplier.
 *
 * <p>The engine initialises the supplier of every vertex on the member coordinating an execution, and only once
 * every one of them has succeeded does it create a single processor anywhere; one that fails fails the
 * execution with no processor created. So a table prepared here is prepared before its first row can be
 * written, once for each execution - including one the engine starts again from the same graph - whichever way
 * the graph came to be submitted.
 */
final class TargetPreparationStart implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;

    private final ProcessorMetaSupplier delegate;
    private final PreparesTargets targets;

    private TargetPreparationStart(ProcessorMetaSupplier delegate, PreparesTargets targets) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.targets = Objects.requireNonNull(targets, "targets");
    }

    /** {@code delegate}, preparing {@code targets}' tables as each execution starts. */
    static ProcessorMetaSupplier of(ProcessorMetaSupplier delegate, PreparesTargets targets) {
        return new TargetPreparationStart(delegate, targets);
    }

    /** What this vertex prepares as each execution starts. */
    PreparesTargets targets() {
        return targets;
    }

    @Override
    public void init(Context context) throws Exception {
        targets.prepareTargets(context.hazelcastInstance());
        delegate.init(context);
    }

    /** Never cooperative: preparing a table is a round trip to the target. */
    @Override
    public boolean initIsCooperative() {
        return false;
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
