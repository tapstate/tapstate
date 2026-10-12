package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.security.Permission;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Starts the accounting an execution lands its progress under before any writer of that execution exists,
 * and is otherwise the wrapped supplier.
 *
 * <p>How far a pipeline has landed a chain is the lowest of what every writer that chain reaches has landed,
 * so the writers are written down first. The engine initialises the supplier of every vertex on the member
 * coordinating an execution before it creates a single processor anywhere, so starting the accounting here
 * puts it ahead of every writer of that execution - of every execution, including one the engine starts
 * again from the same graph, which starts its accounting again rather than inheriting a finished one.
 *
 * <p>One vertex of a graph carries it, whichever sink the graph draws first: the set it writes down is the
 * whole graph's, and a second vertex writing the same set would only write it twice.
 */
final class WriterRunStart implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;

    private final ProcessorMetaSupplier delegate;
    private final SinkAckFactory acks;
    private final Map<String, List<String>> writersByChain;

    private WriterRunStart(ProcessorMetaSupplier delegate, SinkAckFactory acks,
            Map<String, List<String>> writersByChain) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.acks = Objects.requireNonNull(acks, "acks");
        Map<String, List<String>> copy = new LinkedHashMap<>();
        writersByChain.forEach((chain, writers) -> copy.put(chain, List.copyOf(writers)));
        this.writersByChain = Collections.unmodifiableMap(copy);
    }

    /** {@code delegate}, starting {@code acks}' accounting of {@code writersByChain} as each execution starts. */
    static ProcessorMetaSupplier of(ProcessorMetaSupplier delegate, SinkAckFactory acks,
            Map<String, List<String>> writersByChain) {
        return new WriterRunStart(delegate, acks, writersByChain);
    }

    /** The writers each chain is expected to reach, as this vertex writes them down. */
    Map<String, List<String>> writersByChain() {
        return writersByChain;
    }

    @Override
    public void init(Context context) throws Exception {
        acks.beginRun(context.hazelcastInstance(), writersByChain);
        delegate.init(context);
    }

    /** Never cooperative: starting the accounting is a round trip to the coordination store. */
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
