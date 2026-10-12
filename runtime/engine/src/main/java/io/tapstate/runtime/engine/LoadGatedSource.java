package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.lifecycle.HoldsChangesForLoads;
import io.tapstate.core.lifecycle.LoadLandings;
import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A source vertex's supplier that hands every processor it makes the gate its changes wait at, and is
 * otherwise the wrapped supplier.
 *
 * <p>What a source's changes wait for is only known once the whole graph is: which sinks its rows reach
 * unassembled, and what those sinks write them into. The source vertex is drawn first, so the gate reaches
 * its processors this way rather than through the supplier that makes them.
 *
 * <p>A processor that cannot hold its changes is passed through as it is. On a member a pinned source does
 * not run on, the processors made are placeholders that read nothing, and nothing of theirs needs holding.
 */
final class LoadGatedSource implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;

    private final ProcessorMetaSupplier delegate;
    private final LoadGate gate;

    private LoadGatedSource(ProcessorMetaSupplier delegate, LoadGate gate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.gate = Objects.requireNonNull(gate, "gate");
    }

    /** {@code delegate}, with every processor it makes holding its changes at {@code gate}. */
    static ProcessorMetaSupplier of(ProcessorMetaSupplier delegate, LoadGate gate) {
        return new LoadGatedSource(delegate, gate);
    }

    /** The gate the source's processors hold their changes at. */
    LoadGate gate() {
        return gate;
    }

    @Override
    public void init(Context context) throws Exception {
        delegate.init(context);
    }

    @Override
    public boolean initIsCooperative() {
        return delegate.initIsCooperative();
    }

    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        Function<? super Address, ? extends ProcessorSupplier> suppliers = delegate.get(addresses);
        return address -> new Gated(suppliers.apply(address), gate);
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

    /**
     * One member's supplier of the source's processors, handing each the loads its changes wait for and where
     * they stand, as read on this member.
     */
    private static final class Gated implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final ProcessorSupplier delegate;
        private final LoadGate gate;
        // Resolved where the processors are made: the record it reads is bound on the member, not carried.
        private transient LoadLandings landings;

        Gated(ProcessorSupplier delegate, LoadGate gate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.gate = gate;
        }

        @Override
        public void init(Context context) throws Exception {
            landings = gate.landingsOn(context.hazelcastInstance());
            delegate.init(context);
        }

        @Override
        public boolean initIsCooperative() {
            return delegate.initIsCooperative();
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            Collection<? extends Processor> processors = delegate.get(count);
            for (Processor processor : processors) {
                if (processor instanceof HoldsChangesForLoads holder) {
                    holder.holdChangesUntil(gate.awaited(), landings);
                }
            }
            return processors;
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
        public List<Permission> permissions() {
            return delegate.permissions();
        }
    }
}
