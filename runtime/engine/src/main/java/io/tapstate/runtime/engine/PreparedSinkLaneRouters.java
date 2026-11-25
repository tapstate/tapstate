package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Prepares once, then distributes the same target equality evidence to every router in the execution. */
final class PreparedSinkLaneRouters implements ProcessorMetaSupplier {
    private static final long serialVersionUID = 1L;

    private final String node;
    private final Map<String, SinkTarget> targets;
    private final PreparesTargets preparation;
    private final ChainAxes axes;
    private final Map<Integer, List<String>> chains;
    private transient ProcessorMetaSupplier delegate;

    private PreparedSinkLaneRouters(String node, Map<String, SinkTarget> targets, PreparesTargets preparation,
            ChainAxes axes, Map<Integer, List<String>> chains) {
        this.node = Objects.requireNonNull(node, "node");
        this.targets = Map.copyOf(targets);
        this.preparation = Objects.requireNonNull(preparation, "preparation");
        this.axes = axes;
        this.chains = chains == null ? Map.of() : chains.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    static ProcessorMetaSupplier of(String node, Map<String, SinkTarget> targets, PreparesTargets preparation,
            ChainAxes axes, Map<Integer, List<String>> chains, int plannedMembers) {
        if (!preparation.supportsParallelKeyMoves()) {
            throw new IllegalArgumentException("sink does not support inspected parallel key moves");
        }
        return PlannedMembersGuard.of(new PreparedSinkLaneRouters(node, targets, preparation, axes, chains),
                plannedMembers);
    }

    @Override
    public void init(Context context) throws Exception {
        var evidence = Map.copyOf(preparation.prepareTargetConstraints(context.hazelcastInstance()));
        String name = node;
        var targetTables = targets;
        var chainAxes = axes;
        var inputChains = chains;
        SupplierEx<Processor> processors = () -> new SinkLaneRouter(name, targetTables, evidence, chainAxes, inputChains);
        delegate = ProcessorMetaSupplier.of(ProcessorSupplier.of(processors));
        delegate.init(context);
    }

    @Override
    public boolean initIsCooperative() {
        return false;
    }

    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        return Objects.requireNonNull(delegate, "router preparation has not completed").get(addresses);
    }

    @Override
    public void close(Throwable failure) throws Exception {
        if (delegate != null) delegate.close(failure);
    }
}
