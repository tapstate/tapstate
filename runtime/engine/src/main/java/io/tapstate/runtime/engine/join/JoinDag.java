package io.tapstate.runtime.engine.join;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.runtime.engine.PassthroughProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Draws the vertex and edges one join node compiles to: one vertex, one inbound edge per source the
 * plan reads, and an ordinal per source so the vertex can tell which side a change arrived on.
 *
 * <p><b>Every edge is partitioned and distributed, each by the key of the state it is about to
 * change.</b> Fact rows are routed by the fact row's own key, so the mirror entry for one fact row is
 * only ever written from one place and its changes stay in order; dimension rows are routed by the key
 * they are matched on, for the same reason. Nothing has to be co-located with what it <em>reads</em> -
 * the distributed maps answer a key from wherever it is - so the routing carries no other duty.
 *
 * <p>A row that is being removed carries its values in its earlier image rather than its later one, so
 * the key is read from whichever image the change has. Reading only the later one would route every
 * deletion to the partition of a row that is not there.
 */
public final class JoinDag {

    private JoinDag() {
    }

    /**
     * Builds the node into {@code dag} and returns the vertex the rest of the pipeline reads from.
     *
     * @param sourceUpstream what vertices produce each source the plan names
     * @param factKeyColumns the driving source's own key columns, which the fact mirror files under
     */
    public static Vertex attach(DAG dag, JoinPlan plan, String pipelineId, String nodeId,
            List<String> factKeyColumns, Function<String, List<Vertex>> sourceUpstream,
            ToIntFunction<Vertex> nextOutbound, JoinStoresBinding stores) {
        Map<Integer, String> sourceByOrdinal = new LinkedHashMap<>();
        Map<String, List<String>> keyColumns = new LinkedHashMap<>();
        String factSource = plan.factSource().name();
        keyColumns.put(factSource, factKeyColumns);
        sourceByOrdinal.put(0, factSource);
        int ordinal = 1;
        for (JoinTree.Source source : plan.from().sources()) {
            if (source.name().equals(factSource)) {
                continue;
            }
            sourceByOrdinal.put(ordinal++, source.name());
            keyColumns.put(source.name(), dimensionKeyColumns(plan.from(), source.name()));
        }

        Vertex vertex = dag.newVertex(nodeId, ProcessorMetaSupplier.of(new JoinVertexSupplier(
                plan, pipelineId, nodeId, factKeyColumns, Map.copyOf(sourceByOrdinal), stores)));
        sourceByOrdinal.forEach((edge, source) -> {
            List<Vertex> producers = sourceUpstream.apply(source);
            if (producers == null || producers.isEmpty()) {
                throw new IllegalStateException("join source '" + source + "' resolved to no vertex");
            }
            Vertex producer = producers.size() == 1 ? producers.get(0)
                    : merged(dag, vertex, source, producers, nextOutbound);
            dag.edge(Edge.from(producer, nextOutbound.applyAsInt(producer)).to(vertex, edge)
                    .partitioned(keyOf(keyColumns.get(source))).distributed());
        });
        return vertex;
    }

    /** One passthrough that gathers several producers of one source, so the join sees a single edge. */
    private static Vertex merged(DAG dag, Vertex destination, String source, List<Vertex> producers,
            ToIntFunction<Vertex> nextOutbound) {
        Vertex merge = dag.newVertex(destination.getName() + ":" + source,
                PassthroughProcessor.metaSupplier());
        int ordinal = 0;
        for (Vertex producer : producers) {
            dag.edge(Edge.from(producer, nextOutbound.applyAsInt(producer)).to(merge, ordinal++));
        }
        return merge;
    }

    /** The columns one dimension source is matched on, in the order the plan's key pairs name them. */
    private static List<String> dimensionKeyColumns(JoinTree tree, String source) {
        List<String> columns = new ArrayList<>();
        collectKeyColumns(tree, source, columns);
        if (columns.isEmpty()) {
            throw new IllegalStateException("join source '" + source + "' is matched on nothing");
        }
        return List.copyOf(columns);
    }

    private static void collectKeyColumns(JoinTree node, String source, List<String> into) {
        if (!(node instanceof JoinTree.Join join)) {
            return;
        }
        for (JoinTree.KeyPair pair : join.on()) {
            if (pair.right().source().equals(source)) {
                into.add(pair.right().column());
            } else if (pair.left().source().equals(source)) {
                into.add(pair.left().column());
            }
        }
        collectKeyColumns(join.left(), source, into);
        collectKeyColumns(join.right(), source, into);
    }

    /**
     * The key a change is routed by: the named columns of whichever image the change carries. A row
     * being removed has only its earlier image, and routing that by the image it does not have would
     * send every deletion to a partition that holds nothing about it.
     */
    private static FunctionEx<Object, Object> keyOf(List<String> columns) {
        return item -> {
            Envelope event = (Envelope) item;
            Map<String, Object> row = event.after() != null ? event.after() : event.before();
            if (row == null) {
                return "";
            }
            List<Object> values = new ArrayList<>(columns.size());
            for (String column : columns) {
                values.add(row.get(column));
            }
            JoinKey key = JoinKey.of(values);
            // A key with a null in it matches nothing, so where it lands is free - but it still has to
            // land somewhere, and every such row landing together would be a hot partition made of rows
            // that will never match anything.
            return key.matchable() ? key.name() : values.toString();
        };
    }

    /**
     * Supplies one join vertex's processors on one member. It exists rather than a plain supplier
     * because the state is bound to the member and cannot be serialized onto the graph: only the
     * instruction for building it travels, and it is turned into the real thing here, once per member.
     */
    private static final class JoinVertexSupplier implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final JoinPlan plan;
        private final String pipelineId;
        private final String stepId;
        private final List<String> factKeyColumns;
        private final Map<Integer, String> sourceByOrdinal;
        private final JoinStoresBinding binding;
        private transient JoinStores stores;

        private JoinVertexSupplier(JoinPlan plan, String pipelineId, String stepId,
                List<String> factKeyColumns, Map<Integer, String> sourceByOrdinal,
                JoinStoresBinding binding) {
            this.plan = plan;
            this.pipelineId = pipelineId;
            this.stepId = stepId;
            this.factKeyColumns = factKeyColumns;
            this.sourceByOrdinal = sourceByOrdinal;
            this.binding = binding;
        }

        @Override
        public void init(Context context) {
            stores = binding.bind(context.hazelcastInstance(), pipelineId, stepId);
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(new JoinProcessor(
                        new JoinDriver(plan, factKeyColumns, stepId, stores), sourceByOrdinal));
            }
            return processors;
        }
    }
}
