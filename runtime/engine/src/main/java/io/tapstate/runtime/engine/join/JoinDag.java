package io.tapstate.runtime.engine.join;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Draws the state-update and final-projection vertices for one join node. Source edges have distinct
 * ordinals so the state-update vertex can tell which side a change arrived on.
 *
 * <p><b>Every edge is partitioned and distributed, each by the key of the state it is about to
 * change.</b> Fact rows are routed by the fact row's own key, so the mirror entry for one fact row is
 * only ever written from one place and its changes stay in order; dimension rows are routed by the key
 * they are matched on, for the same reason. Final projection is routed by the fact key: changes from
 * different source partitions must be refreshed and published by one ordered processor per output
 * row, or a delayed old image can overwrite a newer match at the sink.
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
                plan, pipelineId, nodeId, factKeyColumns, Map.copyOf(sourceByOrdinal), stores, false)));
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
        Vertex projection = dag.newVertex(nodeId + ":project", ProcessorMetaSupplier.of(new JoinVertexSupplier(
                plan, pipelineId, nodeId, factKeyColumns, Map.of(), stores, true)));
        dag.edge(Edge.from(vertex, nextOutbound.applyAsInt(vertex)).to(projection)
                .partitioned(item -> ((JoinUpdate) item).factKey()).distributed());
        return projection;
    }

    /**
     * One passthrough that gathers several producers of one source, so the join sees a single edge.
     *
     * <p>The gathering is pinned to total parallelism one, which places its only processor on the member
     * owning the vertex's name and leaves every other member running a stand-in that refuses input. An
     * edge handing items to whatever is local therefore delivers everything produced on another member to
     * a stand-in, and the job dies on the first such event - so every edge into it is addressed to the
     * member that runs it. There is no key to spread by here: one processor is the point. On a single
     * member the two are indistinguishable, which is why this is spelled out rather than left to a test.
     */
    private static Vertex merged(DAG dag, Vertex destination, String source, List<Vertex> producers,
            ToIntFunction<Vertex> nextOutbound) {
        String name = destination.getName() + ":" + source;
        Vertex merge = dag.newVertex(name, PassthroughProcessor.metaSupplier(name));
        int ordinal = 0;
        for (Vertex producer : producers) {
            dag.edge(Edge.from(producer, nextOutbound.applyAsInt(producer)).to(merge, ordinal++)
                    .distributed().allToOne(name));
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
        private final boolean projection;
        private transient JoinStores stores;
        private transient JoinGauge gauge;

        private JoinVertexSupplier(JoinPlan plan, String pipelineId, String stepId,
                List<String> factKeyColumns, Map<Integer, String> sourceByOrdinal,
                JoinStoresBinding binding, boolean projection) {
            this.plan = plan;
            this.pipelineId = pipelineId;
            this.stepId = stepId;
            this.factKeyColumns = factKeyColumns;
            this.sourceByOrdinal = sourceByOrdinal;
            this.binding = binding;
            this.projection = projection;
        }

        @Override
        public void init(Context context) {
            stores = binding.bind(context.hazelcastInstance(), pipelineId, stepId);
            if (projection) {
                return;
            }
            // Metered from here and nowhere else: this is the one place a job is what the state is
            // being bound for, and a reading can only be left from a thread running its processors.
            JoinStateStats stats = JoinStateStats.of(context.hazelcastInstance());
            // Handles are kept once obtained, so a reading costs one lookup and one write per number.
            // Concurrent because one gauge serves every processor this vertex runs on this member, and
            // each of them reports from a thread of its own.
            Map<String, Metric> handles = new ConcurrentHashMap<>();
            gauge = new JoinGauge() {
                @Override
                public void bucketWalked(String source, String dimensionKey, int pages) {
                    stats.widestBucket(JoinMaps.reverseIndex(pipelineId, stepId, source), pages);
                }

                /**
                 * Left among the job's own statistics rather than counted on the member. The engine
                 * already collects those on a cadence and hands them out with the job, so a reading
                 * left here is readable from outside the run, and from any member, without a second
                 * channel to keep alive - which a rebuild needs, because whichever member owns the
                 * key's partition is the one doing the work and no other member knows it is happening.
                 *
                 * <p>Under a name carrying the key rather than one slot per namespace: two members
                 * rebuilding two keys would otherwise report one key's name against the other's
                 * progress, which is the one reading here nobody could tell was wrong.
                 */
                @Override
                public void recomputing(String source, String dimensionKey, long rowsDone,
                        long rowsExpected) {
                    if (!JoinRecomputeMetricNames.worthReporting(rowsExpected)) {
                        return;
                    }
                    String namespace = JoinMaps.reverseIndex(pipelineId, stepId, source);
                    // Rendered rather than filed under: a key is matched by an encoding of its columns,
                    // and someone told a rebuild of "AAAAATE" is running cannot say whether it is the row
                    // they just edited - which is the only question this reading is here to answer. Done
                    // past the threshold, so an ordinary edit pays nothing for it.
                    String readable = JoinKey.describe(dimensionKey);
                    handle(JoinRecomputeMetricNames.doneNameOf(namespace, readable)).set(rowsDone);
                    handle(JoinRecomputeMetricNames.expectedNameOf(namespace, readable))
                            .set(rowsExpected);
                }

                private Metric handle(String name) {
                    return handles.computeIfAbsent(name, Metrics::metric);
                }
            };
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(projection
                        ? new JoinProjectionProcessor(new JoinProjection(plan, factKeyColumns, stepId, stores))
                        : new JoinProcessor(new JoinDriver(plan, factKeyColumns, stepId, stores,
                                JoinDriver.DEFAULT_KEYS_PER_READ, gauge), sourceByOrdinal));
            }
            return processors;
        }
    }
}
