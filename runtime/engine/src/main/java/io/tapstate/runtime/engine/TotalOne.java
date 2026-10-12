package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A vertex that runs one processor for the whole cluster, whose stand-ins on the other members pass its bounds on.
 *
 * <p>The engine runs such a vertex's processor on the member owning the vertex's name and a stand-in on every other
 * member, and every edge into the vertex delivers items to that one processor alone. Bounds are not items: each goes
 * to every processor of the vertex, the stand-ins included, and whatever reads the vertex waits for a bound from each
 * of them. The engine's own stand-in passes on only the bound it has combined across all of its edges, and a chain
 * carried by some of them is never promised on the others - so that bound never comes, the stand-in never says
 * anything on the chain, and nothing reading the vertex moves past it again. With one edge in, the combined bound is
 * that edge's own, which is why it shows only on a vertex with more than one: a step over two sources, a nest level
 * over two aliases.
 *
 * <p>So the stand-ins here work the bounds out as the processor they stand in for does - edge by edge, chain by
 * chain, over the edges compiled to carry each chain, and not at all on an edge compiled to carry none - and hold
 * nothing, since they are handed nothing to hold. The processor they stand in for may hold something and answer lower
 * for it; whatever reads the vertex takes the lowest answer of every processor, so a stand-in answering higher lets
 * nothing past a change the real one holds.
 */
final class TotalOne implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;

    private final ProcessorMetaSupplier pinned;
    private final ProcessorSupplier processors;
    private final ProcessorSupplier standIns;

    private TotalOne(ProcessorMetaSupplier pinned, ProcessorSupplier processors, ProcessorSupplier standIns) {
        this.pinned = pinned;
        this.processors = processors;
        this.standIns = standIns;
    }

    /**
     * {@code processors}, running once for the cluster on the member owning {@code vertex}, with stand-ins passing on
     * the bounds of the chains {@code chainsByOrdinal} names everywhere else. A vertex carrying no bounds at all -
     * {@code axes} null - keeps the engine's own stand-ins: its processor passes none on either.
     */
    static ProcessorMetaSupplier passingBounds(ProcessorSupplier processors, String vertex, ChainAxes axes,
            Map<Integer, List<String>> chainsByOrdinal) {
        Objects.requireNonNull(processors, "processors");
        Objects.requireNonNull(vertex, "vertex");
        ProcessorMetaSupplier pinned = ProcessorMetaSupplier.forceTotalParallelismOne(processors, vertex);
        if (axes == null) {
            return pinned;
        }
        Objects.requireNonNull(chainsByOrdinal, "chainsByOrdinal");
        return new TotalOne(pinned, processors, new StandIns(vertex, axes, Map.copyOf(chainsByOrdinal)));
    }

    @Override
    public void init(Context context) throws Exception {
        pinned.init(context);
    }

    @Override
    public boolean initIsCooperative() {
        return pinned.initIsCooperative();
    }

    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        Function<? super Address, ? extends ProcessorSupplier> placed = pinned.get(addresses);
        // The engine hands the member it pins the vertex to the very supplier it was given, and every other member
        // a stand-in. Which member is which is its decision alone - the same one that routes every edge into the
        // vertex - so it is read off here rather than worked out a second time.
        return address -> {
            ProcessorSupplier supplier = placed.apply(address);
            return supplier == processors ? supplier : standIns;
        };
    }

    @Override
    public int preferredLocalParallelism() {
        return pinned.preferredLocalParallelism();
    }

    @Override
    public Permission getRequiredPermission() {
        return pinned.getRequiredPermission();
    }

    @Override
    public Map<String, String> getTags() {
        return pinned.getTags();
    }

    @Override
    public boolean closeIsCooperative() {
        return pinned.closeIsCooperative();
    }

    @Override
    public void close(Throwable error) throws Exception {
        pinned.close(error);
    }

    @Override
    public boolean isReusable() {
        return pinned.isReusable();
    }

    /** The stand-ins of one member where the vertex does not run. */
    static final class StandIns implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final String vertex;
        private final ChainAxes axes;
        private final Map<Integer, List<String>> chainsByOrdinal;

        StandIns(String vertex, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal) {
            this.vertex = vertex;
            this.axes = axes;
            this.chainsByOrdinal = chainsByOrdinal;
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> made = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                made.add(new BoundsStandIn(vertex, new LevelBounds(chainsByOrdinal, axes, LevelBounds.HOLDS_NOTHING),
                        chainsByOrdinal.keySet()));
            }
            return made;
        }
    }

    /** A processor where the vertex does not run: it takes no input, and passes on what each edge promised. */
    static final class BoundsStandIn extends AbstractProcessor {

        private final String vertex;
        private final LevelBounds bounds;
        private final Set<Integer> answered;

        BoundsStandIn(String vertex, LevelBounds bounds, Set<Integer> answered) {
            this.vertex = vertex;
            this.bounds = bounds;
            this.answered = answered;
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            throw new IllegalStateException("vertex '" + vertex + "' runs its one processor on another member, and "
                    + "every edge into it has to deliver there; ordinal " + ordinal + " delivered here: " + item);
        }

        /** What the edge at {@code ordinal} promised, on the chains it carries; an edge carrying none, nothing. */
        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            return !answered.contains(ordinal) || bounds.advance(ordinal, watermark, this::tryEmit);
        }

        /**
         * Refuses to pass on the bound combined across every edge: it is no promise this vertex makes, as the one
         * it stands in for says. The engine forwards it by default, silently, which is why saying otherwise is
         * explicit.
         */
        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            return true;
        }
    }
}
