package io.tapstate.runtime.engine;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestError;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Which axis each of a pipeline's chains puts its bounds on. A bound travels on an axis named by a single
 * byte, so every chain needs a number before the job starts, and the numbering is a property of the job
 * rather than of any one vertex: two vertices numbering the same chain differently would have their bounds
 * combined as though they concerned different chains, and the lowest of two unrelated promises would be
 * taken as the promise for both. It is derived from the chain names in a fixed order for the same reason —
 * an instance numbering chains as it observes them would disagree with an instance that observed them in
 * another order, or never observed one at all.
 *
 * <p>The first axis is spent rather than shared: the engine's own markers, including the one announcing
 * that a queue has gone idle, always carry it, and a chain sharing that axis would have its bound
 * overwritten by one of them.
 *
 * <p>Nothing here is written down. The numbering is in-flight identity only: a restart recomputes it from
 * the same names and arrives at the same answer. It travels to the members with the graph rather than
 * being worked out again on each of them, for the same reason it is not derived from observation: two
 * members that disagree about which chain an axis carries would combine unrelated promises.
 */
public final class ChainAxes implements Serializable {

    /** How many chains a pipeline can draw from: the byte holds 256 axes and the engine keeps the first. */
    public static final int MAX_CHAINS = 255;

    private final Map<String, Byte> axisByChain;
    private final Map<Byte, String> chainByAxis;

    private ChainAxes(Map<String, Byte> axisByChain, Map<Byte, String> chainByAxis) {
        this.axisByChain = Map.copyOf(axisByChain);
        this.chainByAxis = Map.copyOf(chainByAxis);
    }

    /**
     * Numbers {@code chains} from the first axis after the engine's, in name order. Refuses a pipeline
     * drawing from more chains than there are axes: that is a capacity line, diagnosable and refused
     * before the job is built.
     */
    public static ChainAxes assign(Collection<String> chains) {
        List<String> named = chains.stream().distinct().sorted().toList();
        if (named.size() > MAX_CHAINS) {
            throw new TapstateException(NestError.CHAIN_AXIS_LIMIT_EXCEEDED,
                    Map.of("chains", named.size(), "limit", MAX_CHAINS), null);
        }
        Map<String, Byte> axisByChain = new LinkedHashMap<>();
        Map<Byte, String> chainByAxis = new LinkedHashMap<>();
        byte axis = 0;
        for (String chain : named) {
            axis++;
            axisByChain.put(chain, axis);
            chainByAxis.put(axis, chain);
        }
        return new ChainAxes(axisByChain, chainByAxis);
    }

    /** The axis {@code chain} puts its bounds on. */
    public byte axisOf(String chain) {
        Byte axis = axisByChain.get(chain);
        if (axis == null) {
            throw new IllegalArgumentException("this pipeline draws from no chain named " + chain);
        }
        return axis;
    }

    /** The chain whose bounds travel on {@code axis}. */
    public String chainOn(byte axis) {
        String chain = chainByAxis.get(axis);
        if (chain == null) {
            throw new IllegalArgumentException("this pipeline puts no chain on axis " + axis);
        }
        return chain;
    }
}
