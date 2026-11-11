package io.tapstate.core.lifecycle;

import java.util.OptionalDouble;

/**
 * Whether a nest namespace has stopped being served from memory, which nothing else about a running
 * pipeline says out loud.
 *
 * <p>Per-key state is a buffer that backpressure cannot see. An event being consumed makes the resident
 * state larger and fills no edge queue, so a namespace whose working set has outgrown what it is given to
 * hold goes on reporting short queues, flat lag and the throughput it always had, while every event it
 * handles has quietly become a disk seek. There is no limit left for it to cross either — the budget is one
 * number and everything past it lives on a layer with no limit at all — so nothing fails, and the only
 * account of it is a ratio nobody was told to watch.
 *
 * <p>Which ratio is the whole of the judgement here. Two are published side by side and only one of them
 * can trip this:
 *
 * <ul>
 *   <li><b>How much of the reaching went to disk</b> is the symptom itself, and is what this reads.</li>
 *   <li><b>How much of the namespace is resident</b> explains it but cannot raise it. Almost nothing being
 *       resident is the intended shape rather than the alarm: a namespace whose reaching lands on its hot
 *       keys is doing exactly what one number of memory in front of an unlimited layer was set up to do.
 *       Raising an alarm on that would fire on every correctly sized deployment, which is the fastest way
 *       to make an alarm mean nothing. It is carried alongside instead, because it is the first thing
 *       whoever reads the alarm will want.</li>
 * </ul>
 *
 * <p>A floor on how much traffic a window must have carried before it is judged at all, because a ratio
 * drawn from single digits is not a measurement. Three reaches of which two missed is not a namespace under
 * pressure, it is three reaches, and for a namespace nobody is touching most windows look like that.
 *
 * @param servedFromCold the share of a window's reaching that going to the layer behind memory may reach
 *        before it is worth saying so, between 0 and 1
 * @param leastAccesses how much reaching a window must carry before its ratio is judged at all
 */
public record NestColdLayerPressure(double servedFromCold, long leastAccesses) {

    /**
     * Half of a window's reaching going to storage, over at least a hundred reaches. Provisional, and one
     * of the two numbers here is provisional in a way the other is not: the floor only has to be past where
     * single digits produce ratios, while the share is a stand-in until a namespace's memory can be sized
     * against the bytes it actually holds rather than the entries it counts. Half is chosen because it is
     * far from where a correctly sized namespace sits and near where a storage layer's random reads become
     * the thing setting the pace.
     */
    public static final NestColdLayerPressure DEFAULT = new NestColdLayerPressure(0.5, 100);

    public NestColdLayerPressure {
        if (!(servedFromCold >= 0 && servedFromCold <= 1)) {
            throw new IllegalArgumentException("servedFromCold must be a share between 0 and 1: " + servedFromCold);
        }
        if (leastAccesses < 1) {
            throw new IllegalArgumentException("leastAccesses must be at least 1: " + leastAccesses);
        }
    }

    /**
     * Whether {@code window} shows the namespace being served from the layer behind its memory rather than
     * from the memory. A window that carried too little traffic to judge, or none at all, is not over: the
     * threshold is met from below by a continuous quantity, so reaching it exactly is reaching it.
     */
    public boolean isOver(NestStateWindow window) {
        if (window.pressureAccesses() < leastAccesses) {
            return false;
        }
        OptionalDouble share = window.servedFromCold();
        return share.isPresent() && share.getAsDouble() >= servedFromCold;
    }
}
