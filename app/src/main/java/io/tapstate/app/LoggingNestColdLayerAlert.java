package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.NestStateWindow;
import io.tapstate.runtime.engine.nest.NestError;
import io.tapstate.runtime.scheduler.NestColdLayerAlert;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Says that a nest namespace has stopped being served from memory, and later that it is again.
 *
 * <p>This is the whole of the signal. A namespace whose working set has outgrown the memory it is given
 * goes on reporting short queues, flat lag and steady throughput while every event it handles has become a
 * round trip to storage — per-key state fills no edge queue, so there is nothing for backpressure to push
 * back on and nothing else that moves. Nobody is going to notice this by watching, so it is said.
 *
 * <p>The crossing is coded and the recovery is not, because there is no severity that means "this is fine"
 * and dressing an all-clear as a warning would be worse than not coding it. The recovery names the code
 * anyway: whoever went looking did so by the code, and a withdrawal their search does not return is a
 * withdrawal they never read.
 *
 * <p>Numbers are formatted in a fixed locale rather than the process default. Which locale a server boots
 * under is nobody's deliberate choice, and a decimal comma is not cosmetic in a line that gets grepped.
 */
final class LoggingNestColdLayerAlert implements NestColdLayerAlert {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingNestColdLayerAlert.class);

    /** What a number that is not there is called, so a missing one does not read as a truncated line. */
    private static final String UNKNOWN = "unknown";

    @Override
    public void crossed(String pipelineId, String namespace, NestStateWindow window) {
        // Built rather than thrown: the code names what is happening and the pipeline goes on running, which
        // is what this severity means. Throwing it would stop a job over state that is merely slow.
        TapstateException coded =
                new TapstateException(NestError.STATE_SERVED_FROM_COLD_LAYER, argsOf(namespace, window), null);
        LOG.warn("{} (pipeline {})", coded.getMessage(), pipelineId);
    }

    @Override
    public void cleared(String pipelineId, String namespace, NestStateWindow window) {
        LOG.info("{} no longer applies to {} of pipeline {}: {} cold loader callbacks for {} logical "
                        + "state accesses; cold key share {}",
                NestError.STATE_SERVED_FROM_COLD_LAYER.code(), namespace, pipelineId,
                window.backfills(), window.accesses(), share(window));
    }

    private static Map<String, Object> argsOf(String namespace, NestStateWindow window) {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("namespace", namespace);
        args.put("backfills", window.backfills());
        args.put("accesses", window.accesses());
        args.put("share", share(window));
        args.put("millis", millis(window));
        args.put("entries", window.entries());
        args.put("stored", stored(window));
        args.put("resident", resident(window));
        return args;
    }

    /**
     * How much of the namespace is in memory. The share above says the reaching is going to storage; this
     * says how far from resident the namespace is, which is the first thing anyone reading the other number
     * wants and the only one of the two that suggests what to do about it.
     */
    private static String resident(NestStateWindow window) {
        OptionalDouble resident = window.resident();
        return resident.isEmpty() ? UNKNOWN : String.format(Locale.ROOT, "%.1f%%", resident.getAsDouble() * 100);
    }

    /** How much of the window's reaching went to storage, as a whole percent. */
    private static String share(NestStateWindow window) {
        OptionalDouble share = window.servedFromCold();
        return share.isEmpty() ? UNKNOWN : String.format(Locale.ROOT, "%.0f%%", share.getAsDouble() * 100);
    }

    /** What one trip to storage cost on average, which is what turns the share into a time. */
    private static String millis(NestStateWindow window) {
        OptionalDouble millis = window.millisPerBackfill();
        return millis.isEmpty() ? UNKNOWN : String.format(Locale.ROOT, "%.1fms", millis.getAsDouble());
    }

    /** How much the namespace holds altogether, where there is a layer behind the memory to ask. */
    private static String stored(NestStateWindow window) {
        OptionalLong stored = window.stored();
        return stored.isEmpty() ? UNKNOWN : Long.toString(stored.getAsLong());
    }
}
