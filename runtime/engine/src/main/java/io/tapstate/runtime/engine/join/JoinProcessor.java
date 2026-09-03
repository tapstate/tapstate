package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.AbstractProcessor;
import io.tapstate.core.event.Envelope;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The join as a vertex: changes arrive on one edge per source, and what goes out is the changelog of
 * the flat rows they affect.
 *
 * <p><b>It is asked to run again when nothing is arriving, and that is a requirement rather than an
 * optimisation.</b> A change to one dimension row can mean a million rows have to be built again, and
 * that work outlives the change that caused it. If the only moment it were pushed were the arrival of
 * the next change, a stream that has caught up - which is the ordinary state of a stream - would leave
 * the rest of that million unsent for ever: the job running, no errors, the target table half updated
 * and reading as though it had settled. The substrate's idle call is what finishes it, and three other
 * vertices in this runtime already rely on the same one.
 *
 * <p><b>A change is taken into the state once, however many times the item is offered.</b> The
 * substrate re-offers an item whose processing answered "not yet", and a join that took it in again
 * would apply the same change twice - indexing a fact row under one dimension key twice over, and
 * publishing its row twice. So taking it in and sending what it means are separate steps here, and only
 * the sending is retried.
 */
public final class JoinProcessor extends AbstractProcessor {

    private final JoinDriver driver;
    private final Map<Integer, String> sourceByOrdinal;

    /**
     * @param sourceByOrdinal which source arrives on which inbound edge. It comes from whoever drew the
     *                        edges: an event says which stream it was read from, which is not the name
     *                        the join calls that source by - the same table can be joined to itself
     *                        under two aliases, and then one stream is two sources
     */
    public JoinProcessor(JoinDriver driver, Map<Integer, String> sourceByOrdinal) {
        this.driver = Objects.requireNonNull(driver, "driver");
        this.sourceByOrdinal = Map.copyOf(Objects.requireNonNull(sourceByOrdinal, "sourceByOrdinal"));
    }

    /** Whether the change currently being offered has already been taken into the state. */
    private boolean taken;

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        String source = sourceByOrdinal.get(ordinal);
        if (source == null) {
            throw new IllegalStateException(
                    "a join vertex was given an edge on ordinal " + ordinal + ", which names no source");
        }
        if (!taken) {
            driver.absorb(List.of(new SourceChange(source, (Envelope) item)));
            taken = true;
        }
        if (driver.drain(this::tryEmit)) {
            taken = false;
            return true;
        }
        return false;
    }

    /**
     * Run when there is nothing arriving. What is left of a recompute is pushed here, which is the only
     * moment a fan-out larger than one call's worth of room ever finishes on a stream that has caught
     * up.
     */
    @Override
    public boolean tryProcess() {
        return driver.drain(this::tryEmit);
    }

    /**
     * The same again for a source that ends. A bounded run reaches this instead of the idle call, and a
     * recompute half sent when the last row arrived would otherwise be dropped on the floor.
     */
    @Override
    public boolean complete() {
        return driver.drain(this::tryEmit);
    }
}
