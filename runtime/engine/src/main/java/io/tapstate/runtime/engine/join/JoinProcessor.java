package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.Processor;
import io.tapstate.runtime.engine.StageTimer;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.SettledPositions;

import java.util.ArrayList;
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
 * substrate re-offers items whose processing answered "not yet", and a join that took them in again
 * would apply the same change twice - indexing a fact row under one dimension key twice over, and
 * publishing its row twice. So taking them in and sending what they mean are separate steps here, and
 * only the sending is retried.
 *
 * <p><b>Rows are taken in a batch rather than an item at a time.</b> A settlement marker ends a batch
 * and follows all of its output before the next batch is taken in. The
 * driver reads the fact mirror once for the keys a batch is about to ask about, which is one round
 * trip for a delivery instead of one per row; handing it one item per call would leave that read
 * asking for a single key every time, which is the same number of trips the batching exists to
 * remove. The re-offer rule is unchanged and simply applies to the delivery: it is absorbed once, and
 * the items stay where they are until everything they meant has gone out.
 */
public final class JoinProcessor extends AbstractProcessor implements Staged {

    @Override
    public Stage stage() {
        return Stage.JOIN;
    }

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

    /** How many arrivals before the next settlement marker have already been taken into the state. */
    private int taken;

    /**
     * Takes in each run of rows before a settlement marker, then sends what the outbox will hold.
     *
     * <p>The items are left where they are until all of it has gone out, which is how the substrate is
     * told there is more to do: it offers the same delivery again, and {@link #taken} is what keeps it
     * from being absorbed a second time. Nothing new is added to what is being offered in the
     * meantime - the substrate refills only once it has been emptied - so the two cannot interleave.
     */
    // Times each drain of arrivals, which is this stage's unit of work.
    private StageTimer timer = StageTimer.none(Stage.JOIN);

    @Override
    protected void init(Processor.Context context) {
        this.timer = StageTimer.of(stage(), context);
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        long started = timer.begin();
        try {
            processTimed(ordinal, inbox);
        } finally {
            timer.end(started);
        }
    }

    private void processTimed(int ordinal, Inbox inbox) {
        String source = sourceByOrdinal.get(ordinal);
        if (source == null) {
            throw new IllegalStateException(
                    "a join vertex was given an edge on ordinal " + ordinal + ", which names no source");
        }
        while (!inbox.isEmpty()) {
            if (inbox.peek() instanceof SettledPositions) {
                // Finish earlier recomputes before promising that a later position has settled.
                if (!driver.drainUpdates(this::tryEmit) || !tryEmit(inbox.peek())) {
                    return;
                }
                inbox.poll();
                continue;
            }
            if (taken == 0) {
                List<SourceChange> changes = new ArrayList<>(inbox.size());
                // Keep batching contiguous rows, but never absorb rows beyond a settlement marker.
                for (Object item : inbox) {
                    if (item instanceof SettledPositions) {
                        break;
                    }
                    changes.add(new SourceChange(source, (Envelope) item));
                }
                driver.absorb(changes);
                taken = changes.size();
            }
            if (!driver.drainUpdates(this::tryEmit)) {
                return;
            }
            for (int i = 0; i < taken; i++) {
                inbox.poll();
            }
            taken = 0;
        }
    }

    /**
     * Run when there is nothing arriving. What is left of a recompute is pushed here, which is the only
     * moment a fan-out larger than one call's worth of room ever finishes on a stream that has caught
     * up.
     */
    @Override
    public boolean tryProcess() {
        return driver.drainUpdates(this::tryEmit);
    }

    /**
     * The same again for a source that ends. A bounded run reaches this instead of the idle call, and a
     * recompute half sent when the last row arrived would otherwise be dropped on the floor.
     */
    @Override
    public boolean complete() {
        return driver.drainUpdates(this::tryEmit);
    }
}
