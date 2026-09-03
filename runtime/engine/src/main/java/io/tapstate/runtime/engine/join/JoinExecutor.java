package io.tapstate.runtime.engine.join;

import io.tapstate.core.sql.JoinPlan;

import java.util.List;

/**
 * What runs a join. The seam an execution carrier is swapped behind, drawn around the whole carrier
 * rather than around the state layer underneath it: a carrier is a matching strategy, a state layout
 * and an expression evaluator together, and a seam drawn between those hands the next carrier the
 * slowest parts of this one and calls it a choice.
 *
 * <p>Nothing in these signatures names the execution substrate or the SQL library. The plan comes from
 * a front end that cannot see a carrier, and the changelog goes out as the same event envelope every
 * other transform speaks.
 *
 * <p><b>{@code apply} is called with nothing to do, on purpose, and that is a contract rather than a
 * convenience.</b> A change to one dimension row can mean a million rows have to be built again, and
 * that work outlives the change that caused it. If the only moment it were pushed were the arrival of
 * the next change, then a source going quiet - which is the ordinary state of a stream that has caught
 * up - would leave the rest of that million unsent for ever, with the job running, no errors, and the
 * target table half updated. So the carrier is asked again whenever there is nothing arriving, and the
 * answer says whether it has more to do.
 */
public interface JoinExecutor extends AutoCloseable {

    /** Compiles {@code plan}: what is matched on, what is kept, and what each output column is. */
    void open(JoinPlan plan);

    /**
     * Produces the whole result once, as a consistent snapshot of the sources.
     *
     * @return whether it is finished; false means it was refused by the sink and must be called again
     */
    boolean loadFull(JoinSink sink);

    /**
     * Absorbs {@code changes} and pushes as much of the changelog as {@code sink} will take. An empty
     * list is the "nothing is arriving" call described on this interface, and is the only way work
     * that outlives its own change ever finishes.
     *
     * @return whether nothing is left to send; false means there is more and it must be called again
     */
    boolean apply(List<SourceChange> changes, JoinSink sink);

    @Override
    void close();
}
