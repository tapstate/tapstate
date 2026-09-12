package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.join.DimensionRowDisplacedAlert;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Says out loud that a join has lost a dimension row to a key another row already occupied.
 *
 * <p>This is the whole of the signal, and it is the only one there can be. Nothing is repaired: the
 * target simply ends up holding fewer rows than the query describes, and every row it does hold is a
 * perfectly ordinary one built from a dimension row that really is under that key. No count moves, no
 * queue grows and no error is raised, so a reader comparing the target against the query is the only
 * other way this is ever noticed - which is to say, afterwards.
 *
 * <p>Only the first of each decade is logged. A join matching on a column that identifies nothing loses
 * a row per duplicate, which on a first load is as many lines as the source has rows; the count on the
 * line is what says how far past the first this has gone.
 *
 * <p>The count restarts with each vertex on each member, because that is where the object is built. It
 * decides only how often a line is written, never whether the loss happened.
 */
final class LoggingDimensionRowDisplacedAlert implements DimensionRowDisplacedAlert {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(LoggingDimensionRowDisplacedAlert.class);

    private final AtomicLong count = new AtomicLong();

    @Override
    public void displaced(String source, String dimensionKey) {
        long seen = count.incrementAndGet();
        if (!isFirstOfItsDecade(seen)) {
            return;
        }
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("source", source);
        // Rendered rather than filed under: a key is an encoding of the columns it was built from, and
        // somebody told that "AAAAATE" lost a row cannot say which row of their table that is - which is
        // the only question this line is here to answer.
        args.put("key", JoinKey.describe(dimensionKey));
        // Built rather than thrown: the code names what happened and the pipeline goes on running, which
        // is what this severity means. Throwing it would stop a job over data that is merely ambiguous.
        TapstateException coded =
                new TapstateException(EngineError.JOIN_DIMENSION_ROW_DISPLACED, args, null);
        LOG.warn("{} ({} displaced so far on this member)", coded.getMessage(), seen);
    }

    /** How many have been displaced since this was built - what a caller asserts on rather than the log. */
    long count() {
        return count.get();
    }

    private static boolean isFirstOfItsDecade(long seen) {
        for (long decade = 1; decade <= seen; decade *= 10) {
            if (decade == seen) {
                return true;
            }
        }
        return false;
    }
}
