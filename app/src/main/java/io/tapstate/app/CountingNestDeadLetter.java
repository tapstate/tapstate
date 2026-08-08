package io.tapstate.app;

import io.tapstate.runtime.engine.nest.NestDeadLetter;
import io.tapstate.runtime.engine.nest.NestElement;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts the changes a nest can never place in a document, and says so.
 *
 * <p>These are rows whose parent is known gone: no later event will give them a document to sit in. What
 * should become of them is a product question this build has not answered yet, so this answers the one
 * part that is not in question — that they must not vanish unremarked. A no-op here would look identical
 * from the outside to a nest that is working, because the rows it swallows were never going to appear in
 * a document anyway; no assertion about the output could tell the two apart.
 *
 * <p>Only the first of each decade is logged. A parent deleted with many children below it produces one
 * of these per child, so logging every one turns a single ordinary event into a flood that buries the
 * rest of the log — while the count still says exactly how many there were.
 */
final class CountingNestDeadLetter implements NestDeadLetter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CountingNestDeadLetter.class);

    private final AtomicLong count = new AtomicLong();

    @Override
    public void parentAbsent(NestElement element) {
        long seen = count.incrementAndGet();
        if (isFirstOfItsDecade(seen)) {
            LOG.warn("nest dropped a change whose parent row is gone: {} (dropped {} so far on this member)",
                    element.ref(), seen);
        }
    }

    /** How many have been handed over on this member — what a caller asserts on rather than the log. */
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
