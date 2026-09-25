package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.util.Objects;

/**
 * How far one writer of one run has durably landed one table's changes.
 *
 * <p>{@code durableThrough} is the order below or at which nothing this writer received from the table is
 * still unwritten: the highest position it settled, or a bound it was handed while nothing was in flight -
 * which is how a writer that receives none of a table's rows still says it is not holding any of them back.
 * {@code lastTokened} is the highest position it settled that names a place in the source a read can resume
 * from, or null where it has settled none; it never lies above {@code durableThrough}.
 *
 * <p>A pure value over {@code java..} and the event types (rule R2), carried between the sink that reports
 * it and the store that keeps it.
 */
public record WriterProgress(SourceOrder durableThrough, ChainPosition lastTokened) {

    public WriterProgress {
        Objects.requireNonNull(durableThrough, "durableThrough");
        if (lastTokened != null) {
            if (lastTokened.token() == null || lastTokened.order() == null) {
                throw new IllegalArgumentException("a tokened position carries a token and an order: " + lastTokened);
            }
            if (lastTokened.order().compareTo(durableThrough) > 0) {
                throw new IllegalArgumentException("a writer's last tokened position " + lastTokened
                        + " cannot lie above how far it is durable, " + durableThrough);
            }
        }
    }
}
