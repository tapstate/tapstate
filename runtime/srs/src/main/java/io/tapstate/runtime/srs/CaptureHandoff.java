package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Where a capture run hands one pipeline the rows it reads, and where it says that one table's initial load
 * has been handed over in full.
 *
 * <p>The second half is what lets a load be read while the job taking it runs. A reader that knows the load
 * is still arriving holds its ring back and makes no promise about the load, and one that is never told
 * the load has finished waits for good; so every selected table is reported exactly once per run, after its
 * last row -- including a table this run owes no load for, which is reported with no rows before it.
 *
 * <p>Deliberately not a functional interface. A lambda passed where either this or a plain consumer would do
 * is then always the plain consumer, which says nothing about tables, rather than a choice the compiler
 * refuses to make.
 */
public interface CaptureHandoff extends Consumer<Envelope> {

    /**
     * Every row this run reads of {@code table}'s initial load has been accepted. Called once per selected
     * table, after its last row and never before it; not called for a table whose read failed or was
     * abandoned, because what arrived of it is not its load.
     */
    void loaded(String table);

    /** A hand-off that takes {@code rows} and has nobody to tell when a table is done. */
    static CaptureHandoff of(Consumer<Envelope> rows) {
        Objects.requireNonNull(rows, "rows");
        return new CaptureHandoff() {
            @Override
            public void accept(Envelope row) {
                rows.accept(row);
            }

            @Override
            public void loaded(String table) {
                // Nothing reads a table's completion from a plain consumer.
            }
        };
    }
}
