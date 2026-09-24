package io.tapstate.runtime.engine.join;

import io.tapstate.core.sql.JoinPlan;

import java.util.List;
import java.util.Objects;

/**
 * The carrier this product ships, seen through the seam every carrier is swapped at.
 *
 * <p><b>Why this exists as its own class rather than the driver simply implementing the seam.</b>
 * A driver is built around one plan, so the plan is a constructor argument there; the seam has to hand
 * a plan to a carrier that was chosen before any plan was read - which is what {@code open} is - and
 * folding the two together would mean either a constructor nobody can call before parsing the SQL or
 * an {@code open} that only checks the plan it was already given. Here {@code open} is where the
 * driver comes into being, which is the honest reading of it.
 *
 * <p><b>What it buys is that a carrier can be held to its behaviour without naming this one.</b>
 * Whatever compares a carrier's output against a reference takes a {@link JoinExecutor}; given a
 * {@link JoinDriver} instead it would be the built-in carrier's own self-test, and the next carrier
 * would arrive with nothing already applying to it.
 *
 * <p>The state, the fact row's key columns and the stream the changelog carries are settled when the
 * carrier is built, because none of them is derivable from the SQL: the key belongs to whoever
 * discovered the source, and the stream to whoever wired the edge.
 */
public final class BuiltinJoinExecutor implements JoinExecutor {

    private final List<String> factKeyColumns;
    private final String outputStream;
    private final JoinStores stores;
    private final int keysPerRead;
    private final JoinGauge gauge;

    private JoinDriver driver;

    public BuiltinJoinExecutor(List<String> factKeyColumns, String outputStream, JoinStores stores) {
        this(factKeyColumns, outputStream, stores, JoinDriver.DEFAULT_KEYS_PER_READ, JoinGauge.NONE);
    }

    /** As above, with the size of one read named - which is what a case needs to be small. */
    public BuiltinJoinExecutor(List<String> factKeyColumns, String outputStream, JoinStores stores,
            int keysPerRead, JoinGauge gauge) {
        this.factKeyColumns = List.copyOf(Objects.requireNonNull(factKeyColumns, "factKeyColumns"));
        this.outputStream = Objects.requireNonNull(outputStream, "outputStream");
        this.stores = Objects.requireNonNull(stores, "stores");
        this.keysPerRead = keysPerRead;
        this.gauge = Objects.requireNonNull(gauge, "gauge");
    }

    @Override
    public void open(JoinPlan plan) {
        driver = new JoinDriver(Objects.requireNonNull(plan, "plan"), factKeyColumns, outputStream,
                stores, keysPerRead, gauge);
    }

    @Override
    public boolean apply(List<SourceChange> changes, JoinSink sink) {
        if (driver == null) {
            throw new IllegalStateException("this carrier has no plan yet: open one before applying");
        }
        return driver.apply(changes, sink);
    }

    /**
     * Lets the plan go. The state is not cleared: it was handed in rather than created here, and the
     * caller that owns a distributed map does not expect closing a carrier to empty it.
     */
    @Override
    public void close() {
        driver = null;
    }
}
