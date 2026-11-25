package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.model.BatchSpec;
import java.security.Permission;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * A processor that takes its input in the batches its node's author asked for, and hands each batch whole to the
 * processor it wraps, which does all of the work.
 *
 * <p>It takes at most {@code max_records} rows from its inbox and holds them; what does not fit stays in the
 * inbox, which is what holds the rows upstream back. The rows held go to the wrapped processor once there are
 * {@code max_records} of them, once the first of them has waited {@code max_wait} - measured by the clock it is
 * given, never by sleeping - or at once where no wait was asked for. Nothing is held past a bound: a bound, the
 * end of an edge and the end of the input each hand over whatever is held first, so the wrapped processor sees
 * every row before the bound that covers it, in the order the rows arrived on each edge.
 *
 * <p>Deciding when is all it does. Its own time is none of a stage's, so it declares none: the processor it wraps
 * times its own.
 */
final class InputBatches implements Processor {

    private final Processor delegate;
    private final int maxRecords;
    private final long maxWaitNanos;
    private final LongSupplier nanoClock;
    // What is held, oldest first, each with the edge it arrived on.
    private final ArrayDeque<Held> held = new ArrayDeque<>();
    private final HandOver handOver = new HandOver();
    private long firstHeldAt;
    // Set from the moment what is held is due until the last of it has been taken: rows left over because the
    // wrapped processor could not take them all at once are still due, whatever the clock says by then.
    private boolean handingOver;

    InputBatches(Processor delegate, int maxRecords, long maxWaitNanos, LongSupplier nanoClock) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (maxRecords < 1) {
            throw new IllegalArgumentException("a batch holds at least one row, got " + maxRecords);
        }
        if (maxWaitNanos < 0) {
            throw new IllegalArgumentException("a wait cannot be negative, got " + maxWaitNanos);
        }
        this.maxRecords = maxRecords;
        this.maxWaitNanos = maxWaitNanos;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /**
     * {@code delegate}, whose processors each take their input in {@code batch}es; {@code delegate} itself where
     * its author asked for no batch, which leaves the engine to hand the processors their input as it arrives.
     */
    static ProcessorMetaSupplier around(ProcessorMetaSupplier delegate, BatchSpec batch) {
        Objects.requireNonNull(delegate, "delegate");
        return batch == null ? delegate
                : new Metas(delegate, batch.effectiveMaxRecords(),
                        TimeUnit.MILLISECONDS.toNanos(batch.effectiveMaxWaitMillis()));
    }

    /** Whether the processors {@code supplier} makes take their input in batches. */
    static boolean takesInputInBatches(ProcessorMetaSupplier supplier) {
        return supplier instanceof Metas;
    }

    @Override
    public boolean isCooperative() {
        return delegate.isCooperative();
    }

    @Override
    public void init(Outbox outbox, Context context) throws Exception {
        delegate.init(outbox, context);
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        // What is already due goes first, so the rows held stay ahead of the ones about to be taken.
        if (due() && !handOverAll()) {
            return;
        }
        for (Object row; held.size() < maxRecords && (row = inbox.poll()) != null; ) {
            if (held.isEmpty()) {
                firstHeldAt = nanoClock.getAsLong();
            }
            held.addLast(new Held(ordinal, row));
        }
        if (due()) {
            handOverAll();
        }
    }

    @Override
    public boolean tryProcess() {
        if (due() && !handOverAll()) {
            return false;
        }
        return delegate.tryProcess();
    }

    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return handOverAll() && delegate.tryProcessWatermark(watermark);
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        return handOverAll() && delegate.tryProcessWatermark(ordinal, watermark);
    }

    @Override
    public boolean completeEdge(int ordinal) {
        return handOverAll() && delegate.completeEdge(ordinal);
    }

    @Override
    public boolean complete() {
        return handOverAll() && delegate.complete();
    }

    @Override
    public boolean saveToSnapshot() {
        return delegate.saveToSnapshot();
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return delegate.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        return delegate.snapshotCommitFinish(success);
    }

    @Override
    public void restoreFromSnapshot(Inbox inbox) {
        delegate.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean finishSnapshotRestore() {
        return delegate.finishSnapshotRestore();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public boolean closeIsCooperative() {
        return delegate.closeIsCooperative();
    }

    /** Whether what is held has to go now: a full batch, a wait run out or never asked for, or one already going. */
    private boolean due() {
        if (held.isEmpty()) {
            return false;
        }
        return handingOver || held.size() >= maxRecords
                || nanoClock.getAsLong() - firstHeldAt >= maxWaitNanos;
    }

    /**
     * Hands every row held to the wrapped processor, an edge's run of rows at a time; false where it could not
     * take them all yet, in which case the rest go first the next time anything is asked of this one.
     */
    private boolean handOverAll() {
        handingOver = true;
        while (!held.isEmpty()) {
            int ordinal = held.peekFirst().ordinal();
            handOver.ordinal = ordinal;
            delegate.process(ordinal, handOver);
            if (!held.isEmpty() && held.peekFirst().ordinal() == ordinal) {
                return false;
            }
        }
        handingOver = false;
        return true;
    }

    private record Held(int ordinal, Object row) {
    }

    /** The leading run of held rows that arrived on one edge, as the inbox of that edge. */
    private final class HandOver implements Inbox {

        private int ordinal;

        @Override
        public boolean isEmpty() {
            return held.isEmpty() || held.peekFirst().ordinal() != ordinal;
        }

        @Override
        public Object peek() {
            return isEmpty() ? null : held.peekFirst().row();
        }

        @Override
        public Object poll() {
            return isEmpty() ? null : held.pollFirst().row();
        }

        @Override
        public void remove() {
            if (isEmpty()) {
                throw new NoSuchElementException("nothing left of the rows handed over");
            }
            held.removeFirst();
        }

        @Override
        public Iterator<Object> iterator() {
            List<Object> rows = new ArrayList<>();
            for (Held next : held) {
                if (next.ordinal() != ordinal) {
                    break;
                }
                rows.add(next.row());
            }
            return rows.iterator();
        }

        @Override
        public void clear() {
            while (!isEmpty()) {
                held.removeFirst();
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (Held next : held) {
                if (next.ordinal() != ordinal) {
                    break;
                }
                size++;
            }
            return size;
        }
    }

    /** A meta-supplier whose processors each take their input in batches; everything else is the delegate's. */
    private static final class Metas implements ProcessorMetaSupplier {

        private static final long serialVersionUID = 1L;

        private final ProcessorMetaSupplier delegate;
        private final int maxRecords;
        private final long maxWaitNanos;

        Metas(ProcessorMetaSupplier delegate, int maxRecords, long maxWaitNanos) {
            this.delegate = delegate;
            this.maxRecords = maxRecords;
            this.maxWaitNanos = maxWaitNanos;
        }

        @Override
        public void init(Context context) throws Exception {
            delegate.init(context);
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            Function<? super Address, ? extends ProcessorSupplier> suppliers = delegate.get(addresses);
            int records = maxRecords;
            long waitNanos = maxWaitNanos;
            return address -> new Suppliers(suppliers.apply(address), records, waitNanos);
        }

        @Override
        public int preferredLocalParallelism() {
            return delegate.preferredLocalParallelism();
        }

        @Override
        public Permission getRequiredPermission() {
            return delegate.getRequiredPermission();
        }

        @Override
        public Map<String, String> getTags() {
            return delegate.getTags();
        }

        @Override
        public boolean initIsCooperative() {
            return delegate.initIsCooperative();
        }

        @Override
        public boolean closeIsCooperative() {
            return delegate.closeIsCooperative();
        }

        @Override
        public void close(Throwable error) throws Exception {
            delegate.close(error);
        }

        @Override
        public boolean isReusable() {
            return delegate.isReusable();
        }
    }

    /** A supplier whose processors each take their input in batches; everything else is the delegate's. */
    private static final class Suppliers implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final ProcessorSupplier delegate;
        private final int maxRecords;
        private final long maxWaitNanos;

        Suppliers(ProcessorSupplier delegate, int maxRecords, long maxWaitNanos) {
            this.delegate = delegate;
            this.maxRecords = maxRecords;
            this.maxWaitNanos = maxWaitNanos;
        }

        @Override
        public void init(Context context) throws Exception {
            delegate.init(context);
        }

        @Override
        public boolean initIsCooperative() {
            return delegate.initIsCooperative();
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (Processor processor : delegate.get(count)) {
                // A stand-in takes no input to batch, and wrapped it would no longer be known for one.
                processors.add(PinnedStandIns.isStandIn(processor)
                        ? processor : new InputBatches(processor, maxRecords, maxWaitNanos, System::nanoTime));
            }
            return processors;
        }

        @Override
        public boolean closeIsCooperative() {
            return delegate.closeIsCooperative();
        }

        @Override
        public void close(Throwable error) throws Exception {
            delegate.close(error);
        }
    }
}
