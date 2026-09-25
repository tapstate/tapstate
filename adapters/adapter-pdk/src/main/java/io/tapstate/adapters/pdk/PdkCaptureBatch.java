package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.SourcePosition;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A snapshot batch read from the connector while it is being taken, holding the connector open until
 * closed. Every event is a snapshot read (op {@code r}). Closing stops the connector and closes its loader;
 * it is idempotent and may be called before the batch is drained.
 *
 * <p><b>The connector reads on a thread of its own, at most a few of its batches ahead of whoever takes the
 * rows.</b> A connector hands its rows over a batch at a time from inside its own read loop, and that loop
 * is paused, by the hand-over waiting, whenever {@link #READ_AHEAD} decoded batches are already waiting
 * here -- which pauses the fetch from the source with it. Collecting the whole read first was the shape this
 * replaced: a table held twice over on the heap, as the connector's events and again decoded, before a
 * single row of it could go anywhere, so the heap a snapshot needed was the size of its largest table.
 *
 * <p>The seam is sampled before the first row is read and is in hand by the time the batch is. A failure
 * before that is the opening's own and is thrown by it; one afterwards reaches whoever is taking the rows,
 * from {@link #hasNext}, as what it would have been had the whole read been collected first.
 */
final class PdkCaptureBatch implements CaptureBatch {

    /**
     * How many of the connector's decoded batches may wait for the reader of this one. Enough that taking a
     * row never waits on the source while the source has one ready; each batch is at most the size the
     * connector reads in, so this bounds the rows the read holds at once to a few thousand.
     */
    static final int READ_AHEAD = 4;

    /** How long a taker waits between checks that the batch is still open. */
    private static final long POLL_MILLIS = 100;

    private static final long JOIN_MILLIS = 2000;

    /** What the reading thread puts behind the last batch of an ordinary end. */
    private static final Object END = new Object();

    /** What the reading thread puts behind the last batch when the read failed. */
    private record Failure(Throwable cause) {
    }

    /** What a read does: tells {@code reading} the seam once it has one, then hands it the rows. */
    @FunctionalInterface
    interface Read {
        void run(PdkCaptureBatch reading) throws Throwable;
    }

    private final PdkConnector connector;
    private final BlockingQueue<Object> ahead = new ArrayBlockingQueue<>(READ_AHEAD);
    private final CompletableFuture<Optional<SourcePosition>> seam = new CompletableFuture<>();
    private final Thread reader;
    private Iterator<Envelope> current = Collections.emptyIterator();
    private boolean over;
    private volatile boolean closed;

    private PdkCaptureBatch(PdkConnector connector, Read read, String threadName) {
        this.connector = connector;
        this.reader = new Thread(() -> readAll(read), threadName);
        this.reader.setDaemon(true);
    }

    /**
     * Starts {@code read} on a thread of its own and returns once it has sampled the seam. A read that fails
     * before it gets that far is thrown here, having stopped and closed the connector.
     */
    static PdkCaptureBatch start(PdkConnector connector, Read read, String threadName) {
        PdkCaptureBatch batch = new PdkCaptureBatch(connector, read, threadName);
        batch.reader.start();
        try {
            batch.seam.get();
        } catch (InterruptedException interrupted) {
            batch.close();
            Thread.currentThread().interrupt();
            throw new CancellationException("the snapshot read was interrupted before its seam was taken");
        } catch (ExecutionException failed) {
            batch.close();
            throw unchecked(failed.getCause());
        }
        return batch;
    }

    /** The connector this batch was read from, and the state scope it was opened under. */
    PdkConnector connector() {
        return connector;
    }

    /** Called by the read once it has sampled the seam, before it reads a row. */
    void seamSampled(Optional<SourcePosition> position) {
        seam.complete(position);
    }

    /**
     * Called by the read with each of the connector's batches, decoded. Waits while {@link #READ_AHEAD} are
     * already waiting, and gives up if the batch is closed meanwhile.
     */
    void rowsRead(List<Envelope> rows) {
        if (!rows.isEmpty()) {
            put(rows);
        }
    }

    @Override
    public boolean hasNext() {
        while (!current.hasNext()) {
            if (over) {
                return false;
            }
            Object next = take();
            if (next == END) {
                over = true;
                return false;
            }
            if (next instanceof Failure failure) {
                over = true;
                throw unchecked(failure.cause());
            }
            @SuppressWarnings("unchecked")
            List<Envelope> rows = (List<Envelope>) next;
            current = rows.iterator();
        }
        return true;
    }

    @Override
    public Envelope next() {
        if (!hasNext()) {
            throw new NoSuchElementException("the snapshot read has no further row");
        }
        return current.next();
    }

    @Override
    public Optional<SourcePosition> seam() {
        return seam.getNow(Optional.empty());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // The order the change tail closes in: wake the reading thread, stop the connector under it, give
        // it a moment to let go, then release the connector's loader.
        reader.interrupt();
        connector.stopQuietly();
        try {
            reader.join(JOIN_MILLIS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        ahead.clear();
        connector.close();
    }

    private void readAll(Read read) {
        try {
            read.run(this);
            // A read that never took a seam takes none: the batch reports none rather than making one up.
            seam.complete(Optional.empty());
            put(END);
        } catch (CancellationException abandoned) {
            seam.completeExceptionally(abandoned);
        } catch (Throwable failure) {
            if (!seam.completeExceptionally(failure) && !closed) {
                put(new Failure(failure));
            }
        }
    }

    /** Waits for room to put {@code element}, or gives up once the batch is closed. */
    private void put(Object element) {
        try {
            while (!ahead.offer(element, POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                if (closed) {
                    throw new CancellationException("the snapshot read was closed before it was taken");
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CancellationException("the snapshot read was interrupted while it waited for room");
        }
    }

    /**
     * The next thing the reading thread put, waiting for one. A batch closed under a taker is refused rather
     * than ended: what came out of it before the close is a prefix of the read, and a taker told it had
     * reached the end would take that prefix for the whole of it.
     */
    private Object take() {
        try {
            while (true) {
                if (closed) {
                    throw new CancellationException("the snapshot read was closed before it was read through");
                }
                Object next = ahead.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
                if (next != null) {
                    return next;
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CancellationException("the snapshot read was interrupted while it waited for rows");
        }
    }

    private static RuntimeException unchecked(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IllegalStateException(failure);
    }
}
