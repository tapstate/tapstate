package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * One reader's bounded account of the runs of changes it has handed on from its source, and the only thing
 * that moves the chain's durable positions while it reads.
 *
 * <p>A source names one position for a run of changes, and that position is only safe to resume from once
 * every change in and before the run has landed wherever it had to go. On a chain carrying several tables
 * the changes of one run go to several rings, each numbering its own changes, and each table's sinks confirm
 * them independently -- a quiet table's change can land while an earlier change of a busy table is still in
 * flight. So nothing here ranks one table's sequence against another's. Each run is recorded, in the order
 * the source handed it over, with the last sequence it reached in each table, and it is released only when
 * every consumer that selected one of those tables when it was recorded has confirmed that far. Runs are
 * released strictly in order: the position of a later run says the source was read past every earlier one.
 *
 * <p>A run that carried no change -- a heartbeat and its like -- is recorded too, with nothing to wait for.
 * It is still released only behind every run before it, and its position is often the only one a quiet
 * source ever names: a source that reports where a transaction ends only after the fact would otherwise
 * never let the chain move past the last change it delivered.
 *
 * <p>Releasing writes the run's position down as the chain's source read offset, and as the chain-level
 * acknowledgement of each consumer the run carried a change for, and cuts each table's durable log behind
 * the run: everyone who reads that ring has landed everything up to there. A consumer's acknowledgement
 * says where the last change its target confirmed sat, so a run that carried it nothing leaves it where it
 * is -- a source naming positions while nothing changes would otherwise move it for ever. Every write is
 * conditional on the reader still holding the generation it opened; a reader that lost it to another one
 * stops with a code instead of carrying on.
 *
 * <p>The account is held in memory and bounded. A process that stops loses it, and the next reader resumes
 * from the last position released -- which only ever means reading again what had already been read. It never
 * holds the source back. A run nobody confirms for a long while is an ordinary state -- a paused pipeline
 * still owes the changes on its tables, and a table whose last change a transform dropped has nothing coming
 * that would confirm it -- and holding the source back on it would stop every other pipeline reading the
 * source, until a change that can never be read arrives. So quiet runs recorded behind a run still owed are
 * folded into one, and once the account is full each new run is folded into the last one recorded. A fold
 * releases only when everything folded into it is confirmed, and writes down what releasing its runs one by
 * one would have written: how far the source was read, where the last change was, and for each consumer where
 * its own last change was. Nothing is released past an owed run either way; folding only makes what is
 * released afterwards coarser. One daemon thread, shared by every account in the process, re-checks the
 * confirmations while a source is quiet, so a run whose sinks confirm it after the source stopped talking is
 * still released.
 *
 * <p>A direct tail -- a read with the shared ring switched off -- uses the same account with one consumer:
 * the pipeline it streams to. It has no rings to cut and no other readers to fence against, so a release
 * writes that pipeline's acknowledgement and advances the chain's offset the way a direct tail always has.
 */
final class PhysicalSourcePrefix implements AutoCloseable {

    /** How many entries one reader's account holds before each new run is folded into the last one. */
    static final int MAX_PENDING_BATCHES = 256;

    /** How often the shared thread re-checks the confirmations of every open account. */
    static final long TICK_MILLIS = 100;

    /**
     * The longest the shared thread leaves an account between two re-checks while nothing it waits for moves.
     * Each re-check that releases nothing doubles the wait, up to this; one that releases something, or a run
     * the source hands over that does, puts it back to a tick.
     */
    static final long MAX_RECHECK_MILLIS = 32 * TICK_MILLIS;

    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(TICK_MILLIS);
    private static final long MAX_RECHECK_NANOS = TimeUnit.MILLISECONDS.toNanos(MAX_RECHECK_MILLIS);

    private static final Set<PhysicalSourcePrefix> ACTIVE = ConcurrentHashMap.newKeySet();
    private static final ScheduledExecutorService TICKER = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "tapstate-physical-prefix");
        thread.setDaemon(true);
        return thread;
    });

    static {
        TICKER.scheduleWithFixedDelay(() -> ACTIVE.forEach(PhysicalSourcePrefix::tickSafely),
                TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
    }

    private final SrsMetaStore meta;
    private final String chainId;
    private final long epoch;
    private final CaptureHealth health;
    /** The one pipeline a direct tail streams to; null for the chain's shared reader. */
    private final String directPipeline;
    /** Cuts one table's durable log through a sequence; does nothing where there is no log to cut. */
    private final BiConsumer<String, Long> trimThrough;
    private final Deque<Batch> pending = new ArrayDeque<>();
    private long nextBatch;
    /** How many runs have been recorded; a re-check whose read was taken before the latest one is not used. */
    private long recorded;
    private long recheckEveryNanos = TICK_NANOS;
    private long recheckAtNanos = System.nanoTime();
    private boolean started;
    private boolean closed;
    private RuntimeException failure;

    private PhysicalSourcePrefix(SrsMetaStore meta, String chainId, long epoch, CaptureHealth health,
            String directPipeline, BiConsumer<String, Long> trimThrough) {
        this.meta = Objects.requireNonNull(meta, "meta");
        this.chainId = Objects.requireNonNull(chainId, "chainId");
        this.health = Objects.requireNonNull(health, "health");
        this.directPipeline = directPipeline;
        this.trimThrough = Objects.requireNonNull(trimThrough, "trimThrough");
        if (epoch < 1) {
            throw new IllegalArgumentException("a reader's account belongs to an opened generation, got " + epoch);
        }
        this.epoch = epoch;
    }

    /**
     * The account of the chain's shared reader, over the tables it subscribes to.
     *
     * <p>Refuses to open over an offset nobody can vouch for. An offset written before acknowledgements were
     * kept per table may already sit past a change of one table on another table's word; carrying on from it
     * would make that loss permanent, so a chain holding one is opened only when every consumer it records
     * has only ever read the one table this reader reads -- on such a chain an acknowledgement could only
     * speak for that table, and the offset is exact.
     */
    static PhysicalSourcePrefix shared(SrsMetaStore meta, String chainId, long epoch, List<String> tables,
            CaptureHealth health, BiConsumer<String, Long> trimThrough) {
        PhysicalSourcePrefix prefix = new PhysicalSourcePrefix(meta, chainId, epoch, health, null, trimThrough);
        SrsMeta stored = meta.read(chainId)
                .orElseThrow(() -> new IllegalStateException("a reader opened over a chain with no record: " + chainId));
        if (stored.epoch() != epoch) {
            throw new TapstateException(CaptureError.CHAIN_TAKEN_OVER, Map.of("chain", chainId), null);
        }
        if (stored.sourceRead() != null && stored.sourceRead().token() != null
                && !meta.physicalPrefixTrusted(chainId)) {
            if (tables.size() != 1 || !onlyEverRead(tables.getFirst(), stored.consumerOffsets())
                    || !meta.trustSourceReadOffset(chainId, epoch)) {
                throw new TapstateException(CaptureError.SHARED_POSITION_UNVERIFIED, Map.of("chain", chainId), null);
            }
        }
        prefix.open(stored);
        return prefix;
    }

    /** The account of a direct tail streaming to {@code pipelineId} alone. */
    static PhysicalSourcePrefix direct(SrsMetaStore meta, String chainId, long epoch, String pipelineId,
            CaptureHealth health) {
        PhysicalSourcePrefix prefix = new PhysicalSourcePrefix(meta, chainId, epoch, health,
                Objects.requireNonNull(pipelineId, "pipelineId"), (table, seq) -> { });
        prefix.open(meta.read(chainId)
                .orElseThrow(() -> new IllegalStateException("a reader opened over a chain with no record: " + chainId)));
        return prefix;
    }

    /**
     * Whether every consumer recorded on the chain has only ever read {@code table} through its ring, going by
     * everything its record says it read: its selection, the tables it has a cursor or a confirmation in.
     * A consumer that records none of those never read a change of the ring, so it never confirmed one.
     */
    private static boolean onlyEverRead(String table, Collection<ConsumerOffset> consumers) {
        for (ConsumerOffset consumer : consumers) {
            Set<String> read = new java.util.HashSet<>(consumer.perTableSeq().keySet());
            read.addAll(consumer.sinkAckedByTable().keySet());
            if (consumer.selectedTables() != null) {
                read.addAll(consumer.selectedTables());
            }
            read.remove(table);
            if (!read.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void open(SrsMeta stored) {
        SourceOrder prior = stored.sourceRead() == null ? null : stored.sourceRead().order();
        nextBatch = prior != null && prior.epoch() == epoch ? Math.addExact(prior.seq(), 1) : 0L;
        ACTIVE.add(this);
    }

    /**
     * Records where the source began, before it hands over anything: until one of its runs is released,
     * that is the only position this reader can be resumed from.
     *
     * <p>A source that named no start is accepted only where the chain already holds an offset to resume
     * from. Where it holds none, carrying on would leave nothing to resume from at all -- a process stopping
     * before the first release would come back at the present, with every change it had been handed gone --
     * so the shared reader refuses. A direct tail carries on as it always has.
     *
     * <p>A direct tail writes where it began down as the chain's position only on a chain that holds no offset
     * yet and that no other pipeline is on. It reads for its own pipeline alone and begins where that
     * pipeline's own load did, so its start says nothing about where the chain stands for anybody else.
     * Written over a position a pipeline stopped with its state kept still holds, it would become where that
     * pipeline resumes, past the changes it is owed. Written beside a pipeline whose load is still running, it
     * would become where that pipeline's reader opens, past the changes made after that load read its rows.
     * Beside another pipeline, a direct tail with no load of its own writes its start down for its own
     * pipeline instead, where only its own restart reads it.
     */
    synchronized void start(Optional<SourcePosition> position) {
        checkOpen();
        if (started) {
            return;
        }
        started = true;
        String token = position.map(SourcePosition::token).orElse(null);
        if (token == null) {
            boolean resumable = meta.read(chainId).map(SrsMeta::sourceReadOffset).isPresent();
            if (!resumable && directPipeline == null) {
                throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE, Map.of("chain", chainId), null);
            }
            return;
        }
        if (directPipeline != null) {
            Optional<SrsMeta> record = meta.read(chainId);
            if (record.map(SrsMeta::sourceReadOffset).isPresent()) {
                return;
            }
            if (record.isPresent() && record.get().consumerOffsets().stream()
                    .anyMatch(consumer -> !directPipeline.equals(consumer.pipelineId()))) {
                // A start a load already recorded is that load's seam, and stays as it is.
                if (record.get().consumerOffset(directPipeline).map(ConsumerOffset::cdcStartPosition).isEmpty()) {
                    meta.setCdcStart(chainId, directPipeline, token, 0L);
                }
                return;
            }
        }
        boolean anchored = meta.establishPhysicalAnchor(chainId, new ChainPosition(new SourceOrder(epoch, -1L), token));
        if (!anchored && directPipeline == null) {
            throw lostOrUnverified();
        }
    }

    /**
     * Throws what stopped the account, if anything did. Called on the thread the source hands its runs over
     * on, before a run is written anywhere: a reader that can no longer write its positions down writes no
     * more changes either.
     */
    synchronized void checkStillRecording() {
        checkOpen();
    }

    /**
     * Records one run the source handed over, once every change in it is where it had to go: {@code lastSeqByTable}
     * is the last sequence it reached in each table -- empty for a run that carried no change -- and
     * {@code token} the position the source named for it, or null where it named none.
     */
    synchronized void admitted(Map<String, Long> lastSeqByTable, String token) {
        checkOpen();
        if (!started) {
            start(Optional.empty());
        }
        Collection<ConsumerOffset> consumers = meta.consumerOffsets(chainId);
        Map<String, Map<String, Long>> owed = new LinkedHashMap<>();
        for (ConsumerOffset consumer : consumers) {
            if (directPipeline != null && !directPipeline.equals(consumer.pipelineId())) {
                continue;
            }
            if (directPipeline == null && consumer.selectedTables() != null && consumer.selectedTables().isEmpty()) {
                // It reads the chain through a direct tail of its own and never sees this ring.
                continue;
            }
            Map<String, Long> tables = new LinkedHashMap<>();
            lastSeqByTable.forEach((table, seq) -> {
                if (directPipeline != null || consumer.selects(table)) {
                    tables.put(table, seq);
                }
            });
            owed.put(consumer.pipelineId(), Map.copyOf(tables));
        }
        if (directPipeline != null && !owed.containsKey(directPipeline)) {
            owed.put(directPipeline, Map.copyOf(lastSeqByTable));
        }
        Batch run = Batch.of(new ChainPosition(new SourceOrder(epoch, nextBatch++), token), lastSeqByTable, owed);
        recorded++;
        Batch last = pending.peekLast();
        if (last != null && (pending.size() >= MAX_PENDING_BATCHES || run.quiet() && last.quiet())) {
            pending.removeLast();
            pending.addLast(last.fold(run));
        } else {
            pending.addLast(run);
        }
        // Just looked, so the shared thread need not look again before its next turn would have come.
        recheckAtNanos = System.nanoTime() + recheckEveryNanos;
        if (releaseOrStop(consumers)) {
            recheckSoon();
        }
    }

    /**
     * Stops the account because a run the source handed over was not recorded whole: whatever comes after it
     * must not be released past it, so nothing more is recorded or released, and the run says it stopped. A
     * run cut short by its stream being closed stops the account the same way and fails nothing: the stream
     * is going away, and the health it reports on may be shared with the stream that replaces it.
     */
    synchronized void abandon(Throwable cause) {
        if (closed || failure != null) {
            return;
        }
        boolean closing = cause instanceof CancellationException || Thread.currentThread().isInterrupted();
        failure = cause instanceof RuntimeException runtime
                ? runtime : new IllegalStateException("a run of chain " + chainId + " was not recorded", cause);
        ACTIVE.remove(this);
        if (!closing) {
            health.fail(cause);
        }
    }

    /**
     * Re-checks the confirmations, releasing what they now allow. The shared thread calls it while a source is
     * quiet. A store that cannot be read this turn is asked again on the next: nothing can be released on
     * confirmations nobody could read, so skipping a turn is the safe direction, and stopping every pipeline on
     * the source over one failed read is not.
     *
     * <p>The read is made without holding the account, so a slow store never holds back the thread the source
     * hands its runs over on. A run recorded while it was being read may be owed to a pipeline that read does
     * not know of yet, and a release on it would take that pipeline for one that left; so a read overtaken by
     * a run is not used, and the run that overtook it was released on a read of its own.
     */
    void tick() {
        long seen;
        synchronized (this) {
            if (closed || failure != null || pending.isEmpty()) {
                return;
            }
            seen = recorded;
        }
        Collection<ConsumerOffset> consumers;
        try {
            consumers = meta.consumerOffsets(chainId);
        } catch (TapstateException unread) {
            if (unread.code() == IoError.STORE_UNAVAILABLE) {
                return;
            }
            throw unread;
        }
        synchronized (this) {
            if (closed || failure != null || pending.isEmpty() || recorded != seen) {
                return;
            }
            if (releaseOrStop(consumers)) {
                recheckSoon();
            } else {
                recheckLater();
            }
        }
    }

    private void tickSafely() {
        try {
            if (dueForRecheck()) {
                tick();
            }
        } catch (RuntimeException error) {
            health.fail(error);
        }
    }

    private synchronized boolean dueForRecheck() {
        return System.nanoTime() - recheckAtNanos >= 0;
    }

    /** Something moved: the shared thread looks again on its next tick. */
    private void recheckSoon() {
        recheckEveryNanos = TICK_NANOS;
        recheckAtNanos = System.nanoTime();
    }

    /** Nothing moved: the shared thread waits twice as long as last time, up to {@link #MAX_RECHECK_MILLIS}. */
    private void recheckLater() {
        recheckEveryNanos = Math.min(recheckEveryNanos * 2, MAX_RECHECK_NANOS);
        recheckAtNanos = System.nanoTime() + recheckEveryNanos;
    }

    /**
     * Releases what the confirmations allow, answering whether it released anything, and stops the account for
     * good on the first failure: whichever thread noticed it, every later call on the account throws the same
     * failure rather than carry on over a chain it can no longer write down, and the run says it stopped.
     */
    private boolean releaseOrStop(Collection<ConsumerOffset> consumers) {
        try {
            return release(consumers);
        } catch (RuntimeException error) {
            failure = error;
            ACTIVE.remove(this);
            health.fail(error);
            throw error;
        }
    }

    /**
     * Releases runs in order while they are confirmed. A store that cannot take a release this turn leaves the
     * run where it is, for the next turn to write again: every write a release makes only ever moves forward,
     * so writing it a second time, after a failure part way through, lands where the first would have.
     */
    private boolean release(Collection<ConsumerOffset> consumers) {
        Map<String, ConsumerOffset> current = new LinkedHashMap<>();
        consumers.forEach(consumer -> current.put(consumer.pipelineId(), consumer));
        boolean released = false;
        while (!pending.isEmpty()) {
            Batch first = pending.peekFirst();
            if (!confirmed(first, current)) {
                return released;
            }
            if (first.position().token() != null) {
                try {
                    write(first, current);
                } catch (TapstateException unwritten) {
                    if (unwritten.code() == IoError.STORE_UNAVAILABLE) {
                        return released;
                    }
                    throw unwritten;
                }
            }
            pending.removeFirst();
            released = true;
        }
        return released;
    }

    /**
     * Whether every consumer the run was owed to has confirmed it: for each table it selected when the run was
     * recorded and still selects, a confirmation of that generation at or past the run's sequence there -- or,
     * on a ring it arrived on in this generation, a place past it. A consumer the chain no longer records owes
     * nothing any more.
     */
    private boolean confirmed(Batch batch, Map<String, ConsumerOffset> current) {
        for (Map.Entry<String, Map<String, Long>> debt : batch.owed().entrySet()) {
            ConsumerOffset consumer = current.get(debt.getKey());
            if (consumer == null) {
                // A direct tail streams to its one pipeline and to nobody else, so that pipeline's word is the
                // only one that can release its runs -- a record missing for it releases nothing.
                if (directPipeline != null) {
                    return false;
                }
                continue;
            }
            Map<String, Long> ringDone = null;
            for (Map.Entry<String, Long> table : debt.getValue().entrySet()) {
                if (directPipeline == null && !consumer.selects(table.getKey())) {
                    continue;
                }
                ChainPosition ack = consumer.sinkAckedByTable().get(table.getKey());
                if (ack != null && ack.order().epoch() == epoch && ack.order().seq() >= table.getValue()) {
                    continue;
                }
                if (directPipeline != null || !Objects.equals(consumer.selectedTablesEpoch(), epoch)) {
                    return false;
                }
                if (ringDone == null) {
                    try {
                        ringDone = meta.ringDoneThrough(chainId, consumer.pipelineId());
                    } catch (TapstateException unread) {
                        // Not confirmed while it cannot be read; a later turn reads it again.
                        if (unread.code() == IoError.STORE_UNAVAILABLE) {
                            return false;
                        }
                        throw unread;
                    }
                }
                Long done = ringDone.get(table.getKey());
                if (done == null || done < table.getValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Writes a released entry down. Its position becomes how far the source may be told to release its log;
     * a position becomes where a restart resumes, and where a consumer's target stands, only when its run
     * carried a change -- for the consumer, one of its own -- because a run carrying none can name a point
     * that, resumed from, passes over the change that follows it, and that no target ever confirmed. An entry
     * that folded a change and then quiet runs writes the change down as the resume point first, and then
     * the quiet position as how far the source was read: what releasing them one by one would have written.
     */
    private void write(Batch batch, Map<String, ConsumerOffset> current) {
        ChainPosition position = batch.position();
        ChainPosition resumeAt = batch.resumeAt();
        if (directPipeline != null) {
            ChainPosition landed = batch.landedAt().get(directPipeline);
            if (landed != null) {
                meta.advanceSinkAcked(chainId, directPipeline, landed);
            }
            ConsumerOffset own = current.get(directPipeline);
            List<ConsumerOffset> withThisEntry = current.values().stream()
                    .map(consumer -> consumer == own ? consumer.withSinkAcked(position) : consumer)
                    .toList();
            // A position held back to another consumer's is that consumer's run, not this one: nothing says it
            // carried a change, so it is not made a resume point.
            SrsDurableFrontier.safeAdvance(position, own == null ? List.of() : withThisEntry).ifPresent(safe -> {
                if (!safe.equals(position) || resumeAt == null) {
                    meta.advanceSourceReadOffset(chainId, safe, false);
                    return;
                }
                if (!resumeAt.equals(position)) {
                    meta.advanceSourceReadOffset(chainId, resumeAt, true);
                }
                meta.advanceSourceReadOffset(chainId, position, resumeAt.equals(position));
            });
            return;
        }
        // One write for the whole release, however many pipelines it was owed to: it is made on the thread the
        // source hands its runs over on.
        Map<String, ChainPosition> acknowledged = new LinkedHashMap<>();
        batch.landedAt().forEach((pipeline, landed) -> {
            if (current.containsKey(pipeline)) {
                acknowledged.put(pipeline, landed);
            }
        });
        if (!meta.advancePhysicalRelease(chainId, epoch, acknowledged, resumeAt, position)) {
            throw lostOrUnverified();
        }
        batch.lastSeqByTable().forEach(trimThrough);
    }

    /** The failure that says why this reader cannot record where it is, read off the chain as it stands now. */
    private TapstateException lostOrUnverified() {
        long current = meta.read(chainId).map(SrsMeta::epoch).orElse(0L);
        return current != epoch
                ? new TapstateException(CaptureError.CHAIN_TAKEN_OVER, Map.of("chain", chainId), null)
                : new TapstateException(CaptureError.SHARED_POSITION_UNVERIFIED, Map.of("chain", chainId), null);
    }

    private void checkOpen() {
        if (closed) {
            throw new CancellationException("the reader's account of chain " + chainId + " is closed");
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** How many recorded runs are waiting on a confirmation; for the cases. */
    synchronized int pendingBatches() {
        return pending.size();
    }

    @Override
    public synchronized void close() {
        closed = true;
        pending.clear();
        ACTIVE.remove(this);
    }

    /**
     * One entry of the account: a run, or adjacent runs folded together. {@code position} is the last position
     * they named -- how far the source has been read once all of them are released -- and {@code resumeAt} the
     * last one named by a run that carried a change, or null where none did. {@code lastSeqByTable} is the last
     * sequence they reached in each table, {@code owed} what each consumer present when a run was recorded owed
     * it then, and {@code landedAt} for each consumer the last position named by a run that carried one of its
     * changes.
     */
    private record Batch(ChainPosition position, ChainPosition resumeAt, Map<String, Long> lastSeqByTable,
            Map<String, Map<String, Long>> owed, Map<String, ChainPosition> landedAt) {

        static Batch of(ChainPosition position, Map<String, Long> lastSeqByTable,
                Map<String, Map<String, Long>> owed) {
            boolean named = position.token() != null;
            Map<String, ChainPosition> landedAt = new LinkedHashMap<>();
            if (named) {
                owed.forEach((pipeline, tables) -> {
                    if (!tables.isEmpty()) {
                        landedAt.put(pipeline, position);
                    }
                });
            }
            return new Batch(position, named && !lastSeqByTable.isEmpty() ? position : null,
                    Map.copyOf(lastSeqByTable), owed, landedAt);
        }

        /** Whether nothing in it carried a change, so nobody owes it anything. */
        boolean quiet() {
            return lastSeqByTable.isEmpty();
        }

        /**
         * This entry with the run recorded after it folded in. It is confirmed only once both are, by the same
         * test either alone would have had to pass: per consumer and table the higher of the two sequences, which
         * belong to one ring, read in order.
         */
        Batch fold(Batch later) {
            Map<String, Long> seqs = new LinkedHashMap<>(lastSeqByTable);
            later.lastSeqByTable.forEach((table, seq) -> seqs.merge(table, seq, Math::max));
            Map<String, Map<String, Long>> debts = new LinkedHashMap<>();
            owed.forEach((pipeline, tables) -> debts.put(pipeline, new LinkedHashMap<>(tables)));
            later.owed.forEach((pipeline, tables) -> {
                Map<String, Long> debt = debts.computeIfAbsent(pipeline, ignored -> new LinkedHashMap<>());
                tables.forEach((table, seq) -> debt.merge(table, seq, Math::max));
            });
            debts.replaceAll((pipeline, tables) -> Map.copyOf(tables));
            Map<String, ChainPosition> landed = new LinkedHashMap<>(landedAt);
            landed.putAll(later.landedAt);
            return new Batch(later.position.token() != null ? later.position : position,
                    later.resumeAt != null ? later.resumeAt : resumeAt, Map.copyOf(seqs), debts, landed);
        }
    }
}
