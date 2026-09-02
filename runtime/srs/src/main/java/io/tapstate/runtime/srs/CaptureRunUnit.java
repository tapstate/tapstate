package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Assembles one source's capture run: it reads the pipeline's {@link ConsumptionPlan} and dispatches the
 * snapshot phase, the cdc phase, the self-built Jet ring source and the mining-chain coordinator into a
 * single run, wiring the durable meta as it goes.
 *
 * <p>The dispatch is driven entirely by the plan (read mode x {@code srs.enabled}): a snapshot phase drains
 * straight to the pass-through sink; a shared-ring tail provisions the mining chain, attaches the consumer,
 * writes the change ring and exposes a Jet source over it; an srs-disabled tail provisions and attaches the
 * same way and streams straight to the one consumer, with no ring. See {@link #start} for the exact ordering.
 *
 * <p><strong>{@code srs.enabled} decides the buffering and nothing else.</strong> Any tail opens the chain
 * and keeps its durable record, so where a tail resumes from does not depend on the flag: a pipeline that
 * turns the buffering off keeps the position it had, and one that turns it back on finds it still there.
 * The alternative is a second account to move a position between, and the move is the step that loses one.
 *
 * <p>What the flag does decide is where a run with nothing recorded begins, because the two paths read
 * {@code start_from} in different coordinates: a direct tail resolves it against the source's own log,
 * while a buffered one resolves it against changes already mined into the ring and leaves the shared
 * miner on the present. That is a difference in what the setting points into, not in whether it is
 * honoured -- and it holds only for a first run, which is the one a recorded position does not outrank.
 *
 * <p>A shared-ring run reads every configured stream through one connector subscription and routes each
 * stream into its own per-table ring. Where the tail begins and what position each change carries are the
 * source's own, read back from the durable record and learned from the changes respectively.
 */
public final class CaptureRunUnit {

    /**
     * The member user-context key under which the durable coordination store is bound, so a ring source's
     * reader can resolve it member-side to publish its read cursor. The assembly layer binds the store under
     * this key when it makes the member SRS-capable.
     */
    public static final String SRS_META_USER_CONTEXT_KEY = "tapstate.srs.meta";

    /**
     * The member user-context key under which the durable change log is bound. The log is reached through
     * the member rather than through this class's constructor because the member is where it already lives
     * -- the rings resolve it from their own configuration -- and a run without a store binds nothing,
     * which reads here as "there is no log to cut".
     */
    public static final String SRS_LOG_USER_CONTEXT_KEY = "tapstate.srs.log";

    private final CapturePort port;
    private final SrsCoordinator coordinator;
    private final SrsMetaStore meta;
    private final HazelcastInstance hz;

    public CaptureRunUnit(CapturePort port, SrsCoordinator coordinator, SrsMetaStore meta, HazelcastInstance hz) {
        this.port = Objects.requireNonNull(port, "port");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.hz = Objects.requireNonNull(hz, "hz");
    }

    /**
     * Starts the source run for {@code spec}, draining any snapshot rows to {@code passthrough}, and returns
     * a handle on the assembled pieces. The steps run in a fixed order so the meta preconditions hold:
     *
     * <ol>
     *   <li>a tail — buffered or direct — provisions the mining chain first, seeding its meta: the
     *       precondition for recording the cdc-start position, and for resuming at one;</li>
     *   <li>the snapshot phase drains to the pass-through sink: on a shared-ring run it records the cdc-start
     *       position at the seam and marks each selected table's snapshot complete once drained, otherwise it
     *       is a pure drain with no chain to position or mark;</li>
     *   <li>a shared-ring tail then attaches the consumer, runs the cdc phase into the change ring, and
     *       exposes the Jet source; an srs-disabled tail attaches the same consumer and streams straight to
     *       the pass-through sink. Both begin where the durable record says, and where it says nothing
     *       the direct one begins where {@code start_from} asked while the shared miner takes the
     *       present — see {@link #sourceStart}.</li>
     * </ol>
     */
    public CaptureRun start(CaptureRunSpec spec, Consumer<Envelope> passthrough) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(passthrough, "passthrough");
        ConsumptionPlan plan = ConsumptionPlan.of(spec.readMode(), spec.srsEnabled());

        MiningChainId chainId = null;
        boolean merged = false;
        boolean chainCreated = false;
        boolean consumerAttached = false;
        long epoch = 0;
        Optional<Subscription> subscription = Optional.empty();
        List<String> tables = spec.config().streams();
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("capture config must select at least one stream");
        }
        try {
            // Any tail opens the chain, buffered or not. The flag chooses whether changes go through the
            // shared ring; it does not choose whether this source has a durable record, because that record
            // is what the next run reads to know where to start -- a question the flag has no bearing on.
            if (plan.tail()) {
                chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
                ProvisionOutcome provisioned = coordinator
                        .provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
                merged = provisioned.merged();
                epoch = provisioned.epoch();
                chainCreated = !merged;
            }

            long snapshotCount = 0;
            Map<String, Long> snapshotCounts = new LinkedHashMap<>();
            // Which tables a resuming run still owes is asked once, by the snapshot phase, of the durable
            // record -- so it survives the process that answered it last, and a run that owes none reads
            // nothing. Asking the coarser "is the whole load done" here as well put the same question to
            // the same record twice, and two readings of one fact are two things that can disagree.
            if (plan.snapshot()) {
                Consumer<Envelope> snapshotPassthrough = event -> {
                    snapshotCounts.merge(event.src(), 1L, Long::sum);
                    passthrough.accept(event);
                };
                // A chainless read has no ring and so no generation to order its rows against: they carry no
                // order at all, which a stateful node downstream rejects rather than guesses at.
                snapshotCount = chainId != null
                        ? SnapshotPhase.run(port, spec.config(), chainId.value(), tables, epoch,
                                meta, snapshotPassthrough)
                        : SnapshotPhase.drain(port, spec.config(), snapshotPassthrough);
            }

            CaptureHealth health = new CaptureHealth();
            Optional<StreamSource<SrsItem>> ringSource = Optional.empty();
            if (plan.sharedRing()) {
                String cid = chainId.value();
                long ringEpoch = epoch;
                coordinator.attachConsumer(chainId, spec.pipelineId());
                consumerAttached = true;
                // The cursors alone, not the whole record: this is read on every run of changes, and the
                // record also carries a schema history that grows per DDL and is never read here.
                Supplier<Collection<ConsumerOffset>> consumers = () -> meta.consumerOffsets(cid);
                // What the acked position can be attributed to decides whether this chain's log can be
                // cut at all. A chain records one acked position for the whole chain, and the sequence in
                // it came from whichever table's ring held that change -- on a chain of one table there is
                // only one ring it could be, so the frontier bounds that log exactly; on a chain of several
                // there is no way to tell which, and cutting the wrong ring deletes changes that still have
                // to be replayed. So a multi-table chain keeps everything, and will until an acked position
                // is recorded per table. That is not a smaller version of this cut, it is a different
                // record, and it belongs with the work that makes a table recoverable on its own.
                SrsLogStore log = hz.getUserContext().get(SRS_LOG_USER_CONTEXT_KEY) instanceof SrsLogStore
                        bound ? bound : null;
                boolean cuttable = log != null && tables.size() == 1;
                Map<String, CdcPhase.TableRoute> routes = new LinkedHashMap<>();
                for (String table : tables) {
                    String ringName = SrsRingbuffer.ringName(cid, table);
                    SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer(ringName)));
                    // One generation across the chain's tables: they are rebuilt together, so a sequence of
                    // one ring is comparable with a sequence of another exactly when both were opened by the
                    // same provisioning.
                    CdcChain chain = new CdcChain(gate, meta, cid, ringEpoch, spec.schemaVer());
                    LongConsumer trim = cuttable ? seq -> log.trim(ringName, seq) : seq -> { };
                    routes.put(table, new CdcPhase.TableRoute(chain, consumers, trim));
                }
                subscription = Optional.of(CdcPhase.run(
                        port, spec.config(), tailStart(meta, cid, CaptureStart.present()),
                        routes, health));
                String firstTable = tables.getFirst();
                String firstRing = SrsRingbuffer.ringName(cid, firstTable);
                ringSource = Optional.of(SrsRingSource.create(
                        firstRing, spec.startFrom(), readCursorPublisher(cid, spec.pipelineId(), firstTable),
                        spec.retention()));
            } else if (plan.directTail()) {
                // srs.enabled:false: the tail streams straight to the consumer with no shared ring. The ring
                // is the whole of what the flag decides -- the chain is open and its record is kept either
                // way -- so this tail begins where that record says, exactly as a buffered one does. Taking
                // the present here instead is a silent loss: the tail comes up healthy and every change
                // between where it had reached and now is gone.
                //
                // It attaches as a consumer for the same reason a buffered tail does: it is using the chain,
                // and a teardown that could not see it would tear the chain out from under a live reader.
                coordinator.attachConsumer(chainId, spec.pipelineId());
                consumerAttached = true;
                String directChain = chainId.value();
                long directEpoch = epoch;
                Supplier<Collection<ConsumerOffset>> directConsumers = () -> meta.consumerOffsets(directChain);
                AtomicLong forwarded = new AtomicLong();
                AtomicReference<ChainPosition> directLastWritten = new AtomicReference<>();
                subscription = Optional.of(port.cdc(
                        spec.config(), tailStart(meta, directChain, sourceStart(spec.startFrom())),
                        health.recording((events, position) -> forwardDirect(
                                events, position, directChain, directEpoch, forwarded,
                                directConsumers, directLastWritten, passthrough))));
            }

            return new CaptureRun(
                    Optional.ofNullable(chainId), merged, snapshotCount, snapshotCounts, ringSource, subscription, health);
        } catch (RuntimeException | Error failure) {
            RuntimeException cleanupFailure = rollbackStartFailure(
                    chainId, spec.pipelineId(), chainCreated, consumerAttached, subscription);
            if (cleanupFailure != null) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /**
     * Forwards one run of changes straight to the consumer and records how far the source has been read.
     *
     * <p>Each change is stamped with its order before it leaves. A direct tail has no ring, and a buffered
     * change takes its order from the ring's sequence, so the count of changes this run has forwarded
     * stands in for it: monotonic within the generation the chain opened, and taken afresh whenever a new
     * one is. Leaving the order off is not the neutral choice it looks like -- every node that ranks
     * positions drops one carrying none, so an unstamped tail is one nothing downstream can ever confirm,
     * and an account nothing confirms never moves.
     *
     * <p><strong>A forwarded count and a ring sequence are not the same quantity.</strong> A chain read
     * both ways at once therefore has two consumers counting differently, and the only thing ever done
     * with the two is to take the lower: the chain reads as the slower of them, which re-mines more than
     * it has to and can never skip. That direction is the one that cannot lose data, which is why the
     * mismatch is affordable and worth saying out loud.
     *
     * <p>The position the source named for the run rides with the change that closes it and no other,
     * exactly as it does through the ring. Carried on the earlier ones it would say of each that the source
     * had already read past the last, and a run interrupted between them would resume past changes never
     * delivered.
     *
     * <p>The offset then advances, clamped so it never passes what a consumer has durably landed. A direct
     * tail buffers nothing, so a change it forwarded that no sink wrote is gone with the process; an offset
     * that had passed it would step over it on the way back, and nothing would ever fetch it again.
     */
    private void forwardDirect(
            List<Envelope> events,
            Optional<SourcePosition> position,
            String miningChainId,
            long epoch,
            AtomicLong forwarded,
            Supplier<Collection<ConsumerOffset>> consumers,
            AtomicReference<ChainPosition> lastWritten,
            Consumer<Envelope> passthrough) {
        if (events.isEmpty()) {
            // The source handed over only events carrying no change -- a heartbeat and its like. There is
            // nothing to forward, and nothing has been read past, so the offset does not move either.
            return;
        }
        int last = events.size() - 1;
        String token = position.map(SourcePosition::token).orElse(null);
        long closingSeq = -1;
        for (int i = 0; i < events.size(); i++) {
            closingSeq = forwarded.getAndIncrement();
            passthrough.accept(events.get(i).withPosition(
                    new ChainPosition(new SourceOrder(epoch, closingSeq), i == last ? token : null)));
        }
        ChainPosition read = new ChainPosition(new SourceOrder(epoch, closingSeq), token);
        SrsDurableFrontier.safeAdvance(read, consumers.get()).ifPresent(safe -> {
            // Unchanged from the run before means the slowest sink has landed nothing since, so this would
            // write the record the value it already holds -- a synchronous round trip, on the thread the
            // source reads on, to say nothing. The pair is compared, not the token alone: a token that
            // repeats across generations is a different position, and comparing halves would skip it.
            if (safe.equals(lastWritten.get())) {
                return;
            }
            meta.advanceSourceReadOffset(miningChainId, safe);
            lastWritten.set(safe);
        });
    }

    private RuntimeException rollbackStartFailure(
            MiningChainId chainId,
            String pipelineId,
            boolean chainCreated,
            boolean consumerAttached,
            Optional<Subscription> subscription) {
        RuntimeException failure = null;
        if (subscription.isPresent()) {
            failure = runCleanup(subscription.get()::close, failure);
        }
        if (chainId != null && consumerAttached) {
            failure = runCleanup(() -> coordinator.detachConsumer(chainId, pipelineId), failure);
        }
        if (chainId != null && chainCreated) {
            failure = runCleanup(() -> coordinator.teardownSource(chainId), failure);
        }
        return failure;
    }

    private static RuntimeException runCleanup(Runnable cleanup, RuntimeException firstFailure) {
        try {
            cleanup.run();
        } catch (RuntimeException failure) {
            if (firstFailure == null) {
                return failure;
            }
            firstFailure.addSuppressed(failure);
        }
        return firstFailure;
    }

    /**
     * Where this chain's tail begins, read back from the durable record rather than assumed.
     *
     * <p>Three states, in this order, and the order is the whole of it:
     *
     * <ol>
     *   <li>a recorded read offset — the tail ran before and got this far, so it picks up there;</li>
     *   <li>no read offset but a recorded seam — the snapshot ran and the tail has not advanced past
     *       where the snapshot began, so it starts at the seam and the idempotent sink absorbs the
     *       overlap;</li>
     *   <li>neither — nothing has read this chain, so {@code firstRun} decides: the start the
     *       caller resolved for a run that has no position to pick up from.</li>
     * </ol>
     *
     * <p>Taking the present in any of the first two states is the silent loss this exists to prevent: the
     * tail comes up healthy, and every change between where it had reached and now is simply gone.
     */
    private static CaptureStart tailStart(
            SrsMetaStore meta, String miningChainId, CaptureStart firstRun) {
        return meta.read(miningChainId)
                .map(record -> {
                    if (record.sourceReadOffset() != null) {
                        return CaptureStart.resume(new SourcePosition(record.sourceReadOffset()));
                    }
                    return record.cdcStartPosition() == null
                            ? firstRun
                            : CaptureStart.resume(new SourcePosition(record.cdcStartPosition()));
                })
                .orElse(firstRun);
    }

    /**
     * The start a {@code start_from} setting names in the source's own log, for a tail that reads the
     * source directly. Every form is an ask only the source can answer -- which of its positions is
     * the oldest it still retains, or which one a given moment corresponds to -- so each crosses the
     * port as itself rather than as a position worked out here.
     *
     * <p>It resolves a first run and nothing later: a recorded position outranks it, because the
     * setting says where a read begins rather than where every run of it begins. Applied again on the
     * way back it would re-read the stretch already read after every restart, and asking for the whole
     * source again is a separate request.
     *
     * <p><strong>Only a direct tail resolves it here.</strong> Through the shared buffer the same
     * setting is this one pipeline's cursor into changes already mined, and the miner is shared by
     * every consumer of the chain -- so a miner that honoured one consumer's ask would move where all
     * the others' changes came from. {@code latest} is the one form the two readings agree on only by
     * accident: with no buffer holding what was already mined, "only what is written from now on" and
     * "the source's present moment" are the same point, and through the buffer they are not.
     */
    private static CaptureStart sourceStart(StartFrom startFrom) {
        return switch (startFrom) {
            case StartFrom.Earliest ignored -> CaptureStart.earliest();
            case StartFrom.Latest ignored -> CaptureStart.present();
            case StartFrom.At at -> CaptureStart.at(at.instant());
        };
    }

    /**
     * The read-cursor publisher factory for one consumer's reader over one table's ring: carried onto the
     * Jet source, it resolves the coordination store from the member's user context and binds a sink that
     * advances that consumer's durable {@code perTableSeq} as the reader drains, without clobbering its
     * sink-ack. It closes over only the chain, pipeline and table coordinates — never the store — so it
     * stays serializable; a member with no store bound resolves to a no-op sink.
     */
    public static SrsReadCursorPublisherFactory readCursorPublisher(
            String miningChainId, String pipelineId, String table) {
        return member -> {
            Object bound = member.getUserContext().get(SRS_META_USER_CONTEXT_KEY);
            if (!(bound instanceof SrsMetaStore memberMeta)) {
                return lastReadSeq -> { };
            }
            return lastReadSeq -> memberMeta.advanceConsumerReadSeq(miningChainId, pipelineId, table, lastReadSeq);
        };
    }
}
