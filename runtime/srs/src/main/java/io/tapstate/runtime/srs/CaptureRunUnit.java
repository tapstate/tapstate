package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Assembles one source's capture run: it reads the pipeline's {@link ConsumptionPlan} and dispatches the
 * snapshot phase, the cdc phase, the self-built Jet ring source and the mining-chain coordinator into a
 * single run, wiring the durable meta as it goes.
 *
 * <p>The dispatch is driven entirely by the plan (read mode x {@code srs.enabled}): a snapshot phase drains
 * straight to the pass-through sink; a shared-ring tail provisions the mining chain, attaches the consumer,
 * writes the change ring and exposes a Jet source over it; an srs-disabled tail streams straight to the one
 * consumer with no ring or coordinator. See {@link #start} for the exact ordering.
 *
 * <p>A shared-ring run reads every configured stream through one connector subscription and routes each
 * stream into its own per-table ring. The mock watermark and position order the spec carries stand in for
 * real connector machinery.
 */
public final class CaptureRunUnit {

    /**
     * The member user-context key under which the durable coordination store is bound, so a ring source's
     * reader can resolve it member-side to publish its read cursor. The assembly layer binds the store under
     * this key when it makes the member SRS-capable.
     */
    public static final String SRS_META_USER_CONTEXT_KEY = "tapstate.srs.meta";

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
     *   <li>a shared-ring tail provisions the mining chain first, seeding its meta — the precondition for
     *       recording the cdc-start position;</li>
     *   <li>the snapshot phase drains to the pass-through sink: on a shared-ring run it records the cdc-start
     *       position at the seam, otherwise it is a pure drain with no chain to position;</li>
     *   <li>a shared-ring tail then attaches the consumer, runs the cdc phase into the change ring, and
     *       exposes the Jet source; an srs-disabled tail instead streams straight to the pass-through sink.</li>
     * </ol>
     */
    public CaptureRun start(CaptureRunSpec spec, Consumer<Envelope> passthrough) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(passthrough, "passthrough");
        ConsumptionPlan plan = ConsumptionPlan.of(spec.readMode(), spec.srsEnabled());

        MiningChainId chainId = null;
        boolean merged = false;
        List<String> tables = spec.config().streams();
        if (plan.sharedRing()) {
            chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            merged = coordinator
                    .provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention())
                    .merged();
        }

        long snapshotCount = 0;
        Map<String, Long> snapshotCounts = new LinkedHashMap<>();
        if (plan.snapshot()) {
            Consumer<Envelope> snapshotPassthrough = event -> {
                snapshotCounts.merge(event.src(), 1L, Long::sum);
                passthrough.accept(event);
            };
            snapshotCount = chainId != null
                    ? SnapshotPhase.run(port, spec.config(), chainId.value(), spec.cdcStart(), meta, snapshotPassthrough)
                    : SnapshotPhase.drain(port, spec.config(), snapshotPassthrough);
        }

        CaptureHealth health = new CaptureHealth();
        Optional<StreamSource<SrsItem>> ringSource = Optional.empty();
        Optional<Subscription> subscription = Optional.empty();
        if (plan.sharedRing()) {
            String cid = chainId.value();
            coordinator.attachConsumer(chainId, spec.pipelineId());
            Supplier<Collection<ConsumerOffset>> consumers =
                    () -> meta.read(cid).map(SrsMeta::consumerOffsets).orElse(List.of());
            Map<String, CdcPhase.TableRoute> routes = new LinkedHashMap<>();
            for (String table : tables) {
                String ringName = SrsRingbuffer.ringName(cid, table);
                SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer(ringName)));
                CdcChain chain = new CdcChain(
                        gate, meta, cid, spec.watermark(), spec.positionOrder(), spec.schemaVer());
                routes.put(table, new CdcPhase.TableRoute(
                        chain, () -> minConsumerReadSeq(meta, cid, table), consumers));
            }
            subscription = Optional.of(CdcPhase.run(port, spec.config(), routes, health));
            String firstTable = tables.getFirst();
            String firstRing = SrsRingbuffer.ringName(cid, firstTable);
            ringSource = Optional.of(SrsRingSource.create(
                    firstRing, spec.startFrom(), readCursorPublisher(cid, spec.pipelineId(), firstTable)));
        } else if (plan.directTail()) {
            // srs.enabled:false: the tail streams straight to the consumer, with no shared ring,
            // no coordinator chain and no durable meta -- the lightweight direct path.
            subscription = Optional.of(port.cdc(spec.config(), health.recording(passthrough::accept)));
        }

        return new CaptureRun(
                Optional.ofNullable(chainId), merged, snapshotCount, snapshotCounts, ringSource, subscription, health);
    }

    /**
     * The slowest consumer's read cursor into one table's ring — the headroom bound the cdc write gate reads
     * back from the durable consumer offsets. {@link Long#MAX_VALUE} when no consumer constrains the ring
     * (none has a durable cursor yet), and {@code -1} for a consumer that has read nothing of the table.
     */
    static long minConsumerReadSeq(SrsMetaStore meta, String miningChainId, String table) {
        return meta.read(miningChainId)
                .map(m -> m.consumerOffsets().stream()
                        .mapToLong(c -> c.perTableSeq().getOrDefault(table, -1L))
                        .min()
                        .orElse(Long.MAX_VALUE))
                .orElse(Long.MAX_VALUE);
    }

    /**
     * The read-cursor publisher factory for one consumer's reader over one table's ring: carried onto the
     * Jet source, it resolves the coordination store from the member's user context and binds a sink that
     * advances that consumer's durable {@code perTableSeq} as the reader drains, without clobbering its
     * sink-ack. It closes over only the chain, pipeline and table coordinates — never the store — so it
     * stays serializable; a member with no store bound resolves to a no-op sink.
     */
    static SrsReadCursorPublisherFactory readCursorPublisher(String miningChainId, String pipelineId, String table) {
        return member -> {
            Object bound = member.getUserContext().get(SRS_META_USER_CONTEXT_KEY);
            if (!(bound instanceof SrsMetaStore memberMeta)) {
                return lastReadSeq -> { };
            }
            return lastReadSeq -> memberMeta.advanceConsumerReadSeq(miningChainId, pipelineId, table, lastReadSeq);
        };
    }
}
