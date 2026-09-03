package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reads where a pipeline picks up from, and moves it.
 *
 * <p>What it moves is the chain's own read offset, because that is what a run actually starts from. The
 * per-pipeline acked position is reported beside it and never written: it records what a sink confirmed,
 * and no request can make that true.
 *
 * <p>Everything the request could be refused for is decided before anything is written. A write-back that
 * names two chains and is going to be refused for the second must not have moved the first — half of one
 * is a state nobody asked for and no message mentions.
 */
public final class PipelinePositionService {

    private final PipelineChains chains;
    private final SrsMetaStore meta;
    private final ArtifactStore artifacts;
    private final LivePipelines live;
    private final AuditGate auditGate;

    public PipelinePositionService(PipelineChains chains, SrsMetaStore meta, ArtifactStore artifacts,
            LivePipelines live, AuditGate auditGate) {
        this.chains = Objects.requireNonNull(chains, "chains");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.live = Objects.requireNonNull(live, "live");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
    }

    /**
     * Where {@code pipelineId} would pick up, chain by chain. A chain nothing has read yet is still
     * listed, carrying no position: what a reader needs is which chains there are and what stands against
     * each, and an omitted chain answers neither.
     */
    public PipelinePosition read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        List<PipelinePosition.Chain> reported = new ArrayList<>();
        for (PipelineChains.Chain chain : chains.of(pipelineId)) {
            reported.add(report(pipelineId, chain));
        }
        return new PipelinePosition(pipelineId, reported);
    }

    /**
     * Moves where one or more of the pipeline's chains resume from, and answers with the reading
     * afterwards.
     *
     * <p>{@code requested} is what {@link #read} handed out, edited. Only {@code resumeFrom.token} may
     * differ; anything else present and disagreeing is refused by name rather than ignored, because a
     * face that quietly dropped an edit would have the caller believe it landed. A value the caller left
     * out asserts nothing, so the same call takes the whole document back or a two-line one.
     */
    public PipelinePosition writeBack(String caller, String pipelineId, PipelinePosition requested) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(requested, "requested");
        PipelinePosition current = read(pipelineId);
        Map<String, String> moves = new LinkedHashMap<>();
        for (PipelinePosition.Chain asked : requested.chains()) {
            PipelinePosition.Chain stored = matching(current, asked, pipelineId);
            refuseEditsOutsideTheResumePoint(asked, stored);
            String token = asked.resumeFrom() == null ? null : asked.resumeFrom().token();
            if (token != null && !token.equals(tokenOf(stored.resumeFrom()))) {
                moves.put(stored.chainId(), token);
            }
        }
        if (moves.isEmpty()) {
            throw new TapstateException(
                    PositionError.NOTHING_TO_WRITE, Map.of("pipeline", pipelineId), null);
        }
        for (String chainId : moves.keySet()) {
            refuseWhileAnyReaderIsUp(chainId, current);
        }
        return auditGate.dispatch(
                ControlOperations.PIPELINE_SET_POSITION, new AuditContext(caller, pipelineId), () -> {
                    moves.forEach((chainId, token) -> {
                        releaseTheAcksThatWouldOutrankIt(chainId);
                        meta.rewindSourceReadOffset(chainId, token);
                    });
                    return read(pipelineId);
                });
    }

    /**
     * Lets go of every sink-ack recorded on the chain, so that the next run's first advance cannot put the
     * offset back where the write-back took it from.
     *
     * <p>Without this the move survives exactly one change. A stop that keeps state leaves each consumer's
     * record in place, holding the position its sink confirmed in the generation that run used; the run
     * that comes up opens a generation of its own, so its own reading outranks that ack by generation, and
     * the clamp — which takes the lowest — therefore takes the ack. The advance then writes a token
     * sitting <em>ahead</em> of the written-back one, and the store admits it, because a written-back
     * offset carries no order to have to outrank. The tail has already read where to start by then, so the
     * changes do come through; what is lost is the record of it, and with it any second restart.
     *
     * <p>Letting the acks go is the conservative direction, not a loss of one: an ack bounds how far the
     * source read may pass, so with none recorded the chain advances nothing until a sink confirms
     * something in the current generation — which is the first thing the resumed run does. Nobody's
     * confirmation is contradicted; they simply stop bounding a read that was deliberately moved behind
     * them. Every pipeline on the chain is at rest by the time this runs, so none of them is mid-flight.
     */
    private void releaseTheAcksThatWouldOutrankIt(String chainId) {
        for (ConsumerOffset offset : meta.read(chainId).map(SrsMeta::consumerOffsets).orElse(List.of())) {
            if (offset.sinkAcked() != null) {
                // Rewritten rather than deleted: the read cursor and the tables whose initial load this
                // pipeline finished are answers about work that did happen, and moving the tail says
                // nothing about either.
                meta.upsertConsumerOffset(chainId, new ConsumerOffset(offset.pipelineId(),
                        offset.perTableSeq(), null, offset.snapshotCompletedTables()));
            }
        }
    }

    /** The stored chain the request names, or a coded refusal naming the ones this pipeline does read. */
    private static PipelinePosition.Chain matching(
            PipelinePosition current, PipelinePosition.Chain asked, String pipelineId) {
        for (PipelinePosition.Chain chain : current.chains()) {
            if (chain.chainId().equals(asked.chainId())) {
                return chain;
            }
        }
        throw new TapstateException(PositionError.CHAIN_NOT_READ, Map.of(
                "chain", String.valueOf(asked.chainId()),
                "pipeline", pipelineId,
                "known", current.chains().stream().map(PipelinePosition.Chain::chainId).toList()), null);
    }

    /**
     * Refuses a request that changed anything but where the chain resumes from.
     *
     * <p>Everything else in the document is a reading. Let through, a caller who edited one would be told
     * the write-back succeeded and find their edit gone — and the two most likely to be edited by mistake,
     * the acked position and the ring coordinate, are exactly the two that look like they would move the
     * resume point.
     */
    private static void refuseEditsOutsideTheResumePoint(
            PipelinePosition.Chain asked, PipelinePosition.Chain stored) {
        refuseChange("sourceId", asked.chainId(), asked.sourceId(), stored.sourceId());
        refuseChange("tables", asked.chainId(),
                asked.tables().isEmpty() ? null : asked.tables(), stored.tables());
        refuseChange("recordedAt", asked.chainId(), asked.recordedAt(), stored.recordedAt());
        refuseChange("sinkAcked", asked.chainId(), asked.sinkAcked(), stored.sinkAcked());
        refuseChange("sharedWith", asked.chainId(),
                asked.sharedWith().isEmpty() ? null : asked.sharedWith(), stored.sharedWith());
        if (asked.resumeFrom() != null) {
            PipelinePosition.Point held = stored.resumeFrom();
            refuseChange("resumeFrom.epoch", asked.chainId(), asked.resumeFrom().epoch(),
                    held == null ? null : held.epoch());
            refuseChange("resumeFrom.seq", asked.chainId(), asked.resumeFrom().seq(),
                    held == null ? null : held.seq());
        }
    }

    private static void refuseChange(String field, String chainId, Object asked, Object stored) {
        if (asked != null && !asked.equals(stored)) {
            throw new TapstateException(PositionError.FIELD_NOT_EDITABLE,
                    Map.of("field", field, "chain", chainId), null);
        }
    }

    /**
     * Refuses while any pipeline reading the chain is still up.
     *
     * <p>Every pipeline on the chain, not just the one being asked about. A chain is keyed by the source's
     * physical coordinates and excludes the table subset, so pipelines share one by construction and a
     * write-back moves it for all of them at once. Guarding only the named pipeline would leave the case
     * that matters wide open: somebody else's running read, moved out from under it.
     *
     * <p>Who counts as on the chain comes from two places, and neither alone is enough. The chain's own
     * consumer records name whoever has attached to it; the reference graph names whoever declares the
     * source, including a pipeline that has never run and would come straight up on the moved position.
     * Naming too many here only refuses more.
     *
     * <p><strong>The test is "at rest", not the one an artifact edit is guarded by</strong>, and the two
     * differ on exactly the state most likely to be written back from. An artifact edit is safe under a
     * paused pipeline because both ways out of a pause re-read the definition. This is not an artifact:
     * a pause suspends the engine and nothing else, so the capture goes on reading and goes on advancing
     * the very offset being written. Allowed there, a write-back would be overwritten by the next
     * advance — and overwritten silently, because a written-back position carries no order for that
     * advance to have to outrank.
     */
    private void refuseWhileAnyReaderIsUp(String chainId, PipelinePosition current) {
        Set<String> readers = new LinkedHashSet<>();
        readers.add(current.pipelineId());
        for (PipelinePosition.Chain chain : current.chains()) {
            if (chain.chainId().equals(chainId)) {
                readers.addAll(chain.sharedWith());
                readers.addAll(declaring(chain.sourceId()));
            }
        }
        List<String> up = readers.stream().filter(reader -> !live.isAtRest(reader)).sorted().toList();
        if (!up.isEmpty()) {
            throw new TapstateException(PositionError.WRITE_BACK_WHILE_LIVE,
                    Map.of("chain", chainId, "pipelines", up), null);
        }
    }

    /** The stored pipelines declaring {@code sourceId}, whether or not any of them has ever run. */
    private List<String> declaring(String sourceId) {
        List<Resource> stored = artifacts.list();
        Set<String> pipelines = new LinkedHashSet<>();
        for (Resource resource : stored) {
            if (resource instanceof PipelineResource) {
                pipelines.add(resource.id());
            }
        }
        return ReferenceGraph.of(stored).referencedBy(sourceId).stream()
                .map(ReferenceGraph.Edge::id)
                .filter(pipelines::contains)
                .toList();
    }

    /** One chain's reading: where it resumes, when that was written, this pipeline's ack, and who shares it. */
    private PipelinePosition.Chain report(String pipelineId, PipelineChains.Chain chain) {
        Optional<SrsMeta> record = meta.read(chain.chainId());
        Optional<ConsumerOffset> mine = record.stream()
                .flatMap(found -> found.consumerOffsets().stream())
                .filter(offset -> offset.pipelineId().equals(pipelineId))
                .findFirst();
        Set<String> shared = new TreeSet<>();
        record.ifPresent(found -> found.consumerOffsets().forEach(offset -> {
            if (!offset.pipelineId().equals(pipelineId)) {
                shared.add(offset.pipelineId());
            }
        }));
        return new PipelinePosition.Chain(
                chain.chainId(),
                chain.sourceId(),
                chain.tables(),
                pointOf(record.map(SrsMeta::sourceRead).orElse(null)),
                record.map(SrsMeta::sourceReadAt).map(Instant::toString).orElse(null),
                pointOf(mine.map(ConsumerOffset::sinkAcked).orElse(null)),
                List.copyOf(shared));
    }

    private static PipelinePosition.Point pointOf(ChainPosition position) {
        if (position == null || position.token() == null) {
            return null;
        }
        return position.order() == null
                ? PipelinePosition.Point.at(position.token())
                : new PipelinePosition.Point(
                        position.token(), position.order().epoch(), position.order().seq());
    }

    private static String tokenOf(PipelinePosition.Point point) {
        return point == null ? null : point.token();
    }
}
