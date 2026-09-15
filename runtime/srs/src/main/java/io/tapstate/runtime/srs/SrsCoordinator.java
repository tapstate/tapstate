package io.tapstate.runtime.srs;

import io.tapstate.spi.store.SrsMetaStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The single-node coordinator for SRS mining chains: it force-merges cdc sources onto shared chains and
 * enforces the SRS lifecycle boundary. It holds the coordination truth for one node — which sources and
 * consumer pipelines each chain carries, and each chain's unioned table set — over a durable
 * {@link SrsMetaStore} that outlives it. Distributed shared mining (cross-node chain ownership) is out of
 * scope here.
 *
 * <p>The boundary is structural, one method per lifecycle act:
 * <ul>
 *   <li>{@link #provisionSource} opens a chain, bound to the source run-unit — a chain exists the moment its
 *       source is applied, with or without any pipeline consuming it. The first source to reach a chain
 *       seeds its meta; every later same-chain source is force-merged, joining and unioning its tables
 *       without reseeding.</li>
 *   <li>{@link #attachConsumer} / {@link #releaseConsumer} touch only the calling pipeline's membership — a
 *       pipeline start adds its own consumer entry and a stop gives it back, and by construction neither
 *       reaches the shared chain's tables, its other consumers or the durable meta. The release closes the
 *       chain when it is the last one out, which is a chain with nobody on it rather than a teardown.</li>
 *   <li>{@link #planSourceTeardown} then {@link #teardownSource} make source-level cleanup a separate,
 *       explicit two-step act: the plan first lists every consumer pipeline the teardown would affect (it is
 *       never triggered implicitly by a pipeline detaching), and only an explicit teardown removes a shared
 *       chain out from under whoever is still on it.</li>
 * </ul>
 *
 * <p>The durable per-consumer read cursor ({@code consumer_offsets[].perTableSeq}) is published later, when
 * the capture run unit is wired; attaching a consumer here registers its membership, the input to that
 * wiring and to the affected-pipeline list, not the cursor itself. Methods are synchronized: lifecycle acts
 * are few and already serialized by the control plane, and the check-then-act steps must stay atomic.
 */
public final class SrsCoordinator {

    private final SrsMetaStore meta;
    private final Map<String, ChainState> chains = new LinkedHashMap<>();

    public SrsCoordinator(SrsMetaStore meta) {
        this.meta = Objects.requireNonNull(meta, "meta");
    }

    /**
     * Opens the chain for a cdc source, bound to the source run-unit. The first source to reach a chain
     * seeds its durable meta (carrying the pass-through retention) and opens a ring generation; a later
     * same-chain source is force-merged — no reseed, no new generation, it just joins and unions its
     * {@code streams}. Returns whether the chain was already open (a merge), the chain's table set after
     * this source, and the generation it now reads under.
     *
     * <p>Opening the chain is what establishes the ring, so it is where a generation is taken: a restart
     * or a re-mine arrives here with no chain state and takes a new one, while a source merging onto a
     * running chain reads under the generation already in flight. Taking one per source instead would make
     * two sources of one chain order their changes against each other by which arrived first.
     */
    public synchronized ProvisionOutcome provisionSource(
            String sourceId, MiningChainId chainId, List<String> streams, String retention) {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(chainId, "chainId");
        Objects.requireNonNull(streams, "streams");
        ChainState state = chains.get(chainId.value());
        boolean merged = state != null;
        if (state == null) {
            // Seed only a chain that has none, and open regardless. The durable record outlives this
            // process, so after a restart the chain is opened again with its record already there; seeding
            // is insert-only precisely so an accumulated offset / cursor / schema truth is never discarded,
            // and skipping the seed is how that is honoured. Opening still happens either way -- a rebuilt
            // ring is a new generation.
            //
            // Re-opening a chain that already has durable history stays a quiet, ordinary act, and the
            // reason has changed rather than merely survived: the record it preserves is now read back by
            // the run that follows -- the position its tail starts from, and whether its full load already
            // finished. Refusing here, or branching on "this chain has been read before", would announce
            // what the caller is about to ask the store anyway, and would refuse the one case resuming is
            // for. What would be wrong is discarding the record, which insert-only seeding already stops.
            if (meta.read(chainId.value()).isEmpty()) {
                meta.create(chainId.value(), retention);
            }
            state = new ChainState(chainId, meta.openEpoch(chainId.value()));
            chains.put(chainId.value(), state);
        }
        state.sources.add(sourceId);
        state.tables.addAll(streams);
        return new ProvisionOutcome(chainId, merged, List.copyOf(state.tables), state.epoch);
    }

    /** Whether the chain has been opened by a source. */
    public synchronized boolean isProvisioned(MiningChainId chainId) {
        return chains.containsKey(chainId.value());
    }

    /** The chain's unioned table set. The chain must be provisioned. */
    public synchronized List<String> tablesOf(MiningChainId chainId) {
        return List.copyOf(require(chainId).tables);
    }

    /** The cdc sources force-merged onto the chain. The chain must be provisioned. */
    public synchronized List<String> sourcesOf(MiningChainId chainId) {
        return List.copyOf(require(chainId).sources);
    }

    /**
     * Registers a pipeline as a consumer of the chain — the "apply pipeline" step, which requires the chain
     * already open (else a caller ordering error). This records membership only; the durable read cursor is
     * published when the capture run unit is wired.
     */
    public synchronized void attachConsumer(MiningChainId chainId, String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        require(chainId).consumers.add(pipelineId);
    }

    /**
     * Removes a pipeline's consumer membership — the "pipeline stop / start" step. It touches only this
     * pipeline's own entry: never the shared chain, its tables, its other consumers, or the durable meta.
     * Removing a pipeline that is not a consumer is a no-op.
     */
    public synchronized void detachConsumer(MiningChainId chainId, String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        require(chainId).consumers.remove(pipelineId);
    }

    /**
     * Gives back this pipeline's hold on the chain and closes the chain when it was the last one holding it,
     * answering whether the chain closed. This is what a pipeline stop does; a stop does not reach
     * {@link #teardownSource}, which takes a chain away from whoever is still on it.
     *
     * <p>One act rather than a detach followed by a look at {@link #affectedConsumers}, for two reasons.
     * The question and what is done about it have to be decided under the same lock, or a consumer
     * attaching in between makes the answer stale before it is used. And the boolean it returns is meant
     * to be <em>the</em> reading of "is anybody still on this chain" that the rest of a stop goes by --
     * the chain-level durable state has to be cleared on exactly the stop that closes the chain here, and
     * two places working that out from their own scan would disagree the first time the two scans ran at
     * different moments.
     *
     * <p>A chain nobody consumes is not the same as a chain nobody opened: a source can hold one open with
     * no pipeline reading it, and that chain is left standing here because no consumer was ever on it to
     * give one back. Taking that one away is the source-level act.
     */
    public synchronized boolean releaseConsumer(MiningChainId chainId, String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        ChainState state = require(chainId);
        state.consumers.remove(pipelineId);
        if (!state.consumers.isEmpty()) {
            return false;
        }
        chains.remove(chainId.value());
        return true;
    }

    /**
     * The consumer pipelines currently on the chain — the list a source-level teardown must present before it
     * runs, so the boundary is never crossed implicitly. The chain must be provisioned.
     */
    public synchronized List<String> affectedConsumers(MiningChainId chainId) {
        return List.copyOf(require(chainId).consumers);
    }

    /**
     * Computes what a source-level teardown of the chain would affect — its consumer pipelines and its
     * per-table ring names — without changing anything. This is the list a caller must present before an
     * actual teardown, so tearing a chain down is never implicit. The chain must be provisioned.
     */
    public synchronized SourceTeardownPlan planSourceTeardown(MiningChainId chainId) {
        ChainState state = require(chainId);
        List<String> ringNames = new ArrayList<>();
        for (String table : state.tables) {
            ringNames.add(SrsRingbuffer.ringName(state.chainId.value(), table));
        }
        return new SourceTeardownPlan(state.chainId, List.copyOf(state.consumers), ringNames);
    }

    /**
     * Closes the chain — the source-level cleanup, a separate explicit act a pipeline stop never reaches. It
     * removes the chain's single-node coordination state; resetting the durable meta and destroying the
     * member's rings (named by {@link #planSourceTeardown}) is the executor's follow-up. The chain must be
     * provisioned.
     *
     * <p>Unlike {@link #releaseConsumer} this does not ask who is on the chain, and that is the difference
     * between the two rather than an oversight: a source being taken away takes its chain with it, and
     * {@link #planSourceTeardown} exists so the consumers that costs are named to somebody first.
     */
    public synchronized void teardownSource(MiningChainId chainId) {
        require(chainId);
        chains.remove(chainId.value());
    }

    private ChainState require(MiningChainId chainId) {
        ChainState state = chains.get(Objects.requireNonNull(chainId, "chainId").value());
        if (state == null) {
            throw new IllegalStateException("mining chain not provisioned: " + chainId.value());
        }
        return state;
    }

    /**
     * One mining chain's single-node coordination state: its member sources, unioned tables, consumers, and
     * the ring generation opened when the chain was. The generation is held here so every source that
     * merges onto the chain reads under the one already running rather than taking its own.
     */
    private static final class ChainState {
        private final MiningChainId chainId;
        private final long epoch;
        private final Set<String> sources = new LinkedHashSet<>();
        private final Set<String> tables = new LinkedHashSet<>();
        private final Set<String> consumers = new LinkedHashSet<>();

        ChainState(MiningChainId chainId, long epoch) {
            this.chainId = chainId;
            this.epoch = epoch;
        }
    }
}
