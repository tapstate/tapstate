package io.tapstate.control.restapi;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The dependent stores a removed pipeline's reclaim would touch, for HTTP-face tests that never remove a
 * pipeline. Every method refuses rather than quietly doing nothing: a call on any of them means the
 * reclaim ran where no bookkeeping exists to reclaim, and a no-op stub would let that pass as green —
 * the same silent-success shape that hides a reclaim which was never wired up at all.
 */
final class NoReclaimStores {

    private NoReclaimStores() {
    }

    private static AssertionError unexpected(String call) {
        return new AssertionError("no reclaim is expected in this test, but " + call + " was called");
    }

    static DesiredStore desired() {
        return new DesiredStore() {
            @Override
            public void save(DesiredState desired) {
                throw unexpected("DesiredStore.save");
            }

            @Override
            public Optional<DesiredState> read(String pipelineId) {
                throw unexpected("DesiredStore.read");
            }

            @Override
            public List<String> pipelineIds() {
                throw unexpected("DesiredStore.pipelineIds");
            }

            @Override
            public void delete(String pipelineId) {
                throw unexpected("DesiredStore.delete");
            }
        };
    }

    static StateStore state() {
        return new StateStore() {
            @Override
            public Optional<CheckpointDoc> read(String pipelineId) {
                throw unexpected("StateStore.read");
            }

            @Override
            public void create(String pipelineId, String stateJson, Instant touchTime) {
                throw unexpected("StateStore.create");
            }

            @Override
            public CasOutcome compareAndSwap(
                    String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
                throw unexpected("StateStore.compareAndSwap");
            }

            @Override
            public void delete(String pipelineId) {
                throw unexpected("StateStore.delete");
            }
        };
    }

    static ObservationStore observations() {
        return new ObservationStore() {
            @Override
            public void save(Observation observation) {
                throw unexpected("ObservationStore.save");
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                throw unexpected("ObservationStore.read");
            }

            @Override
            public void delete(String pipelineId) {
                throw unexpected("ObservationStore.delete");
            }
        };
    }

    static SrsMetaStore srsMeta() {
        return new SrsMetaStore() {
            @Override
            public Optional<SrsMeta> read(String miningChainId) {
                throw unexpected("SrsMetaStore.read");
            }

            @Override
            public void create(String miningChainId, String retention) {
                throw unexpected("SrsMetaStore.create");
            }

            @Override
            public void advanceSourceReadOffset(String miningChainId, io.tapstate.core.event.ChainPosition position) {
                throw unexpected("SrsMetaStore.advanceSourceReadOffset");
            }

            @Override
            public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
                throw unexpected("SrsMetaStore.upsertConsumerOffset");
            }

            @Override
            public void advanceConsumerReadSeq(
                    String miningChainId, String pipelineId, String table, long lastReadSeq) {
                throw unexpected("SrsMetaStore.advanceConsumerReadSeq");
            }

            @Override
            public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition acked) {
                throw unexpected("SrsMetaStore.advanceSinkAcked");
            }

            @Override
            public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
                throw unexpected("SrsMetaStore.setCdcStart");
            }

            @Override
            public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
                throw unexpected("SrsMetaStore.markSnapshotComplete");
            }

            @Override
            public long openEpoch(String miningChainId) {
                throw unexpected("SrsMetaStore.openEpoch");
            }

            @Override
            public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
                throw unexpected("SrsMetaStore.appendSchemaVersion");
            }

            @Override
            public List<String> miningChainIdsWithConsumer(String pipelineId) {
                throw unexpected("SrsMetaStore.miningChainIdsWithConsumer");
            }

            @Override
            public void detachConsumer(String miningChainId, String pipelineId) {
                throw unexpected("SrsMetaStore.detachConsumer");
            }

            @Override
            public void dropChain(String miningChainId) {
                throw unexpected("SrsMetaStore.dropChain");
            }
        };
    }
}
