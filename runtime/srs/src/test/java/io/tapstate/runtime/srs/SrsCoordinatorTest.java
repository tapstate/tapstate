package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The single-node SRS coordinator: it force-merges cdc sources onto shared mining chains (a chain is opened
 * once, then every same-config source joins and unions its tables) and enforces the SRS lifecycle boundary
 * — a source opens a chain independent of any pipeline, a pipeline attaches and detaches only its own
 * consumer membership without ever touching the shared chain, and tearing a chain down is a separate
 * explicit act that first lists the consumer pipelines it would affect. The durable per-consumer read
 * cursor is written later, when the run unit is wired; here the coordinator owns identity, membership and
 * the boundary.
 */
class SrsCoordinatorTest {

    private static final MiningChainId CHAIN = MiningChainId.ofKey("orders-db");

    // ---- provision + forced merge ------------------------------------------------

    @Test
    void provisioningANewChainSeedsItsMetaOnce() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);

        ProvisionOutcome out = coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");

        assertThat(out.merged()).isFalse();
        assertThat(out.chainId()).isEqualTo(CHAIN);
        assertThat(out.tables()).containsExactlyInAnyOrder("orders");
        assertThat(meta.created).containsEntry(CHAIN.value(), "7d");
        assertThat(coord.isProvisioned(CHAIN)).isTrue();
    }

    @Test
    void provisioningASecondSourceOnTheSameChainForceMergesWithoutReseeding() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);
        coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");
        int mutationsAfterFirst = meta.mutations.size();

        ProvisionOutcome out = coord.provisionSource("src-b", CHAIN, List.of("customers", "orders"), "30d");

        // The chain is already open: no second seed (create is insert-only), the joining source just unions.
        assertThat(out.merged()).isTrue();
        assertThat(meta.mutations).hasSize(mutationsAfterFirst);
        assertThat(out.tables()).containsExactlyInAnyOrder("orders", "customers");
        assertThat(coord.tablesOf(CHAIN)).containsExactlyInAnyOrder("orders", "customers");
        // Both sources are recorded on the one chain -- the force-merge, observable.
        assertThat(coord.sourcesOf(CHAIN)).containsExactlyInAnyOrder("src-a", "src-b");
    }

    @Test
    void openingTheChainOpensARingGeneration() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);

        ProvisionOutcome out = coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");

        // Opening the chain is what establishes the ring, so it is where a generation is taken. Every
        // order on this chain compares generation first, and a restart arrives here with no chain state.
        assertThat(out.epoch()).isEqualTo(1L);
        assertThat(meta.mutations).contains("openEpoch:" + CHAIN.value());
    }

    @Test
    void aSourceMergingOntoAnOpenChainReadsUnderTheGenerationAlreadyRunning() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);
        long opened = coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d").epoch();

        ProvisionOutcome merged = coord.provisionSource("src-b", CHAIN, List.of("customers"), "30d");

        // One ring, one generation. A source that took its own would make two sources of the same chain
        // order their changes against each other by which of them happened to be provisioned first.
        assertThat(merged.epoch()).isEqualTo(opened);
        assertThat(meta.mutations).filteredOn(m -> m.startsWith("openEpoch:")).hasSize(1);
    }

    @Test
    void reopeningAChainAfterARestartTakesTheNextGenerationWithoutReseeding() {
        FakeMeta meta = new FakeMeta();
        long first = new SrsCoordinator(meta).provisionSource("src-a", CHAIN, List.of("orders"), "7d").epoch();

        // A restart loses the in-process chain state but not the durable record, so the rebuilt ring is a
        // new generation -- which is what lets its changes win against state the previous one left behind.
        // Seeding is insert-only, so a chain that already has a record must be opened without being seeded
        // again: re-seeding it would either fail the restart outright or discard the offsets it accumulated.
        long second = new SrsCoordinator(meta).provisionSource("src-a", CHAIN, List.of("orders"), "7d").epoch();

        assertThat(second).isGreaterThan(first);
        assertThat(meta.mutations).filteredOn("create:%s".formatted(CHAIN.value())::equals).hasSize(1);
    }

    @Test
    void aSourceOpensItsChainEvenWithNoPipelineConsuming() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);

        // Provision is bound to the source run-unit, not to a pipeline: the chain opens unconsumed.
        coord.provisionSource("src-a", CHAIN, List.of("orders"), null);

        assertThat(coord.isProvisioned(CHAIN)).isTrue();
    }

    // ---- consumer attach / detach boundary ---------------------------------------

    @Test
    void attachingConsumersTracksEachPipelineDistinctlyWithoutTouchingMeta() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);
        coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");
        int mutationsAfterProvision = meta.mutations.size();

        coord.attachConsumer(CHAIN, "p1");
        coord.attachConsumer(CHAIN, "p2");

        assertThat(coord.affectedConsumers(CHAIN)).containsExactlyInAnyOrder("p1", "p2");
        // Membership only at this stage: the durable per-consumer cursor is published when the run unit is wired.
        assertThat(meta.mutations).hasSize(mutationsAfterProvision);
    }

    @Test
    void attachingAConsumerToAnUnopenedChainIsAnOrderingError() {
        SrsCoordinator coord = new SrsCoordinator(new FakeMeta());

        // A pipeline can only consume a chain its source has already opened.
        assertThatThrownBy(() -> coord.attachConsumer(CHAIN, "p1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void detachingAConsumerTouchesOnlyItsOwnMembership() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);
        coord.provisionSource("src-a", CHAIN, List.of("orders", "customers"), "7d");
        coord.attachConsumer(CHAIN, "p1");
        coord.attachConsumer(CHAIN, "p2");
        int mutationsBeforeDetach = meta.mutations.size();

        coord.detachConsumer(CHAIN, "p1");

        // Only p1 is gone; the shared chain -- its other consumer, its tables, its durable meta -- is untouched.
        assertThat(coord.affectedConsumers(CHAIN)).containsExactly("p2");
        assertThat(coord.isProvisioned(CHAIN)).isTrue();
        assertThat(coord.tablesOf(CHAIN)).containsExactlyInAnyOrder("orders", "customers");
        assertThat(meta.mutations).hasSize(mutationsBeforeDetach);
    }

    @Test
    void detachingANonConsumerIsANoOp() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);
        coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");
        coord.attachConsumer(CHAIN, "p1");

        // A pipeline stop that had not started consuming clears nothing.
        coord.detachConsumer(CHAIN, "never-attached");

        assertThat(coord.affectedConsumers(CHAIN)).containsExactly("p1");
    }

    @Test
    void detachingTheLastConsumerLeavesTheChainOpen() {
        SrsCoordinator coord = new SrsCoordinator(new FakeMeta());
        coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");
        coord.attachConsumer(CHAIN, "p1");

        coord.detachConsumer(CHAIN, "p1");

        // The chain's lifecycle is the source's, not its consumers': an empty chain stays open, still mining.
        assertThat(coord.affectedConsumers(CHAIN)).isEmpty();
        assertThat(coord.isProvisioned(CHAIN)).isTrue();
    }

    // ---- source teardown: never implicit, lists affected first -------------------

    @Test
    void planningATeardownListsAffectedConsumersAndRingsWithoutChangingAnything() {
        FakeMeta meta = new FakeMeta();
        SrsCoordinator coord = new SrsCoordinator(meta);
        coord.provisionSource("src-a", CHAIN, List.of("orders", "customers"), "7d");
        coord.attachConsumer(CHAIN, "p1");
        coord.attachConsumer(CHAIN, "p2");
        int mutationsBefore = meta.mutations.size();

        SourceTeardownPlan plan = coord.planSourceTeardown(CHAIN);

        assertThat(plan.affectedConsumers()).containsExactlyInAnyOrder("p1", "p2");
        assertThat(plan.ringNames()).containsExactlyInAnyOrder(
                SrsRingbuffer.ringName(CHAIN.value(), "orders"),
                SrsRingbuffer.ringName(CHAIN.value(), "customers"));
        // Planning is read-only: the chain, its consumers and its durable meta are all still there.
        assertThat(coord.isProvisioned(CHAIN)).isTrue();
        assertThat(coord.affectedConsumers(CHAIN)).containsExactlyInAnyOrder("p1", "p2");
        assertThat(meta.mutations).hasSize(mutationsBefore);
    }

    @Test
    void tearingDownASourceClosesTheChainAndIsReachedOnlyByAnExplicitCall() {
        SrsCoordinator coord = new SrsCoordinator(new FakeMeta());
        coord.provisionSource("src-a", CHAIN, List.of("orders"), "7d");
        coord.attachConsumer(CHAIN, "p1");

        // A detach never reaches here (see detachingTheLastConsumerLeavesTheChainOpen); only teardown closes it.
        coord.teardownSource(CHAIN);

        assertThat(coord.isProvisioned(CHAIN)).isFalse();
    }

    @Test
    void planningOrTearingDownAnUnopenedChainIsAnOrderingError() {
        SrsCoordinator coord = new SrsCoordinator(new FakeMeta());

        assertThatThrownBy(() -> coord.planSourceTeardown(CHAIN)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> coord.teardownSource(CHAIN)).isInstanceOf(IllegalStateException.class);
    }

    /**
     * An in-memory {@link SrsMetaStore} that records every mutation. {@code created} maps a seeded chain to
     * its retention; {@code mutations} is the ordered log used to assert a step touched — or did not touch —
     * the durable store. {@code create} is insert-only, matching the contract.
     */
    private static final class FakeMeta implements SrsMetaStore {
        final Map<String, String> created = new LinkedHashMap<>();
        final Map<String, SrsMeta> records = new LinkedHashMap<>();
        final List<String> mutations = new ArrayList<>();
        long epoch;

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public void create(String miningChainId, String retention) {
            if (records.containsKey(miningChainId)) {
                throw new IllegalStateException("chain already seeded: " + miningChainId);
            }
            created.put(miningChainId, retention);
            records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
            mutations.add("create:" + miningChainId);
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
            mutations.add("advance:" + miningChainId);
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            mutations.add("upsert:" + miningChainId + ":" + offset.pipelineId());
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            mutations.add("readSeq:" + miningChainId + ":" + pipelineId + ":" + table);
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            mutations.add("sinkAck:" + miningChainId + ":" + pipelineId);
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            mutations.add("cdcStart:" + miningChainId);
        }

        @Override
        public long openEpoch(String miningChainId) {
            mutations.add("openEpoch:" + miningChainId);
            return ++epoch;
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String table) {
            mutations.add("snapshotComplete:" + miningChainId + "/" + table);
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            mutations.add("schema:" + miningChainId);
        }
    }
}
