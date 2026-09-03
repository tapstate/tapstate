package io.tapstate.spi.store;

import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.ChainPosition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SrsMetaTest {

    private static ConsumerOffset consumer() {
        return new ConsumerOffset("orders-pipeline", Map.of("orders", 42L), new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100"));
    }

    private static ConsumerOffset consumer(String pipelineId, String... completed) {
        return new ConsumerOffset(pipelineId, Map.of(), null, List.of(completed));
    }

    private static SchemaVersion schema() {
        return new SchemaVersion(1L, Map.of("id", "long"), 0L);
    }

    @Test
    void holdsTheMiningChainMetaFields() {
        SrsMeta meta = new SrsMeta("chain-1",
                new ChainPosition(new SourceOrder(1L, 120L), "gtid:aaa-1:120"),
                List.of(consumer()), "gtid:aaa-1:0", List.of(schema()), "7d");
        assertThat(meta.miningChainId()).isEqualTo("chain-1");
        // The pair, and the token on its own: the order is what a later comparison runs on, the token is
        // what a read resumes from, and dropping either leaves a record no advance can be ranked against.
        assertThat(meta.sourceRead()).isEqualTo(new ChainPosition(new SourceOrder(1L, 120L), "gtid:aaa-1:120"));
        assertThat(meta.sourceReadOffset()).isEqualTo("gtid:aaa-1:120");
        assertThat(meta.consumerOffsets()).containsExactly(consumer());
        assertThat(meta.cdcStartPosition()).isEqualTo("gtid:aaa-1:0");
        assertThat(meta.schemaHistory()).containsExactly(schema());
        assertThat(meta.retention()).isEqualTo("7d");
    }

    /**
     * Snapshot completion is read per pipeline, and two pipelines on one chain get different answers.
     *
     * <p>The chain is keyed by the physical source coordinate and deliberately excludes the table subset,
     * so pipelines reading one database share this record while writing to targets of their own. "Has this
     * table's initial load landed" is therefore each pipeline's own question. Asserting both readings in
     * one case is what discriminates: an implementation that answered from a single chain-level list would
     * hand both pipelines the same tables and pass any case that asked only one of them.
     */
    @Test
    void readsSnapshotCompletionPerPipelineBecauseEachWritesToItsOwnTarget() {
        SrsMeta meta = new SrsMeta("chain-1", null,
                List.of(consumer("pipe-a", "orders", "order_items"), consumer("pipe-b", "orders")),
                null, List.of(), null);

        assertThat(meta.snapshotCompletedTables("pipe-a")).containsExactly("orders", "order_items");
        assertThat(meta.snapshotCompletedTables("pipe-b")).containsExactly("orders");
    }

    /**
     * A pipeline with no record on the chain has finished nothing, which is the answer and not a refusal.
     *
     * <p>It is the state a pipeline new to a chain is in, and the reading it needs: it owes every table it
     * selected. Refusing here instead would make the common case an error, and answering with another
     * pipeline's tables is the defect this reading exists to make impossible.
     */
    @Test
    void aPipelineWithNoRecordOnTheChainHasFinishedNothing() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(consumer("pipe-a", "orders")),
                null, List.of(), null);
        assertThat(meta.snapshotCompletedTables("pipe-b")).isEmpty();
    }

    /**
     * Completion stays per table as well as per pipeline: one chain carries many tables, each snapshotted
     * by its own capture run and finishing at its own time, so a per-pipeline flag could not say which
     * table it meant.
     */
    @Test
    void tracksSnapshotCompletionPerTableBecauseOneChainCarriesManyTables() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(consumer("pipe-a", "orders", "order_items")),
                null, List.of(), null);
        assertThat(meta.snapshotCompletedTables("pipe-a")).containsExactly("orders", "order_items");
    }

    @Test
    void carriesTheRingGenerationAndTheGenerationItsSnapshotIsPinnedTo() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), "gtid:aaa-1:0", List.of(), null, 4L, 3L);
        assertThat(meta.epoch()).isEqualTo(4L);
        assertThat(meta.snapshotEpoch()).isEqualTo(3L);
    }

    @Test
    void theTwoGenerationsAreSeparateBecauseASnapshotOutlivesTheRingItStartedUnder() {
        // A restart opens a new ring generation while a snapshot that had not drained keeps the one it
        // began in -- so its rows can never win against changes the earlier generation already applied.
        // One field could not hold both, and reading the current generation for a rerun's rows is exactly
        // the reversal this record exists to prevent.
        SrsMeta midSnapshotRestart = new SrsMeta("chain-1", null, List.of(), "gtid:aaa-1:0", List.of(), null,
                5L, 4L);
        assertThat(midSnapshotRestart.epoch()).isNotEqualTo(midSnapshotRestart.snapshotEpoch());
    }

    @Test
    void theShorterConstructorOpensNoGenerationAndPinsNoSnapshot() {
        SrsMeta sixArg = new SrsMeta("chain-1", null, List.of(), null, List.of(), null);
        assertThat(sixArg.epoch()).isZero();
        assertThat(sixArg.snapshotEpoch()).isZero();
    }

    @Test
    void allowsNullableOffsetsBeforeAnyCdcHasBeenRead() {
        // A freshly seeded mining chain: no source read offset yet, no cdc-start position, no retention
        // set, and no consumers or schema versions attached.
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, List.of(), null);
        assertThat(meta.sourceReadOffset()).isNull();
        assertThat(meta.cdcStartPosition()).isNull();
        assertThat(meta.retention()).isNull();
        assertThat(meta.consumerOffsets()).isEmpty();
        assertThat(meta.schemaHistory()).isEmpty();
    }

    @Test
    void copiesTheConsumerListSoALaterMutationDoesNotLeakIn() {
        List<ConsumerOffset> live = new ArrayList<>();
        live.add(consumer());
        SrsMeta meta = new SrsMeta("chain-1", null, live, null, List.of(), null);
        live.clear();
        assertThat(meta.consumerOffsets()).containsExactly(consumer());
    }

    @Test
    void copiesTheSchemaHistorySoALaterMutationDoesNotLeakIn() {
        List<SchemaVersion> live = new ArrayList<>();
        live.add(schema());
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, live, null);
        live.clear();
        assertThat(meta.schemaHistory()).containsExactly(schema());
    }

    @Test
    void rejectsMutationOfTheReturnedLists() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, List.of(), null);
        assertThatThrownBy(() -> meta.consumerOffsets().add(consumer()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> meta.schemaHistory().add(schema()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsABlankMiningChainId() {
        assertThatThrownBy(() -> new SrsMeta("  ", null, List.of(), null, List.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullLists() {
        assertThatThrownBy(() -> new SrsMeta("chain-1", null, null, null, List.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SrsMeta("chain-1", null, List.of(), null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
