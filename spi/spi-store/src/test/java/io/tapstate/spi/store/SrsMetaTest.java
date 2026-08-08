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

    private static SchemaVersion schema() {
        return new SchemaVersion(1L, Map.of("id", "long"), 0L);
    }

    @Test
    void holdsTheMiningChainMetaFields() {
        SrsMeta meta = new SrsMeta("chain-1", "gtid:aaa-1:120",
                List.of(consumer()), "gtid:aaa-1:0", List.of(schema()), "7d", List.of("orders"));
        assertThat(meta.miningChainId()).isEqualTo("chain-1");
        assertThat(meta.sourceReadOffset()).isEqualTo("gtid:aaa-1:120");
        assertThat(meta.consumerOffsets()).containsExactly(consumer());
        assertThat(meta.cdcStartPosition()).isEqualTo("gtid:aaa-1:0");
        assertThat(meta.schemaHistory()).containsExactly(schema());
        assertThat(meta.retention()).isEqualTo("7d");
        assertThat(meta.snapshotCompletedTables()).containsExactly("orders");
    }

    @Test
    void tracksSnapshotCompletionPerTableBecauseOneChainCarriesManyTables() {
        // A mining chain is keyed by the physical source coordinate and deliberately excludes the table
        // subset, so two sources reading different tables of one database share this record. The cdc tail
        // is one log read and its offset is a chain-level scalar, but each table is snapshotted by its own
        // capture run -- so completion is per table, and a chain-level flag could not say which table it
        // meant. A table is listed only once its own snapshot drained.
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, List.of(), null,
                List.of("orders", "order_items"));
        assertThat(meta.snapshotCompletedTables()).containsExactly("orders", "order_items");
    }

    @Test
    void theSixArgConstructorLeavesNoTableMarkedSnapshotComplete() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, List.of(), null);
        assertThat(meta.snapshotCompletedTables()).isEmpty();
    }

    @Test
    void carriesTheRingGenerationAndTheGenerationItsSnapshotIsPinnedTo() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), "gtid:aaa-1:0", List.of(), null,
                List.of("orders"), 4L, 3L);
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
                List.of(), 5L, 4L);
        assertThat(midSnapshotRestart.epoch()).isNotEqualTo(midSnapshotRestart.snapshotEpoch());
    }

    @Test
    void theShorterConstructorsOpenNoGenerationAndPinNoSnapshot() {
        SrsMeta sixArg = new SrsMeta("chain-1", null, List.of(), null, List.of(), null);
        assertThat(sixArg.epoch()).isZero();
        assertThat(sixArg.snapshotEpoch()).isZero();
        SrsMeta sevenArg = new SrsMeta("chain-1", null, List.of(), null, List.of(), null, List.of("orders"));
        assertThat(sevenArg.epoch()).isZero();
        assertThat(sevenArg.snapshotEpoch()).isZero();
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
    void copiesTheSnapshotCompletedTablesSoALaterMutationDoesNotLeakIn() {
        List<String> live = new ArrayList<>();
        live.add("orders");
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, List.of(), null, live);
        live.clear();
        assertThat(meta.snapshotCompletedTables()).containsExactly("orders");
    }

    @Test
    void rejectsMutationOfTheReturnedLists() {
        SrsMeta meta = new SrsMeta("chain-1", null, List.of(), null, List.of(), null);
        assertThatThrownBy(() -> meta.consumerOffsets().add(consumer()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> meta.schemaHistory().add(schema()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> meta.snapshotCompletedTables().add("orders"))
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
        assertThatThrownBy(() -> new SrsMeta("chain-1", null, List.of(), null, List.of(), null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
