package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CaptureResumeWitnessTest {
    @Test
    void aFreshFullSnapshotCannotBorrowAnotherReadersCheckpointAsItsOwnSeam() {
        var witness = witness(ReadMode.SNAPSHOT_AND_CDC, true, null, 0, null,
                new ChainPosition(new SourceOrder(4, 200), "physical-reader-200"));
        assertThat(witness.requestedPosition("capture").orElseThrow().kind())
                .isEqualTo(ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED);
    }

    @Test
    void aDirectResumeUsesThisConsumersConfirmedPrefixInsteadOfAnAheadRead() {
        ChainPosition confirmed = new ChainPosition(new SourceOrder(4, 100), "confirmed-100");
        var witness = witness(ReadMode.CDC_ONLY, false, null, 0, confirmed,
                new ChainPosition(new SourceOrder(4, 200), "ahead-200"));
        assertThat(witness.requestedPosition("capture").orElseThrow().position()).isEqualTo(confirmed);
    }

    @Test
    void everyDirectTableMustConfirmTheCommonPrefixBeforeAnAheadReadCanResume() {
        ChainPosition orders = new ChainPosition(new SourceOrder(4, 100), "opaque-orders-100");
        ChainPosition customers = new ChainPosition(new SourceOrder(4, 70), "opaque-customers-70");
        ChainPosition ahead = new ChainPosition(new SourceOrder(4, 200), "opaque-read-200");
        var complete = directTables(List.of("orders", "customers"), Map.of("orders", orders, "customers", customers), ahead);
        var requested = complete.requestedPosition("capture").orElseThrow();
        assertThat(requested.position()).isEqualTo(customers);
        assertThat(requested.provenance()).isEqualTo("mongo-consumer-confirmed-prefix");
        assertThat(requested.captureId()).isEqualTo("capture");
        var incomplete = directTables(List.of("orders", "customers", "products"),
                Map.of("orders", orders, "customers", customers), ahead);
        assertThat(incomplete.requestedPosition("capture")).isEmpty();
    }

    @Test
    void anUnorderedTableAcknowledgementCannotTurnAnAheadReadIntoAResumePoint() {
        var witness = directTables(List.of("orders", "customers"), Map.of(
                "orders", new ChainPosition(new SourceOrder(4, 100), "orders-100"),
                "customers", new ChainPosition(null, "unordered-customer")),
                new ChainPosition(new SourceOrder(4, 200), "ahead-200"));
        assertThat(witness.requestedPosition("capture")).isEmpty();
    }

    @Test
    void aReadBeforeTheConfirmedFloorKeepsItsExactOpaquePoint() {
        ChainPosition read = new ChainPosition(new SourceOrder(4, 60), "opaque-read-60");
        var witness = directTables(List.of("orders", "customers"), Map.of(
                "orders", new ChainPosition(new SourceOrder(4, 100), "orders-100"),
                "customers", new ChainPosition(new SourceOrder(4, 70), "customers-70")), read);
        assertThat(witness.requestedPosition("capture").orElseThrow().position()).isEqualTo(read);
    }

    @Test
    void anUnfinishedSnapshotUsesItsOwnSeamAheadOfTheSharedReadersCheckpoint() {
        var witness = new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer",
                ReadMode.SNAPSHOT_AND_CDC, true, List.of("orders", "customers"), true, 4,
                new ChainPosition(new SourceOrder(4, 200), "another-reader-200"), true, true,
                List.of("orders"), "own-snapshot-seam", 3, ConsumerProgressKind.SRS, null, Map.of());
        var requested = witness.requestedPosition("capture").orElseThrow();
        assertThat(requested.position()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(3), "own-snapshot-seam"));
        assertThat(requested.provenance()).isEqualTo("mongo-consumer-snapshot-seam");
        assertThat(requested.durableStateReference()).isEqualTo("chain/qualified-consumer");
    }

    private static CaptureResumeWitness directTables(List<String> tables, Map<String, ChainPosition> confirmed,
            ChainPosition read) {
        return new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer", ReadMode.CDC_ONLY,
                false, tables, true, 4, read, false, true, List.of(), null, 0,
                ConsumerProgressKind.DIRECT_SOURCE, null, confirmed);
    }

    private static CaptureResumeWitness witness(ReadMode mode, boolean shared, String seam, long snapshotEpoch,
            ChainPosition ack, ChainPosition read) {
        return new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer", mode, shared,
                List.of("orders"), true, 4, read, shared, true, List.of(), seam, snapshotEpoch,
                shared ? ConsumerProgressKind.SRS : ConsumerProgressKind.DIRECT_SOURCE, ack, Map.of());
    }
}
