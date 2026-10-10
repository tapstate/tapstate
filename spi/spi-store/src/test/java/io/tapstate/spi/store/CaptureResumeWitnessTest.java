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

    private static CaptureResumeWitness witness(ReadMode mode, boolean shared, String seam, long snapshotEpoch,
            ChainPosition ack, ChainPosition read) {
        return new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer", mode, shared,
                List.of("orders"), true, 4, read, shared, true, List.of(), seam, snapshotEpoch,
                shared ? ConsumerProgressKind.SRS : ConsumerProgressKind.DIRECT_SOURCE, ack, Map.of());
    }
}
