package io.tapstate.core.event;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SourceOrderTest {

    @Test
    void aHigherEpochWinsWhateverTheSequence() {
        SourceOrder earlierGeneration = new SourceOrder(7L, 999L);
        SourceOrder laterGeneration = new SourceOrder(8L, 0L);
        assertThat(laterGeneration).isGreaterThan(earlierGeneration);
        assertThat(earlierGeneration).isLessThan(laterGeneration);
    }

    @Test
    void withinOneEpochTheSequenceDecides() {
        assertThat(new SourceOrder(7L, 5L)).isGreaterThan(new SourceOrder(7L, 4L));
        assertThat(new SourceOrder(7L, 4L)).isLessThan(new SourceOrder(7L, 5L));
    }

    @Test
    void equalOrdersCompareEqualAndHashAlike() {
        SourceOrder one = new SourceOrder(7L, 5L);
        SourceOrder same = new SourceOrder(7L, 5L);
        assertThat(one).isEqualByComparingTo(same).isEqualTo(same);
        assertThat(one.hashCode()).isEqualTo(same.hashCode());
    }

    @Test
    void aReservedSequenceBelowTheRingOrdersBeforeEveryChangeOfItsEpoch() {
        SourceOrder snapshotRow = new SourceOrder(7L, SourceOrder.SNAPSHOT_SEQ);
        assertThat(snapshotRow).isLessThan(new SourceOrder(7L, 0L));
        assertThat(snapshotRow).isGreaterThan(new SourceOrder(6L, Long.MAX_VALUE));
    }

    @Test
    void everySnapshotRowOfOneGenerationCarriesTheSameReservedSequence() {
        assertThat(SourceOrder.snapshotRow(7L)).isEqualTo(new SourceOrder(7L, SourceOrder.SNAPSHOT_SEQ));
        assertThat(SourceOrder.snapshotRow(7L)).isEqualByComparingTo(SourceOrder.snapshotRow(7L));
    }

    @Test
    void noSequenceTheRingCanAssignReachesDownToTheReservedOne() {
        // The ring assigns from zero upwards, so reserving the very bottom of the range keeps the
        // guarantee total: no change of a generation can ever tie with, let alone precede, its snapshot.
        assertThat(SourceOrder.SNAPSHOT_SEQ).isEqualTo(Long.MIN_VALUE);
        assertThat(SourceOrder.snapshotRow(7L)).isLessThan(new SourceOrder(7L, Long.MIN_VALUE + 1));
    }

    @Test
    void sortsIntoTheOrderTheEventsWereAssigned() {
        List<SourceOrder> shuffled = new ArrayList<>(List.of(
                new SourceOrder(8L, 0L),
                new SourceOrder(7L, 1L),
                new SourceOrder(7L, 0L)));
        shuffled.sort(null);
        assertThat(shuffled).containsExactly(
                new SourceOrder(7L, 0L),
                new SourceOrder(7L, 1L),
                new SourceOrder(8L, 0L));
    }

    @Test
    void survivesSerializationSoOperatorStateCanBeStored() throws Exception {
        SourceOrder order = new SourceOrder(7L, 5L);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(order);
        }
        Object read;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            read = in.readObject();
        }
        assertThat(read).isEqualTo(order);
    }
}
