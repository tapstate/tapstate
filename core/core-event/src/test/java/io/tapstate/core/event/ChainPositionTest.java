package io.tapstate.core.event;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ChainPositionTest {

    @Test
    void theOrderAndTheTokenTravelTogether() {
        ChainPosition position = new ChainPosition(new SourceOrder(7L, 500L), "binlog.000042:1024");
        assertThat(position.order()).isEqualTo(new SourceOrder(7L, 500L));
        assertThat(position.token()).isEqualTo("binlog.000042:1024");
    }

    @Test
    void aSnapshotRowIsOrderedWithNoTokenOfItsOwn() {
        ChainPosition snapshotRow = new ChainPosition(new SourceOrder(7L, Long.MIN_VALUE), null);
        assertThat(snapshotRow.token()).isNull();
        assertThat(snapshotRow.order()).isNotNull();
    }

    @Test
    void anEventWithNoPositionAtAllIsAllowedToCarryNeither() {
        ChainPosition synthetic = new ChainPosition(null, null);
        assertThat(synthetic.order()).isNull();
        assertThat(synthetic.token()).isNull();
    }

    @Test
    void twoPositionsOfTheSameEventAreEqual() {
        ChainPosition one = new ChainPosition(new SourceOrder(7L, 5L), "t");
        ChainPosition same = new ChainPosition(new SourceOrder(7L, 5L), "t");
        assertThat(one).isEqualTo(same);
        assertThat(one.hashCode()).isEqualTo(same.hashCode());
    }

    @Test
    void survivesSerializationSoAHeldEventKeepsItsPosition() throws Exception {
        ChainPosition position = new ChainPosition(new SourceOrder(7L, 5L), "t");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(position);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(position);
        }
    }
}
