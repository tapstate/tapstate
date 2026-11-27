package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SinkLaneCommandTest {
    @Test
    void nativeCommandsPreserveTheirRowsAndAcknowledgmentAddressAcrossTheWire() {
        com.hazelcast.internal.serialization.InternalSerializationService serialization = new DefaultSerializationServiceBuilder()
                .setConfig(new Config().getSerializationConfig().addSerializerConfig(new SerializerConfig()
                        .setTypeClass(Envelope.class).setImplementation(new EnvelopeSerializer())))
                .build();
        try {
            var ticket = new SinkLaneAcks.Ticket(UUID.randomUUID(), UUID.randomUUID(), 7);
            var position = new ChainPosition(new SourceOrder(3, 42), "resume");
            var rows = List.of(
                    Envelope.update(7, "orders", Map.of("id", 1), Map.of("id", 2), null).withPosition(position),
                    Envelope.insert(8, "orders", Map.of("id", 1), null).withPosition(position),
                    Envelope.delete(9, "orders", Map.of("id", 2), null).withPosition(position));
            for (Envelope row : rows) {
                var written = new SinkLaneCommand.Move("orders/new", row, ticket);
                SinkLaneCommand.Move read = serialization.toObject(serialization.toData(written));
                assertThat(read.lane()).isEqualTo(written.lane());
                assertThat(read.row()).isEqualTo(row);
                assertThat(read.reply()).isEqualTo(ticket);
            }
            var fence = new SinkLaneCommand.Fence("orders/old", ticket);
            Object read = serialization.toObject(serialization.toData(fence));
            assertThat(read).isEqualTo(fence);
        } finally {
            serialization.dispose();
        }
    }
}
