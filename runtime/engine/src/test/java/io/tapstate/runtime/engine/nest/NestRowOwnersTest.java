package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.EnvelopeSerializer;
import io.tapstate.runtime.engine.SettledPositions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NestRowOwnersTest {
    @Test
    void aPartialUpdateCannotProveTheRowVacatedItsKey() throws Exception {
        var withoutOldKey = Envelope.update(1, "customers", Map.of("name", "Ada"), Map.of("id", 2), null);
        var withoutNewKey = Envelope.update(2, "customers", Map.of("id", 2), Map.of("name", "Grace"), null);
        assertThat(throughOneSlot(withoutOldKey, withoutNewKey)).containsExactly(withoutOldKey, withoutNewKey);
    }

    @Test
    void aRefusedArrivalStaysAheadOfOldKeyReuseAndTheProgressMarker() throws Exception {
        var position = new ChainPosition(new SourceOrder(1, 9), "p9");
        var move = Envelope.update(9, "customers", Map.of("id", 1), Map.of("id", 2), null)
                .withPosition(position);
        var reuse = Envelope.insert(10, "customers", Map.of("id", 1), null);
        var marker = new SettledPositions(move.positions());
        var output = throughOneSlot(move, reuse, marker);
        assertThat(output).containsExactly(Tuple2.tuple2(List.of(1), move), move, reuse, marker);
    }

    @Test
    void aDepartureSentToAnotherMemberKeepsTheOriginalEvent() throws Exception {
        var move = Envelope.update(9, "customers", Map.of("id", 1), Map.of("id", 2), null)
                .withOrder(new SourceOrder(1, 9));
        var outgoing = throughOneSlot(move);
        InternalSerializationService serialization = new DefaultSerializationServiceBuilder()
                .setConfig(new Config().getSerializationConfig().addSerializerConfig(new SerializerConfig()
                        .setTypeClass(Envelope.class).setImplementation(new EnvelopeSerializer())))
                .build();
        try {
            Object received = serialization.toObject(serialization.toData(outgoing.getFirst()));
            assertThat(received).isEqualTo(Tuple2.tuple2(List.of(1), move));
        } finally {
            serialization.dispose();
        }
    }

    @Test
    void ordinaryRootInputsKeepTheirOriginalObjectsAndNoDepartureIsSent() throws Exception {
        var stable = Envelope.update(1, "customers", Map.of("id", 1, "v", 1),
                Map.of("id", 1, "v", 2), null);
        var inserted = Envelope.insert(2, "customers", Map.of("id", 2), null);
        assertThat(throughOneSlot(stable, inserted)).containsExactly(stable, inserted);
    }

    private static List<Object> throughOneSlot(Object... items) throws Exception {
        var processor = new NestRowOwners(List.of("id"), "root-input-test");
        var outbox = new TestOutbox(new int[] {1, 1}, 1);
        processor.init(outbox, new TestProcessorContext());
        var inbox = new TestInbox(List.of(items));
        List<Object> output = new ArrayList<>();
        for (int i = 0; i <= items.length * 2 && !inbox.isEmpty(); i++) {
            processor.process(0, inbox);
            outbox.drainQueueAndReset(0, output, false);
            assertThat(outbox.queue(1)).isEmpty();
        }
        assertThat(inbox.isEmpty()).isTrue();
        return output;
    }
}
