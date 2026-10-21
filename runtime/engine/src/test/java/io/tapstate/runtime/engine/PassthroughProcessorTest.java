package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.test.TestSupport;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PassthroughProcessorTest {

    @Test
    void a_settlement_marker_stays_between_the_rows_on_either_side() {
        Envelope before = Envelope.insert(1, "orders", Map.of("id", 1), null);
        Envelope after = Envelope.insert(2, "orders", Map.of("id", 2), null);
        SettledPositions word = new SettledPositions(
                Map.of("orders", new ChainPosition(new SourceOrder(1, 7), "p7")));
        TestSupport.verifyProcessor(() -> new PassthroughProcessor())
                .input(List.of(before, word, after))
                .expectOutput(List.of(before, word, after));
    }
}
