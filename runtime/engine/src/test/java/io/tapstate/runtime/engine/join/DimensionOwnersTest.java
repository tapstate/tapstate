package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.datamodel.Tuple2;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.runtime.engine.SettledPositions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DimensionOwnersTest {
    @Test
    void aRefusedArrivalKeepsItsPositionAheadOfTheFollowingMarker() throws Exception {
        var position = new ChainPosition(new SourceOrder(1, 9), "p9");
        var move = Envelope.update(9, "c", Map.of("id", 1), Map.of("id", 2), null).withPosition(position);
        var marker = new SettledPositions(move.positions());
        assertThat(throughOneSlot(move, marker)).containsExactly(
                Tuple2.tuple2(JoinKey.of(List.of(1)).name(), move),
                Tuple2.tuple2(JoinKey.of(List.of(2)).name(), move), marker);
    }

    @Test
    void equivalentCompositeKeysStayOnTheOrdinaryPathAndNullHasNoDimensionBucket() throws Exception {
        var before = Map.<String, Object>of("id", new BigDecimal("1.0"), "region", "east");
        var after = Map.<String, Object>of("id", new BigDecimal("1.00"), "region", "east");
        var stable = Envelope.update(1, "c", before, after, null);
        var nullRegion = new java.util.HashMap<>(after);
        nullRegion.put("region", null);
        var leaving = Envelope.update(2, "c", after, nullRegion, null);
        var arriving = Envelope.update(3, "c", nullRegion, after, null);
        assertThat(throughOneSlot(List.of("id", "region"), stable, leaving, arriving))
                .containsExactly(stable, Tuple2.tuple2(JoinKey.of(List.of(after.get("id"), "east")).name(), leaving),
                        arriving);
    }

    private static List<Object> throughOneSlot(Object... items) throws Exception {
        return throughOneSlot(List.of("id"), items);
    }

    private static List<Object> throughOneSlot(List<String> columns, Object... items) throws Exception {
        var processor = new DimensionOwners(columns);
        var outbox = new TestOutbox(new int[] {1}, 1);
        processor.init(outbox, new TestProcessorContext());
        var inbox = new TestInbox(List.of(items));
        List<Object> output = new ArrayList<>();
        for (int i = 0; i <= items.length * 2 && !inbox.isEmpty(); i++) {
            processor.process(0, inbox);
            outbox.drainQueueAndReset(0, output, false);
        }
        assertThat(inbox.isEmpty()).isTrue();
        return output;
    }
}
