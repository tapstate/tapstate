package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PreparedTransformRowsTest {
    @Test
    void preparedFanOutAndDroppedBranchesSurviveRetriesBeforeTheFollowingOrdinaryRow() throws Exception {
        var position = new ChainPosition(new SourceOrder(1, 7), "p7");
        var first = Envelope.insert(7, "orders", Map.of("id", 1), null).withPosition(position);
        var second = Envelope.insert(7, "orders", Map.of("id", 2), null).withPosition(position);
        var dropped = new SettledPositions(first.positions());
        var firstBranch = new PreparedTransformRows(List.of(first, dropped));
        var secondBranch = new PreparedTransformRows(List.of(second));
        var prepared = new PreparedTransformRows(List.of(firstBranch, secondBranch));
        var ordinary = Envelope.insert(8, "orders", Map.of("id", 3), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 8), "p8"));
        var evaluations = new AtomicInteger();
        var processor = new TransformProcessor(event -> {
            evaluations.incrementAndGet();
            return List.of(event);
        });
        var context = new TestProcessorContext();
        context.setVertexName("terminal");
        var out = new TestOutbox(1);
        processor.init(out, context);
        var input = new TestInbox(List.of(prepared, ordinary));
        var emitted = new ArrayList<Object>();
        assertThat(out.offer("occupied")).isTrue();
        processor.process(0, input);
        assertThat(input.size()).as("a refused carrier stays on the inbox").isEqualTo(2);
        out.drainQueueAndReset(0, new ArrayList<>(), false);
        for (int attempt = 0; attempt < 8 && !input.isEmpty(); attempt++) {
            processor.process(0, input);
            out.drainQueueAndReset(0, emitted, false);
        }
        assertThat(input.isEmpty()).isTrue();
        assertThat(emitted).containsExactly(firstBranch, secondBranch, ordinary);
        assertThat(evaluations.get()).as("only the following ordinary event runs the intermediate port").isEqualTo(1);
        processor.close();

        var finalEvaluations = new AtomicInteger();
        var terminal = new TransformProcessor(event -> {
            finalEvaluations.incrementAndGet();
            return List.of(event);
        });
        var finalOut = new TestOutbox(1);
        terminal.init(finalOut, new TestProcessorContext());
        var finalInput = new TestInbox(emitted);
        var finalRows = new ArrayList<Object>();
        for (int attempt = 0; attempt < 8 && !finalInput.isEmpty(); attempt++) {
            terminal.process(0, finalInput);
            finalOut.drainQueueAndReset(0, finalRows, false);
        }
        assertThat(finalInput.isEmpty()).isTrue();
        assertThat(finalRows).containsExactly(first, dropped, second, ordinary);
        assertThat(finalEvaluations.get()).as("only the following ordinary event runs the final port").isEqualTo(1);
        terminal.close();
    }
}
