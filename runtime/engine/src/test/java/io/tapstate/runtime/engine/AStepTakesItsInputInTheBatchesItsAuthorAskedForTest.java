package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A step whose author asked for batches takes its input in them: at most {@code max_records} rows at a time,
 * handed on at once where no wait was asked for, or once the first of them has waited {@code max_wait} - found
 * out by reading the clock, never by sleeping. A full batch does not wait. Nothing is held past a bound, the end
 * of an edge or the end of the input, and rows keep the edge they arrived on and the order they arrived in.
 */
class AStepTakesItsInputInTheBatchesItsAuthorAskedForTest {

    private static final long MINUTE_NANOS = TimeUnit.MINUTES.toNanos(1);

    private final AtomicLong nanos = new AtomicLong();
    private final Recording step = new Recording();

    @Test
    void withNoWaitTheRowsAlreadyThereGoAtOnceABatchAtATime() {
        InputBatches batches = batches(2, 0);
        TestInbox inbox = inbox(1, 2, 3, 4, 5);

        batches.process(0, inbox);
        batches.process(0, inbox);
        batches.process(0, inbox);

        assertThat(step.log).containsExactly("0:[1, 2]", "0:[3, 4]", "0:[5]");
        assertThat(inbox).isEmpty();
    }

    @Test
    void withAWaitTheRowsGoOnceTheFirstOfThemHasWaitedItOut() {
        InputBatches batches = batches(10, TimeUnit.MILLISECONDS.toNanos(50));

        batches.process(0, inbox(1, 2));
        elapse(30);
        batches.process(0, inbox(3));
        elapse(19);
        batches.tryProcess();
        assertThat(step.log).as("the first row has waited 49ms of 50").isEmpty();

        elapse(1);
        batches.tryProcess();

        assertThat(step.log).containsExactly("0:[1, 2, 3]");
    }

    @Test
    void aFullBatchGoesWithoutWaitingAndTheRestStaysUpstream() {
        InputBatches batches = batches(3, MINUTE_NANOS);
        TestInbox inbox = inbox(1, 2, 3, 4);

        batches.process(0, inbox);

        assertThat(step.log).containsExactly("0:[1, 2, 3]");
        assertThat(inbox).as("left where it was, holding the rows behind it back").containsExactly(4);
    }

    @Test
    void aWaitIsFoundOutByReadingTheClockNeverBySleeping() {
        InputBatches batches = batches(10, MINUTE_NANOS);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            batches.process(0, inbox(1));
            batches.tryProcess();
            batches.process(0, inbox(2));
        });

        assertThat(step.log).isEmpty();
    }

    @Test
    void aBoundHandsOverWhatIsHeldBeforeItGoesOn() {
        InputBatches batches = batches(10, MINUTE_NANOS);
        batches.process(0, inbox(1, 2));

        assertThat(batches.tryProcessWatermark(0, new Watermark(7))).isTrue();
        assertThat(batches.tryProcessWatermark(new Watermark(7))).isTrue();

        assertThat(step.log).containsExactly("0:[1, 2]", "bound@0", "bound");
    }

    @Test
    void aBoundWaitsUntilEveryRowBeforeItHasBeenTaken() {
        step.perCall = 1;
        InputBatches batches = batches(10, MINUTE_NANOS);
        batches.process(0, inbox(1, 2, 3));

        assertThat(batches.tryProcessWatermark(0, new Watermark(7))).isFalse();
        assertThat(batches.tryProcessWatermark(0, new Watermark(7))).isFalse();
        assertThat(batches.tryProcessWatermark(0, new Watermark(7))).isTrue();

        assertThat(step.log).containsExactly("0:[1]", "0:[2]", "0:[3]", "bound@0");
    }

    @Test
    void theEndOfAnEdgeAndOfTheInputEachHandOverWhatIsHeldFirst() {
        InputBatches batches = batches(10, MINUTE_NANOS);

        batches.process(0, inbox(1, 2));
        assertThat(batches.completeEdge(0)).isTrue();
        batches.process(1, inbox(3));
        assertThat(batches.complete()).isTrue();

        assertThat(step.log).containsExactly("0:[1, 2]", "edge-end@0", "1:[3]", "end");
    }

    @Test
    void rowsReachTheStepOnTheEdgeTheyArrivedOnInTheOrderTheyArrived() {
        InputBatches batches = batches(10, MINUTE_NANOS);

        batches.process(0, inbox("a", "b"));
        batches.process(1, inbox("c"));
        batches.process(0, inbox("d"));
        batches.complete();

        assertThat(step.log).containsExactly("0:[a, b]", "1:[c]", "0:[d]", "end");
    }

    @Test
    void rowsTheStepCouldNotTakeYetGoBeforeAnyTakenAfterThem() {
        step.perCall = 1;
        InputBatches batches = batches(2, MINUTE_NANOS);
        TestInbox inbox = inbox(1, 2, 3);

        batches.process(0, inbox);
        assertThat(step.log).as("a full batch, of which the step could take one row").containsExactly("0:[1]");

        step.perCall = Integer.MAX_VALUE;
        batches.process(0, inbox);
        assertThat(step.log).as("the rest of the full batch goes without waiting - and alone: row 3 waits its own turn")
                .containsExactly("0:[1]", "0:[2]");

        nanos.addAndGet(MINUTE_NANOS);
        batches.tryProcess();
        assertThat(step.log).containsExactly("0:[1]", "0:[2]", "0:[3]");
    }

    @Test
    void aStepIsDrawnTakingItsInputInBatchesOnlyWhereItsAuthorAskedForThem() {
        PipelineResource pipeline = new PipelineResource("p", null, List.of(SourceRef.bare("orders")),
                List.of(
                        Step.inline("batched", FromClause.list(FromRef.literal("orders")), new TransformBody.Js("row"),
                                new ExecutionSpec(null, new BatchSpec(2, "1m")), null),
                        Step.inline("as_it_comes", FromClause.list(FromRef.literal("batched")),
                                new TransformBody.Js("row"), null),
                        Step.inline("merged", FromClause.list(FromRef.literal("as_it_comes")),
                                new TransformBody.Union(), new ExecutionSpec(null, new BatchSpec(8, null)), null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("merged"),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
        DagBindings bindings = new DagBindings(
                sourceId -> ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of((SupplierEx<Processor>) TestNoOp::new), sourceId),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) NoWrites::new,
                ref -> List.of(((FromRef.Literal) ref).ref()));

        DAG dag = PipelineDagBuilder.build(pipeline, bindings);

        assertThat(InputBatches.takesInputInBatches(dag.getVertex("batched").getMetaSupplier())).isTrue();
        assertThat(InputBatches.takesInputInBatches(dag.getVertex("as_it_comes").getMetaSupplier())).isFalse();
        assertThat(InputBatches.takesInputInBatches(dag.getVertex("merged").getMetaSupplier()))
                .as("a union takes its input in batches like any other step").isTrue();
    }

    /** A source that emits nothing: the graph is drawn, never run. */
    private static final class TestNoOp extends com.hazelcast.jet.core.AbstractProcessor {
    }

    /** A writer the drawn graph names and never calls. */
    private static final class NoWrites implements SinkWriter {

        @Override
        public java.util.concurrent.CompletionStage<io.tapstate.spi.sink.WriteResult> write(
                List<io.tapstate.core.event.Envelope> records) {
            throw new AssertionError("the graph is drawn, never run");
        }

        @Override
        public void close() {
        }
    }

    private InputBatches batches(int maxRecords, long maxWaitNanos) {
        return new InputBatches(step, maxRecords, maxWaitNanos, nanos::get);
    }

    private void elapse(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    private static TestInbox inbox(Object... rows) {
        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(rows));
        return inbox;
    }

    /** A step that notes what it is handed, each call on its own line, taking at most {@code perCall} rows. */
    private static final class Recording implements Processor {

        private final List<String> log = new ArrayList<>();
        private int perCall = Integer.MAX_VALUE;

        @Override
        public void process(int ordinal, Inbox inbox) {
            List<Object> taken = new ArrayList<>();
            for (Object row; taken.size() < perCall && (row = inbox.peek()) != null; ) {
                taken.add(row);
                inbox.remove();
            }
            if (!taken.isEmpty()) {
                log.add(ordinal + ":" + taken);
            }
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            log.add("bound");
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            log.add("bound@" + ordinal);
            return true;
        }

        @Override
        public boolean completeEdge(int ordinal) {
            log.add("edge-end@" + ordinal);
            return true;
        }

        @Override
        public boolean complete() {
            log.add("end");
            return true;
        }
    }
}
