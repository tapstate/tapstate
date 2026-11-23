package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import io.tapstate.core.dsl.aggregate.RollupGuidance;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.junit.jupiter.api.Test;

/**
 * The width a rollup level averages when a pipeline is applied cannot see one skewed document, so the run says
 * so itself: once per document that grows past the width rollups keep up with, and only where the tree
 * carries rollup fields at all.
 */
class ARollupDocumentPastItsWidthIsReportedOnceTest {

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;

    @Test
    void aWideDocumentWithRollupsIsReportedOnceAndANarrowOneNotAtAll() throws Exception {
        Embed policies = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies", null,
                List.of("policy_no"), null, null, null,
                Map.of("policy_count", RollupField.of(RollupFunction.COUNT, true, null, null, null)));
        List<String> warnings = run(nest("customer", List.of("customer_id"), policies));

        assertThat(warnings).hasSize(1);
        assertThat(warnings.getFirst()).startsWith("TS0460003: document")
                .contains("C-wide").contains(String.valueOf(RollupGuidance.elementsPerDocument(0)));
    }

    @Test
    void aWideDocumentWithoutRollupsIsNotReported() throws Exception {
        List<String> warnings = run(nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"))));
        assertThat(warnings).isEmpty();
    }

    /** One document past the width, fed twice, and one within it; answers what was logged as a warning. */
    private static List<String> run(TransformBody.Nest tree) throws Exception {
        NestTopology topology = NestTopology.compile("p", "doc", tree, tables());
        AssemblerProcessor processor = new AssemblerProcessor(topology.assembler(), topology.slots(),
                new HeapNestStore<>(), "doc");
        Recording logger = new Recording();
        TestOutbox outbox = new TestOutbox(1 << 16);
        processor.init(outbox, new TestProcessorContext().setLogger(logger));
        long seq = 1;
        feed(processor, outbox, ROOT_ROWS, root(seq++, "C-wide"), root(seq++, "C-narrow"));
        // Wider than the widest boundary any tree here could get, with or without rollups.
        int wide = (int) RollupGuidance.elementsPerDocument(0) + 1;
        List<Object> elements = new ArrayList<>();
        for (int i = 0; i < wide; i++) {
            elements.add(policy(seq++, "C-wide", "W" + i));
        }
        for (int i = 0; i < 5; i++) {
            elements.add(policy(seq++, "C-narrow", "N" + i));
        }
        feed(processor, outbox, FROM_POLICIES, elements.toArray());
        feed(processor, outbox, FROM_POLICIES, policy(seq, "C-wide", "W-extra"));
        return logger.warnings;
    }

    private static void feed(AssemblerProcessor processor, TestOutbox outbox, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(List.of(items));
        processor.process(ordinal, inbox);
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    private static Envelope root(long seq, String id) {
        return Envelope.insert(seq, "customer", row("customer_id", id), null).withOrder(new SourceOrder(1L, seq));
    }

    /** A policy row as its source sends it: a leaf under the root reaches the assembler as the row itself. */
    private static Envelope policy(long seq, String customerId, String policyId) {
        return Envelope.insert(seq, "policy", row("policy_no", policyId, "customer_id", customerId), null)
                .withOrder(new SourceOrder(1L, seq));
    }

    /** Keeps what was logged at warning level. */
    private static final class Recording extends AbstractLogger {
        final List<String> warnings = new ArrayList<>();

        @Override
        public void log(Level level, String message) {
            log(level, message, null);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            if (level == Level.WARNING) {
                warnings.add(message);
            }
        }

        @Override
        public void log(LogEvent event) {
            log(event.getLogRecord().getLevel(), event.getLogRecord().getMessage(), null);
        }

        @Override
        public Level getLevel() {
            return Level.ALL;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }
    }
}
