package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@code nest progress} prints: one line saying what the step is doing that names the tables holding it,
 * the documents sent against the root table's estimate, and one row per table the step depends on. A number
 * the pipeline did not report prints as {@code -}, never as zero.
 */
class NestProgressRenderingTest {

    @Test
    void aLoadingStepNamesTheTableItWaitsOnAndShowsWhatWasRead() {
        NestProgressOutcome.Found found = new NestProgressOutcome.Found("order_doc", "RUNNING", "SNAPSHOTTING",
                List.of(new NestProgressOutcome.Step("doc", "orders", null, 300L, "LOADING", List.of("orders"),
                        List.of(new NestProgressOutcome.Table("orders", "", "PENDING", null, 300L, null),
                                new NestProgressOutcome.Table("order_items", "items", "LANDING", 1000L, 1000L,
                                        null)))));

        StringWriter text = new StringWriter();
        Repl.renderNestProgress(new PrintWriter(text, true), found);

        assertThat(text.toString()).contains(
                "order_doc  running  initial load: snapshotting",
                "step doc: loading; waiting for orders to be read",
                "documents sent: - of about 300 orders rows",
                "orders       (root)  pending  -     300",
                "order_items  items   landing  1000  1000");
    }

    @Test
    void aStepThatHasLoadedSaysItIsApplyingChanges() {
        NestProgressOutcome.Found found = new NestProgressOutcome.Found("order_doc", "RUNNING", "STREAMING",
                List.of(new NestProgressOutcome.Step("doc", "orders", 300L, 300L, "MAINTAINING", List.of(),
                        List.of(new NestProgressOutcome.Table("orders", "", "LANDED", 300L, 300L, null)))));

        StringWriter text = new StringWriter();
        Repl.renderNestProgress(new PrintWriter(text, true), found);

        assertThat(text.toString()).contains("step doc: load complete; applying changes as they arrive",
                "documents sent: 300 of about 300 orders rows");
    }

    @Test
    void aFinishedRunAndAPartlyReportedLoadEachSayWhatTheyAre() {
        NestProgressOutcome.Found found = new NestProgressOutcome.Found("order_doc", "COMPLETED", "DONE",
                List.of(new NestProgressOutcome.Step("doc", "orders", 300L, 300L, "COMPLETED", List.of(),
                                List.of(new NestProgressOutcome.Table("orders", "", "LANDED", 300L, 300L, null))),
                        new NestProgressOutcome.Step("lines", "orders", null, 300L, "UNKNOWN", List.of(),
                                List.of(new NestProgressOutcome.Table("orders", "", "LANDED", 300L, 300L, null)))));

        StringWriter text = new StringWriter();
        Repl.renderNestProgress(new PrintWriter(text, true), found);

        assertThat(text.toString()).contains("step doc: load complete; the pipeline has finished",
                "step lines: no load reported for some or all of its tables");
    }
}
