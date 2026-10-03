package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.store.StartLoad;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The first start check, row by row: what a start does to each target decides whether it is asked about
 * at all, and what the target holds and what its policy says decide the answer.
 */
class TargetNotEmptyCheckTest {

    private static final String COORDINATE = "warehouse/orders";

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final PipelineResource definition = (PipelineResource) new DslParser().parse("""
            version: tapstate/v1
            kind: pipeline
            id: pl
            source: src
            view: { id: lead, from: orders, primary_key: id }
            serve:
              from: orders
              sync: [ { id: out, source: warehouse }, { id: copy, source: archive } ]
            """);

    @AfterEach
    void close() {
        executor.close();
    }

    @Test
    void aStartThatLoadsNothingAfreshIsNotAskedAbout() {
        AtomicInteger probed = new AtomicInteger();
        TargetProbe counting = (connection, table) -> {
            probed.incrementAndGet();
            return new TargetProbe.TargetRows(false, 5L, false);
        };
        for (StartLoad load : List.of(StartLoad.RESUME, StartLoad.CDC_ONLY)) {
            assertThat(evaluate(plan(load, entry("out", OnFullLoad.APPEND, null)), counting)).isEmpty();
        }
        assertThat(probed).as("a target nobody loads is not looked at").hasValue(0);
    }

    @Test
    void anEmptyTargetPasses() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.APPEND, null)),
                (connection, table) -> TargetProbe.TargetRows.EMPTY));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.PASS);
        assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_EMPTY);
        assertThat(finding.key()).isEqualTo("target-not-empty/" + COORDINATE);
        assertThat(finding.params()).containsEntry("rows", 0L).containsEntry("rowsExact", true);
    }

    @Test
    void aTargetSetToBeClearedPasses() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.CLEAR, null)),
                rows(1465L)));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.PASS);
        assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_SET_TO_CLEAR);
        assertThat(finding.params()).containsEntry("rows", 1465L).containsEntry("holding", "1465 rows");
    }

    @Test
    void aTargetThatRefusesANonEmptyLoadRefusesTheStart() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.FAIL, null)),
                rows(3L)));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.BLOCK);
        assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_NOT_EMPTY_REFUSED);
        assertThat(finding.actions()).isEmpty();
    }

    @Test
    void aTargetKeptAsItIsAsksWhetherToClearItOrKeepIt() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.APPEND, null)),
                rows(5L)));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.CONFIRM);
        assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_NOT_EMPTY);
        assertThat(finding.params()).containsEntry("rows", 5L).containsEntry("rowsExact", false)
                .containsEntry("onFullLoad", "append").containsEntry("element", "out");
        assertThat(finding.actions()).extracting(StartAction::id).containsExactly("clear", "keep");
        StartAction clear = finding.action("clear").orElseThrow();
        assertThat(clear.kind()).isEqualTo(StartAction.CHANGE_DEFINITION);
        assertThat(clear.destructive()).isTrue();
        assertThat(clear.changes()).containsExactly(new StartAction.Change("out", "on_full_load", "append", "clear"));
        assertThat(finding.action("keep").orElseThrow().acknowledges()).isTrue();
    }

    @Test
    void aTargetFromASharedDefinitionIsOnlyOfferedKeepAndNamesTheDefinition() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.APPEND, "shared_out")),
                rows(5L)));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.CONFIRM);
        assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_NOT_EMPTY_SHARED);
        assertThat(finding.params()).containsEntry("definition", "shared_out");
        assertThat(finding.actions()).extracting(StartAction::id).containsExactly("keep");
    }

    @Test
    void aTargetThatCannotBeLookedAtIsSaidToBeSoAndNeverPasses() {
        TapstateException unreachable = new TapstateException(
                DataBrowserError.CONNECTOR_NOT_BROWSABLE, Map.of("connector", "mysql", "browsable", "mongodb"), null);
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.FAIL, null)),
                (connection, table) -> {
                    throw unreachable;
                }));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.WARN);
        assertThat(finding.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
        assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_ROWS_UNKNOWN);
        assertThat(finding.params()).containsEntry("reasonCode", "data-browser.connector-not-browsable");
    }

    @Test
    void aTargetThatDoesNotAnswerInTimeIsSaidToBeSo() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.APPEND, null)),
                (connection, table) -> {
                    try {
                        Thread.sleep(Duration.ofSeconds(5));
                    } catch (InterruptedException stopped) {
                        Thread.currentThread().interrupt();
                    }
                    return TargetProbe.TargetRows.EMPTY;
                }, Duration.ofMillis(100)));
        assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.WARN);
        assertThat(finding.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
        assertThat((String) finding.params().get("reason")).contains("did not answer");
    }

    @Test
    void aDefectInTheProbeIsNotLaunderedIntoAWarning() {
        assertThatThrownBy(() -> evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.APPEND, null)),
                (connection, table) -> {
                    throw new IllegalStateException("probe defect");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("probe defect");
    }

    @Test
    void aCountTheTargetDidNotGiveIsNotGuessed() {
        StartFinding finding = only(evaluate(plan(StartLoad.FULL_LOAD, entry("out", OnFullLoad.APPEND, null)),
                (connection, table) -> new TargetProbe.TargetRows(false, null, false)));
        assertThat(finding.params()).containsEntry("rows", null).containsEntry("holding", "at least one row");
    }

    @Test
    void clearingFirstChangesTheNamedElementsAndNothingElse() {
        PipelineResource cleared = TargetNotEmptyCheck.clearedFirst(definition, List.of("out", "lead"));
        ServeBlock.Inline serve = (ServeBlock.Inline) cleared.serve();
        assertThat(serve.sync()).extracting(element -> element.id() + "=" + element.onFullLoad())
                .containsExactly("out=CLEAR", "copy=null");
        assertThat(((ViewBlock.Inline) cleared.view()).onFullLoad()).isEqualTo(OnFullLoad.CLEAR);
        assertThat(cleared.sources()).isEqualTo(definition.sources());
        assertThat(cleared.transforms()).isEqualTo(definition.transforms());
    }

    private List<StartFinding> evaluate(StartPlan plan, TargetProbe probe) {
        return evaluate(plan, probe, Duration.ofSeconds(2));
    }

    private List<StartFinding> evaluate(StartPlan plan, TargetProbe probe, Duration perTarget) {
        StartCheckContext context = new StartCheckContext("pl", StartIntent.START, definition, "hash", plan, null,
                probe, executor, java.time.Clock.systemUTC(), java.time.Instant.now().plusSeconds(5), perTarget,
                coded -> coded.code().code());
        return new TargetNotEmptyCheck().evaluate(context);
    }

    private static StartFinding only(List<StartFinding> findings) {
        assertThat(findings).hasSize(1);
        return findings.getFirst();
    }

    private static TargetProbe rows(long count) {
        return (connection, table) -> new TargetProbe.TargetRows(false, count, false);
    }

    private static StartPlan plan(StartLoad load, PipelineWriteTargets.WriteTarget... targets) {
        return new StartPlan(load, java.util.Arrays.stream(targets).map(target -> new StartPlan.Entry(target, load)).toList());
    }

    private static PipelineWriteTargets.WriteTarget entry(String element, OnFullLoad policy, String definedIn) {
        return new PipelineWriteTargets.WriteTarget(
                element, PipelineWriteTargets.WriteTarget.Kind.SYNC, "warehouse", "orders", policy, definedIn);
    }
}
