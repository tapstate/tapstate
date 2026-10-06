package io.tapstate.cli;

import io.tapstate.core.common.JsonReader;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static io.tapstate.cli.StartCheckReports.HASH;
import static io.tapstate.cli.StartCheckReports.NEEDS_CONFIRMATION;
import static io.tapstate.cli.StartCheckReports.applied;
import static io.tapstate.cli.StartCheckReports.blocked;
import static io.tapstate.cli.StartCheckReports.notEmpty;
import static io.tapstate.cli.StartCheckReports.notEmptyShared;
import static io.tapstate.cli.StartCheckReports.passed;
import static io.tapstate.cli.StartCheckReports.report;
import static io.tapstate.cli.StartCheckReports.started;
import static io.tapstate.cli.StartCheckReports.stoppedBy;
import static io.tapstate.cli.StartCheckReports.warning;
import static io.tapstate.cli.StartCheckReports.withHash;
import static io.tapstate.cli.StartCheckReports.withIntent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Starting through the start checks, the one way every verb that ends in a start does it: what is shown,
 * what is asked, what is sent, and what a script gets back.
 *
 * <p>The server is scripted, and its reports are the wire shape read by the parser the HTTP client uses. A
 * question at a terminal is answered through the real line-reading prompter over a dumb terminal, because
 * what a bare Enter picks is decided by the two together: the prompter answers an empty line with the last
 * option, and the flow is what puts cancelling there.
 */
class StartFlowTest {

    /**
     * The versioned report every client renders without code of its own for any check. One file for every
     * client in this repository, read where it lives; another repository copies it by version.
     */
    private static final Path CLIENT_FIXTURE = Path.of("..").toAbsolutePath().normalize()
            .resolve("control/rest-api/src/test/resources/start-checks/client-fixture-v1.json");

    private final StringWriter out = new StringWriter();
    private final StringWriter err = new StringWriter();
    private final List<String> reported = new ArrayList<>();
    private final FakeServer server = new FakeServer();

    // ---- the fixture every client reads ---------------------------------------------------------------

    @Test
    void everyQuestionInTheVersionedFixtureIsShownAsWrittenAndAnswered() throws IOException {
        Map<String, Object> fixture = fixture();
        assertThat(fixture.get("fixtureVersion")).as("the fixture version this client is written against")
                .isEqualTo(1L);
        Map<String, Object> report = section(fixture, "answerable");
        server.starts.add(stoppedBy(report));
        server.starts.add(started(report, List.of(), List.of()));

        // the first answer of each question: a change to the definition, then an action no client knows
        StartFlow.Result result = flow(true, typing("1\n1\n")).start("lead_view", asked());

        assertThat(result.exitCode()).isZero();
        assertThat(server.startsSent()).containsExactly(
                "start lead_view []",
                "start lead_view [target-not-empty/views/lead=clear, derived-schema-drift/join_orders=accept]"
                        + " if-match 3f9a6c1d2e4b5a69788796a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5");
        // shown as the server worded it, the check this client has never heard of included
        assertThat(out.toString())
                .contains("lead on views already holds 1465 rows.")
                .contains("Clear lead before the full load, and set on_full_load: clear on lead")
                .contains("[changes the pipeline: lead on_full_load append -> clear]")
                .contains("join_orders now produces 2 columns it did not produce when it was last accepted.")
                .contains("Accept the new columns and start")
                .contains("src_pg keeps its change log for 6 hours")
                // what passed is not a question and not a warning, so it is not in the way
                .doesNotContain("orders_archive on warehouse is empty");
    }

    @Test
    void aBehaviorThisClientDoesNotKnowRefusesTheStartWhateverIsAnswered() throws IOException {
        Map<String, Object> report = section(fixture(), "unknownBehavior");
        server.starts.add(stoppedBy(report));
        ScriptedPrompter prompter = new ScriptedPrompter();

        StartFlow.Result asked = flow(true, prompter).start("lead_view", asked());
        StartFlow.Result unattended = flow(true, prompter).start("lead_view", unattended());

        // read as a refusal: nothing to answer, nothing sent again, and never a success
        assertThat(asked.exitCode()).isNotZero();
        assertThat(unattended.exitCode()).isNotZero();
        assertThat(prompter.offered).isEmpty();
        assertThat(server.startsSent()).containsExactly("start lead_view []", "start lead_view []");
        assertThat(out.toString()).contains("QUARANTINE (refuses)")
                .contains("src_pg is quarantined until an operator releases it.");
    }

    @Test
    void anOutcomeThisClientDoesNotKnowRefusesTheStartWhateverIsAnswered() throws IOException {
        Map<String, Object> report = section(fixture(), "unknownOutcome");
        server.starts.add(stoppedBy(report));
        ScriptedPrompter prompter = new ScriptedPrompter();

        StartFlow.Result result = flow(true, prompter).start("lead_view", unattended());

        assertThat(result.exitCode()).isNotZero();
        assertThat(prompter.offered).isEmpty();
        assertThat(server.startsSent()).containsExactly("start lead_view []");
    }

    // ---- asked at a terminal --------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"1", "2"})
    void theAnswerPickedAtTheTerminalIsSentAgainstTheDefinitionTheChecksRead(String typed) throws IOException {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"));
        server.starts.add(stoppedBy(report));
        server.starts.add(started(report, List.of(), List.of()));

        StartFlow.Result result = flow(true, typing(typed + "\n")).start("pl1", asked());

        assertThat(result.exitCode()).isZero();
        String answer = typed.equals("1") ? "clear" : "keep";
        assertThat(server.startsSent()).containsExactly(
                "start pl1 []", "start pl1 [target-not-empty/warehouse/orders=" + answer + "] if-match " + HASH);
    }

    /**
     * A bare Enter, the end of input and Ctrl-C each give the prompter an empty reply, which picks the last
     * option. The flow puts cancelling there; a menu that ended with "keep" would make the commonest key
     * press on a terminal an answer that starts the pipeline over rows it was asked about.
     */
    @ParameterizedTest
    @ValueSource(strings = {"\n", "", "\u0003"})
    void enterEndOfInputAndCtrlCEachCancelAndNothingMoreIsSent(String typed) throws IOException {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"));
        server.starts.add(stoppedBy(report));
        server.starts.add(started(report, List.of(), List.of()));

        StartFlow.Result result = flow(true, typing(typed)).start("pl1", asked());

        assertThat(result.exitCode()).isNotZero();
        assertThat(server.startsSent()).containsExactly("start pl1 []");
        assertThat(out.toString()).contains("start: cancelled; pl1 was left as it is");
    }

    @Test
    void aDefinitionChangedWhileAnsweringIsReadAgainAndAskedAgain() throws IOException {
        Map<String, Object> first = report("NEEDS_CONFIRMATION", notEmpty("orders"));
        Map<String, Object> second = withHash(report("NEEDS_CONFIRMATION", notEmpty("orders")), "2" + HASH.substring(1));
        server.starts.add(stoppedBy(first));
        server.starts.add(new StartAttempt.Rejected("pipeline.version-conflict", Map.of("id", "pl1"),
                "pl1 changed", List.of()));
        server.starts.add(started(second, List.of(), List.of()));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(second));

        StartFlow.Result result = flow(true, typing("2\n2\n")).start("pl1", asked());

        assertThat(result.exitCode()).isZero();
        assertThat(server.calls).containsExactly(
                "start pl1 []",
                "start pl1 [target-not-empty/warehouse/orders=keep] if-match " + HASH,
                "checks start pl1",
                "start pl1 [target-not-empty/warehouse/orders=keep] if-match 2" + HASH.substring(1));
        assertThat(err.toString()).contains("pl1 was changed while you were answering");
    }

    // ---- nobody to ask --------------------------------------------------------------------------------

    @Test
    void withNoTerminalTheStartIsRefusedWithTheCommandsThatAnswerIt() {
        server.starts.add(stoppedBy(report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmpty("customers"))));
        ScriptedPrompter prompter = new ScriptedPrompter();

        StartFlow.Result result = flow(false, prompter).start("pl1", asked());

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(prompter.offered).isEmpty();
        assertThat(server.startsSent()).containsExactly("start pl1 []");
        assertThat(err.toString()).contains("cli.start-needs-an-answer")
                .contains("2 start check question(s) need an answer")
                // one command per answer, written for the whole check because both questions offer it
                .contains("tapstate start pl1 --decide target-not-empty=clear | "
                        + "tapstate start pl1 --decide target-not-empty=keep | tapstate start pl1 -y");
        // the questions themselves are on the screen, not only the commands that answer them
        assertThat(out.toString()).contains("orders on warehouse already holds 5 rows.")
                .contains("customers on warehouse already holds 5 rows.");
    }

    @Test
    void anAnswerOnlySomeQuestionsOfferIsPrintedForEachQuestionThatOffersIt() {
        server.starts.add(stoppedBy(report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmptyShared("customers"))));

        flow(false, new ScriptedPrompter()).start("pl1", asked());

        // no printed command answers a question with an answer it does not offer
        assertThat(err.toString()).contains("tapstate start pl1 --decide target-not-empty/warehouse/orders=clear | "
                + "tapstate start pl1 --decide target-not-empty=keep | tapstate start pl1 -y");
    }

    @Test
    void minusYAnswersEveryQuestionByGoingAheadAsConfigured() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmptyShared("customers"));
        server.starts.add(stoppedBy(report));
        server.starts.add(started(report, List.of(), List.of()));
        ScriptedPrompter prompter = new ScriptedPrompter();

        StartFlow.Result result = flow(true, prompter).start("pl1", unattended());

        assertThat(result.exitCode()).isZero();
        assertThat(prompter.offered).isEmpty();
        assertThat(server.startsSent()).containsExactly("start pl1 []",
                "start pl1 [target-not-empty/warehouse/orders=keep, target-not-empty/warehouse/customers=keep]"
                        + " if-match " + HASH);
    }

    // ---- --decide -------------------------------------------------------------------------------------

    @Test
    void aCheckWideDecideAnswersEveryQuestionThatCheckAsksInOneStart() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmpty("customers"));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(started(report, List.of(), List.of()));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1",
                deciding(false, "target-not-empty=clear"));

        assertThat(result.exitCode()).isZero();
        // read first, so the answers go out with the start that carries them and nothing goes out bare
        assertThat(server.calls).containsExactly("checks start pl1",
                "start pl1 [target-not-empty/warehouse/orders=clear, target-not-empty/warehouse/customers=clear]"
                        + " if-match " + HASH);
    }

    @Test
    void aCheckWideDecideOneQuestionDoesNotOfferIsRefusedHereAndNothingIsSent() {
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(
                report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmptyShared("customers"))));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1",
                deciding(false, "target-not-empty=clear"));

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(server.startsSent()).isEmpty();
        assertThat(err.toString()).contains("cli.decide-not-offered")
                .contains("target-not-empty/warehouse/customers");
    }

    @Test
    void aDecideAnswersItsQuestionAndMinusYAnswersTheRest() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmpty("customers"),
                notEmpty("items"));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(started(report, List.of(), List.of()));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1",
                deciding(true, "target-not-empty/warehouse/orders=clear"));

        assertThat(result.exitCode()).isZero();
        assertThat(server.startsSent()).containsExactly("start pl1 [target-not-empty/warehouse/orders=clear, "
                + "target-not-empty/warehouse/customers=keep, target-not-empty/warehouse/items=keep] if-match " + HASH);
    }

    @Test
    void theOneQuestionAnswerWinsOverTheCheckWideOne() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"), notEmpty("customers"));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(started(report, List.of(), List.of()));

        flow(false, new ScriptedPrompter()).start("pl1", deciding(false,
                "target-not-empty=clear", "target-not-empty/warehouse/orders=keep"));

        assertThat(server.startsSent()).containsExactly("start pl1 [target-not-empty/warehouse/orders=keep, "
                + "target-not-empty/warehouse/customers=clear] if-match " + HASH);
    }

    @ParameterizedTest
    @ValueSource(strings = {"target-not-empty=keep", "target-not-empty/warehouse/orders=keep"})
    void anAnswerToARefusalIsRefusedHereAndNothingIsSent(String decide) {
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report("BLOCKED", blocked("orders"))));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1", deciding(false, decide));

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(server.startsSent()).isEmpty();
        assertThat(err.toString()).contains("cli.decide-on-a-refusal");
        // the refusal says what it needs, so it is on the screen with the answer that cannot change it
        assertThat(out.toString()).contains("orders on warehouse already holds 5 rows, and on_full_load is fail.");
    }

    @Test
    void aDecideAgainstAServerWithoutStartChecksNeverStartsAnything() {
        server.checks = new StartChecksOutcome.NotSupported();

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1",
                deciding(false, "target-not-empty=clear"));

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        // the server would take the start and drop the answer, and the rows would stay with nobody told
        assertThat(server.calls).containsExactly("checks start pl1");
        assertThat(err.toString()).contains("cli.start-checks-unsupported").contains("--decide");
    }

    @Test
    void aCheckWideDecideThatAnswersNothingIsSaidToHaveBeenUnused() {
        Map<String, Object> report = report("READY", passed("orders"));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(started(report, List.of(), List.of()));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1",
                deciding(false, "target-not-empty=clear"));

        assertThat(result.exitCode()).isZero();
        assertThat(out.toString()).contains("--decide target-not-empty=clear answers no question this start asks");
    }

    // ---- what a script reads --------------------------------------------------------------------------

    @Test
    void aStartedStartPrintsTheServersAnswerAsTheDocumentAndItsWarningsOnStandardError() {
        Map<String, Object> report = report("READY", warning("source-retention", "src_pg keeps its log for 6 hours."));
        StartAttempt.Started started = started(report, List.of(), List.of());
        server.starts.add(started);

        StartFlow.Result result = flow(true, new ScriptedPrompter()).start("pl1",
                StartFlow.Answering.asked(OutputFormat.JSON));

        assertThat(result.exitCode()).isZero();
        assertThat(JsonReader.parse(out.toString())).isEqualTo(started.raw());
        assertThat(err.toString()).contains("warning: src_pg keeps its log for 6 hours.");
    }

    @Test
    void aStartStoppedToAskPrintsTheRefusalWithTheReportAndAsksNothing() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"));
        server.starts.add(stoppedBy(report));
        ScriptedPrompter prompter = new ScriptedPrompter();

        StartFlow.Result result = flow(true, prompter).start("pl1", StartFlow.Answering.asked(OutputFormat.JSON));

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(prompter.offered).isEmpty();
        @SuppressWarnings("unchecked")
        Map<String, Object> document = (Map<String, Object>) JsonReader.parse(out.toString());
        assertThat(document).containsOnlyKeys("code", "params", "message", "startChecks");
        assertThat(document.get("code")).isEqualTo(NEEDS_CONFIRMATION);
        assertThat(document.get("startChecks")).isEqualTo(report);
    }

    @Test
    void aStartRefusedForAnotherReasonPrintsTheCodedRefusal() {
        server.starts.add(new StartAttempt.Rejected("lifecycle.pipeline-not-runnable", Map.of("pipeline", "pl1"),
                "pl1 is not runnable", List.of()));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1",
                StartFlow.Answering.asked(OutputFormat.JSON));

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(JsonReader.parse(out.toString())).isEqualTo(Map.of("code", "lifecycle.pipeline-not-runnable",
                "params", Map.of("pipeline", "pl1"), "message", "pl1 is not runnable"));
    }

    @ParameterizedTest
    @EnumSource(value = OutputFormat.class, names = {"JSON", "YAML"})
    void aStructuredOutputNeverAsksEvenAtATerminal(OutputFormat format) {
        server.starts.add(stoppedBy(report("NEEDS_CONFIRMATION", notEmpty("orders"))));
        ScriptedPrompter prompter = new ScriptedPrompter();

        flow(true, prompter).start("pl1", StartFlow.Answering.asked(format));

        assertThat(prompter.offered).isEmpty();
        assertThat(out.toString()).contains(NEEDS_CONFIRMATION);
    }

    @Test
    void aStartThatGotNoAnswerIsNotSentAgainAndIsSaidToHaveMaybeGoneAhead() {
        server.starts.add(new StartAttempt.Unreachable(true));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1", unattended());

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(server.startsSent()).containsExactly("start pl1 []");
        assertThat(reported).containsExactly("unanswered pl1");
    }

    @Test
    void aStartThatReachedNobodyIsReportedAsUnreachable() {
        server.starts.add(new StartAttempt.Unreachable(false));

        flow(false, new ScriptedPrompter()).start("pl1", unattended());

        assertThat(reported).containsExactly("unreachable");
    }

    // ---- --checks-only --------------------------------------------------------------------------------

    @Test
    void checksOnlyExitsZeroWhenNothingNeedsAnAnswerAndStartsNothing() {
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report("READY", passed("orders"))));

        StartFlow.Result result = flow(true, new ScriptedPrompter()).checksOnly("pl1", OutputFormat.TEXT);

        assertThat(result.exitCode()).isZero();
        assertThat(server.calls).containsExactly("checks start pl1");
        assertThat(out.toString()).contains("Start checks for pl1: nothing to ask").contains("outcome: ready");
    }

    @Test
    void checksOnlyExitsOneWhenAStartWouldBeAskedOrRefused() {
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(
                report("NEEDS_CONFIRMATION", notEmpty("orders"))));
        int asked = flow(true, new ScriptedPrompter()).checksOnly("pl1", OutputFormat.TEXT).exitCode();
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report("BLOCKED", blocked("orders"))));
        int refused = flow(true, new ScriptedPrompter()).checksOnly("pl1", OutputFormat.TEXT).exitCode();

        assertThat(asked).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(refused).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(server.startsSent()).isEmpty();
        assertThat(out.toString()).contains("outcome: needs confirmation").contains("outcome: blocked");
    }

    @Test
    void checksOnlyPrintsTheReportAsTheDocument() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));

        flow(true, new ScriptedPrompter()).checksOnly("pl1", OutputFormat.JSON);

        assertThat(JsonReader.parse(out.toString())).isEqualTo(Map.of("startChecks", report));
    }

    @Test
    void checksOnlyAgainstAServerWithoutStartChecksSaysSo() {
        server.checks = new StartChecksOutcome.NotSupported();

        StartFlow.Result result = flow(true, new ScriptedPrompter()).checksOnly("pl1", OutputFormat.TEXT);

        assertThat(result.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(err.toString()).contains("cli.start-checks-unsupported").contains("--checks-only");
    }

    // ---- what the server changed ----------------------------------------------------------------------

    @Test
    void aChangeTheServerMadeIsToldAgainstTheWorkspaceFileThatStillSaysOtherwise() {
        Map<String, Object> report = report("NEEDS_CONFIRMATION", notEmpty("orders"));
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(started(report, List.of(applied("orders")),
                List.of(Map.of("finding", "target-not-empty/warehouse/gone", "action", "keep"))));

        flow(false, new ScriptedPrompter()).start("pl1", deciding(false, "target-not-empty=clear"));

        assertThat(out.toString())
                .contains("Updated pl1: orders on_full_load append -> clear.")
                .contains("pipelines/pl1.tap.yml still says on_full_load: append for orders; applying it again "
                        + "sets it back, and the next new full load asks again.")
                .contains("note: the answer keep to target-not-empty/warehouse/gone was not needed any more");
    }

    @Test
    void aStartFromAServerThatRanNoChecksSaysSo() {
        server.starts.add(started(null, List.of(), List.of()));

        StartFlow.Result result = flow(false, new ScriptedPrompter()).start("pl1", asked());

        assertThat(result.exitCode()).isZero();
        assertThat(err.toString()).contains("cli.start-checks-not-run");
    }

    // ---- restart --rerun ------------------------------------------------------------------------------

    @Test
    void aRerunAsksEverythingBeforeAnythingIsStoppedAndACancelStopsNothing() throws IOException {
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(
                withIntent(report("NEEDS_CONFIRMATION", notEmpty("orders")), "RERUN")));

        StartFlow.RerunPlan plan = rerunFlow(true, typing("\n")).planRerun("pl1", asked());

        assertThat(plan).isInstanceOf(StartFlow.RerunPlan.Stop.class);
        assertThat(server.calls).containsExactly("checks rerun pl1");
        assertThat(out.toString()).contains("restart: cancelled; pl1 was left as it is");
    }

    @Test
    void aRerunWithNobodyToAskPrintsTheRerunCommandsThatAnswerIt() {
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(
                withIntent(report("NEEDS_CONFIRMATION", notEmpty("orders")), "RERUN")));

        StartFlow.RerunPlan plan = rerunFlow(false, new ScriptedPrompter()).planRerun("pl1", asked());

        assertThat(plan).isInstanceOf(StartFlow.RerunPlan.Stop.class);
        // the start command would not rerun anything: the pipeline is still running, untouched
        assertThat(err.toString()).contains("tapstate restart pl1 --rerun --decide target-not-empty=clear | "
                + "tapstate restart pl1 --rerun --decide target-not-empty=keep | tapstate restart pl1 --rerun -y");
        // shown once, before the refusal
        assertThat(out.toString().split("orders on warehouse already holds 5 rows", -1)).hasSize(2);
    }

    @Test
    void aRerunAnsweredUpFrontStartsWithThoseAnswersAfterTheStop() {
        Map<String, Object> report = withIntent(report("NEEDS_CONFIRMATION", notEmpty("orders")), "RERUN");
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(started(report, List.of(), List.of()));
        StartFlow flow = rerunFlow(false, new ScriptedPrompter());

        StartFlow.RerunPlan plan = flow.planRerun("pl1", deciding(false, "target-not-empty=clear"));
        assertThat(plan).isInstanceOf(StartFlow.RerunPlan.Ready.class);
        StartFlow.Result result = flow.startAfterRerunStop("pl1", (StartFlow.RerunPlan.Ready) plan,
                deciding(false, "target-not-empty=clear"));

        assertThat(result.exitCode()).isZero();
        assertThat(server.calls).containsExactly("checks rerun pl1",
                "start pl1 [target-not-empty/warehouse/orders=clear] if-match " + HASH);
    }

    @Test
    void aRerunWhoseStartIsRefusedAfterTheStopSaysWhereThatLeftThePipeline() {
        Map<String, Object> report = withIntent(report("NEEDS_CONFIRMATION", notEmpty("orders")), "RERUN");
        server.checks = new StartChecksOutcome.Found(StartChecks.parse(report));
        server.starts.add(new StartAttempt.Rejected("pipeline.version-conflict", Map.of("id", "pl1"),
                "pl1 changed", List.of()));
        StartFlow flow = rerunFlow(false, new ScriptedPrompter());

        StartFlow.RerunPlan.Ready plan = (StartFlow.RerunPlan.Ready) flow.planRerun("pl1",
                deciding(false, "target-not-empty=keep"));
        StartFlow.Result result = flow.startAfterRerunStop("pl1", plan, deciding(false, "target-not-empty=keep"));

        assertThat(result.exitCode()).isNotZero();
        assertThat(reported).containsExactly("pipeline.version-conflict");
        assertThat(err.toString()).contains("restart: pl1 was stopped and its state cleared, but it has not been "
                + "started").contains("Start it with: tapstate start pl1 --decide target-not-empty=<answer> (or -y)");
    }

    @Test
    void aRerunAgainstAServerWithoutStartChecksIsTheStopAndStartItAlwaysWas() {
        server.checks = new StartChecksOutcome.NotSupported();

        assertThat(rerunFlow(false, new ScriptedPrompter()).planRerun("pl1", asked()))
                .isInstanceOf(StartFlow.RerunPlan.Unchecked.class);
        assertThat(rerunFlow(false, new ScriptedPrompter()).planRerun("pl1",
                deciding(false, "target-not-empty=clear"))).isInstanceOf(StartFlow.RerunPlan.Stop.class);
        assertThat(err.toString()).contains("cli.start-checks-unsupported");
    }

    // ---- harness --------------------------------------------------------------------------------------

    private StartFlow flow(boolean terminal, Prompter prompter) {
        return flowFor("start", terminal, prompter);
    }

    private StartFlow rerunFlow(boolean terminal, Prompter prompter) {
        return flowFor("restart", terminal, prompter);
    }

    private StartFlow flowFor(String verb, boolean terminal, Prompter prompter) {
        StartFlow.Reporting reporting = new StartFlow.Reporting() {
            @Override
            public int rejection(String code, String message, Map<String, Object> params) {
                reported.add(code);
                return Cli.EXIT_DIAGNOSTIC;
            }

            @Override
            public int unreachable() {
                reported.add("unreachable");
                return Cli.EXIT_DIAGNOSTIC;
            }

            @Override
            public int unanswered(String pipelineId) {
                reported.add("unanswered " + pipelineId);
                return Cli.EXIT_DIAGNOSTIC;
            }
        };
        return new StartFlow(verb, server, new PrintWriter(out), new PrintWriter(err), () -> prompter,
                () -> terminal, reporting, id -> "pipelines/" + id + ".tap.yml");
    }

    private static StartFlow.Answering asked() {
        return StartFlow.Answering.asked(OutputFormat.TEXT);
    }

    private static StartFlow.Answering unattended() {
        return new StartFlow.Answering(true, List.of(), OutputFormat.TEXT);
    }

    private static StartFlow.Answering deciding(boolean unattended, String... decides) {
        return new StartFlow.Answering(unattended, List.of(decides), OutputFormat.TEXT);
    }

    /** The line-reading prompter over a dumb terminal that reads {@code typed}. */
    private static JLinePrompter typing(String typed) throws IOException {
        DumbTerminal terminal = new DumbTerminal("test", "dumb",
                new ByteArrayInputStream(typed.getBytes(StandardCharsets.UTF_8)), new ByteArrayOutputStream(),
                StandardCharsets.UTF_8);
        return new JLinePrompter(terminal, true);
    }

    /** A scripted server: each start takes the next answer, the last one repeating; every call is logged. */
    private static final class FakeServer implements StartFlow.Server {
        final Deque<StartAttempt> starts = new ArrayDeque<>();
        StartChecksOutcome checks = new StartChecksOutcome.NotSupported();
        final List<String> calls = new ArrayList<>();

        @Override
        public StartAttempt start(String pipelineId, List<StartDecision> decisions, String ifMatch) {
            calls.add("start " + pipelineId + " " + decisions.stream()
                    .map(decision -> decision.finding() + "=" + decision.action()).toList()
                    + (ifMatch == null ? "" : " if-match " + ifMatch));
            return starts.size() == 1 ? starts.peek() : starts.poll();
        }

        @Override
        public StartChecksOutcome checks(String pipelineId, String intent) {
            calls.add("checks " + intent + " " + pipelineId);
            return checks;
        }

        List<String> startsSent() {
            return calls.stream().filter(call -> call.startsWith("start ")).toList();
        }
    }

    // ---- the fixture ----------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fixture() throws IOException {
        return (Map<String, Object>) JsonReader.parse(Files.readString(CLIENT_FIXTURE));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> fixture, String name) {
        return (Map<String, Object>) fixture.get(name);
    }
}
