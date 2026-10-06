package io.tapstate.cli;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Starting a pipeline from the command line, through its start checks: one piece of code for every verb
 * that ends in a start -- {@code start}, {@code restart} in each of its forms, and {@code up} -- so a
 * question is shown and answered the same way whichever of them asked it.
 *
 * <p>Three ways to answer, and the server decides which questions there are. {@code --decide} answers named
 * questions, {@code -y} answers every other one with "go ahead as configured", and at a terminal the rest
 * are asked, one at a time, with cancelling as the answer a bare Enter gives -- never an answer that keeps
 * or changes anything. With none of the three available, the start is refused, and the refusal prints the
 * commands that answer it. A question nothing answered is never answered by default.
 *
 * <p>Nothing here interprets a check: findings and answers are shown as the server worded them, a behavior
 * this client does not know is read as a refusal, and the one answer recognised by name is the one that
 * goes ahead as configured.
 */
final class StartFlow {

    /** How many times a start is sent before the checks are said to keep changing under it. */
    private static final int ROUNDS = 3;
    private static final String CANCEL = "cancel";
    private static final String VERSION_CONFLICT = "pipeline.version-conflict";

    /**
     * How the person asked to be answered for.
     *
     * @param unattended whether to answer every open question with "go ahead as configured" ({@code -y})
     * @param decides    the {@code --decide} answers, as written
     * @param format     how the result is printed; anything but text never asks
     * @param embedded   whether a verb that prints its own summary -- {@code up} -- owns the output: the flow
     *                   then asks what it has to ask and leaves telling the result to that verb
     */
    record Answering(boolean unattended, List<String> decides, OutputFormat format, boolean embedded) {

        Answering {
            decides = List.copyOf(decides);
        }

        Answering(boolean unattended, List<String> decides, OutputFormat format) {
            this(unattended, decides, format, false);
        }

        static Answering asked(OutputFormat format) {
            return new Answering(false, List.of(), format, false);
        }
    }

    /** The server, for one pipeline's start. */
    interface Server {
        StartAttempt start(String pipelineId, List<StartDecision> decisions, String ifMatch);

        StartChecksOutcome checks(String pipelineId, String intent);
    }

    /** What the session reports for this flow the way it reports everything else. */
    interface Reporting {
        /** A coded refusal from the server, rendered; answers the exit code. */
        int rejection(String code, String message, Map<String, Object> params);

        /** A server that could not be reached; answers the exit code. */
        int unreachable();

        /** A start that was sent and got no answer in time, so it may have gone ahead; answers the exit code. */
        int unanswered(String pipelineId);
    }

    /**
     * How a start ended.
     *
     * @param attempt the last answer from the server, null when nothing was sent
     * @param checks  the last report seen, null when there was none
     */
    record Result(int exitCode, StartAttempt attempt, StartChecks checks) {
    }

    /** A rerun's questions, asked before anything is stopped. */
    sealed interface RerunPlan {

        /** Everything is answered; the stop may go ahead, then the start with these answers. */
        record Ready(List<StartDecision> decisions, StartChecks checks) implements RerunPlan {
        }

        /** The server runs no start checks; the rerun is the stop and the start it always was. */
        record Unchecked() implements RerunPlan {
        }

        /** Stop here, with this exit code; nothing was stopped. */
        record Stop(int exitCode) implements RerunPlan {
        }
    }

    private final String verb;
    private final Server server;
    private final PrintWriter out;
    private final PrintWriter err;
    private final Supplier<Prompter> prompter;
    private final BooleanSupplier terminal;
    private final Reporting reporting;
    private final Function<String, String> fileOf;
    /** Whether the verb currently using this flow owns the output; set at each entry point. */
    private boolean embeddedNow;

    /**
     * @param verb   the verb the person typed, which every line this prints speaks for
     * @param fileOf the workspace file declaring a pipeline, or null when there is none to name; a change the
     *               server made to the definition is told to the person against that file
     */
    StartFlow(String verb, Server server, PrintWriter out, PrintWriter err, Supplier<Prompter> prompter,
            BooleanSupplier terminal, Reporting reporting, Function<String, String> fileOf) {
        this.verb = verb;
        this.server = server;
        this.out = out;
        this.err = err;
        this.prompter = prompter;
        this.terminal = terminal;
        this.reporting = reporting;
        this.fileOf = fileOf;
    }

    // ---- start ---------------------------------------------------------------------------------------

    /** Starts {@code id}, answering its start checks as {@code answering} says. */
    Result start(String id, Answering answering) {
        embeddedNow = answering.embedded();
        List<StartDecision> decided = new ArrayList<>();
        String ifMatch = null;
        StartChecks lastChecks = null;
        if (!answering.decides().isEmpty()) {
            // Asked before anything is started: a server that runs no start checks would take the start and
            // drop the answers, and the start would go ahead as configured with nobody told.
            StartChecksOutcome read = server.checks(id, "start");
            StartChecks checks;
            switch (read) {
                case StartChecksOutcome.Found found -> checks = found.checks();
                case StartChecksOutcome.NotSupported ignored -> {
                    return new Result(unsupported("--decide"), null, null);
                }
                case StartChecksOutcome.Rejected rejected -> {
                    return new Result(reporting.rejection(rejected.code(), rejected.message(), rejected.params()),
                            null, null);
                }
                case StartChecksOutcome.Unreachable ignored -> {
                    return new Result(reporting.unreachable(), null, null);
                }
            }
            lastChecks = checks;
            show(checks, answering.format());
            // The answers first: one that answers a refusal is told so, which says more than the refusal does.
            Integer refused = expand(answering.decides(), checks, decided, answering.format());
            if (refused != null) {
                return new Result(refused, null, checks);
            }
            if (checks.refused()) {
                return new Result(refusedOutright(id, checks, answering.format()), null, checks);
            }
            Integer stopped = answerTheRest(id, checks, decided, answering, startCommand(id));
            if (stopped != null) {
                return new Result(stopped, null, checks);
            }
            ifMatch = checks.contentHash();
        }
        return send(id, decided, ifMatch, lastChecks, answering);
    }

    /**
     * Sends the start, and answers what stops it, round after round. A question that appears after an answer
     * was given -- the target changed, a check was added -- is asked like the first ones; a definition that
     * changed under the answers is read again and asked about again at a terminal.
     */
    private Result send(String id, List<StartDecision> decided, String ifMatch, StartChecks lastChecks,
            Answering answering) {
        StartAttempt attempt = null;
        StartChecks checks = lastChecks;
        // what was last put in front of the person, so a report is shown again only when it says something new
        StartChecks shown = lastChecks;
        for (int round = 0; round < ROUNDS; round++) {
            attempt = server.start(id, decided, ifMatch);
            switch (attempt) {
                case StartAttempt.Started started -> {
                    return new Result(started(id, started, answering), started, started.checks());
                }
                case StartAttempt.Unreachable unreachable -> {
                    // Never sent again: one that got no answer may have gone ahead.
                    return new Result(answering.embedded() ? Cli.EXIT_DIAGNOSTIC
                            : unreachable.sent() ? reporting.unanswered(id) : reporting.unreachable(), attempt, checks);
                }
                case StartAttempt.Rejected rejected -> {
                    if (VERSION_CONFLICT.equals(rejected.code()) && interactive(answering) && !decided.isEmpty()) {
                        tellAppliedChanges(id, rejected.decisionsApplied());
                        err.println(verb + ": " + id + " was changed while you were answering; reading its start "
                                + "checks again");
                        err.flush();
                        StartChecksOutcome reread = server.checks(id, "start");
                        if (!(reread instanceof StartChecksOutcome.Found found)) {
                            return new Result(reporting.rejection(rejected.code(), rejected.message(),
                                    rejected.params()), attempt, checks);
                        }
                        checks = found.checks();
                        show(checks, answering.format());
                        shown = checks;
                        if (checks.refused()) {
                            return new Result(refusedOutright(id, checks, answering.format()), attempt, checks);
                        }
                        decided.clear();
                        Integer stopped = answerTheRest(id, checks, decided, answering, startCommand(id));
                        if (stopped != null) {
                            return new Result(stopped, attempt, checks);
                        }
                        ifMatch = checks.contentHash();
                        continue;
                    }
                    return new Result(answering.embedded() ? Cli.EXIT_DIAGNOSTIC
                            : rejected(id, rejected, answering.format()), attempt, checks);
                }
                case StartAttempt.Stopped stopped -> {
                    checks = stopped.checks();
                    if (!sameFindings(shown, checks)) {
                        show(checks, answering.format());
                        shown = checks;
                    }
                    if (checks.refused()) {
                        return new Result(answering.embedded() ? Cli.EXIT_DIAGNOSTIC
                                : refusedByChecks(id, stopped, answering.format()), attempt, checks);
                    }
                    Integer answered = answerTheRest(id, checks, decided, answering, startCommand(id));
                    if (answered != null) {
                        if (answering.format() != OutputFormat.TEXT && !answering.embedded()) {
                            printStructured(stopDocument(stopped), answering.format());
                        }
                        return new Result(answered, attempt, checks);
                    }
                    ifMatch = checks.contentHash();
                }
            }
        }
        err.println(verb + ": " + id + " was not started: its start checks kept changing while they were answered; "
                + "run the start again");
        err.flush();
        return new Result(Cli.EXIT_DIAGNOSTIC, attempt, checks);
    }

    /** Whether two reports say the same things, so showing the second would only repeat the first. */
    private static boolean sameFindings(StartChecks before, StartChecks now) {
        if (before == null || now == null || before.findings().size() != now.findings().size()) {
            return false;
        }
        for (int i = 0; i < now.findings().size(); i++) {
            StartChecks.Finding was = before.findings().get(i);
            StartChecks.Finding is = now.findings().get(i);
            if (!was.key().equals(is.key()) || !was.behavior().equals(is.behavior())
                    || !was.message().equals(is.message())) {
                return false;
            }
        }
        return true;
    }

    // ---- read only -----------------------------------------------------------------------------------

    /** {@code start --checks-only}: what a start would be asked, and nothing started. */
    Result checksOnly(String id, OutputFormat format) {
        switch (server.checks(id, "start")) {
            case StartChecksOutcome.Found found -> {
                StartChecks checks = found.checks();
                if (format == OutputFormat.TEXT) {
                    show(checks, format);
                    if (checks.findings().stream().noneMatch(finding -> !StartChecks.PASS.equals(finding.behavior()))) {
                        out.println("Start checks for " + id + ": nothing to ask");
                    }
                    out.println("outcome: " + checks.outcome().toLowerCase(Locale.ROOT).replace('_', ' '));
                    out.flush();
                } else {
                    printStructured(Map.of("startChecks", checks.raw()), format);
                }
                boolean ready = "READY".equals(checks.outcome()) && !checks.refused();
                return new Result(ready ? Cli.EXIT_OK : Cli.EXIT_DIAGNOSTIC, null, checks);
            }
            case StartChecksOutcome.NotSupported ignored -> {
                return new Result(unsupported("--checks-only"), null, null);
            }
            case StartChecksOutcome.Rejected rejected -> {
                return new Result(reporting.rejection(rejected.code(), rejected.message(), rejected.params()),
                        null, null);
            }
            case StartChecksOutcome.Unreachable ignored -> {
                return new Result(reporting.unreachable(), null, null);
            }
        }
    }

    // ---- rerun ---------------------------------------------------------------------------------------

    /**
     * A rerun's questions, asked while the pipeline still runs untouched: what its start checks would ask of
     * the start that follows the clearing stop, and -- at a terminal, unless told not to ask -- whether to
     * clear at all. Every one is answered, or nothing is stopped.
     */
    RerunPlan planRerun(String id, Answering answering) {
        embeddedNow = answering.embedded();
        StartChecks checks;
        switch (server.checks(id, "rerun")) {
            case StartChecksOutcome.Found found -> checks = found.checks();
            case StartChecksOutcome.NotSupported ignored -> {
                if (!answering.decides().isEmpty()) {
                    return new RerunPlan.Stop(unsupported("--decide"));
                }
                return new RerunPlan.Unchecked();
            }
            case StartChecksOutcome.Rejected rejected -> {
                return new RerunPlan.Stop(reporting.rejection(rejected.code(), rejected.message(), rejected.params()));
            }
            case StartChecksOutcome.Unreachable ignored -> {
                return new RerunPlan.Stop(reporting.unreachable());
            }
        }
        show(checks, answering.format());
        List<StartDecision> decided = new ArrayList<>();
        Integer refused = expand(answering.decides(), checks, decided, answering.format());
        if (refused != null) {
            return new RerunPlan.Stop(refused);
        }
        if (checks.refused()) {
            return new RerunPlan.Stop(refusedOutright(id, checks, answering.format()));
        }
        Integer stopped = answerTheRest(id, checks, decided, answering, "tapstate restart " + id + " --rerun");
        if (stopped != null) {
            return new RerunPlan.Stop(stopped);
        }
        return new RerunPlan.Ready(decided, checks);
    }

    /**
     * The start half of a rerun, after the clearing stop went through. A start refused now leaves the
     * pipeline stopped with its state cleared, and that is said plainly, with the command that starts it.
     */
    Result startAfterRerunStop(String id, RerunPlan.Ready plan, Answering answering) {
        Result result = send(id, new ArrayList<>(plan.decisions()), plan.checks().contentHash(), plan.checks(),
                answering);
        if (result.exitCode() != Cli.EXIT_OK) {
            err.println(verb + ": " + id + " was stopped and its state cleared, but it has not been started; "
                    + "it reads its whole source when it is. Start it with: " + startCommand(id)
                    + answersFor(result.checks()));
            err.flush();
        }
        return result;
    }

    // ---- answering -----------------------------------------------------------------------------------

    /**
     * Turns each {@code --decide} into answers to this report's questions. A check-wide answer applies to every
     * question that check asks, and is refused here when one of them does not offer it; an answer to a refusal
     * is refused here too. An answer to a question this report does not ask is sent as given -- the server
     * reports it as no longer needed -- and a check-wide one that answers nothing is said to have been unused.
     *
     * @return the exit code to stop with, or null when every answer was taken
     */
    private Integer expand(List<String> decides, StartChecks checks, List<StartDecision> decided,
            OutputFormat format) {
        Map<String, String> byKey = new LinkedHashMap<>();
        Map<String, String> explicit = new LinkedHashMap<>();
        for (String decide : decides) {
            int at = decide.lastIndexOf('=');
            String target = decide.substring(0, at);
            String action = decide.substring(at + 1);
            if (target.contains("/")) {
                StartChecks.Finding finding = checks.finding(target);
                if (finding != null && finding.refuses()) {
                    return diagnostic(CliError.DECIDE_ON_A_REFUSAL, Map.of("decide", decide));
                }
                if (finding != null && finding.asks() && finding.action(action) == null) {
                    return diagnostic(CliError.DECIDE_NOT_OFFERED, Map.of("decide", decide, "findings", target));
                }
                explicit.put(target, action);
                continue;
            }
            List<StartChecks.Finding> asked = checks.questions().stream()
                    .filter(finding -> finding.check().equals(target)).toList();
            List<String> notOffering = asked.stream().filter(finding -> finding.action(action) == null)
                    .map(StartChecks.Finding::key).toList();
            if (!notOffering.isEmpty()) {
                return diagnostic(CliError.DECIDE_NOT_OFFERED,
                        Map.of("decide", decide, "findings", String.join(", ", notOffering)));
            }
            if (asked.isEmpty() && checks.findings().stream()
                    .anyMatch(finding -> finding.check().equals(target) && finding.refuses())) {
                return diagnostic(CliError.DECIDE_ON_A_REFUSAL, Map.of("decide", decide));
            }
            if (asked.isEmpty()) {
                note("the answer --decide " + decide + " answers no question this start asks, and was not used",
                        format);
            }
            asked.forEach(finding -> byKey.put(finding.key(), action));
        }
        byKey.putAll(explicit);
        byKey.forEach((key, action) -> decided.add(new StartDecision(key, action)));
        return null;
    }

    /**
     * Answers every question not answered yet: with "go ahead as configured" when told not to ask, at a
     * terminal by asking, and otherwise not at all -- which refuses the start with the commands that answer it.
     *
     * @param command the command line that runs this again, which a refusal prints with each answer added
     * @return the exit code to stop with, or null when every question has an answer
     */
    private Integer answerTheRest(String id, StartChecks checks, List<StartDecision> decided, Answering answering,
            String command) {
        Set<String> answered = new LinkedHashSet<>();
        decided.forEach(decision -> answered.add(decision.finding()));
        List<StartChecks.Finding> open = checks.questions().stream()
                .filter(finding -> !answered.contains(finding.key())).toList();
        if (open.isEmpty()) {
            return null;
        }
        if (answering.unattended()) {
            for (StartChecks.Finding finding : open) {
                StartChecks.Action goAhead = finding.acknowledgement();
                if (goAhead == null) {
                    // Every question offers one; one that does not is a server this client cannot answer for.
                    return diagnostic(CliError.DECIDE_NOT_OFFERED, Map.of("decide", "-y", "findings", finding.key()));
                }
                decided.add(new StartDecision(finding.key(), goAhead.id()));
            }
            return null;
        }
        Prompter asking = interactive(answering) ? prompter.get() : null;
        if (asking == null) {
            if (answering.embedded()) {
                return Cli.EXIT_DIAGNOSTIC;
            }
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("pipeline", id);
            args.put("count", open.size());
            args.put("commands", commandsFor(command, open));
            Diagnostics.printText(err, CliError.START_NEEDS_AN_ANSWER, args);
            err.flush();
            return Cli.EXIT_DIAGNOSTIC;
        }
        for (StartChecks.Finding finding : open) {
            List<String> options = new ArrayList<>();
            for (StartChecks.Action action : finding.actions()) {
                options.add(optionText(action));
            }
            options.add(CANCEL);
            String picked = asking.choose(finding.check() + " on " + finding.subject()
                    + " -- choose an answer, or press Enter to cancel", options);
            int index = options.indexOf(picked);
            if (index < 0 || index == options.size() - 1) {
                out.println(verb + ": cancelled; " + id + " was left as it is");
                out.flush();
                return Cli.EXIT_DIAGNOSTIC;
            }
            decided.add(new StartDecision(finding.key(), finding.actions().get(index).id()));
        }
        return null;
    }

    /** Whether questions may be asked here: a terminal, a person-readable output, and nobody said not to ask. */
    private boolean interactive(Answering answering) {
        return !answering.unattended() && answering.format() == OutputFormat.TEXT && terminal.getAsBoolean();
    }

    private static String optionText(StartChecks.Action action) {
        StringBuilder text = new StringBuilder(action.id()).append("  ").append(action.message());
        if (action.destructive() || !action.changes().isEmpty()) {
            text.append("  [changes the pipeline");
            for (StartChecks.Change change : action.changes()) {
                text.append(": ").append(change.element()).append(' ').append(change.field()).append(' ')
                        .append(change.from()).append(" -> ").append(change.to());
            }
            text.append(']');
        }
        return text.toString();
    }

    /**
     * {@code command} once with each answer {@code open} offers, then once with {@code -y}. An answer every
     * question of a check offers is written for the whole check, so one command answers all of them; one that
     * only some offer is written for each question that does, so no printed command is refused for asking a
     * question an answer it does not offer.
     */
    private static String commandsFor(String command, List<StartChecks.Finding> open) {
        Map<String, List<StartChecks.Finding>> byCheck = new LinkedHashMap<>();
        open.forEach(finding -> byCheck.computeIfAbsent(finding.check(), check -> new ArrayList<>()).add(finding));
        Set<String> commands = new LinkedHashSet<>();
        byCheck.forEach((check, findings) -> {
            Set<String> offered = new LinkedHashSet<>();
            findings.forEach(finding -> finding.actions().forEach(action -> offered.add(action.id())));
            for (String action : offered) {
                if (findings.stream().allMatch(finding -> finding.action(action) != null)) {
                    commands.add(command + " --decide " + check + "=" + action);
                } else {
                    findings.stream().filter(finding -> finding.action(action) != null)
                            .forEach(finding -> commands.add(command + " --decide " + finding.key() + "=" + action));
                }
            }
        });
        commands.add(command + " -y");
        return String.join(" | ", commands);
    }

    private static String startCommand(String id) {
        return "tapstate start " + id;
    }

    private static String answersFor(StartChecks checks) {
        if (checks == null || checks.questions().isEmpty()) {
            return "";
        }
        return " --decide " + checks.questions().getFirst().check() + "=<answer> (or -y)";
    }

    // ---- telling -------------------------------------------------------------------------------------

    /** Shows a report: refusals, then questions, then warnings; what passed is left out. */
    private void show(StartChecks checks, OutputFormat format) {
        if (checks == null) {
            return;
        }
        if (embeddedNow && format != OutputFormat.TEXT) {
            return;
        }
        if (format != OutputFormat.TEXT) {
            // Structured output is one document on stdout; what a person must still see goes to stderr.
            for (StartChecks.Finding finding : checks.findings()) {
                if (StartChecks.WARN.equals(finding.behavior())) {
                    err.println("warning: " + finding.message());
                }
            }
            err.flush();
            return;
        }
        List<StartChecks.Finding> shown = new ArrayList<>();
        checks.findings().stream().filter(StartChecks.Finding::refuses).forEach(shown::add);
        checks.findings().stream().filter(StartChecks.Finding::asks).forEach(shown::add);
        checks.findings().stream().filter(finding -> StartChecks.WARN.equals(finding.behavior())).forEach(shown::add);
        if (shown.isEmpty()) {
            return;
        }
        out.println("Start checks for " + checks.pipelineId() + (checks.loadsAfresh() ? " (new full load)" : "") + ":");
        for (StartChecks.Finding finding : shown) {
            out.println();
            String label = finding.refuses() ? StartChecks.BLOCK.equals(finding.behavior()) ? "BLOCK"
                    : finding.behavior() + " (refuses)" : finding.behavior();
            out.println("  " + label + "  " + finding.check() + "  " + finding.subject());
            out.println("    " + finding.message());
            if (finding.asks()) {
                for (int i = 0; i < finding.actions().size(); i++) {
                    out.println("      " + (i + 1) + ") " + optionText(finding.actions().get(i)));
                }
            }
        }
        out.println();
        out.flush();
    }

    private int started(String id, StartAttempt.Started started, Answering answering) {
        OutputFormat format = answering.format();
        if (answering.embedded()) {
            if (format == OutputFormat.TEXT) {
                tellAppliedChanges(id, started.decisionsApplied());
            }
            return Cli.EXIT_OK;
        }
        if (format != OutputFormat.TEXT) {
            show(started.checks(), format);
            printStructured(started.raw(), format);
        } else {
            if (started.checks() != null) {
                started.checks().findings().stream()
                        .filter(finding -> StartChecks.WARN.equals(finding.behavior()))
                        .forEach(finding -> err.println("warning: " + finding.message()));
                err.flush();
            }
            tellAppliedChanges(id, started.decisionsApplied());
            for (Map<String, Object> stale : started.staleDecisions()) {
                note("the answer " + stale.get("action") + " to " + stale.get("finding")
                        + " was not needed any more, and was not used", format);
            }
            out.println(started.pipelineId() + "  " + started.targetState().toLowerCase(Locale.ROOT));
            out.flush();
        }
        warnIfUnchecked(id, started);
        return Cli.EXIT_OK;
    }

    private void warnIfUnchecked(String id, StartAttempt.Started started) {
        if (started.checks() == null) {
            Diagnostics.printWarning(err, CliError.START_CHECKS_NOT_RUN, Map.of("pipeline", id));
            err.flush();
        }
    }

    /** Tells the person what the server changed in the definition, and that their own file still says otherwise. */
    private void tellAppliedChanges(String id, List<Map<String, Object>> decisionsApplied) {
        for (Map<String, Object> applied : decisionsApplied) {
            if (!(applied.get("changes") instanceof List<?> changes) || changes.isEmpty()) {
                continue;
            }
            for (Object item : changes) {
                if (!(item instanceof Map<?, ?> change)) {
                    continue;
                }
                out.println("Updated " + id + ": " + change.get("element") + " " + change.get("field") + " "
                        + change.get("from") + " -> " + change.get("to") + ".");
                String file = fileOf == null ? null : fileOf.apply(id);
                out.println("  " + (file == null ? "Your workspace copy of " + id : file) + " still says "
                        + change.get("field") + ": " + change.get("from") + " for " + change.get("element")
                        + "; applying it again sets it back, and the next new full load asks again.");
            }
        }
        out.flush();
    }

    private int refusedByChecks(String id, StartAttempt.Stopped stopped, OutputFormat format) {
        if (format != OutputFormat.TEXT) {
            printStructured(stopDocument(stopped), format);
            return Cli.EXIT_DIAGNOSTIC;
        }
        return reporting.rejection(stopped.code(), stopped.message(), stopped.params());
    }

    private int refusedOutright(String id, StartChecks checks, OutputFormat format) {
        if (format != OutputFormat.TEXT) {
            printStructured(Map.of("startChecks", checks.raw()), format);
        } else {
            err.println(verb + ": " + id + " cannot be started as it is: a start check refuses it, whatever is "
                    + "answered; nothing was changed");
            err.flush();
        }
        return Cli.EXIT_DIAGNOSTIC;
    }

    private int rejected(String id, StartAttempt.Rejected rejected, OutputFormat format) {
        tellAppliedChanges(id, rejected.decisionsApplied());
        if (format != OutputFormat.TEXT) {
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("code", rejected.code());
            document.put("params", rejected.params());
            document.put("message", rejected.message());
            if (!rejected.decisionsApplied().isEmpty()) {
                document.put("decisionsApplied", rejected.decisionsApplied());
            }
            printStructured(document, format);
            return Cli.EXIT_DIAGNOSTIC;
        }
        return reporting.rejection(rejected.code(), rejected.message(), rejected.params());
    }

    private static Map<String, Object> stopDocument(StartAttempt.Stopped stopped) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("code", stopped.code());
        document.put("params", stopped.params());
        document.put("message", stopped.message());
        document.put("startChecks", stopped.checks().raw());
        return document;
    }

    private int unsupported(String option) {
        Diagnostics.printText(err, CliError.START_CHECKS_UNSUPPORTED, Map.of("option", option));
        err.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    private int diagnostic(CliError code, Map<String, Object> args) {
        Diagnostics.printText(err, code, args);
        err.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    /** A remark beside the result: with it in text, and kept off a structured document's stream. */
    private void note(String text, OutputFormat format) {
        PrintWriter to = format == OutputFormat.TEXT ? out : err;
        to.println("note: " + text);
        to.flush();
    }

    private void printStructured(Map<String, Object> document, OutputFormat format) {
        out.println(format == OutputFormat.YAML ? YamlOut.write(document) : JsonOut.write(document));
        out.flush();
    }

    /**
     * Whether {@code spec} reads as {@code <check>[/<subject>]=<action>}: an answer with something on both
     * sides of its last {@code =}.
     */
    static boolean wellFormedDecide(String spec) {
        int at = spec.lastIndexOf('=');
        return at > 0 && at < spec.length() - 1;
    }
}
