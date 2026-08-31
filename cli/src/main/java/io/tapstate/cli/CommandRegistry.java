package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.schema.SchemaNavigator;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.UsageMessageSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The command contract shared by argument mode, the REPL, and the terminal workbench.
 *
 * <p>The registry owns the Picocli table and derives completion and help from that same table. Surfaces
 * only supply input and render the {@link CommandResult} they receive from a session dispatcher.
 */
final class CommandRegistry {

    private static final String SECTION_REPL_BUILTINS = "replBuiltins";

    private final CommandLine commandLine;
    private final SchemaNavigator schema;
    private TapstateCompleter completer;

    private CommandRegistry(CommandLine commandLine, SchemaNavigator schema) {
        this.commandLine = commandLine;
        this.schema = schema;
    }

    static CommandRegistry standard(SchemaNavigator schema) {
        CommandLine commandLine = new CommandLine(new Cli());
        commandLine.addSubcommand(new CommandLine.HelpCommand());
        commandLine.addSubcommand(new AuthCmd());
        commandLine.addSubcommand(new ContextCmd());
        for (String verb : Cli.CONNECTED_VERBS) {
            commandLine.addSubcommand(verb, new ConnectedVerb());
        }
        for (String verb : Cli.LIVE_VIEW_VERBS) {
            commandLine.addSubcommand(verb, new ConnectedVerb());
        }
        for (String verb : Cli.UNIMPLEMENTED_COMPOSITE_VERBS) {
            commandLine.addSubcommand(verb, new UnimplementedVerb());
        }
        for (CommandLine subcommand : commandLine.getSubcommands().values()) {
            subcommand.getCommandSpec().version(Cli.VERSION);
        }
        Cli.VERB_HELP.forEach((verb, help) -> {
            CommandSpec verbSpec = commandLine.getSubcommands().get(verb).getCommandSpec();
            verbSpec.usageMessage()
                    .customSynopsis(commandLine.getCommandName() + " " + verb + " " + help.operands() + " [-hV]")
                    .description(help.summary(), verbSpec.usageMessage().description()[0]);
        });
        addReplBuiltinHelpSection(commandLine);
        reportReplBuiltinsInsteadOfGuessingASpelling(commandLine);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return new CommandRegistry(commandLine, schema);
    }

    static CommandRegistry forCommandLine(CommandLine commandLine, SchemaNavigator schema) {
        return new CommandRegistry(commandLine, schema);
    }

    CommandLine commandLine() {
        return commandLine;
    }

    TapstateCompleter completer() {
        if (completer == null) {
            TreeSet<String> verbs = new TreeSet<>(commandLine.getSubcommands().keySet());
            verbs.addAll(Repl.BUILTINS);
            completer = new TapstateCompleter(verbs, schema, TapstateCatalog.load());
        }
        return completer;
    }

    CommandInvocation invocation(String line) {
        return CommandInvocation.parse(line);
    }

    CommandInvocation invocation(List<String> words) {
        return new CommandInvocation(words);
    }

    CommandResult dispatch(Repl repl, CommandInvocation invocation) {
        return repl.dispatchInvocation(invocation);
    }

    private static void addReplBuiltinHelpSection(CommandLine commandLine) {
        int column = Cli.BUILTIN_HELP.entrySet().stream().mapToInt(entry -> call(entry).length()).max().orElse(0);
        commandLine.getHelpSectionMap().put(SECTION_REPL_BUILTINS, help -> {
            StringBuilder text = new StringBuilder(String.format(
                    "%nSession commands (type these at the prompt, after starting `tapstate`):%n"));
            Cli.BUILTIN_HELP.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> text.append(String.format(
                            "  %-" + column + "s  %s%n", call(entry), entry.getValue().summary())));
            return text.toString();
        });
        List<String> sections = new ArrayList<>(commandLine.getHelpSectionKeys());
        sections.add(sections.indexOf(UsageMessageSpec.SECTION_KEY_COMMAND_LIST) + 1, SECTION_REPL_BUILTINS);
        commandLine.setHelpSectionKeys(sections);
    }

    private static String call(Map.Entry<String, Cli.VerbHelp> builtin) {
        String operands = builtin.getValue().operands();
        return operands.isEmpty() ? builtin.getKey() : builtin.getKey() + " " + operands;
    }

    private static void reportReplBuiltinsInsteadOfGuessingASpelling(CommandLine commandLine) {
        CommandLine.IParameterExceptionHandler fallback = commandLine.getParameterExceptionHandler();
        commandLine.setParameterExceptionHandler((ex, args) -> {
            String first = args.length > 0 ? args[0] : "";
            if (Repl.BUILTINS.contains(first)) {
                CommandLine offending = ex.getCommandLine();
                Diagnostics.printText(offending.getErr(), CliError.REPL_BUILTIN_ONLY, Map.of("verb", first));
                offending.getErr().flush();
                return Cli.EXIT_VERB_UNAVAILABLE;
            }
            return fallback.handleParseException(ex, args);
        });
    }
}
