package io.tapstate.cli;

import io.tapstate.core.model.SourceMode;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The local single-resource scaffolder. This is the named home for the resource path that used to
 * be exposed only as {@code new --kind}; the implementation is delegated to {@link NewCmd} so the
 * compatibility alias and the new verb cannot drift in questions, validation, or output.
 */
@Command(name = "add", mixinStandardHelpOptions = true,
        description = "Add one source, pipeline, transform, view or serve artifact to a workspace.")
final class AddCmd implements java.util.concurrent.Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Parameters(index = "0", arity = "1", paramLabel = "KIND",
            description = "Resource kind: source, pipeline, transform, view or serve.")
    String kind;

    @Option(names = {"-y", "--yes", "--non-interactive"},
            description = "Never prompt; take every answer from flags (scripting / AI).")
    boolean nonInteractive;

    @Option(names = "--type", paramLabel = "TYPE",
            description = "Transform type: filter, map, js, union, nest or join.")
    String type;

    @Option(names = {"-c", "--connector"}, paramLabel = "ID",
            description = "Connector id from the catalog (source kind).")
    String connector;

    @Option(names = "--id", paramLabel = "ID",
            description = "Top-level id of the artifact.")
    String id;

    @Option(names = {"-m", "--mode"}, paramLabel = "MODE",
            description = "Source read mode (cdc, snapshot, stream, file, api) — must suit the connector.")
    SourceMode mode;

    @Option(names = "--primary-key", paramLabel = "FIELD",
            description = "Field that uniquely identifies a record in a view - required for view kind.")
    String primaryKey;

    @Option(names = "--set", paramLabel = "KEY=VALUE",
            description = "A connector config entry (repeatable).")
    Map<String, String> config = new LinkedHashMap<>();

    @Option(names = "--source", paramLabel = "ID",
            description = "Source id the pipeline reads from (repeatable).")
    List<String> sources = new ArrayList<>();

    @Option(names = "--sync-to", paramLabel = "ID",
            description = "Target source id to sync the pipeline output to (repeatable).")
    List<String> syncTo = new ArrayList<>();

    @Option(names = "--out", paramLabel = "DIR",
            description = "Write the artifact flat into this exact directory, bypassing the workspace layout.")
    String out;

    @Option(names = "--force",
            description = "Overwrite an existing artifact at the target path.")
    boolean force;

    @Option(names = "--dry-run",
            description = "Preview the canonical artifact on stdout without writing any file.")
    boolean dryRun;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format for the result report: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    /** Test seam shared with the existing single-resource command path. */
    Prompter prompter;

    @Override
    public Integer call() {
        if (kind == null || kind.isBlank()) {
            PrintWriter err = CliIo.err(spec);
            err.println("add: provide a resource kind: source, pipeline, transform, view or serve");
            err.flush();
            return NewCmd.EXIT_USAGE;
        }
        NewCmd delegate = new NewCmd();
        delegate.spec = spec;
        delegate.workspace = workspace;
        delegate.kind = kind;
        delegate.nonInteractive = nonInteractive;
        delegate.type = type;
        delegate.connector = connector;
        delegate.id = id;
        delegate.mode = mode;
        delegate.primaryKey = primaryKey;
        delegate.config = config;
        delegate.sources = sources;
        delegate.syncTo = syncTo;
        delegate.out = out;
        delegate.force = force;
        delegate.dryRun = dryRun;
        delegate.output = output;
        delegate.prompter = prompter;
        return delegate.call();
    }
}
