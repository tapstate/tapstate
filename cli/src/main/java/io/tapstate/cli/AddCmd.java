package io.tapstate.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;

/**
 * The local single-resource scaffolder. This is the named home for the resource path that used to
 * be exposed only as {@code new --kind}; the implementation is delegated to {@link NewCmd} so the
 * compatibility alias and the new verb cannot drift in questions, validation, or output.
 */
@Command(name = "add", mixinStandardHelpOptions = true,
        description = "Add one source, pipeline, transform, view or serve artifact to a workspace.")
final class AddCmd extends SingleResourceOptions implements java.util.concurrent.Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Parameters(index = "0", arity = "1", paramLabel = "KIND",
            description = "Resource kind: source, pipeline, transform, view or serve.")
    String kind;

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
