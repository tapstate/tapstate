package io.tapstate.cli;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Path;

/**
 * The project-root option shared by the authoring verbs. A project is a directory holding the
 * {@code *.tap.yml} artifacts a user is editing, laid out by kind ({@code <root>/<kind>/<id>.tap.yml}).
 * Resolution order is flag &gt; {@code TAPSTATE_WORKDIR} env &gt; the nearest directory holding a
 * {@code project.tap.yml} &gt; the conventional {@code tap-work} ({@link ProjectRoot}).
 *
 * <p>Mixed into each verb that operates on a project; the REPL injects the session project by
 * passing {@code --workdir} on the dispatched line.
 */
final class WorkspaceOption {

    @Option(names = {"-w", "--workdir"}, paramLabel = "DIR",
            description = "Project root directory (default: $TAPSTATE_WORKDIR, else the nearest directory "
                    + "holding a project.tap.yml, else tap-work).")
    String workdir;

    /** The resolved project root as a path; never null. */
    Path root() {
        return ProjectRoot.resolve(workdir);
    }

    /**
     * Resolves the project root from top-level args using this option's own precedence
     * ({@link ProjectRoot}). Used to seed the REPL when {@code tapstate}
     * is launched with no verb: the same default expression drives both the verbs and the seed, so they
     * cannot drift. Throws if the args are malformed (e.g. a {@code -w} with no value).
     */
    static Path resolve(String... args) {
        WorkspaceOption option = new WorkspaceOption();
        new CommandLine(option).parseArgs(args);
        return option.root();
    }
}
