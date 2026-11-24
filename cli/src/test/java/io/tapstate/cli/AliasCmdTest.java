package io.tapstate.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * The {@code alias} verb, driven the way a user drives it. {@link AliasLinksTest} covers what a link
 * must be; this covers the command around it — that both halves are reachable from the command line,
 * that each says which directory it acted on, and that a directory holding no {@code tapstate} entry
 * is refused rather than silently managed.
 *
 * <p>The install directory is located through {@code user.home} here because the environment variable
 * the command also honours cannot be set from inside a running JVM. The default path is the one a user
 * who never set that variable takes, so it is the branch worth pinning.
 */
class AliasCmdTest {

    private record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    private static Run run(String... args) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    /** Runs with {@code user.home} pointed at a temp tree, restoring it whatever happens. */
    private static Run runWithHome(Path home, String... args) {
        String previous = System.getProperty("user.home");
        System.setProperty("user.home", home.toString());
        try {
            return run(args);
        } finally {
            System.setProperty("user.home", previous);
        }
    }

    private static Path installedTree(Path home) throws IOException {
        Path binDir = home.resolve(".tapstate").resolve("bin");
        Files.createDirectories(binDir);
        Files.createFile(binDir.resolve("tapstate"));
        return binDir;
    }

    @Test
    void install_creates_the_shortcut_and_says_where(@TempDir Path home) throws IOException {
        Path binDir = installedTree(home);

        Run run = runWithHome(home, "alias", "install");

        assertThat(run.code()).isZero();
        assertThat(Files.isSymbolicLink(binDir.resolve("tap"))).isTrue();
        // Naming the directory is what lets a user with more than one installation tell which one
        // just changed; "created the shortcut" alone does not.
        assertThat(run.all()).contains(binDir.toString());
    }

    @Test
    void uninstall_removes_the_shortcut_and_leaves_the_real_command(@TempDir Path home)
            throws IOException {
        Path binDir = installedTree(home);
        runWithHome(home, "alias", "install");

        Run run = runWithHome(home, "alias", "uninstall");

        assertThat(run.code()).isZero();
        assertThat(Files.exists(binDir.resolve("tap"), LinkOption.NOFOLLOW_LINKS)).isFalse();
        // The whole point of the verb is that it is reversible without touching what it shortcuts.
        assertThat(Files.exists(binDir.resolve("tapstate"))).isTrue();
    }

    @Test
    void uninstall_on_a_machine_that_never_had_the_shortcut_succeeds(@TempDir Path home)
            throws IOException {
        // Removing something already absent is the state the user asked for. Failing here would make
        // "make sure the shortcut is gone" a command nobody can run twice.
        installedTree(home);

        Run run = runWithHome(home, "alias", "uninstall");

        assertThat(run.code()).isZero();
    }

    @Test
    void a_directory_with_no_tapstate_in_it_is_refused(@TempDir Path home) throws IOException {
        // An install directory holding no tapstate entry is one this command was pointed at by
        // mistake. Guessing another would manage a shortcut for an installation the user did not mean,
        // and the mistake would surface later as a shortcut that resolves to nothing.
        Files.createDirectories(home.resolve(".tapstate").resolve("bin"));

        Run run = runWithHome(home, "alias", "install");

        assertThat(run.code()).isNotZero();
        assertThat(run.all()).contains("TS0330007");
    }

    @Test
    void the_alias_verb_is_reachable_and_describes_itself() {
        // It is registered as a meta verb: it is about the CLI rather than about a resource, and
        // projects no operation. A verb that stopped being reachable would still pass every test that
        // only ever calls its subcommands directly.
        Run run = run("alias", "--help");

        assertThat(run.code()).isZero();
        assertThat(run.all()).contains("install", "uninstall");
        // tapstate is the only real command; the help must not imply otherwise.
        assertThat(run.all()).contains("tapstate");
    }
}
