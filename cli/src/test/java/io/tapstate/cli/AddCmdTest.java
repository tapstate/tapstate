package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code add} verb is the named home for single-resource scaffolding. These tests keep its
 * canonical artifacts byte-identical to the compatibility form and cover the interactive source path.
 */
class AddCmdTest {

    private record Run(int code, String out, String err) {
    }

    private static Run run(String... args) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        return new Run(cl.execute(args), out.toString(), err.toString());
    }

    @Test
    void addProducesTheSameCanonicalArtifactsAsTheDeprecatedAlias(@TempDir Path dir) throws Exception {
        String[][] cases = {
                {"source", "--connector", "mysql", "--id", "source_a"},
                {"pipeline", "--id", "pipeline_a", "--source", "source_a", "--sync-to", "target_a"},
                {"transform", "--id", "transform_a", "--type", "union"},
                {"view", "--id", "view_a", "--primary-key", "id"},
                {"serve", "--id", "serve_a"}
        };

        for (String[] resourceArgs : cases) {
            Path addDir = dir.resolve("add-" + resourceArgs[0]);
            Path aliasDir = dir.resolve("alias-" + resourceArgs[0]);
            String[] addArgs = withOut(concat(new String[]{"add"}, resourceArgs), addDir);
            String[] aliasArgs = withOut(concat(new String[]{"new", "--kind"}, resourceArgs), aliasDir);

            assertThat(run(addArgs).code()).as(resourceArgs[0] + " add").isZero();
            Run alias = run(aliasArgs);
            assertThat(alias.code()).as(resourceArgs[0] + " alias").isZero();
            assertThat(alias.err()).containsOnlyOnce("deprecated")
                    .contains("tapstate add " + resourceArgs[0]);

            String id = resourceArgs[resourceArgs.length - 1];
            if (resourceArgs[0].equals("source")) {
                id = "source_a";
            } else if (resourceArgs[0].equals("pipeline")) {
                id = "pipeline_a";
            } else if (resourceArgs[0].equals("transform")) {
                id = "transform_a";
            } else if (resourceArgs[0].equals("view")) {
                id = "view_a";
            } else {
                id = "serve_a";
            }
            assertThat(Files.readString(addDir.resolve(id + ".tap.yml")))
                    .isEqualTo(Files.readString(aliasDir.resolve(id + ".tap.yml")));
        }
    }

    @Test
    void addSourceUsesTheExistingInteractiveWizard(@TempDir Path dir) throws Exception {
        CommandLine cl = Cli.newCommandLine();
        AddCmd cmd = cl.getSubcommands().get("add").getCommand();
        cmd.prompter = new ScriptedPrompter("elasticsearch", "src_es");
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));

        assertThat(cl.execute("add", "source", "--out", dir.toString())).isZero();
        assertThat(dir.resolve("src_es.tap.yml")).exists();
        assertThat(Files.readString(dir.resolve("src_es.tap.yml"))).contains("connector: elasticsearch");
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void addRejectsAnUnknownKind() {
        Run r = run("add", "widget", "--id", "x", "--dry-run");

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).contains("add: --kind must");
    }

    private static String[] concat(String[] first, String[] second) {
        String[] result = new String[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static String[] withOut(String[] args, Path dir) {
        String[] result = new String[args.length + 2];
        System.arraycopy(args, 0, result, 0, args.length);
        result[args.length] = "--out";
        result[args.length + 1] = dir.toString();
        return result;
    }
}
