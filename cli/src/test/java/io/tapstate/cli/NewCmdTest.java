package io.tapstate.cli;

import io.tapstate.core.dsl.WorkspaceLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The {@code new} scaffolding wizard. This suite drives the non-interactive (flag-supplied) path —
 * the scriptable / AI surface that shares one entry with the interactive prompt flow — asserting the
 * emitted artifact is byte-for-byte canonical, the output contract ({@code --out} / {@code --force} /
 * {@code --dry-run} / {@code -o}) holds, and bad input is reported the established way.
 */
class NewCmdTest {

    /** Captured outcome of one one-shot CLI invocation. */
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

    @Test
    void newSourceNonInteractiveWritesCanonicalArtifact(@TempDir Path dir) throws Exception {
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my",
                "--set", "host=10.0.0.1", "--set", "username=writer",
                "--out", dir.toString());

        assertThat(r.code()).isZero();
        Path artifact = dir.resolve("src_my.tap.yml");
        assertThat(artifact).exists();
        assertThat(Files.readString(artifact)).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config:
                  host: 10.0.0.1
                  username: writer
                """);
    }

    @Test
    void newWritesIntoTheKindSubdirOfTheWorkspace(@TempDir Path ws) throws Exception {
        // with a workspace root (-w) and no explicit --out, the artifact lands under
        // <workspace>/<kind>/ — the structural layout the browser and strictness gate rely on
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my", "--set", "host=10.0.0.1",
                "-w", ws.toString());

        assertThat(r.code()).isZero();
        Path artifact = ws.resolve("source").resolve("src_my.tap.yml");
        assertThat(artifact).exists();
        assertThat(Files.readString(artifact)).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config:
                  host: 10.0.0.1
                """);
    }

    @Test
    void newRefusesToOverwriteAnExistingArtifact(@TempDir Path dir) throws Exception {
        Path artifact = dir.resolve("src_my.tap.yml");
        Files.writeString(artifact, "pre-existing\n");

        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my", "--out", dir.toString());

        // a coded domain diagnostic (exit 1), not a usage error, with the established --force remedy
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("cli.artifact-exists");
        // the refusal is non-destructive — the existing file is left exactly as it was
        assertThat(Files.readString(artifact)).isEqualTo("pre-existing\n");
    }

    @Test
    void newForceOverwritesAnExistingArtifact(@TempDir Path dir) throws Exception {
        Path artifact = dir.resolve("src_my.tap.yml");
        Files.writeString(artifact, "pre-existing\n");

        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my", "--out", dir.toString(), "--force");

        assertThat(r.code()).isZero();
        assertThat(Files.readString(artifact)).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                """);
    }

    @Test
    void newDryRunPreviewsCanonicalYamlAndWritesNoFile(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my",
                "--set", "host=10.0.0.1", "--out", dir.toString(), "--dry-run");

        assertThat(r.code()).isZero();
        // stdout carries exactly the canonical artifact (so `tapstate new --dry-run > x.tap.yml` works)
        assertThat(r.out()).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config:
                  host: 10.0.0.1
                """);
        assertThat(dir.resolve("src_my.tap.yml")).doesNotExist();
    }

    @Test
    void newJsonEmitsACreatedEnvelopeOnStdout(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my", "--out", dir.toString(), "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"created\"")
                .contains("\"kind\": \"source\"")
                .contains("\"id\": \"src_my\"")
                .contains("\"connector\": \"mysql\"")
                .contains("\"path\":");
        assertThat(dir.resolve("src_my.tap.yml")).exists();
    }

    @Test
    void newJsonEmitsADiagnosticEnvelopeWhenRefused(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("src_my.tap.yml"), "pre-existing\n");

        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_my", "--out", dir.toString(), "-o", "json");

        assertThat(r.code()).isEqualTo(1);
        // the coded refusal travels as a structured diagnostic on stdout, mirroring validate
        assertThat(r.out()).contains("\"status\": \"error\"")
                .contains("\"code\": \"cli.artifact-exists\"")
                .contains("\"severity\": \"ERROR\"")
                .contains("\"message\":");
    }

    @Test
    void newRejectsAnUnknownConnector(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "not-a-real-connector", "--id", "src_x", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("cli.unknown-connector");
        // an unknown connector aborts before writing anything
        assertThat(dir.resolve("src_x.tap.yml")).doesNotExist();
    }

    @Test
    void newWritesASupportedMode(@TempDir Path dir) throws Exception {
        // mysql is a database connector with trustworthy modes [cdc, snapshot]
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--mode", "cdc", "--id", "src_my",
                "--set", "host=10.0.0.1", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("src_my.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config:
                  host: 10.0.0.1
                mode: cdc
                """);
    }

    @Test
    void newRejectsAModeTheConnectorDoesNotSupport(@TempDir Path dir) {
        // mysql's trustworthy capability matrix is [cdc, snapshot] — stream is not legal
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--mode", "stream", "--id", "src_my", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("dsl.unsupported-mode");
        assertThat(dir.resolve("src_my.tap.yml")).doesNotExist();
    }

    @Test
    void interactiveNewRunsTheWizardAndWritesTheArtifact(@TempDir Path dir) throws Exception {
        // inject a scripted prompter into the command instance: this forces the interactive path,
        // exercising the wizard -> shared output contract wiring without a terminal
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        // elasticsearch resolves no source mode, so the wizard skips the mode question and two
        // scripted answers are the whole flow. SourceWizardTest guards that premise directly.
        cmd.prompter = new ScriptedPrompter("elasticsearch", "src_es");
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));

        int code = cl.execute("new", "--out", dir.toString());

        assertThat(code).isZero();
        assertThat(Files.readString(dir.resolve("src_es.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: src_es
                connector: elasticsearch
                """);
    }

    @Test
    void newPipelineNonInteractiveWritesCanonicalArtifact(@TempDir Path dir) throws Exception {
        Run r = run("new", "--non-interactive", "--kind", "pipeline",
                "--id", "p1", "--source", "src_a", "--sync-to", "tgt_b",
                "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("p1.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void newPipelineNonInteractiveRequiresIdSourceAndSyncTo(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "pipeline",
                "--id", "p1", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--source").contains("--sync-to");
        assertThat(dir.resolve("p1.tap.yml")).doesNotExist();
    }

    @Test
    void newPipelineJsonEmitsACreatedEnvelopeWithSourcesNotConnector(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "pipeline",
                "--id", "p1", "--source", "src_a", "--sync-to", "tgt_b",
                "--out", dir.toString(), "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"created\"")
                .contains("\"kind\": \"pipeline\"")
                .contains("\"id\": \"p1\"")
                .contains("\"sources\"")
                .doesNotContain("\"connector\"");
    }

    @Test
    void interactiveNewPipelineRunsTheWizardAndWritesTheArtifact(@TempDir Path dir) throws Exception {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b");
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));

        int code = cl.execute("new", "--kind", "pipeline", "--out", dir.toString());

        assertThat(code).isZero();
        assertThat(Files.readString(dir.resolve("p1.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void newRejectsAnUnknownKind(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "widget",
                "--id", "x", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--kind");
    }

    @Test
    void newPipelineNonInteractiveSupportsMultipleSyncTargets(@TempDir Path dir) throws Exception {
        Run r = run("new", "--non-interactive", "--kind", "pipeline",
                "--id", "p1", "--source", "src_a", "--sync-to", "tgt_b", "--sync-to", "tgt_c",
                "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("p1.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: tgt_b
                    - id: sync_2
                      source: tgt_c
                """);
    }

    @Test
    void newPipelineRejectsSourceKindFlags(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "pipeline",
                "--id", "p1", "--source", "src_a", "--sync-to", "tgt_b", "--connector", "mysql",
                "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--connector");
        assertThat(dir.resolve("p1.tap.yml")).doesNotExist();
    }

    @Test
    void newSourceRejectsPipelineKindFlags(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "s1", "--source", "x", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--source");
        assertThat(dir.resolve("s1.tap.yml")).doesNotExist();
    }

    @Test
    void aScaffoldedPipelineAndItsSourcesValidateAsAWorkspace(@TempDir Path dir) {
        // the new -> validate round-trip: scaffold the two sources then the pipeline that wires them,
        // and the whole directory loads clean (references resolve, X17 satisfied)
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_a", "--out", dir.toString()).code()).isZero();
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "tgt_b", "--out", dir.toString()).code()).isZero();
        assertThat(run("new", "--non-interactive", "--kind", "pipeline",
                "--id", "p1", "--source", "src_a", "--sync-to", "tgt_b",
                "--out", dir.toString()).code()).isZero();

        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void aScaffoldedCombinedViewAndServePipelineValidatesAsAWorkspace(@TempDir Path dir) throws Exception {
        // the combined output shape: the interactive wizard builds a pipeline carrying BOTH a view and
        // a serve (serve reads from the view); with its sources scaffolded, the workspace loads clean
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_a", "--out", dir.toString()).code()).isZero();
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "tgt_b", "--out", dir.toString()).code()).isZero();

        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter(
                "p1", "src_a", "(done)", "sync", "tgt_b", "inline", "v1", "cust_id");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));
        assertThat(cl.execute("new", "--kind", "pipeline", "--out", dir.toString())).isZero();

        // the pipeline carries both surfaces, serve wired exactly to the view id (not the raw source)
        assertThat(Files.readString(dir.resolve("p1.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  id: v1
                  from: /.*/
                  primary_key: cust_id
                serve:
                  id: serve
                  from: v1
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void aCombinedViewNamedLikeTheServeBlockStillValidates(@TempDir Path dir) throws Exception {
        // a user who names the inline view "serve" would collide with the inline serve block's id and
        // crash validate with a duplicate id; the wizard re-prompts, so the workspace still loads clean
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_a", "--out", dir.toString()).code()).isZero();
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "tgt_b", "--out", dir.toString()).code()).isZero();

        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter(
                "p1", "src_a", "(done)", "sync", "tgt_b", "inline", "serve", "v_real", "");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));
        assertThat(cl.execute("new", "--kind", "pipeline", "--out", dir.toString())).isZero();

        assertThat(Files.readString(dir.resolve("p1.tap.yml"))).contains("id: v_real");
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void aScaffoldedPipelineReusingViewAndServeDefinitionsValidates(@TempDir Path dir) throws Exception {
        // the use-reference shape: the pipeline references a kind:view and a kind:serve definition by id.
        // With those definitions present, the whole workspace must resolve and load clean.
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_a", "--out", dir.toString()).code()).isZero();
        Files.writeString(dir.resolve("v_cust.tap.yml"),
                """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: customer_id
                storage:
                  warm:
                    collection: cust
                """);
        Files.writeString(dir.resolve("std_api.tap.yml"),
                """
                version: tapstate/v1
                kind: serve
                id: std_api
                query:
                  - type: rest
                """);

        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        // the wizard discovers v_cust / std_api and offers them; picking each yields a use-reference
        cmd.prompter = new ScriptedPrompter("p1", "src_a", "(done)", "std_api", "v_cust");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));
        assertThat(cl.execute("new", "--kind", "pipeline", "--out", dir.toString())).isZero();

        String pipe = Files.readString(dir.resolve("p1.tap.yml"));
        assertThat(pipe).contains("use: v_cust").contains("use: std_api");
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void aScaffoldedPipelineReusingATransformDefinitionValidates(@TempDir Path dir) throws Exception {
        // the full reuse loop end to end: scaffold a standalone kind:transform, then let the pipeline
        // wizard discover it in the workspace and pick it via use:; the assembled workspace must load clean.
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "src_a", "--out", dir.toString()).code()).isZero();
        assertThat(run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "tgt_b", "--out", dir.toString()).code()).isZero();
        assertThat(run("new", "--non-interactive", "--kind", "transform",
                "--id", "mask_pii", "--type", "filter", "--out", dir.toString()).code()).isZero();

        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        // the wizard offers mask_pii in the transform menu; (use) -> mask_pii yields a use-reference,
        // then a sync serve to tgt_b. The trailing view stage is left to skip (no view definition).
        ScriptedPrompter prompter = new ScriptedPrompter(
                "p1", "src_a", "(use)", "mask_pii", "customers", "(done)", "sync", "tgt_b");
        cmd.prompter = prompter;
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));
        assertThat(cl.execute("new", "--kind", "pipeline", "--out", dir.toString())).isZero();

        // the reuse submenu was fed exactly the workspace-discovered transform id, not a free-typed one:
        // offered[1] = transform menu (offers (use) only because discovery found a definition),
        // offered[2] = "Reuse which transform?" = the discovered transform list itself.
        assertThat(prompter.offered.get(1)).containsExactly("filter", "map", "js", "(use)", "(done)");
        assertThat(prompter.offered.get(2)).containsExactly("mask_pii");

        String pipe = Files.readString(dir.resolve("p1.tap.yml"));
        assertThat(pipe).contains("use: mask_pii").contains("source: tgt_b");
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void newViewNonInteractiveWritesAMinimalArtifact(@TempDir Path dir) throws Exception {
        // the non-interactive path scaffolds the minimal view skeleton (id only); the richer fields
        // (primary key, storage, schema) are authored interactively or by hand
        Run r = run("new", "--non-interactive", "--kind", "view", "--id", "v_cust",
                "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("v_cust.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: view
                id: v_cust
                """);
    }

    @Test
    void newServeNonInteractiveWritesAMinimalArtifact(@TempDir Path dir) throws Exception {
        // the minimal serve skeleton carries no surface yet — the user adds sync / query / push by hand
        Run r = run("new", "--non-interactive", "--kind", "serve", "--id", "std_api",
                "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("std_api.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_api
                """);
        // the surface-less minimal scaffold must still load clean — the documented hand-edit starting point
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void newTransformNonInteractiveScaffoldsAUnionByType(@TempDir Path dir) throws Exception {
        // a union has a complete empty body; the non-interactive scaffold is the artifact itself
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_union",
                "--type", "union", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("t_union.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: t_union
                type: union
                """);
    }

    @Test
    void newTransformNonInteractiveScaffoldsAFilterPlaceholder(@TempDir Path dir) throws Exception {
        // a content-bearing type scaffolds an editable placeholder body (the live-rows-only filter)
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_filter",
                "--type", "filter", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("t_filter.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: t_filter
                type: filter
                expr: "op != 'd'"
                """);
    }

    @Test
    void newTransformNonInteractiveScaffoldsANestPlaceholder(@TempDir Path dir) throws Exception {
        // a nest scaffold carries a placeholder root alias (abstract, bound at the use site) and no children
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_nest",
                "--type", "nest", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("t_nest.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: t_nest
                type: nest
                root:
                  from: main
                """);
    }

    @Test
    void everyNonInteractiveTransformScaffoldValidatesAsAWorkspace(@TempDir Path dir) {
        // each type's placeholder body must parse + load clean (the scaffold -> validate green contract)
        for (String type : java.util.List.of("filter", "map", "js", "union", "nest", "join")) {
            assertThat(run("new", "--non-interactive", "--kind", "transform", "--id", "t_" + type,
                    "--type", type, "--out", dir.toString()).code())
                    .as("scaffold of type %s", type).isZero();
        }
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void newTransformNonInteractiveScaffoldsAMapPlaceholder(@TempDir Path dir) throws Exception {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_map",
                "--type", "map", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("t_map.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: t_map
                type: map
                fields:
                  id: $id
                """);
    }

    @Test
    void newTransformNonInteractiveScaffoldsAJsPlaceholder(@TempDir Path dir) throws Exception {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_js",
                "--type", "js", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("t_js.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: t_js
                type: js
                script: |
                  emit(after)
                """);
    }

    @Test
    void newTransformNonInteractiveScaffoldsAJoinPlaceholder(@TempDir Path dir) throws Exception {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_join",
                "--type", "join", "--out", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(Files.readString(dir.resolve("t_join.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: t_join
                type: join
                engine: duckdb
                sql: |
                  SELECT * FROM a
                """);
    }

    @Test
    void newTransformNonInteractiveRequiresIdAndType(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t1",
                "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--type");
        assertThat(dir.resolve("t1.tap.yml")).doesNotExist();
    }

    @Test
    void newTransformNonInteractiveAlsoRequiresIdWhenOnlyTypeGiven(@TempDir Path dir) {
        // the symmetric missing-required-flag case: --type without --id under -y is a usage error
        Run r = run("new", "--non-interactive", "--kind", "transform", "--type", "union",
                "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--id");
        assertThat(dir.resolve("union.tap.yml")).doesNotExist();
    }

    @Test
    void newTransformRejectsAnUnknownType(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t1",
                "--type", "widget", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--type");
        assertThat(dir.resolve("t1.tap.yml")).doesNotExist();
    }

    @Test
    void newTransformRejectsSourceAndPipelineFlags(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t1",
                "--type", "union", "--connector", "mysql", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--connector");
        assertThat(dir.resolve("t1.tap.yml")).doesNotExist();
    }

    @Test
    void newTypeFlagIsRejectedForNonTransformKinds(@TempDir Path dir) {
        // --type is transform-only; bundling it with another kind is a usage error
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "mysql", "--id", "s1", "--type", "filter", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--type");
        assertThat(dir.resolve("s1.tap.yml")).doesNotExist();
    }

    @Test
    void newTransformJsonEmitsACreatedEnvelopeWithType(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "transform", "--id", "t_filter",
                "--type", "filter", "--out", dir.toString(), "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"created\"")
                .contains("\"kind\": \"transform\"")
                .contains("\"id\": \"t_filter\"")
                .contains("\"type\": \"filter\"")
                .contains("\"path\":");
    }

    @Test
    void interactiveNewTransformRunsTheWizardAndWritesTheArtifact(@TempDir Path dir) throws Exception {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("live_only", "filter", "op != 'd'");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("new", "--kind", "transform", "--out", dir.toString());

        assertThat(code).isZero();
        assertThat(Files.readString(dir.resolve("live_only.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: live_only
                type: filter
                expr: "op != 'd'"
                """);
    }

    @Test
    void aScaffoldedStandaloneNestTransformValidatesUnbound(@TempDir Path dir) throws Exception {
        // the X19 abstract-alias proof end to end: a nest definition's root.from / embed.from aliases are
        // bound only at a pipeline use site, so the definition alone loads clean
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter(
                "c360_shape", "nest", "customer", "customer_id",
                "embed", "policy", "CUST_ID", "customer_id", "", "array", "policies", "POLICY_ID",
                "(done)", "(done)");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));
        assertThat(cl.execute("new", "--kind", "transform", "--out", dir.toString())).isZero();

        // the full interactive command path (wizard -> emit -> file write) produces the exact nest artifact
        assertThat(Files.readString(dir.resolve("c360_shape.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: c360_shape
                type: nest
                root:
                  from: customer
                  key: [customer_id]
                  embed:
                    - from: policy
                      on:
                        CUST_ID: customer_id
                      as: array
                      path: policies
                      arrayKey: [POLICY_ID]
                """);
        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void newViewRejectsSourceAndPipelineFlags(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "view", "--id", "v1",
                "--connector", "mysql", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--connector");
        assertThat(dir.resolve("v1.tap.yml")).doesNotExist();
    }

    @Test
    void newServeRejectsSourceAndPipelineFlags(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "serve", "--id", "s1",
                "--source", "src_a", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--source");
        assertThat(dir.resolve("s1.tap.yml")).doesNotExist();
    }

    @Test
    void newViewRejectsTheModeFlag(@TempDir Path dir) {
        // each definition-incompatible flag is rejected on its own merit (not just when bundled with others)
        Run r = run("new", "--non-interactive", "--kind", "view", "--id", "v1",
                "--mode", "cdc", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(dir.resolve("v1.tap.yml")).doesNotExist();
    }

    @Test
    void newViewRejectsTheSetFlag(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "view", "--id", "v1",
                "--set", "host=x", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(dir.resolve("v1.tap.yml")).doesNotExist();
    }

    @Test
    void newServeRejectsTheSyncToFlag(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "serve", "--id", "s1",
                "--sync-to", "tgt_b", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(2);
        assertThat(dir.resolve("s1.tap.yml")).doesNotExist();
    }

    @Test
    void newViewJsonEmitsACreatedEnvelope(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "view", "--id", "v_cust",
                "--out", dir.toString(), "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"created\"")
                .contains("\"kind\": \"view\"")
                .contains("\"id\": \"v_cust\"")
                .contains("\"path\":")
                // a keyless view omits the primary_key field entirely (a kind-field appears only when it has content)
                .doesNotContain("primary_key");
    }

    @Test
    void newServeJsonEmitsACreatedEnvelope(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "serve", "--id", "std_api",
                "--out", dir.toString(), "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"created\"")
                .contains("\"kind\": \"serve\"")
                .contains("\"id\": \"std_api\"")
                .contains("\"path\":")
                // a surface-less scaffold omits surfaces — same convention as view's primary_key (siblings agree)
                .doesNotContain("surfaces");
    }

    @Test
    void interactiveNewServeJsonEmitsSurfacesWhenPresent(@TempDir Path dir) {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("std_sink", "sync", "tgt_b");
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("new", "--kind", "serve", "--out", dir.toString(), "-o", "json");

        assertThat(code).isZero();
        // a serve that actually carries a surface lists it under surfaces (present-case of the convention)
        assertThat(out.toString()).contains("\"surfaces\"").contains("\"sync\"");
    }

    @Test
    void interactiveNewViewJsonEmitsThePrimaryKeyWhenPresent(@TempDir Path dir) {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("v_cust", "customer_id");
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("new", "--kind", "view", "--out", dir.toString(), "-o", "json");

        assertThat(code).isZero();
        assertThat(out.toString()).contains("\"primary_key\": \"customer_id\"");
    }

    @Test
    void interactiveNewViewRunsTheWizardAndWritesTheArtifact(@TempDir Path dir) throws Exception {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("v_cust", "customer_id");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("new", "--kind", "view", "--out", dir.toString());

        assertThat(code).isZero();
        assertThat(Files.readString(dir.resolve("v_cust.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: customer_id
                """);
    }

    @Test
    void interactiveNewServeRunsTheWizardAndWritesTheArtifact(@TempDir Path dir) throws Exception {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("std_sink", "sync", "tgt_b");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("new", "--kind", "serve", "--out", dir.toString());

        assertThat(code).isZero();
        assertThat(Files.readString(dir.resolve("std_sink.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_sink
                sync:
                  - id: sync_1
                    source: tgt_b
                """);
    }

    @Test
    void aScaffoldedStandaloneServeWithASyncValidatesUnbound(@TempDir Path dir) throws Exception {
        // a standalone serve names a target source that is not (yet) in the workspace; its references
        // are only bound at a pipeline use site, so the definition alone loads clean (X19 abstract-alias)
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = new ScriptedPrompter("std_sink", "sync", "tgt_b");
        cl.setOut(new PrintWriter(new StringWriter()));
        cl.setErr(new PrintWriter(new StringWriter()));
        assertThat(cl.execute("new", "--kind", "serve", "--out", dir.toString())).isZero();

        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    @Test
    void aScaffoldedStandaloneViewValidates(@TempDir Path dir) {
        assertThat(run("new", "--non-interactive", "--kind", "view", "--id", "v_cust",
                "--out", dir.toString()).code()).isZero();

        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }
}
