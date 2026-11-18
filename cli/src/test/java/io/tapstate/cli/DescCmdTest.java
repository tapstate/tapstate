package io.tapstate.cli;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code desc} — the rich single-resource description. Where {@code ls} lists, {@code desc} resolves
 * one id within the workspace and reports a header (kind / id / path), a per-kind field summary, the
 * workspace validation status, and the reference relationships in both directions (what it references
 * and who references it). This suite drives the one-shot surface and the REPL session wiring: the
 * per-kind summaries, the {@code valid} / {@code invalid} validation status, the forward / reverse
 * references, the {@code -o json|yaml} envelope, the {@code TS0330017} coded miss, and
 * that {@code desc} is registered as an offline verb.
 */
class DescCmdTest {

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

    /**
     * Scaffolds a valid reuse-assembly workspace (corpus s11): a source, a reusable transform / view /
     * serve, and a pipeline that references all four. Files are written straight into their kind dirs.
     */
    private static void scaffoldReuseAssembly(Path ws) throws Exception {
        write(ws, "source", "src_crm", """
                version: tapstate/v1
                kind: source
                id: src_crm
                connector: mysql
                config: { host: 10.10.0.4 }
                mode: cdc
                tables: [ customers ]
                """);
        write(ws, "transform", "mask_pii", """
                version: tapstate/v1
                kind: transform
                id: mask_pii
                type: map
                fields: { ssn: false }
                """);
        write(ws, "view", "v_cust", """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: customer_id
                """);
        write(ws, "serve", "std_api", """
                version: tapstate/v1
                kind: serve
                id: std_api
                query: [ { type: rest } ]
                """);
        write(ws, "pipeline", "crm_pack", """
                version: tapstate/v1
                kind: pipeline
                id: crm_pack
                source: src_crm
                transforms:
                  - { type: filter, from: [customers], expr: "op != 'd'" }
                  - mask_pii
                view:  v_cust
                serve: std_api
                """);
    }

    private static void write(Path ws, String kind, String id, String body) throws Exception {
        Path dir = ws.resolve(kind);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(id + ".tap.yml"), body);
    }

    // ---- header + per-kind summary ------------------------------------------------------

    @Test
    void descShowsHeaderAndSourceSummary(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "src_crm", "-w", ws.toString());
        assertThat(r.code()).isZero();
        // header: kind + id + the relative file path
        assertThat(r.out()).contains("source").contains("src_crm").contains("source/src_crm.tap.yml");
        // source summary: connector, mode and table names
        assertThat(r.out()).contains("mysql").contains("cdc").contains("customers");
    }

    @Test
    void descPipelineSummaryShowsSourcesAndSurfaces(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "crm_pack", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("pipeline").contains("crm_pack");
        // source list, transform count, and both output surfaces present
        assertThat(r.out()).contains("src_crm");
        assertThat(r.out()).containsIgnoringCase("transform").contains("2");
        assertThat(r.out()).containsIgnoringCase("view").containsIgnoringCase("serve");
    }

    @Test
    void descTransformShowsItsType(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "mask_pii", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("transform").contains("mask_pii").contains("map");
    }

    @Test
    void descViewShowsPrimaryKey(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "v_cust", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("view").contains("v_cust").contains("customer_id");
    }

    @Test
    void descSourceSummaryRendersRegexTablesWithSlashes(@TempDir Path ws) throws Exception {
        write(ws, "source", "src_re", """
                version: tapstate/v1
                kind: source
                id: src_re
                connector: mysql
                mode: cdc
                tables: [ /cust.*/ ]
                """);
        Run r = run("desc", "src_re", "-w", ws.toString());
        assertThat(r.code()).isZero();
        // a regex selector renders wrapped in slashes — distinct from a literal table named 'cust.*'
        assertThat(r.out()).contains("/cust.*/");
    }

    @Test
    void descServeShowsSurfaceCounts(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "std_api", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("serve").contains("std_api");
        // a serve definition's surfaces: this one carries a single query
        assertThat(r.out()).containsIgnoringCase("query");
    }

    // ---- validation status --------------------------------------------------------------

    @Test
    void descShowsValidWorkspaceStatus(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "src_crm", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).containsIgnoringCase("valid");
    }

    @Test
    void descShowsInvalidStatusWithTheDiagnostic(@TempDir Path ws) throws Exception {
        // a pipeline whose view use: names a definition that does not exist — closure flags it
        write(ws, "source", "src_a", """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mode: cdc
                """);
        write(ws, "pipeline", "p1", """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  use: ghost_view
                  from: /.*/
                """);
        Run r = run("desc", "p1", "-w", ws.toString());
        // desc describes the resource regardless; validation is reported as invalid with the code
        assertThat(r.code()).isZero();
        assertThat(r.out()).containsIgnoringCase("invalid").contains("TS0010005");
    }

    // ---- references: forward and reverse -----------------------------------------------

    @Test
    void descShowsForwardReferences(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "crm_pack", "-w", ws.toString());
        assertThat(r.code()).isZero();
        // the four edges land on the forward 'references' line, not 'referenced by' — bind the direction
        // so a forward/reverse swap in text rendering cannot pass (the ids would move to 'referenced by')
        assertThat(referencesLine(r.out())).contains("src_crm").contains("mask_pii").contains("v_cust").contains("std_api");
        // the pipeline is referenced by nobody: a swap would fill this line with the four ids instead
        assertThat(r.out()).contains("referenced by (none)");
    }

    @Test
    void descShowsReverseReferences(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "mask_pii", "-w", ws.toString());
        assertThat(r.code()).isZero();
        // the reusable transform references nobody forward, and is referenced by the pipeline that uses it
        assertThat(referencesLine(r.out())).contains("(none)");
        assertThat(r.out()).contains("crm_pack");
    }

    /** The text-mode forward 'references' line (distinct from the 'referenced by' line). */
    private static String referencesLine(String out) {
        return out.lines().filter(l -> l.trim().startsWith("references ")).findFirst().orElse("");
    }

    // ---- structured envelope ------------------------------------------------------------

    @Test
    void descJsonEnvelopeHasHeaderSummaryValidationAndReferences(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "crm_pack", "-o", "json", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out())
                .contains("\"id\": \"crm_pack\"")
                .contains("\"kind\": \"pipeline\"")
                .contains("\"path\":")
                .contains("\"summary\":")
                .contains("\"validation\":")
                .contains("\"status\": \"valid\"")
                .contains("\"references\":")
                .contains("\"referencedBy\":");
        // forward edges carry id + kind; the pipeline references nobody in reverse
        assertThat(r.out()).contains("\"id\": \"src_crm\"").contains("\"kind\": \"source\"");
        assertThat(r.out()).contains("\"referencedBy\": []");
    }

    @Test
    void descJsonReverseEdgeNamesTheReferrer(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "src_crm", "-o", "json", "-w", ws.toString());
        assertThat(r.code()).isZero();
        // a source references nothing forward, and is referenced by the pipeline
        assertThat(r.out()).contains("\"references\": []");
        assertThat(r.out()).contains("\"id\": \"crm_pack\"").contains("\"kind\": \"pipeline\"");
    }

    @Test
    void descYamlEnvelopeRendersTheDescribeObject(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "src_crm", "-o", "yaml", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("id: src_crm").contains("kind: source")
                .contains("summary:").contains("validation:").contains("referencedBy:");
    }

    // ---- resource-not-found -------------------------------------------------------------

    @Test
    void descUnknownIdIsAResourceNotFoundDiagnostic(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "nope", "-w", ws.toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("TS0330017").contains("nope");
    }

    @Test
    void descUnknownIdJsonEmitsACodedDiagnostic(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        Run r = run("desc", "nope", "-o", "json", "-w", ws.toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.out()).contains("\"code\": \"TS0330017\"");
        // the failure envelope carries the top-level status discriminator, like new / validate
        assertThat(r.out()).contains("\"status\": \"error\"");
    }

    @Test
    void descInEmptyWorkspaceIsResourceNotFound(@TempDir Path empty) {
        Run r = run("desc", "anything", "-w", empty.toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("TS0330017");
    }

    // ---- honesty: unreadable / malformed ------------------------------------------------

    @Test
    void descUnreadableTargetIsSurfacedWithCodedValidationNotARawStack(@TempDir Path ws) throws Exception {
        Files.createDirectories(ws.resolve("source"));
        Files.writeString(ws.resolve("source").resolve("broken.tap.yml"), "[unterminated\n");
        Run r = run("desc", "broken", "-w", ws.toString());
        // an unreadable target is still described (found by stem), exit 0 — desc is a describe, not a gate
        assertThat(r.code()).isZero();
        assertThat(r.all()).containsIgnoringCase("unreadable");
        // its validation is the coded malformed-yaml — never a raw snakeyaml stack at the user boundary
        assertThat(r.all()).contains("TS0010017");
        assertThat(r.all()).doesNotContain("org.yaml.snakeyaml");
    }

    @Test
    void descWithAMalformedSiblingReportsCodedValidationNotARawStack(@TempDir Path ws) throws Exception {
        write(ws, "source", "src_ok", """
                version: tapstate/v1
                kind: source
                id: src_ok
                connector: mysql
                mode: cdc
                """);
        Files.writeString(ws.resolve("source").resolve("broken.tap.yml"), "[unterminated\n");
        Run r = run("desc", "src_ok", "-w", ws.toString());
        // describing a readable resource must not crash on a malformed neighbour the validate run hits
        assertThat(r.code()).isZero();
        assertThat(r.all()).contains("TS0010017");
        assertThat(r.all()).doesNotContain("org.yaml.snakeyaml");
    }

    @Test
    void descUnreadableTargetJsonShapeFlagsReadableFalseWithNoSummary(@TempDir Path ws) throws Exception {
        Files.createDirectories(ws.resolve("source"));
        Files.writeString(ws.resolve("source").resolve("broken.tap.yml"), "[unterminated\n");
        Run r = run("desc", "broken", "-o", "json", "-w", ws.toString());
        assertThat(r.code()).isZero();
        // the machine shape is honest: readable:false and NO summary key (which would claim a shape it lacks)
        assertThat(r.out()).contains("\"readable\": false").doesNotContain("\"summary\"");
        assertThat(r.out()).contains("TS0010017");
    }

    @Test
    void descMisplacedResourceIsFlaggedNotMisrenderedInJson(@TempDir Path ws) throws Exception {
        // a kind:source document physically in pipeline/: structural slot is pipeline, but it must be
        // flagged misplaced — never rendered with source-only fields under a pipeline discriminator
        Files.createDirectories(ws.resolve("pipeline"));
        Files.writeString(ws.resolve("pipeline").resolve("stray.tap.yml"),
                "version: tapstate/v1\nkind: source\nid: stray_src\nconnector: mysql\nmode: cdc\n");
        Run r = run("desc", "stray_src", "-o", "json", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"kind\": \"pipeline\"")
                .contains("\"misplaced\": true")
                .contains("\"declaredKind\": \"source\"");
        // no source-only fields nor a summary leak under the pipeline discriminator
        assertThat(r.out()).doesNotContain("connector").doesNotContain("\"summary\"");
        // and validation surfaces the same conflict as a coded kind-dir mismatch
        assertThat(r.out()).contains("TS0330016");
    }

    @Test
    void descMisplacedResourceIsFlaggedInTextToo(@TempDir Path ws) throws Exception {
        Files.createDirectories(ws.resolve("pipeline"));
        Files.writeString(ws.resolve("pipeline").resolve("stray.tap.yml"),
                "version: tapstate/v1\nkind: source\nid: stray_src\nconnector: mysql\nmode: cdc\n");
        Run r = run("desc", "stray_src", "-w", ws.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("stray_src").containsIgnoringCase("misplaced").contains("source");
        // the wrong-kind summary must not be rendered (no connector field bleeding through)
        assertThat(r.out()).doesNotContain("connector");
    }

    @Test
    void descReportsValidationErrorOnAnUnreadableArtifact(@TempDir Path ws) throws Exception {
        // a readable target, but a sibling artifact the validate load cannot read -> status: error
        write(ws, "source", "src_ok", """
                version: tapstate/v1
                kind: source
                id: src_ok
                connector: mysql
                mode: cdc
                """);
        Path locked = ws.resolve("source").resolve("locked.tap.yml");
        Files.writeString(locked, "version: tapstate/v1\nkind: source\nid: locked\nconnector: mysql\n");
        boolean blocked = locked.toFile().setReadable(false);
        Assumptions.assumeTrue(blocked && !Files.isReadable(locked),
                "filesystem does not enforce owner-unreadable; skipping IO-fault validation test");
        Run r = run("desc", "src_ok", "-w", ws.toString());
        // desc still describes the resource (exit 0), reporting the IO fault as an error validation status
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("src_ok");
        assertThat(r.all()).containsIgnoringCase("error");
        assertThat(r.all()).doesNotContain("org.yaml.snakeyaml").doesNotContain("UncheckedIOException");
    }

    // ---- registration + REPL ------------------------------------------------------------

    @Test
    void descIsRegisteredAsAnOfflineVerb() {
        // that the whole table and the whitelist stay in lockstep is asserted once, in CliTest; this
        // one claims only desc's own place in it
        assertThat(Cli.OFFLINE_VERBS).contains("desc");
        assertThat(Cli.newCommandLine().getSubcommands().keySet()).contains("desc");
    }

    @Test
    void descThroughTheReplUsesTheSessionWorkspace(@TempDir Path ws) throws Exception {
        scaffoldReuseAssembly(ws);
        CommandLine cl = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter pw = new PrintWriter(sink);
        cl.setOut(pw);
        cl.setErr(pw);
        Repl repl = new Repl(cl, ws);
        // dispatched bare through the REPL, desc inherits the session workspace via -w injection
        assertThat(repl.dispatch("desc src_crm")).isTrue();
        assertThat(sink.toString()).contains("src_crm").contains("mysql");
    }
}
