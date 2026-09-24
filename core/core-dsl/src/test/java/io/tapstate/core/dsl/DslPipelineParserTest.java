package io.tapstate.core.dsl;

import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B3-3: parse kind:pipeline documents (inline transforms/view/serve + use-references) with
 * full sugar normalization and anonymous id generation. The strongest checks are the two
 * golden samples canonical-form.md §7 spells out verbatim: parse the authored corpus file,
 * re-emit via {@link CanonicalWriter}, and assert byte-for-byte equality with the golden.
 */
class DslPipelineParserTest {

    private static final Path CORPUS = Path.of("src", "test", "resources", "corpus");

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    @Test
    void goldenB_wholeSourceMirror_s01() {
        // canonical-form.md §7 sample B: source scalar; serve inline (anonymous -> id: serve,
        // 2026-06-15 coverage; from /.*/, one sync); write_mode: upsert and
        // options.auto_create_table: true are defaults -> omitted.
        String golden = """
                version: tapstate/v1
                kind: pipeline
                id: ora2my_ods
                source: src_ora
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: my_ods
                      source: tgt_my
                      rename:
                        map:
                          ORDERS: ods_orders
                        case: lower
                        prefix: ods_
                      ddl: apply
                """;

        String canonical = writer.write(parser.parse(corpus("valid/s01-mirror-rename-ddl/ora2my_ods.tap.yml")));

        assertThat(canonical).isEqualTo(golden);
    }

    @Test
    void goldenC_reuseAssembly_s11() {
        // canonical-form.md §7 sample C: anonymous inline filter -> id filter_1; string step
        // mask_pii -> use-object; omitted from -> natural-order wiring; view/serve use-sugar;
        // use-local-id == use -> omitted.
        String golden = """
                version: tapstate/v1
                kind: pipeline
                id: crm_pack
                source: src_crm
                transforms:
                  - id: filter_1
                    type: filter
                    from: [customers]
                    expr: "op != 'd'"
                  - use: mask_pii
                    from: [filter_1]
                view:
                  use: v_cust
                  from: mask_pii
                serve:
                  use: std_api
                  from: v_cust
                """;

        String canonical = writer.write(parser.parse(corpus("valid/s11-reuse-assembly/crm_pack.tap.yml")));

        assertThat(canonical).isEqualTo(golden);
    }

    /**
     * Round-trip self-consistency over every valid corpus doc — all five kinds (source /
     * pipeline / transform / view / serve): canonical is a fixed point of parse∘write.
     * Exercises the whole parse surface across all §14 scenarios (crash-free, no §11.5 false
     * positives, sugar normalized stably) without per-scenario golden files — those land in
     * B5; this is the completeness driver for B3-3 (pipelines) + B3-4 (definition bodies).
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("allValidDocs")
    void canonicalIsAFixedPoint(Path doc) {
        String once = writer.write(parser.parse(read(doc)));
        String twice = writer.write(parser.parse(once));
        assertThat(twice).as("canonical must be a fixed point for %s", doc).isEqualTo(once);
    }

    static Stream<Path> allValidDocs() throws IOException {
        try (Stream<Path> walk = Files.walk(CORPUS.resolve("valid"))) {
            List<Path> docs = walk
                    .filter(p -> p.getFileName().toString().endsWith(".tap.yml"))
                    .sorted()
                    .toList();
            return docs.stream();
        }
    }

    @Test
    void mapFieldRules_allFourForms() {
        // $ rename, false drop, =CEL computed, bare literal — discrimination on the dialect-typed value
        String yaml = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src
                transforms:
                  - id: m
                    from: [t]
                    type: map
                    fields: { renamed: $orig, gone: false, calc: "=now()", lit: 5 }
                """;

        PipelineResource p = (PipelineResource) parser.parse(yaml);
        Map<String, FieldRule> fields = ((TransformBody.MapProjection) ((Step.Inline) p.transforms().get(0)).body()).fields();

        assertThat(fields.get("renamed")).isEqualTo(FieldRule.rename("orig"));
        assertThat(fields.get("gone")).isInstanceOf(FieldRule.Drop.class);
        assertThat(fields.get("calc")).isEqualTo(FieldRule.computed("now()"));
        assertThat(fields.get("lit")).isEqualTo(FieldRule.literal(5));
    }

    @Test
    void settings_parsedAndDefaultsOmittedOnReemit() {
        String yaml = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src
                serve:
                  from: /.*/
                  sync: [ { id: s, source: tgt } ]
                settings: { error_policy: skip, batch_size: 500, parallelism: 4, schedule: "0 2 * * *" }
                """;

        PipelineResource p = (PipelineResource) parser.parse(yaml);
        Settings s = p.settings();

        assertThat(s.batchSize()).isEqualTo(500);
        assertThat(s.parallelism()).isEqualTo(4);
        assertThat(s.schedule()).isEqualTo("0 2 * * *");
        assertThat(s.errorPolicy().yaml()).isEqualTo("skip");
        // non-default settings survive the canonical round-trip
        assertThat(writer.write(parser.parse(writer.write(p)))).isEqualTo(writer.write(p));
    }

    @Test
    void readAxisSettings_parsedAndSurviveRoundTrip() {
        // read amendment: read_mode / start_from are pipeline-level settings (parse-only, no validate).
        String yaml = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src
                serve:
                  from: /.*/
                  sync: [ { id: s, source: tgt } ]
                settings: { read_mode: cdc_only, start_from: earliest }
                """;

        PipelineResource p = (PipelineResource) parser.parse(yaml);
        Settings s = p.settings();

        assertThat(s.readMode()).isEqualTo(ReadMode.CDC_ONLY);
        assertThat(s.startFrom()).isEqualTo("earliest");
        assertThat(writer.write(parser.parse(writer.write(p)))).isEqualTo(writer.write(p));
    }

    @Test
    void serveFromAcceptsLegacyScalarAndExplicitMultipleReferences() {
        String single = """
                version: tapstate/v1
                kind: pipeline
                id: single
                source: db_src
                serve:
                  from: db_src.Player
                  sync: [ { id: sync, source: db_tgt } ]
                """;
        String multiple = """
                version: tapstate/v1
                kind: pipeline
                id: multiple
                source: db_src
                serve:
                  from: [db_src.Player, db_src.BB_0727]
                  sync: [ { id: sync, source: db_tgt } ]
                """;

        ServeBlock.Inline singleServe = (ServeBlock.Inline) ((PipelineResource) parser.parse(single)).serve();
        ServeBlock.Inline multipleServe = (ServeBlock.Inline) ((PipelineResource) parser.parse(multiple)).serve();

        assertThat(singleServe.from()).isEqualTo(FromClause.list(FromRef.literal("db_src.Player")));
        assertThat(multipleServe.from()).isEqualTo(FromClause.list(
                FromRef.literal("db_src.Player"), FromRef.literal("db_src.BB_0727")));
        assertThat(writer.write(parser.parse(single))).contains("from: db_src.Player");
        assertThat(writer.write(parser.parse(multiple))).contains(
                "from: [db_src.Player, db_src.BB_0727]");
    }

    @Test
    void regexSourceTable_stripsSlashes() {
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src
                connector: mysql
                mode: cdc
                tables: [ /.*/, orders ]
                """;

        SourceResource s = (SourceResource) parser.parse(yaml);

        assertThat(s.tables()).containsExactly(TableRef.regex(".*"), TableRef.literal("orders"));
    }

    private static String read(Path doc) {
        try {
            return Files.readString(doc);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String corpus(String relative) {
        return read(CORPUS.resolve(relative));
    }
}
