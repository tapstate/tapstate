package io.tapstate.core.dsl;

import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * B3-4: parse the three reusable definition kinds — {@code kind: transform / view / serve}
 * (ADR-0016 §5/§7/§8, X19). A definition body is pure logic; {@code from:} is forbidden
 * (wiring belongs to the referencing pipeline step). The bodies reuse the same payload
 * grammar as inline pipeline blocks, so the assertions here mirror the inline ones minus the
 * {@code from:} wiring. Canonical key order for definitions is canonical-form.md §3 (rows
 * "kind: transform/view/serve definition body").
 */
class DslDefinitionParserTest {

    private static final Path CORPUS = Path.of("src", "test", "resources", "corpus");

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    @Test
    void parsesTransformDefinition_mapBody_noWiring() {
        TransformResource t = (TransformResource) parser.parse(
                corpus("valid/s11-reuse-assembly/mask_pii.tap.yml"));

        assertThat(t.id()).isEqualTo("mask_pii");
        assertThat(t.kind()).isEqualTo("transform");
        Map<String, FieldRule> fields = ((TransformBody.MapProjection) t.body()).fields();
        assertThat(fields.get("ssn")).isInstanceOf(FieldRule.Drop.class);
        assertThat(fields.get("phone")).isInstanceOf(FieldRule.Drop.class);
    }

    @Test
    void transformDefinitionCanonicalForm() {
        String golden = """
                version: tapstate/v1
                kind: transform
                id: mask_pii
                type: map
                fields:
                  ssn: false
                  phone: false
                """;

        String canonical = writer.write(parser.parse(corpus("valid/s11-reuse-assembly/mask_pii.tap.yml")));

        assertThat(canonical).isEqualTo(golden);
    }

    @Test
    void parsesViewDefinition_mdmSink() {
        ViewResource v = (ViewResource) parser.parse(
                corpus("valid/s11-reuse-assembly/v_cust.tap.yml"));

        assertThat(v.id()).isEqualTo("v_cust");
        assertThat(v.kind()).isEqualTo("view");
        assertThat(v.primaryKey()).isEqualTo("customer_id");
        assertThat(v.storage().warm().collection()).isEqualTo("cust");
    }

    @Test
    void viewDefinitionCanonicalForm() {
        String golden = """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: customer_id
                storage:
                  warm:
                    collection: cust
                """;

        String canonical = writer.write(parser.parse(corpus("valid/s11-reuse-assembly/v_cust.tap.yml")));

        assertThat(canonical).isEqualTo(golden);
    }

    @Test
    void parsesServeDefinition_publishSurface() {
        ServeResource s = (ServeResource) parser.parse(
                corpus("valid/s11-reuse-assembly/std_api.tap.yml"));

        assertThat(s.id()).isEqualTo("std_api");
        assertThat(s.kind()).isEqualTo("serve");
        assertThat(s.query()).hasSize(2);
        assertThat(s.query().get(0).type().yaml()).isEqualTo("rest");
        assertThat(s.query().get(1).type().yaml()).isEqualTo("mcp");
    }

    @Test
    void serveDefinitionCanonicalForm() {
        String golden = """
                version: tapstate/v1
                kind: serve
                id: std_api
                query:
                  - type: rest
                  - type: mcp
                """;

        String canonical = writer.write(parser.parse(corpus("valid/s11-reuse-assembly/std_api.tap.yml")));

        assertThat(canonical).isEqualTo(golden);
    }

    @Test
    void transformDefinitionCarriesMetadataAndOptions() {
        // The s11 corpus definitions are bare; this pins the metadata/options/body wiring that
        // transformDefinition() reads but no corpus doc exercises (canonical-form.md §3 row 55).
        String yaml = """
                version: tapstate/v1
                kind: transform
                id: tagged_filter
                metadata:
                  labels: { team: data, tier: gold }
                  description: drop deleted rows
                type: filter
                expr: "op != 'd'"
                options: { window: 5m }
                """;

        TransformResource t = (TransformResource) parser.parse(yaml);

        assertThat(t.metadata().description()).isEqualTo("drop deleted rows");
        assertThat(t.metadata().labels()).containsEntry("team", "data").containsEntry("tier", "gold");
        assertThat(t.options()).containsEntry("window", "5m");
        assertThat(((TransformBody.Filter) t.body()).expr()).isEqualTo("op != 'd'");
        // metadata + options survive the canonical round-trip (fixed point)
        assertThat(writer.write(parser.parse(writer.write(t)))).isEqualTo(writer.write(t));
    }

    @Test
    void viewDefinitionCarriesSchema() {
        // Pins viewDefinition()'s schema/storage wiring beyond the bare s11 v_cust doc.
        String yaml = """
                version: tapstate/v1
                kind: view
                id: enforced_view
                primary_key: id
                schema: { enforce: true, evolution: additive }
                """;

        ViewResource v = (ViewResource) parser.parse(yaml);

        assertThat(v.schema().enforce()).isTrue();
        assertThat(v.schema().evolution()).isEqualTo("additive");
        assertThat(writer.write(parser.parse(writer.write(v)))).isEqualTo(writer.write(v));
    }

    @Test
    void parsesNestDefinition_abstractAliasesAccepted() {
        // A standalone nest body names stream aliases (root.from / embed.from) that are bound only at
        // the use site; outside a pipeline they are abstract and resolve against nothing. The closure
        // resolves a nest step's alias-map wiring, not these internal references, so a definition body
        // carrying unbound aliases is accepted (X19).
        String yaml = """
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
                """;

        TransformResource t = (TransformResource) parser.parse(yaml);

        assertThat(t.id()).isEqualTo("c360_shape");
        TransformBody.Nest nest = (TransformBody.Nest) t.body();
        assertThat(nest.root().from()).isEqualTo("customer");
        assertThat(nest.root().key()).containsExactly("customer_id");
        assertThat(nest.root().embed()).hasSize(1);
        assertThat(nest.root().embed().get(0).from()).isEqualTo("policy");
        assertThat(nest.root().embed().get(0).path()).isEqualTo("policies");
        // the child-to-parent join map and the array key — where the abstract aliases actually bind
        assertThat(nest.root().embed().get(0).on()).containsEntry("CUST_ID", "customer_id");
        assertThat(nest.root().embed().get(0).arrayKey()).containsExactly("POLICY_ID");
        // the definition body survives the canonical round-trip (fixed point)
        assertThat(writer.write(parser.parse(writer.write(t)))).isEqualTo(writer.write(t));
    }

    @Test
    void nestTracksStructuralKeyChangesAtTheRootAndAtEachEmbed() {
        // One switch per declaring level covers every structural key that level owns — the column
        // giving an element its identity in the array, the column saying which parent it hangs
        // under, and the column its own children point at. The root carries its own switch because
        // the root key is the identity the whole document is filed under and nothing above the root
        // declares it.
        String yaml = """
                version: tapstate/v1
                kind: transform
                id: c360_shape
                type: nest
                root:
                  from: customer
                  key: [customer_id]
                  trackKeyChanges: true
                  embed:
                    - from: policy
                      on:
                        CUST_ID: customer_id
                      as: array
                      path: policies
                      arrayKey: [POLICY_ID]
                      trackKeyChanges: true
                """;

        TransformResource t = (TransformResource) parser.parse(yaml);

        TransformBody.Nest nest = (TransformBody.Nest) t.body();
        assertThat(nest.root().trackKeyChanges()).isTrue();
        assertThat(nest.root().embed().get(0).trackKeyChanges()).isTrue();
        // Re-parsing the canonical output is what discriminates the writer: a writer that drops
        // either switch still yields a stable fixed point, so comparing text to text would pass.
        TransformBody.Nest again = (TransformBody.Nest) ((TransformResource) parser.parse(writer.write(t))).body();
        assertThat(again.root().trackKeyChanges()).isTrue();
        assertThat(again.root().embed().get(0).trackKeyChanges()).isTrue();
    }

    @Test
    void parsesJoinDefinition_multilineSqlAndAbstractTables() {
        // A standalone join body's SQL names table aliases (c / o) bound at the use site; the definition
        // is pure logic with no from: wiring (X19).
        String yaml = """
                version: tapstate/v1
                kind: transform
                id: cust_wide
                type: join
                engine: duckdb
                sql: |
                  SELECT c.id AS customer_id, count(*) AS order_cnt
                  FROM c JOIN o ON o.customer_id = c.id GROUP BY c.id
                """;

        TransformResource t = (TransformResource) parser.parse(yaml);

        TransformBody.Join join = (TransformBody.Join) t.body();
        assertThat(join.engine()).isEqualTo("duckdb");
        assertThat(join.sql()).contains("SELECT c.id").contains("GROUP BY c.id");
        assertThat(writer.write(parser.parse(writer.write(t)))).isEqualTo(writer.write(t));
    }

    @Test
    void parsesUnionDefinition_emptyBody() {
        // A standalone union body carries no fields; its inputs are merged at the use site.
        String yaml = """
                version: tapstate/v1
                kind: transform
                id: merged
                type: union
                """;

        TransformResource t = (TransformResource) parser.parse(yaml);

        assertThat(t.body()).isInstanceOf(TransformBody.Union.class);
        assertThat(writer.write(t)).isEqualTo("""
                version: tapstate/v1
                kind: transform
                id: merged
                type: union
                """);
    }

    @Test
    void definitionBodyRejectsFrom() {
        // corpus invalid/s11: from: on a kind: transform definition body. Outside a pipeline
        // there is no table universe and no step namespace, so this wiring is forbidden (X19).
        Throwable thrown = catchThrowable(() -> parser.parse(
                corpus("invalid/s11-definition-body-with-from/mask_pii_wired.tap.yml")));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.FORBIDDEN_FIELD);
        assertThat(ex.path()).isEqualTo("from");
    }

    private static String corpus(String relative) {
        try {
            return Files.readString(CORPUS.resolve(relative));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
