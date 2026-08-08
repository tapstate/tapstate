package io.tapstate.core.model.canonical;

import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ErrorPolicy;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SrsSchemaEvolution;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.ViewSchema;
import io.tapstate.core.model.WriteMode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Canonical-form serialization, locked by docs/reference/canonical-form.md (2026-06-12
 * user review). Expected strings below are normative golden text: changing any of them
 * means changing the canonical rules, which requires updating the document and a new
 * user review (poc1 plan R4).
 */
class CanonicalWriterTest {

    private final CanonicalWriter writer = new CanonicalWriter();

    @Nested
    class SourceResources {

        @Test
        void writesSourceWithFixedKeyOrderAndSortedFreeMaps() {
            // canonical-form.md sample A: key order per §3, config/options sorted per §6,
            // scalar-only sequences in flow style.
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("host", "10.20.0.15");
            config.put("port", 1521);
            config.put("service_name", "ORCL");
            config.put("username", "cdc_user");
            config.put("password", "Ora_2026");
            Map<String, Object> options = new LinkedHashMap<>();
            options.put("include_ddl", true);
            options.put("heartbeat_interval", "10s");

            SourceResource src = new SourceResource("src_ora", null, "oracle", config,
                    SourceMode.CDC,
                    List.of(TableRef.literal("ORDERS"), TableRef.literal("ORDER_ITEMS"),
                            TableRef.literal("CUSTOMERS")),
                    options, null, null);

            assertThat(writer.write(src)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: src_ora
                    connector: oracle
                    config:
                      host: 10.20.0.15
                      password: Ora_2026
                      port: 1521
                      service_name: ORCL
                      username: cdc_user
                    mode: cdc
                    tables: [ORDERS, ORDER_ITEMS, CUSTOMERS]
                    options:
                      heartbeat_interval: 10s
                      include_ddl: true
                    """);
        }

        @Test
        void omitsAbsentBlocksForPureConnectionSupplier() {
            // X18 dual-role: a pure connection supplier has no mode/tables/options/srs.
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("host", "10.30.0.5");
            config.put("username", "writer");
            config.put("password", "My_2026");

            SourceResource tgt = new SourceResource("tgt_my", null, "mysql", config,
                    null, null, null, null, null);

            assertThat(writer.write(tgt)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: tgt_my
                    connector: mysql
                    config:
                      host: 10.30.0.5
                      password: My_2026
                      username: writer
                    """);
        }

        @Test
        void writesSrsBlockInDeclaredKeyOrder() {
            // §3: srs key order = key, retention, schema_evolution, queryable, enabled.
            // enabled defaults true and is omitted here.
            SourceResource src = new SourceResource("src_ins", null, "oracle",
                    Map.of("host", "10.20.0.16"), SourceMode.CDC,
                    List.of(TableRef.literal("CUSTOMERS")),
                    null,
                    new Srs(null, "30d", SrsSchemaEvolution.TRACK, true, null), null);

            assertThat(writer.write(src)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: src_ins
                    connector: oracle
                    config:
                      host: 10.20.0.16
                    mode: cdc
                    tables: [CUSTOMERS]
                    srs:
                      retention: 30d
                      schema_evolution: track
                      queryable: true
                    """);
        }

        @Test
        void writesSrsEnabledFalseAndOmitsDefaultTrue() {
            // enabled: false is the SRS off switch (default true is omitted). It sits last, after queryable.
            SourceResource off = new SourceResource("src_lite", null, "mysql",
                    Map.of("host", "10.10.0.9"), SourceMode.CDC,
                    List.of(TableRef.literal("orders")),
                    null, new Srs(null, null, null, null, false), null);

            assertThat(writer.write(off)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: src_lite
                    connector: mysql
                    config:
                      host: 10.10.0.9
                    mode: cdc
                    tables: [orders]
                    srs:
                      enabled: false
                    """);
        }

        @Test
        void writesObjectTablesAsBlockSequenceWithPerTableConfig() {
            // §6: a sequence containing any mapping is block style; tables[] object
            // key order = name, filter, pk, options (§3).
            SourceResource src = new SourceResource("src_gh", null, "quickapi",
                    Map.of("base_url", "https://api.github.com", "token", "ghp_a1b2c3d4"),
                    SourceMode.API,
                    List.of(TableRef.spec("issues", null, List.of("id"), null),
                            TableRef.spec("pulls", null, List.of("id"), null)),
                    Map.of("poll_interval", "5m", "cursor", "updated_at"), null, null);

            assertThat(writer.write(src)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: src_gh
                    connector: quickapi
                    config:
                      base_url: https://api.github.com
                      token: ghp_a1b2c3d4
                    mode: api
                    tables:
                      - name: issues
                        pk: [id]
                      - name: pulls
                        pk: [id]
                    options:
                      cursor: updated_at
                      poll_interval: 5m
                    """);
        }

        @Test
        void quotesScalarsThatWouldChangeTypeOrValueWhenPlain() {
            // §6: plain wherever safe; double quotes only when a plain rendering would
            // re-resolve as another type (10.30 -> float, true -> bool, 1521 -> int).
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("host", "10.30");
            config.put("flag", "true");
            config.put("port_str", "1521");
            config.put("filter", "deleted == 0");

            SourceResource src = new SourceResource("tgt_x", null, "mysql", config,
                    null, null, null, null, null);

            assertThat(writer.write(src)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: tgt_x
                    connector: mysql
                    config:
                      filter: deleted == 0
                      flag: "true"
                      host: "10.30"
                      port_str: "1521"
                    """);
        }

        @Test
        void writesRegexTableRefsAndCelTableFilterQuoted() {
            // §4 of ADR-0016: /…/ regex form; tables[].filter is a CEL expression and
            // CEL fields are always double-quoted (§6).
            SourceResource src = new SourceResource("src_mix", null, "mysql",
                    Map.of("host", "10.0.0.1"), SourceMode.CDC,
                    List.of(TableRef.literal("orders"),
                            TableRef.spec("order_items", "deleted == 0", List.of("id"), null),
                            TableRef.regex("ORD_.*")),
                    null, null, null);

            assertThat(writer.write(src)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: src_mix
                    connector: mysql
                    config:
                      host: 10.0.0.1
                    mode: cdc
                    tables:
                      - orders
                      - name: order_items
                        filter: "deleted == 0"
                        pk: [id]
                      - /ORD_.*/
                    """);
        }

        @Test
        void writesMetadataLabelsSortedAndExperimentalLast() {
            // §3: metadata sits after id; experimental is always the last key (§11.6).
            Map<String, String> labels = new LinkedHashMap<>();
            labels.put("team", "data");
            labels.put("env", "prod");

            SourceResource src = new SourceResource("src_m", new Metadata(labels, "demo"),
                    "mysql", Map.of("host", "10.0.0.1"), null, null, null, null,
                    Map.of("wasm_runtime", true));

            assertThat(writer.write(src)).isEqualTo("""
                    version: tapstate/v1
                    kind: source
                    id: src_m
                    metadata:
                      labels:
                        env: prod
                        team: data
                      description: demo
                    connector: mysql
                    config:
                      host: 10.0.0.1
                    experimental:
                      wasm_runtime: true
                    """);
        }
    }

    @Nested
    class PipelineResources {

        @Test
        void omitsConstantDefaultsInSyncElements() {
            // canonical-form.md sample B: write_mode upsert and auto_create_table true are
            // documented constant defaults (§4) — dropped; ddl apply is non-default — kept.
            PipelineResource p = new PipelineResource("ora2my_ods", null, List.of("src_ora"),
                    null, null,
                    new ServeBlock.Inline(null, FromRef.regex(".*"),
                            List.of(new SyncElement("my_ods", "tgt_my", WriteMode.UPSERT,
                                    new RenameSpec(Map.of("ORDERS", "ods_orders"),
                                            RenameCase.LOWER, "ods_", null),
                                    DdlPolicy.APPLY, Map.of("auto_create_table", true))),
                            null, null),
                    null, null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: ora2my_ods
                    source: src_ora
                    serve:
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
                    """);
        }

        @Test
        void expandsUseSugarAndOmitsUseEqualLocalIds() {
            // canonical-form.md sample C: string sugar becomes use: objects, from is always
            // explicit (auto-generated step id filter_1), id == use is omitted (§5).
            PipelineResource p = new PipelineResource("crm_pack", null, List.of("src_crm"),
                    List.of(Step.inline("filter_1", FromClause.list(FromRef.literal("customers")),
                                    new TransformBody.Filter("op != 'd'"), null, null),
                            Step.use(null, "mask_pii",
                                    FromClause.list(FromRef.literal("filter_1")), null)),
                    new ViewBlock.Use(null, "v_cust", FromRef.literal("mask_pii")),
                    new ServeBlock.Use(null, "std_api", FromRef.literal("v_cust")),
                    null, null);

            assertThat(writer.write(p)).isEqualTo("""
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
                    """);
        }

        @Test
        void writesMapAndNestFullTreeWithSortedAliasMaps() {
            // ADR-0016 §14.2: map projection keeps declared field order (§6); nest/join
            // alias maps and on maps sort lexicographically; full-tree embed key order per §3.
            LinkedHashMap<String, FieldRule> fields = new LinkedHashMap<>();
            fields.put("customer_id", FieldRule.rename("CUST_ID"));
            fields.put("name", FieldRule.rename("CUST_NAME"));
            fields.put("segment", FieldRule.rename("SEG_CODE"));

            PipelineResource p = new PipelineResource("customer_360", null, List.of("src_ins"),
                    List.of(Step.inline("clean", FromClause.list(FromRef.literal("CUSTOMERS")),
                                    new TransformBody.MapProjection(fields), null, null),
                            Step.inline("c360",
                                    FromClause.aliases(Map.of(
                                            "customer", FromRef.literal("clean"),
                                            "policy", FromRef.literal("POLICIES"),
                                            "claim", FromRef.literal("CLAIMS"))),
                                    new TransformBody.Nest(null, null,
                                            new NestRoot("customer", List.of("customer_id"), null, null,
                                                    List.of(new Embed("policy",
                                                            Map.of("CUST_ID", "customer_id"),
                                                            EmbedAs.ARRAY, "policies",
                                                            List.of("POLICY_ID"), null, null,
                                                            List.of(new Embed("claim",
                                                                    Map.of("POLICY_ID", "POLICY_ID"),
                                                                    EmbedAs.ARRAY, "claims",
                                                                    List.of("CLAIM_ID"), null, null,
                                                                    null)))))),
                                    null, null)),
                    new ViewBlock.Inline("customer_360", FromRef.literal("c360"), "customer_id",
                            new Storage(new Storage.Hot("1h"),
                                    new Storage.Warm("customer_360", List.of("customer_id")), null),
                            null),
                    new ServeBlock.Inline(null, FromRef.literal("customer_360"), null,
                            List.of(new QueryElement(QueryType.REST, null)), null),
                    null, null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: customer_360
                    source: src_ins
                    transforms:
                      - id: clean
                        type: map
                        from: [CUSTOMERS]
                        fields:
                          customer_id: $CUST_ID
                          name: $CUST_NAME
                          segment: $SEG_CODE
                      - id: c360
                        type: nest
                        from:
                          claim: CLAIMS
                          customer: clean
                          policy: POLICIES
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
                              embed:
                                - from: claim
                                  on:
                                    POLICY_ID: POLICY_ID
                                  as: array
                                  path: claims
                                  arrayKey: [CLAIM_ID]
                    view:
                      id: customer_360
                      from: c360
                      primary_key: customer_id
                      storage:
                        hot:
                          ttl: 1h
                        warm:
                          collection: customer_360
                          indexes: [customer_id]
                    serve:
                      from: customer_360
                      query:
                        - type: rest
                    """);
        }

        @Test
        void writesMultiSourceListAndJoinSqlAsLiteralBlock() {
            // ADR-0016 §14.8: multi-source = flow list (X13); join sql is user content,
            // emitted as a literal block with value-driven chomping (§6).
            PipelineResource p = new PipelineResource("cust_stats", null,
                    List.of("src_crm", "src_erp"),
                    List.of(Step.inline("cust_orders",
                            FromClause.aliases(Map.of(
                                    "c", FromRef.literal("customers"),
                                    "o", FromRef.literal("orders"))),
                            new TransformBody.Join("duckdb",
                                    "SELECT c.id AS customer_id, count(*) AS order_cnt, sum(o.amount) AS total\n"
                                            + "FROM c JOIN o ON o.customer_id = c.id GROUP BY c.id\n"),
                            null, null)),
                    new ViewBlock.Inline("cust_stats", FromRef.literal("cust_orders"),
                            "customer_id",
                            new Storage(null, new Storage.Warm("cust_stats", null), null), null),
                    null, null, null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: cust_stats
                    source: [src_crm, src_erp]
                    transforms:
                      - id: cust_orders
                        type: join
                        from:
                          c: customers
                          o: orders
                        engine: duckdb
                        sql: |
                          SELECT c.id AS customer_id, count(*) AS order_cnt, sum(o.amount) AS total
                          FROM c JOIN o ON o.customer_id = c.id GROUP BY c.id
                    view:
                      id: cust_stats
                      from: cust_orders
                      primary_key: customer_id
                      storage:
                        warm:
                          collection: cust_stats
                    """);
        }

        @Test
        void writesJsScriptAsLiteralBlockAndKeepsNonDefaultWriteMode() {
            // ADR-0016 §14.4: js escape hatch; append is non-default so it stays.
            PipelineResource p = new PipelineResource("kfk2my", null, List.of("src_kfk"),
                    List.of(Step.inline("parse", FromClause.list(FromRef.literal("orders_topic")),
                            new TransformBody.Js(
                                    "function process(record, ctx) { record.after = JSON.parse(record.after.value); return record; }\n"),
                            null, null)),
                    null,
                    new ServeBlock.Inline(null, FromRef.literal("parse"),
                            List.of(new SyncElement("my", "tgt_my", WriteMode.APPEND, null, null,
                                    null)),
                            null, null),
                    null, null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: kfk2my
                    source: src_kfk
                    transforms:
                      - id: parse
                        type: js
                        from: [orders_topic]
                        script: |
                          function process(record, ctx) { record.after = JSON.parse(record.after.value); return record; }
                    serve:
                      from: parse
                      sync:
                        - id: my
                          source: tgt_my
                          write_mode: append
                    """);
        }

        @Test
        void writesPushElementsWithCelFormatQuoted() {
            // ADR-0016 §14.5 + X11: push element key order id, source, topic, format,
            // options; CEL format is always double-quoted with the = marker.
            PipelineResource p = new PipelineResource("my2kfk", null, List.of("src_my"),
                    null, null,
                    new ServeBlock.Inline(null, FromRef.literal("orders"), null, null,
                            List.of(new PushElement(null, "tgt_kfk", "orders_events", null, null),
                                    new PushElement(null, "tgt_hook", null,
                                            PushFormat.cel("after"), null))),
                    null, null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: my2kfk
                    source: src_my
                    serve:
                      from: orders
                      push:
                        - source: tgt_kfk
                          topic: orders_events
                        - source: tgt_hook
                          format: "=after"
                    """);
        }

        @Test
        void omitsSettingsBlockWhenAllFieldsAreDefaults() {
            // §4: error_policy fail / batch_size 1000 / parallelism 1 are constant
            // defaults; a settings block reduced to nothing disappears.
            PipelineResource p = new PipelineResource("p_min", null, List.of("src_a"),
                    null, null,
                    new ServeBlock.Inline(null, FromRef.regex(".*"),
                            List.of(new SyncElement(null, "tgt_b", null, null, null, null)),
                            null, null),
                    new Settings(ErrorPolicy.FAIL, 1000, 1, null, ReadMode.SNAPSHOT_AND_CDC, "latest"), null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: p_min
                    source: src_a
                    serve:
                      from: /.*/
                      sync:
                        - source: tgt_b
                    """);
        }

        @Test
        void keepsOnlyNonDefaultSettingsFields() {
            PipelineResource p = new PipelineResource("p_set", null, List.of("src_a"),
                    null, null,
                    new ServeBlock.Inline(null, FromRef.regex(".*"),
                            List.of(new SyncElement(null, "tgt_b", null, null, null, null)),
                            null, null),
                    new Settings(ErrorPolicy.DEAD_LETTER, 1000, 4, "0 2 * * *", null, null), null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: p_set
                    source: src_a
                    serve:
                      from: /.*/
                      sync:
                        - source: tgt_b
                    settings:
                      error_policy: dead_letter
                      parallelism: 4
                      schedule: 0 2 * * *
                    """);
        }

        @Test
        void writesReadAxisAfterCrossCuttingFieldsAndOmitsDefaults() {
            // read axis renders after schedule; read_mode: snapshot_and_cdc and start_from: latest
            // are the defaults and drop out — only the non-default read_mode / start_from survive.
            PipelineResource p = new PipelineResource("p_read", null, List.of("src_a"),
                    null, null,
                    new ServeBlock.Inline(null, FromRef.regex(".*"),
                            List.of(new SyncElement(null, "tgt_b", null, null, null, null)),
                            null, null),
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: p_read
                    source: src_a
                    serve:
                      from: /.*/
                      sync:
                        - source: tgt_b
                    settings:
                      read_mode: cdc_only
                      start_from: earliest
                    """);
        }

        @Test
        void writesStepOptionsAfterBodyAndExperimentalLast() {
            // §3: step key order id, type, from, <body>, options, experimental.
            PipelineResource p = new PipelineResource("p_opt", null, List.of("src_a"),
                    List.of(Step.inline("flt", FromClause.list(FromRef.literal("orders")),
                            new TransformBody.Filter("op != 'd'"),
                            Map.of("error_policy", "dead_letter", "parallelism", 4),
                            Map.of("vectorized", true))),
                    null,
                    new ServeBlock.Inline(null, FromRef.literal("flt"),
                            List.of(new SyncElement(null, "tgt_b", null, null, null, null)),
                            null, null),
                    null, null);

            assertThat(writer.write(p)).isEqualTo("""
                    version: tapstate/v1
                    kind: pipeline
                    id: p_opt
                    source: src_a
                    transforms:
                      - id: flt
                        type: filter
                        from: [orders]
                        expr: "op != 'd'"
                        options:
                          error_policy: dead_letter
                          parallelism: 4
                        experimental:
                          vectorized: true
                    serve:
                      from: flt
                      sync:
                        - source: tgt_b
                    """);
        }
    }

    @Nested
    class DefinitionBodies {

        @Test
        void writesTransformDefinitionWithoutFrom() {
            // ADR-0016 §14.11 / X19: definition body = pure logic, from is forbidden;
            // drop rule renders as boolean false.
            TransformResource t = new TransformResource("mask_pii", null,
                    new TransformBody.MapProjection(orderedFields()), null, null);

            assertThat(writer.write(t)).isEqualTo("""
                    version: tapstate/v1
                    kind: transform
                    id: mask_pii
                    type: map
                    fields:
                      ssn: false
                      phone: false
                    """);
        }

        private LinkedHashMap<String, FieldRule> orderedFields() {
            LinkedHashMap<String, FieldRule> fields = new LinkedHashMap<>();
            fields.put("ssn", FieldRule.drop());
            fields.put("phone", FieldRule.drop());
            return fields;
        }

        @Test
        void writesViewDefinitionBody() {
            ViewResource v = new ViewResource("v_cust", null, "customer_id",
                    new Storage(null, new Storage.Warm("cust", null), null),
                    new ViewSchema(true, "additive"), null);

            assertThat(writer.write(v)).isEqualTo("""
                    version: tapstate/v1
                    kind: view
                    id: v_cust
                    primary_key: customer_id
                    storage:
                      warm:
                        collection: cust
                    schema:
                      enforce: true
                      evolution: additive
                    """);
        }

        @Test
        void writesServeDefinitionBody() {
            ServeResource s = new ServeResource("std_api", null, null,
                    List.of(new QueryElement(QueryType.REST, null),
                            new QueryElement(QueryType.MCP, null)),
                    null, null);

            assertThat(writer.write(s)).isEqualTo("""
                    version: tapstate/v1
                    kind: serve
                    id: std_api
                    query:
                      - type: rest
                      - type: mcp
                    """);
        }
    }
}
