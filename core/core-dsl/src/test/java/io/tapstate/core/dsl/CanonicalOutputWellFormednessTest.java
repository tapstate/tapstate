package io.tapstate.core.dsl;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-check: the hand-rolled canonical emitter (core-model) must produce text that a
 * real YAML parser reads back losslessly under the Tapstate dialect — YAML 1.2 core schema
 * semantics, i.e. only true/false are booleans, yes/no/on/off are plain strings
 * (canonical-form.md §2, review decision 2026-06-12). It reads back through the same
 * production {@link TapstateDialectResolver} the B3 parse layer uses, so the writer is
 * verified against the actual parser dialect rather than a copy of it.
 */
class CanonicalOutputWellFormednessTest {

    private final CanonicalWriter writer = new CanonicalWriter();

    private Map<String, Object> parse(String yamlText) {
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()),
                new org.yaml.snakeyaml.representer.Representer(new org.yaml.snakeyaml.DumperOptions()),
                new org.yaml.snakeyaml.DumperOptions(), new LoaderOptions(),
                new TapstateDialectResolver());
        return yaml.load(yamlText);
    }

    @Test
    @SuppressWarnings("unchecked")
    void nestPipelineOutputParsesBackLosslessly() {
        // The gnarliest shape: alias maps, full nest tree with the `on` key (a YAML 1.1
        // boolean literal!), quoted expression, flow sequences.
        PipelineResource p = new PipelineResource("customer_360", null, List.of("src_ins"),
                List.of(Step.inline("clean", FromClause.list(FromRef.literal("CUSTOMERS")),
                                new TransformBody.MapProjection(fields()), null, null),
                        Step.inline("c360",
                                FromClause.aliases(Map.of(
                                        "customer", FromRef.literal("clean"),
                                        "policy", FromRef.literal("POLICIES"))),
                                new TransformBody.Nest(null, null,
                                        new NestRoot("customer", List.of("customer_id"), null, null,
                                                List.of(new Embed("policy",
                                                        Map.of("CUST_ID", "customer_id"),
                                                        EmbedAs.ARRAY, "policies",
                                                        List.of("POLICY_ID"), null, null, null)))),
                                null, null)),
                new ViewBlock.Inline("customer_360", FromRef.literal("c360"), "customer_id",
                        new Storage(new Storage.Hot("1h"), null, null), null),
                new ServeBlock.Inline(null, FromRef.literal("customer_360"), null,
                        List.of(new QueryElement(QueryType.REST, null)), null),
                null, null);

        Map<String, Object> doc = parse(writer.write(p));

        assertThat(doc.get("version")).isEqualTo("tapstate/v1");
        assertThat(doc.get("kind")).isEqualTo("pipeline");
        List<Map<String, Object>> transforms = (List<Map<String, Object>>) doc.get("transforms");
        Map<String, Object> mapStep = transforms.get(0);
        assertThat((Map<String, Object>) mapStep.get("fields"))
                .containsEntry("customer_id", "$CUST_ID")
                .containsEntry("active", Boolean.FALSE)
                .containsEntry("ingested_at", "=now()");
        Map<String, Object> nest = transforms.get(1);
        Map<String, Object> root = (Map<String, Object>) nest.get("root");
        Map<String, Object> embed = ((List<Map<String, Object>>) root.get("embed")).get(0);
        // dialect check: `on` stays a string key, never Boolean.TRUE
        assertThat(embed).containsKey("on");
        assertThat((Map<String, Object>) embed.get("on"))
                .containsEntry("CUST_ID", "customer_id");
    }

    private LinkedHashMap<String, FieldRule> fields() {
        LinkedHashMap<String, FieldRule> fields = new LinkedHashMap<>();
        fields.put("customer_id", FieldRule.rename("CUST_ID"));
        fields.put("active", FieldRule.drop());
        fields.put("ingested_at", FieldRule.computed("now()"));
        return fields;
    }

    @Test
    @SuppressWarnings("unchecked")
    void quotedTypePreservingScalarsSurviveTheRoundTrip() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", "10.30");
        config.put("flag", "true");
        config.put("port", 1521);
        config.put("enabled", true);
        config.put("answer", "yes");

        SourceResource src = new SourceResource("tgt_x", null, "mysql", config,
                SourceMode.CDC, List.of(TableRef.regex(".*")), null, null, null);

        Map<String, Object> doc = parse(writer.write(src));

        Map<String, Object> parsed = (Map<String, Object>) doc.get("config");
        assertThat(parsed.get("host")).isEqualTo("10.30");
        assertThat(parsed.get("flag")).isEqualTo("true");
        assertThat(parsed.get("port")).isEqualTo(1521);
        assertThat(parsed.get("enabled")).isEqualTo(Boolean.TRUE);
        // dialect: yes is a plain string, not a boolean — emitted unquoted, read back as string
        assertThat(parsed.get("answer")).isEqualTo("yes");
        assertThat(doc.get("tables")).isEqualTo(List.of("/.*/"));
    }
}
