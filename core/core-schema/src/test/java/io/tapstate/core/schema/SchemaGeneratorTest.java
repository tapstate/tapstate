package io.tapstate.core.schema;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaGeneratorTest {

    private final SchemaGenerator generator = new SchemaGenerator();

    @Test
    void declaresDraft2020DialectAndTapstateV1Id() {
        String schema = generator.generate();
        assertThat(schema)
                .contains("\"$schema\": \"https://json-schema.org/draft/2020-12/schema\"")
                .contains("\"$id\": \"https://tapstate.io/schema/tapstate/v1.json\"");
    }

    @Test
    void topLevelConstrainsADocumentToTheResourceUnion() {
        Json.Obj tree = generator.generateTree();
        assertThat(tree.get("$ref")).isEqualTo(new Json.Str("#/$defs/Resource"));

        Json.Obj defs = (Json.Obj) tree.get("$defs");
        Json.Obj resource = (Json.Obj) defs.get("Resource");
        Json.Arr oneOf = (Json.Arr) resource.get("oneOf");
        assertThat(oneOf.items()).containsExactly(
                Json.ref("SourceResource"), Json.ref("PipelineResource"),
                Json.ref("TransformResource"), Json.ref("ViewResource"), Json.ref("ServeResource"));
    }

    @Test
    void recordPropertiesCarryTypeAndDocDescription() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");
        Json.Obj metadata = (Json.Obj) defs.get("Metadata");
        assertThat(metadata.get("type")).isEqualTo(new Json.Str("object"));

        Json.Obj props = (Json.Obj) metadata.get("properties");
        Json.Obj description = (Json.Obj) props.get("description");
        assertThat(description.get("type")).isEqualTo(new Json.Str("string"));
        assertThat(description.get("description"))
                .isEqualTo(new Json.Str("Free-text description of the resource; never identity."));
    }

    @Test
    void scalarFormTypesAreInlinedNotEmittedAsObjects() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");

        // FromRef: both subtypes are the string form, so the union collapses to a single string.
        Json.Obj fromRef = (Json.Obj) defs.get("FromRef");
        assertThat(fromRef.get("type")).isEqualTo(new Json.Str("string"));
        assertThat(fromRef.get("oneOf")).isNull();

        // Scalar subtypes are inlined — they get no $def of their own.
        assertThat(defs.get("FromRef.Literal")).isNull();
        assertThat(defs.get("TableRef.Literal")).isNull();
        assertThat(defs.get("TableRef.Regex")).isNull();

        // TableRef: a bare string (literal or regex) OR the Spec object.
        Json.Obj tableRef = (Json.Obj) defs.get("TableRef");
        Json.Arr oneOf = (Json.Arr) tableRef.get("oneOf");
        assertThat(oneOf.items()).containsExactly(
                new Json.Obj(List.of(new Json.Entry("type", new Json.Str("string")))),
                Json.ref("TableRef.Spec"));
        assertThat(defs.get("TableRef.Spec")).isNotNull();
    }

    @Test
    void fieldRuleCollapsesToPermissiveAndPushFormatIsStringOrObject() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");

        // FieldRule has an "any literal" form, so the value is unconstrained (its forms live in
        // the description, not in a oneOf that nothing could satisfy uniquely).
        Json.Obj fieldRule = (Json.Obj) defs.get("FieldRule");
        assertThat(fieldRule.get("type")).isNull();
        assertThat(fieldRule.get("oneOf")).isNull();
        assertThat(defs.get("FieldRule.Rename")).isNull();
        assertThat(defs.get("FieldRule.Drop")).isNull();
        assertThat(defs.get("FieldRule.Literal")).isNull();

        // PushFormat: a "=CEL" string OR a bare field-rule map (Fields unwraps to its map).
        Json.Obj pushFormat = (Json.Obj) defs.get("PushFormat");
        Json.Arr oneOf = (Json.Arr) pushFormat.get("oneOf");
        assertThat(oneOf.items()).containsExactly(
                new Json.Obj(List.of(new Json.Entry("type", new Json.Str("string")))),
                new Json.Obj(List.of(
                        new Json.Entry("type", new Json.Str("object")),
                        new Json.Entry("additionalProperties", Json.ref("FieldRule")))));
        assertThat(defs.get("PushFormat.Fields")).isNull();
    }

    @Test
    void resourceObjectsCarryVersionAndKindConstants() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");
        Json.Obj props = (Json.Obj) ((Json.Obj) defs.get("SourceResource")).get("properties");
        assertThat(((Json.Obj) props.get("version")).get("const")).isEqualTo(new Json.Str("tapstate/v1"));
        assertThat(((Json.Obj) props.get("kind")).get("const")).isEqualTo(new Json.Str("source"));
    }

    @Test
    void objectsAreClosedAndListRequiredProperties() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");
        Json.Obj source = (Json.Obj) defs.get("SourceResource");
        assertThat(source.get("additionalProperties")).isEqualTo(new Json.Bool(false));
        Json.Arr required = (Json.Arr) source.get("required");
        assertThat(required.items()).containsExactly(
                new Json.Str("version"), new Json.Str("kind"),
                new Json.Str("id"), new Json.Str("connector"));
    }

    @Test
    void enumsCarryTypeAndPerValueDescriptions() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");
        Json.Obj mode = (Json.Obj) defs.get("SourceMode");
        assertThat(mode.get("type")).isEqualTo(new Json.Str("string"));
        assertThat(mode.get("description")).isInstanceOf(Json.Str.class);

        Json.Arr oneOf = (Json.Arr) mode.get("oneOf");
        assertThat(oneOf.items().getFirst()).isEqualTo(new Json.Obj(List.of(
                new Json.Entry("const", new Json.Str("cdc")),
                new Json.Entry("description",
                        new Json.Str("Change data capture — an unbounded stream of inserts, updates and deletes.")))));
    }

    @Test
    void fromClauseIsAListOrAnAliasMapOfReferences() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");

        Json.Obj fromClause = (Json.Obj) defs.get("FromClause");
        Json.Arr oneOf = (Json.Arr) fromClause.get("oneOf");
        assertThat(oneOf.items()).containsExactly(
                new Json.Obj(List.of(
                        new Json.Entry("type", new Json.Str("array")),
                        new Json.Entry("items", Json.ref("FromRef")))),
                new Json.Obj(List.of(
                        new Json.Entry("type", new Json.Str("object")),
                        new Json.Entry("additionalProperties", Json.ref("FromRef")))));
        assertThat(defs.get("FromClause.Flow")).isNull();
        assertThat(defs.get("FromClause.Aliases")).isNull();

        // FromRef itself collapses to a bare string.
        assertThat(((Json.Obj) defs.get("FromRef")).get("type")).isEqualTo(new Json.Str("string"));
    }

    @Test
    void transformBodyIsFlattenedWithATypeDiscriminator() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");

        // Step.Inline composes the body variants in via allOf, staying open to their fields.
        Json.Obj inline = (Json.Obj) defs.get("Step.Inline");
        assertThat(inline.get("unevaluatedProperties")).isEqualTo(new Json.Bool(false));
        assertThat(inline.get("additionalProperties")).isNull();
        assertThat(((Json.Arr) inline.get("allOf")).items()).containsExactly(Json.ref("TransformBody"));
        Json.Obj inlineProps = (Json.Obj) inline.get("properties");
        assertThat(inlineProps.get("body")).isNull();   // flattened, not a nested property
        assertThat(inlineProps.get("from")).isNotNull();

        // Each variant is a partial: a required `type` const plus its fields, left open.
        Json.Obj js = (Json.Obj) defs.get("TransformBody.Js");
        Json.Obj jsProps = (Json.Obj) js.get("properties");
        assertThat(((Json.Obj) jsProps.get("type")).get("const")).isEqualTo(new Json.Str("js"));
        assertThat(jsProps.get("script")).isNotNull();
        assertThat(js.get("additionalProperties")).isNull();
        assertThat(((Json.Arr) js.get("required")).items()).contains(new Json.Str("type"));
    }

    @Test
    void documentedDefaultsAreEmittedWithTheRightJsonType() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");
        Json.Obj settings = (Json.Obj) ((Json.Obj) defs.get("Settings")).get("properties");
        assertThat(((Json.Obj) settings.get("batch_size")).get("default")).isEqualTo(new Json.Num("1000"));
        assertThat(((Json.Obj) settings.get("error_policy")).get("default")).isEqualTo(new Json.Str("fail"));

        Json.Obj sync = (Json.Obj) ((Json.Obj) defs.get("SyncElement")).get("properties");
        assertThat(((Json.Obj) sync.get("write_mode")).get("default")).isEqualTo(new Json.Str("upsert"));
    }

    @Test
    void yamlKeysMatchTheCanonicalWriterIncludingItsExceptions() {
        Json.Obj defs = (Json.Obj) generator.generateTree().get("$defs");

        // pipeline `source` (singular) is scalar-or-list — not the Java field name `sources`.
        Json.Obj pipeline = (Json.Obj) ((Json.Obj) defs.get("PipelineResource")).get("properties");
        assertThat(pipeline.get("sources")).isNull();
        Json.Obj source = (Json.Obj) pipeline.get("source");
        assertThat(source).isNotNull();
        assertThat(((Json.Arr) source.get("oneOf")).items()).contains(
                new Json.Obj(List.of(new Json.Entry("type", new Json.Str("string")))));

        // embed keeps camelCase keys (the canonical writer's exception to snake_case).
        Json.Obj embed = (Json.Obj) ((Json.Obj) defs.get("Embed")).get("properties");
        assertThat(embed.get("arrayKey")).isNotNull();
        assertThat(embed.get("array_key")).isNull();
        assertThat(embed.get("ignoreUpdates")).isNotNull();
        assertThat(embed.get("trackKeyChanges")).isNotNull();

        // the root carries its own key-tracking switch, in camelCase like the embed's
        Json.Obj nestRoot = (Json.Obj) ((Json.Obj) defs.get("NestRoot")).get("properties");
        assertThat(nestRoot.get("trackKeyChanges")).isNotNull();
        assertThat(nestRoot.get("track_key_changes")).isNull();
    }

    @Test
    void everyGrammarElementIsDocumented() {
        assertThat(generator.undocumented())
                .as("every grammar type, field and enum constant needs an @Doc — an undocumented "
                        + "element would reach the schema without a description")
                .isEmpty();
    }
}
