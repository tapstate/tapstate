package io.tapstate.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class SchemaNavigatorTest {

    private final SchemaNavigator nav = SchemaNavigator.bundled();

    @Test
    void rootListsTheFiveResourceKindsAndTheProjectFile() {
        SchemaNode root = nav.navigate("").orElseThrow();
        assertThat(root.path()).isEmpty();
        assertThat(root.description()).contains("top-level");
        assertThat(root.children()).containsExactly("source", "pipeline", "transform", "view", "serve", "project");
    }

    @Test
    void navigatesToAResourceKindByDiscriminator() {
        SchemaNode source = nav.navigate("source").orElseThrow();
        assertThat(source.type()).isEqualTo("object");
        assertThat(source.description()).contains("kind: source");
        assertThat(source.children()).contains("connector", "mode", "tables", "config", "srs");
        assertThat(source.required()).contains("version", "kind", "id", "connector");
    }

    @Test
    void scalarFieldCarriesItsReferenceSiteDescription() {
        SchemaNode connector = nav.navigate("source.connector").orElseThrow();
        assertThat(connector.type()).isEqualTo("string");
        assertThat(connector.description()).contains("connector");
        assertThat(connector.isRequired()).isTrue();
    }

    @Test
    void enumFieldExposesItsAllowedValuesWithDescriptions() {
        SchemaNode mode = nav.navigate("source.mode").orElseThrow();
        assertThat(mode.type()).isEqualTo("enum");
        assertThat(mode.isRequired()).isFalse();
        assertThat(mode.enumValues()).extracting(SchemaNode.EnumValue::value)
                .containsExactly("cdc", "snapshot", "stream", "file", "api");
        assertThat(mode.enumValues()).allSatisfy(v -> assertThat(v.description()).isNotBlank());
    }

    @Test
    void arrayFieldForwardsIntoItsElementType() {
        SchemaNode tables = nav.navigate("source.tables").orElseThrow();
        assertThat(tables.type()).isEqualTo("array");
        // TableRef is oneOf[string, TableRef.Spec]; the object variant's fields are the children
        assertThat(tables.children()).contains("name", "filter", "pk");

        SchemaNode name = nav.navigate("source.tables.name").orElseThrow();
        assertThat(name.type()).isEqualTo("string");
        // TableRef is a union of a bare string or an object; 'name' is required only in the object
        // form, so it is conditionally — not unconditionally — required across the union
        assertThat(name.isRequired()).isFalse();
    }

    @Test
    void openMapFieldHasNoChildren() {
        SchemaNode config = nav.navigate("source.config").orElseThrow();
        assertThat(config.type()).isEqualTo("object");
        assertThat(config.children()).isEmpty();
    }

    @Test
    void transformMergesBaseAndTypeSpecificFields() {
        // TransformResource = base properties + allOf[oneOf of typed bodies]; explain surfaces both.
        SchemaNode transform = nav.navigate("transform").orElseThrow();
        assertThat(transform.children()).contains("id", "metadata");      // base
        assertThat(transform.children()).contains("type", "script", "expr", "sql");  // variant fields
    }

    @Test
    void requirednessAcrossAUnionHoldsOnlyWhenEveryBranchRequiresIt() {
        // pipeline.transforms is an array of Step = oneOf[Step.Inline, Step.Use]
        // 'from' is required in BOTH branches -> genuinely required
        assertThat(nav.navigate("pipeline.transforms.from").orElseThrow().isRequired()).isTrue();
        // 'id' is required in Step.Inline but optional in Step.Use -> not unconditionally required
        assertThat(nav.navigate("pipeline.transforms.id").orElseThrow().isRequired()).isFalse();
        // 'script' exists only in the js transform variant -> not unconditionally required
        assertThat(nav.navigate("pipeline.transforms.script").orElseThrow().isRequired()).isFalse();
        // serve is oneOf[Inline, Use]: 'from' required in both, 'use' only in the Use branch
        assertThat(nav.navigate("pipeline.serve.from").orElseThrow().isRequired()).isTrue();
        assertThat(nav.navigate("pipeline.serve.use").orElseThrow().isRequired()).isFalse();
    }

    @Test
    void scalarFieldUnderARefPrefersItsReferenceSiteDescription() {
        // source.mode references SourceMode; the property site says "Read mode...", the def says
        // "Source read mode...". The reference-site wording wins.
        assertThat(nav.navigate("source.mode").orElseThrow().description()).startsWith("Read mode");
    }

    @Test
    void unknownPathsAreAbsent() {
        assertThat(nav.navigate("nope")).isEmpty();
        assertThat(nav.navigate("source.nope")).isEmpty();
        assertThat(nav.navigate("source.mode.nope")).isEmpty();
    }

    @Test
    void completesTopLevelKinds() {
        assertThat(nav.complete("")).containsExactly(
                "pipeline", "project", "serve", "source", "transform", "view");
        assertThat(nav.complete("sou")).containsExactly("source");
    }

    @Test
    void completesFieldPathsUnderAResource() {
        assertThat(nav.complete("source.m")).containsExactly("source.metadata", "source.mode");
        assertThat(nav.complete("source.tables.")).contains(
                "source.tables.filter", "source.tables.name", "source.tables.pk");
    }

    @Test
    void completionOfAnUnknownParentIsEmpty() {
        assertThat(nav.complete("nope.")).isEmpty();
    }
}
