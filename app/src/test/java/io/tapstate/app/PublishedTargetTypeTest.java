package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Source spelling remains available only while it describes the column. Portable inferred types
 * and field attributes travel separately to the target connector's type mapping.
 */
class PublishedTargetTypeTest {

    @Test
    void expansionPublishesParentMetadataAndTheElementsPortableTypeTogether() {
        var number = new io.tapstate.core.common.NumericType(null, true, false, null, null, null, 18, 4);
        var string = new io.tapstate.core.common.StringType(36L, false, true, 255L, 2);
        NodeColumns source = shared("o_id", "INT64 NOT NULL", "o_region", "STRING NULL",
                "amount", "DECIMAL NULL", "items", "ARRAY NULL")
                .withNumericTypes(Map.of("amount", number)).withStringTypes(Map.of("o_region", string));
        NodeColumns projected = NodeColumns.of(new io.tapstate.core.model.TransformBody.MapProjection(
                Map.of("order_id", io.tapstate.core.model.FieldRule.rename("o_id"))), Map.of("in", source), null);
        NodeColumns expanded = NodeColumns.of(new io.tapstate.core.model.TransformBody.Unwind(
                "items", "item_no", false, null, "STRING"), Map.of("in", projected), null);
        TargetTable target = StoreBackedDagSource.publishedAs(source(), expanded, source);
        Map<String, TargetField> fields = new LinkedHashMap<>();
        target.fields().forEach(field -> fields.put(field.name(), field));
        assertThat(fields.get("amount").numericType()).isEqualTo(number);
        assertThat(fields.get("o_region").stringType()).isEqualTo(string);
        assertThat(fields.get("items").type()).isNull();
        assertThat(fields.get("items").inferredType()).isEqualTo(io.tapstate.core.common.TapstateType.STRING);
        assertThat(fields.get("item_no").inferredType()).isEqualTo(io.tapstate.core.common.TapstateType.INT64);
        assertThat(target.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .containsExactly("order_id", "item_no");
    }

    @Test
    void expansionDoesNotInheritParentUniqueness() {
        var uniqueParent = new io.tapstate.spi.sink.TargetIndex(List.of("o_id"), true);
        var uniqueRegion = new io.tapstate.spi.sink.TargetIndex(List.of("o_region"), true);
        var lookup = new io.tapstate.spi.sink.TargetIndex(List.of("o_region"), false);
        TargetTable base = new TargetTable("orders", source().fields(), List.of(uniqueParent, uniqueRegion, lookup));
        NodeColumns expanded = NodeColumns.of(new io.tapstate.core.model.TransformBody.Unwind(
                "items", "item_no", false, null, "STRING"), Map.of("in", atTheSource()), null);
        TargetTable target = StoreBackedDagSource.publishedAs(base, expanded, atTheSource());
        // The expanded key is unique because this pipeline says so, not because anything discovered it,
        // so it carries the claim without the proof the parent's own index had.
        assertThat(target.indexes()).containsExactly(lookup,
                new io.tapstate.spi.sink.TargetIndex(List.of("o_id", "item_no"), true, false));
    }

    /** A source table as the sink sees it: the store's own type tokens, one key column. */
    private static TargetTable source() {
        return new TargetTable("orders", List.of(
                new TargetField("o_id", "bigint", true),
                new TargetField("o_region", "text", false),
                new TargetField("items", "json", false)));
    }

    /** The same table as the derivation sees it: the shared vocabulary. */
    private static NodeColumns shared(String... namesAndTypes) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            columns.put(namesAndTypes[i], namesAndTypes[i + 1]);
        }
        return NodeColumns.known(columns);
    }

    private static NodeColumns atTheSource() {
        return shared("o_id", "INT64 NOT NULL", "o_region", "STRING NULL", "items", "ARRAY NULL");
    }

    private static Map<String, String> typesOf(TargetTable published) {
        Map<String, String> byName = new LinkedHashMap<>();
        published.fields().forEach(field -> byName.put(field.name(), field.type()));
        return byName;
    }

    @Test
    @DisplayName("a column the pipeline did not retype keeps the word the source declared")
    void anUntouchedColumnKeepsTheSourcesOwnTypeToken() {
        TargetTable published = StoreBackedDagSource.publishedAs(
                source(), atTheSource(), atTheSource());

        assertThat(typesOf(published))
                .containsEntry("o_id", "bigint")
                .containsEntry("o_region", "text")
                .containsEntry("items", "json");
    }

    /**
     * The case this rule exists for. An expansion writes one element where the list was, so the
     * source's word for that column now describes the wrong thing - a column built from it holds a
     * list's type and receives an element, and nothing anywhere reports the mismatch.
     */
    @Test
    @DisplayName("an expanded column carries no type, so the connector infers one")
    void aColumnTheSourceNoLongerDescribesCarriesNoType() {
        NodeColumns expanded =
                shared("o_id", "INT64 NOT NULL", "o_region", "STRING NULL", "items", "STRING NULL");

        TargetTable published = StoreBackedDagSource.publishedAs(source(), expanded, atTheSource());

        assertThat(typesOf(published)).containsEntry("items", null);
        // Only that one column. A rule that dropped the neighbours too would pass a test asserting
        // the expanded column alone, and would throw away every width and precision the source
        // declared - which no row-level test notices until a value stops fitting.
        assertThat(typesOf(published))
                .containsEntry("o_id", "bigint")
                .containsEntry("o_region", "text");
    }

    /**
     * The comparison is between types and never nullability. A merge widens a column to nullable
     * because one of its inputs does not carry it, and that says nothing about what the column
     * holds where it is present.
     */
    @Test
    @DisplayName("a column merely widened to nullable keeps its type token")
    void wideningToNullableIsNotARetype() {
        NodeColumns widened =
                shared("o_id", "INT64 NULL", "o_region", "STRING NULL", "items", "ARRAY NULL");

        TargetTable published = StoreBackedDagSource.publishedAs(source(), widened, atTheSource());

        assertThat(typesOf(published)).containsEntry("o_id", "bigint");
    }

    /**
     * A source copy that has not been taken yet is not evidence that anything changed. Reading it as
     * one would strip every type token off every column of a pipeline whose source was never
     * discovered - the state in which authoring is explicitly allowed.
     */
    @Test
    @DisplayName("no source copy to compare against leaves every type token alone")
    void anUnreadableSourceCopyChangesNothing() {
        NodeColumns expanded =
                shared("o_id", "INT64 NOT NULL", "o_region", "STRING NULL", "items", "STRING NULL");

        assertThat(typesOf(StoreBackedDagSource.publishedAs(source(), expanded, null)))
                .containsEntry("items", "json");
        assertThat(typesOf(StoreBackedDagSource.publishedAs(
                        source(), expanded, NodeColumns.unknown("js: dark"))))
                .containsEntry("items", "json");
    }

    @Test
    @DisplayName("a column the source never had still carries no type")
    void aColumnTheSourceNeverHadCarriesNoType() {
        NodeColumns withAnOrdinal = shared("o_id", "INT64 NOT NULL", "items", "STRING NULL",
                "item_no", "INT64 NOT NULL");

        TargetTable published =
                StoreBackedDagSource.publishedAs(source(), withAnOrdinal, atTheSource());

        assertThat(typesOf(published)).containsEntry("item_no", null);
    }

    @Test
    @DisplayName("the key is still whatever of the source's key still travels")
    void theKeySurvivesTheRule() {
        NodeColumns expanded =
                shared("o_id", "INT64 NOT NULL", "o_region", "STRING NULL", "items", "STRING NULL");

        TargetTable published = StoreBackedDagSource.publishedAs(source(), expanded, atTheSource());

        assertThat(published.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .containsExactly("o_id");
    }
}
