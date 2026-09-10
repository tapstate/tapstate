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
 * Which target columns keep the type token the source declared for them, and which stop.
 *
 * <p>A published target's type is the target store's own word for a column - the string a connector
 * builds the column from. There is exactly one place that word can come from, the source's own
 * declaration, because nothing on this side translates the shared vocabulary into any store's DDL.
 * So a column whose meaning changed on the way through has no second word available, and the answer
 * is no word at all: the connector infers one, which is what every column nobody could resolve a
 * type for already does.
 *
 * <p><b>Both halves are held here on purpose.</b> Dropping the word too eagerly is as wrong as
 * carrying a stale one - it throws away the width and precision the source declared, which is the
 * whole reason the word is carried in the first place - and the two failures look identical from a
 * green test run over rows that happen to fit either way.
 */
class PublishedTargetTypeTest {

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
