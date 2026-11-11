package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.ReservedField;
import io.tapstate.core.common.ReservedFieldError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceTable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReservedColumnTest {

    private static final String RESERVED = ReservedField.NAME;
    private static final String RENAMED = ReservedField.DEFAULT_SOURCE_RENAME;

    private static SourceField column(String name) {
        return new SourceField(name, "varchar(32)", TapstateType.STRING);
    }

    @Test
    @DisplayName("a table with a reserved column is known by the new name in its fields, key and indexes")
    void aReservedColumnIsRenamedEverywhereTheTableNamesIt() {
        SourceTable table = new SourceTable("events",
                List.of(column("id"), column(RESERVED), column("v")),
                List.of("id", RESERVED),
                List.of(new SourceIndex("by_meta", List.of(RESERVED, "v"), true)), 3L);

        SourceTable entered = ReservedColumn.entered("cosmos_main", table);

        assertThat(entered.fields()).extracting(SourceField::name).containsExactly("id", RENAMED, "v");
        assertThat(entered.primaryKey()).containsExactly("id", RENAMED);
        assertThat(entered.indexes().get(0).fields()).containsExactly(RENAMED, "v");
        assertThat(entered.indexes().get(0).unique()).isTrue();
        assertThat(entered.approximateRowCount()).isEqualTo(3L);
        // The original name is kept on the field, for apply to report and for reads against the source.
        assertThat(entered.fields().get(1).sourceName()).isEqualTo(RESERVED);
        assertThat(ReservedColumn.atSource(entered, RENAMED)).isEqualTo(RESERVED);
        assertThat(ReservedColumn.atSource(entered, "v")).isEqualTo("v");
    }

    @Test
    @DisplayName("a table without a reserved column is returned as it is")
    void anOrdinaryTableIsUntouched() {
        SourceTable table = new SourceTable("orders", List.of(column("id"), column(RENAMED)),
                List.of("id"), List.of());

        assertThat(ReservedColumn.entered("src", table)).isSameAs(table);
    }

    @Test
    @DisplayName("a table holding both the reserved name and the new name is refused with the conflict code")
    void aSecondCollisionIsRefused() {
        SourceTable table = new SourceTable("events", List.of(column(RESERVED), column(RENAMED)),
                List.of(), List.of());

        assertThatThrownBy(() -> ReservedColumn.entered("cosmos_main", table))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ReservedFieldError.RENAME_CONFLICT);
                    assertThat(error.args()).containsEntry("source", "cosmos_main")
                            .containsEntry("table", "events").containsEntry("renamed", RENAMED);
                });
    }

    @Test
    @DisplayName("a captured row carries the column under the new name in both images, values and order kept")
    void aCapturedRowIsRenamedInBothImages() {
        Map<String, Object> before = new LinkedHashMap<>();
        before.put("id", 1);
        before.put(RESERVED, "old");
        before.put("v", 7);
        Map<String, Object> after = new LinkedHashMap<>(before);
        after.put(RESERVED, "new");

        Envelope entered = ReservedColumn.entered("mysql", Envelope.update(5L, "events", before, after, null));

        assertThat(entered.before()).containsExactly(
                Map.entry("id", 1), Map.entry(RENAMED, "old"), Map.entry("v", 7));
        assertThat(entered.after()).containsEntry(RENAMED, "new").doesNotContainKey(RESERVED);
        assertThat(entered.src()).isEqualTo("events");
        assertThat(entered.ts()).isEqualTo(5L);
    }

    @Test
    @DisplayName("the same name inside a nested value is customer data and stays")
    void aNestedFieldOfTheSameNameStays() {
        Envelope event = Envelope.insert(1L, "events",
                Map.of("id", 1, "payload", Map.of(RESERVED, "theirs")), null);

        assertThat(ReservedColumn.entered("mysql", event)).isSameAs(event);
    }

    @Test
    @DisplayName("a row arriving with both names, after a schema change, is refused with the conflict code")
    void aRowWithBothNamesIsRefused() {
        Envelope event = Envelope.insert(1L, "events", Map.of(RESERVED, 1, RENAMED, 2), null);

        assertThatThrownBy(() -> ReservedColumn.entered("mysql", event))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(ReservedFieldError.RENAME_CONFLICT));
    }
}
