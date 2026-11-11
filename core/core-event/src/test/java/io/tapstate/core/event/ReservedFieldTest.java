package io.tapstate.core.event;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.ReservedField.Owner;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReservedFieldTest {

    private static final Map<String, Object> ORDER = Map.of("e", 4, "q", 1882031L);
    private static final Map<String, Object> MARKER = Map.of("by_day", Map.of("status", "failed"));

    private static Map<String, Object> marked() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("region", "EU");
        row.put("_ts", 1731290000L);
        Map<String, Object> withOrder = ReservedField.with(row, Owner.SRS, ORDER);
        return ReservedField.with(withOrder, Owner.AGGREGATE, MARKER);
    }

    @Test
    @DisplayName("a served row keeps the deliverable owner's entry and loses the internal one")
    void servedKeepsOnlyDeliverableEntries() {
        Map<String, Object> served = ReservedField.served(marked(), true);

        assertThat(ReservedField.read(served, Owner.AGGREGATE)).isEqualTo(MARKER);
        assertThat(ReservedField.read(served, Owner.SRS)).isNull();
        assertThat(served).containsEntry("region", "EU").containsEntry("_ts", 1731290000L);
    }

    @Test
    @DisplayName("a target that opts out receives nothing under the reserved field")
    void strippedRowCarriesNoReservedField() {
        Map<String, Object> served = ReservedField.served(marked(), false);

        assertThat(served).doesNotContainKey(ReservedField.NAME).containsEntry("region", "EU");
    }

    @Test
    @DisplayName("a row holding only internal data is served without the reserved field at all")
    void internalOnlyRowLosesTheFieldWhole() {
        Map<String, Object> row = ReservedField.with(Map.of("id", 1), Owner.SRS, ORDER);

        assertThat(ReservedField.served(row, true)).isEqualTo(Map.of("id", 1));
    }

    @Test
    @DisplayName("an entry under a sub-key nobody registered does not leave")
    void unregisteredEntriesAreDropped() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put(ReservedField.NAME, Map.of("someone_else", "x"));

        assertThat(ReservedField.served(row, true)).isEqualTo(Map.of("id", 1));
    }

    @Test
    @DisplayName("a row carrying nothing reserved is returned as it is")
    void untouchedRowIsTheSameInstance() {
        Map<String, Object> row = Map.of("id", 1);

        assertThat(ReservedField.served(row, true)).isSameAs(row);
        assertThat(ReservedField.served(row, false, List.of(List.of("items")))).isSameAs(row);
    }

    @Test
    @DisplayName("rows embedded at a known path are served by the same rule, as an object and as array elements")
    void embeddedRowsAreServedAtTheirPaths() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("id", 7);
        document.put("stats", marked());
        document.put("lines", List.of(marked(), Map.of("sku", "A-7")));

        Map<String, Object> served = ReservedField.served(
                document, true, List.of(List.of("stats"), List.of("lines")));

        @SuppressWarnings("unchecked")
        Map<String, Object> stats = (Map<String, Object>) served.get("stats");
        assertThat(ReservedField.read(stats, Owner.SRS)).isNull();
        assertThat(ReservedField.read(stats, Owner.AGGREGATE)).isEqualTo(MARKER);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> lines = (List<Map<String, Object>>) served.get("lines");
        assertThat(ReservedField.read(lines.get(0), Owner.SRS)).isNull();
        assertThat(lines.get(1)).isEqualTo(Map.of("sku", "A-7"));
    }

    @Test
    @DisplayName("the same name inside a customer's own sub-document is left alone")
    void aNestedFieldOfTheSameNameIsCustomerData() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("payload", Map.of(ReservedField.NAME, "theirs"));

        assertThat(ReservedField.served(row, false)).isSameAs(row);
    }

    @Test
    @DisplayName("removing an owner's last entry removes the field, and other owners are untouched")
    void withNullRemoves() {
        Map<String, Object> row = marked();

        Map<String, Object> withoutMarker = ReservedField.with(row, Owner.AGGREGATE, null);
        assertThat(ReservedField.read(withoutMarker, Owner.SRS)).isEqualTo(ORDER);
        assertThat(ReservedField.with(withoutMarker, Owner.SRS, null)).doesNotContainKey(ReservedField.NAME);
        assertThat(ReservedField.read(row, Owner.AGGREGATE)).isEqualTo(MARKER);
    }
}
