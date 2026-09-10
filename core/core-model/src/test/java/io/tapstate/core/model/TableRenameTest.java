package io.tapstate.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The target name a rename spec gives a source table (§8, X4). These cases are the shared contract
 * between the validate gate that rejects a workspace by these names and the write side that creates tables
 * by them, so they live with the computation rather than at either use site.
 */
class TableRenameTest {

    @Test
    void leaves_the_source_name_when_no_rule_reaches_it() {
        assertThat(TableRename.apply("orders", null)).isEqualTo("orders");
        assertThat(TableRename.apply("orders", new RenameSpec(Map.of("other", "x"), null, null, null)))
                .isEqualTo("orders");
    }

    @Test
    void explicit_table_map_takes_precedence_over_bulk_rename_rules() {
        assertThat(TableRename.apply("PlayerAddress",
                new RenameSpec(Map.of("PlayerAddress", "player_address"), RenameCase.UPPER, "ods_", "_v1")))
                .isEqualTo("player_address");
    }

    @Test
    void bulk_table_rename_applies_case_before_prefix_and_suffix() {
        assertThat(TableRename.apply("PLAYER_ADDRESS", new RenameSpec(null, RenameCase.CAMEL, "ods_", "_v1")))
                .isEqualTo("ods_playerAddress_v1");
    }

    @Test
    void bulk_table_rename_supports_pascal_case() {
        assertThat(TableRename.apply("player_address", new RenameSpec(null, RenameCase.PASCAL, null, null)))
                .isEqualTo("PlayerAddress");
    }

    @Test
    void bulk_table_rename_supports_upper_and_lower_case() {
        assertThat(TableRename.apply("PlayerAddress", new RenameSpec(null, RenameCase.UPPER, null, null)))
                .isEqualTo("PLAYERADDRESS");
        assertThat(TableRename.apply("PlayerAddress", new RenameSpec(null, RenameCase.LOWER, null, null)))
                .isEqualTo("playeraddress");
    }

    @Test
    void bulk_table_rename_preserves_acronym_and_digit_boundaries() {
        assertThat(TableRename.apply("HTTP2ServerV1", new RenameSpec(null, RenameCase.PASCAL, null, null)))
                .isEqualTo("Http2ServerV1");
    }

    @Test
    void bulk_table_rename_splits_acronyms_before_a_word() {
        RenameSpec pascal = new RenameSpec(null, RenameCase.PASCAL, null, null);

        assertThat(TableRename.apply("HTTPServer", pascal)).isEqualTo("HttpServer");
        assertThat(TableRename.apply("XMLHttpRequest", pascal)).isEqualTo("XmlHttpRequest");
    }

    @Test
    void bulk_table_rename_keeps_letters_outside_the_ascii_range() {
        assertThat(TableRename.apply("naïve_orders", new RenameSpec(null, RenameCase.CAMEL, null, null)))
                .isEqualTo("naïveOrders");
    }

    @Test
    void a_source_name_of_separators_alone_leaves_no_name_for_the_validate_layer_to_reject() {
        assertThat(TableRename.apply("___", new RenameSpec(null, RenameCase.CAMEL, null, null))).isEmpty();
    }
}
