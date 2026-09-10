package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Builds a PDK {@link TapTable} descriptor from a resolved tapstate {@link TargetTable}: the target's
 * fields become typed columns and the primary-key fields become the key an upsert matches on, in field
 * order. This is the write-side model a connector reads to create the table and to coerce each row
 * value; the projection is exercised in isolation here, apart from any live connector.
 */
class TargetTapTableTest {

    @Test
    void inferredTypesReachTheConnectorAsPortableTypesBeforeDatabaseConversion() {
        TapTable table = TargetTapTable.build(new TargetTable("orders", List.of(
                new TargetField("id", "SOURCE_INTEGER", true,
                        io.tapstate.core.common.TapstateType.INT64),
                new TargetField("payload", "SOURCE_TEXT", false,
                        io.tapstate.core.common.TapstateType.STRING))));
        assertThat(table.getNameFieldMap().get("id").getTapType())
                .isInstanceOf(io.tapdata.entity.schema.type.TapNumber.class);
        assertThat(table.getNameFieldMap().get("payload").getTapType())
                .isInstanceOf(io.tapdata.entity.schema.type.TapString.class);
    }

    @Test
    void aNewTableDoesNotReceiveItsPrimaryKeyIndexTwice() {
        TargetTable target = new TargetTable("orders", List.of(new TargetField("id", "bigint", true)),
                List.of(new TargetIndex(List.of("id"), true),
                        new TargetIndex(List.of("email"), false)));
        assertThat(TargetTapTable.createIndexEvent(target, true).getIndexList()).singleElement()
                .satisfies(index -> assertThat(index.getIndexFields()).singleElement()
                        .satisfies(field -> assertThat(field.getName()).isEqualTo("email")));
        assertThat(TargetTapTable.createIndexEvent(target, false).getIndexList()).hasSize(2);
    }

    @Test
    void projectsEachTargetIndexIntoTheCreateEventKeepingItsUniqueness() {
        TapCreateIndexEvent event = TargetTapTable.createIndexEvent(new TargetTable("orders",
                List.of(new TargetField("id", "bigint", true)),
                List.of(new TargetIndex(List.of("id"), true),
                        new TargetIndex(List.of("customer_id"), false))));

        assertThat(event.getIndexList()).hasSize(2);
        TapIndex key = event.getIndexList().get(0);
        assertThat(key.getUnique()).isTrue();
        assertThat(key.getIndexFields()).singleElement()
                .satisfies(f -> assertThat(f.getName()).isEqualTo("id"));
        assertThat(event.getIndexList().get(1).getUnique()).isFalse();
    }

    @Test
    void aTargetWithNoIndexesProducesNoCreateEvent() {
        // A store that is handed an empty event would still be asked to do work it was never given.
        assertThat(TargetTapTable.createIndexEvent(
                new TargetTable("orders", List.of(new TargetField("id", "bigint", true))))).isNull();
    }

    @Test
    void carriesTheTargetNameAndEachFieldWithItsType() {
        TapTable table = TargetTapTable.build(new TargetTable("orders",
                List.of(new TargetField("id", "bigint", true), new TargetField("name", "varchar", false))));

        assertThat(table.getName()).isEqualTo("orders");
        assertThat(table.getNameFieldMap()).containsOnlyKeys("id", "name");
        assertThat(table.getNameFieldMap().get("id").getDataType()).isEqualTo("bigint");
        assertThat(table.getNameFieldMap().get("name").getDataType()).isEqualTo("varchar");
    }

    @Test
    void derivesThePrimaryKeyFromTheFlaggedFieldsInFieldOrder() {
        TapTable table = TargetTapTable.build(new TargetTable("orders", List.of(
                new TargetField("region", "varchar", true),
                new TargetField("payload", "text", false),
                new TargetField("id", "bigint", true))));

        // A composite key in field order: region before id, and the non-key column excluded.
        assertThat(table.primaryKeys()).containsExactly("region", "id");
    }

    @Test
    void hasNoPrimaryKeyWhenNoFieldIsFlagged() {
        TapTable table = TargetTapTable.build(new TargetTable("events",
                List.of(new TargetField("payload", "text", false), new TargetField("seq", "int", false))));

        assertThat(table.primaryKeys()).isEmpty();
        // Asserted on the positions too, not only on the key they derive: a position is what marks a
        // column as part of the key, so this is where a model carrying no key physically ends up. It
        // holds whatever any rule above decides, and stays true if one is bypassed or replaced - which
        // is the point of pinning it here rather than only where the refusal is decided.
        assertThat(table.getNameFieldMap().values())
                .allSatisfy(field -> assertThat(field.getPrimaryKeyPos()).isNull());
    }

    @Test
    void anUnresolvedFieldTypePassesThroughAsNullForTheConnectorToInfer() {
        TapTable table = TargetTapTable.build(new TargetTable("orders",
                List.of(new TargetField("id", null, true))));

        assertThat(table.getNameFieldMap().get("id").getDataType()).isNull();
    }

    /**
     * A descriptor carrying no columns still answers what a connector asks of it.
     *
     * <p>Declaring no columns is an ordinary outcome - a stream whose table was never discovered reaches
     * the sink with its name and nothing else, and the contract for that case is that the connector decides
     * the structure. What is not ordinary is the descriptor being unable to answer at all, and that is what
     * it did: the column map is created by the first column added, so a descriptor that never got one
     * carried a null map and every read of its key threw. The throw surfaced inside the connector, several
     * frames below anything that knew the model was absent, and was reported as the connector failing to
     * write rather than as the model never having been resolved.
     */
    @Test
    void aDescriptorForAModelWithNoColumnsAnswersAnEmptyKeyRatherThanThrowing() {
        TapTable table = TargetTapTable.build(new TargetTable("orders", List.of()));

        assertThatCode(table::primaryKeys)
                .as("reading the key of a descriptor built from a model carrying no columns")
                .doesNotThrowAnyException();
        assertThat(table.primaryKeys()).isEmpty();
    }

    @Test
    void aDescriptorForAStreamWithNoModelAnswersAnEmptyKeyRatherThanThrowing() {
        TapTable table = TargetTapTable.bare("order_doc");

        assertThat(table.getName()).isEqualTo("order_doc");
        assertThatCode(table::primaryKeys)
                .as("reading the key of a descriptor for a stream no model was resolved for")
                .doesNotThrowAnyException();
        assertThat(table.primaryKeys()).isEmpty();
    }

    @Test
    void aBareDescriptorCarriesNoColumns() {
        assertThat(TargetTapTable.bare("order_doc").getNameFieldMap()).isEmpty();
    }
}
