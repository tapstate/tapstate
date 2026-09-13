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
    void conflictingNumericAttributesCannotChangeAnExactDecimalToFloatingPoint() {
        var approximate = new io.tapstate.core.common.NumericType(64, false, false, null,
                java.math.BigDecimal.ZERO, java.math.BigDecimal.TEN, 10, 2);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> TargetTapTable.build(new TargetTable("orders", List.of(
                new TargetField("amount", "source_decimal", false, io.tapstate.core.common.TapstateType.DECIMAL, approximate)))))
                .hasMessageContaining("amount").hasMessageContaining("orders").hasMessageContaining("disagrees");
    }

    @Test
    void targetFieldsSerializedBeforeNumericMetadataStillReadWithAbsentAttributes() throws Exception {
        // Serialized by the previous four-component record, not by the current implementation.
        byte[] bytes = java.util.Base64.getDecoder().decode(
                "rO0ABXNyACBpby50YXBzdGF0ZS5zcGkuc2luay5UYXJnZXRGaWVsZAAAAAAAAAAAAgAEWgAKcHJpbWFyeUtleUwADGluZmVycmVkVHlwZXQAJkxpby90YXBzdGF0ZS9jb3JlL2NvbW1vbi9UYXBzdGF0ZVR5cGU7TAAEbmFtZXQAEkxqYXZhL2xhbmcvU3RyaW5nO0wABHR5cGVxAH4AAnhwAH5yACRpby50YXBzdGF0ZS5jb3JlLmNvbW1vbi5UYXBzdGF0ZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAdERUNJTUFMdAAGYW1vdW50dAANZGVjaW1hbCgxOCw0KQ==");
        try (var input = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))) {
            var field = (TargetField) input.readObject();
            assertThat(field).isEqualTo(new TargetField("amount", "decimal(18,4)", false,
                    io.tapstate.core.common.TapstateType.DECIMAL));
            assertThat(field.numericType()).isNull();
        }
    }

    @Test
    void numericAttributesDoNotOverrideAnUnknownOrNonnumericPortableType() {
        var number = new io.tapstate.core.common.NumericType(64, false, false, false,
                java.math.BigDecimal.ZERO, java.math.BigDecimal.TEN, 10, 2);
        for (var type : List.of(io.tapstate.core.common.TapstateType.UNKNOWN, io.tapstate.core.common.TapstateType.STRING,
                io.tapstate.core.common.TapstateType.BOOLEAN)) {
            var restored = TargetTapTable.build(new TargetTable("orders", List.of(new TargetField("value", "source", false,
                    type, number)))).getNameFieldMap().get("value").getTapType();
            assertThat(restored).isNotInstanceOf(io.tapdata.entity.schema.type.TapNumber.class);
            assertThat(restored.getClass()).isEqualTo(PdkTypeMapping.targetType(type).getClass());
        }
    }

    @Test
    void restoresEveryDeclaredNumericAttributeWithoutWidening() {
        var number = new io.tapstate.core.common.NumericType(128, true, false, true, new java.math.BigDecimal("-99999999999999.9999"), new java.math.BigDecimal("99999999999999.9999"), 18, 4);
        var target = new TargetTable("orders", List.of(new TargetField("amount", "source_decimal", false,
                io.tapstate.core.common.TapstateType.DECIMAL, number)));
        var restored = TargetTapTable.build(target).getNameFieldMap().get("amount").getTapType();
        assertThat(PdkTypeMapping.numericType(restored)).isEqualTo(number);
    }

    @Test
    void oldAndIncompleteDecimalModelsRequireRediscovery() {
        for (var target : List.of(
                new TargetField("amount", "source_decimal", false, io.tapstate.core.common.TapstateType.DECIMAL),
                new TargetField("amount", "source_decimal", false, io.tapstate.core.common.TapstateType.DECIMAL,
                        new io.tapstate.core.common.NumericType(null, true, null, null, null, null, 18, 4)))) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                    TargetTapTable.build(new TargetTable("orders", List.of(target))))
                    .hasMessageContaining("amount").hasMessageContaining("orders").hasMessageContaining("rediscover");
        }
    }

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
