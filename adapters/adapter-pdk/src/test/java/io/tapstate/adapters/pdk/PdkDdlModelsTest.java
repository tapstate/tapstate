package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PdkDdlModelsTest {
    @Test
    void typedColumnChangesAdvanceFrozenModelsWithoutReadingTheLiveDatabase() {
        java.util.function.Consumer<TapTable> fillTypes = table -> {
            table.getNameFieldMap().values().forEach(field -> {
                if (field.getTapType() == null) field.setTapType(field.getDataType().startsWith("NUMBER")
                        ? new TapNumber().precision(38) : new TapString());
            });
        };
        TapTable table = new TapTable("T");
        table.add(new TapField("ID", "NUMBER(19)").primaryKeyPos(1).tapType(new TapNumber().precision(19)));
        table.add(new TapField("V", "VARCHAR2(64)").tapType(new TapString()));
        var original = PdkSchemaDiscoverer.structuralModel(List.of(table)).tables().getFirst();
        PdkRowSchema frozen = PdkRowSchema.of(table);
        // A later mutation of the connector's own table map cannot revise an earlier model.
        table.getNameFieldMap().get("V").setDataType("NUMBER(38)");
        assertThat(frozen.fields().get("V").getDataType()).isEqualTo("VARCHAR2(64)");
        var added = PdkDdlModels.apply(fillTypes, frozen, original,
                new TapNewFieldEvent().field(new TapField("C", "VARCHAR2(16)").tapType(new TapString())));
        assertThat(added.change().before()).isEqualTo(original);
        assertThat(added.change().after().fields()).extracting("name").containsExactly("ID", "V", "C");
        assertThat(frozen.fields()).doesNotContainKey("C");
        var renamed = PdkDdlModels.apply(fillTypes, added.rows(), added.change().after(),
                new TapAlterFieldNameEvent().nameChange(new ValueChange<>("V", "W")));
        assertThat(renamed.rows().fields()).containsKeys("W", "C").doesNotContainKey("V");
        var altered = PdkDdlModels.apply(fillTypes, renamed.rows(), renamed.change().after(),
                new TapAlterFieldAttributesEvent().fieldName("W").dataType(new ValueChange<>("VARCHAR2(64)", "NUMBER(38)")));
        assertThat(altered.change().after().fields().stream().filter(field -> field.name().equals("W")).findFirst().orElseThrow()
                .numericType().precision()).isEqualTo(38);
        var dropped = PdkDdlModels.apply(fillTypes, altered.rows(), altered.change().after(), new TapDropFieldEvent().fieldName("C"));
        assertThat(dropped.change().after().fields()).extracting("name").containsExactly("ID", "W");
        assertThat(dropped.change().after().primaryKey()).containsExactly("ID");
    }

    @Test void anExactReplayedAddIsIdempotentButAConflictingExistingColumnIsNot() {
        var table = new TapTable("T").add(new TapField("ID", "NUMBER").tapType(new TapNumber()))
                .add(new TapField("C", "VARCHAR(16)").primaryKeyPos(null).tapType(new TapString().bytes(16L)));
        var model = PdkSchemaDiscoverer.structuralModel(List.of(table)).tables().getFirst();
        java.util.function.Consumer<TapTable> fill = value -> {};
        var replay = new TapNewFieldEvent().field(new TapField("C", "VARCHAR(16)").tapType(new TapString().bytes(16L)));
        var result = PdkDdlModels.apply(fill, PdkRowSchema.of(table), model, replay);
        assertThat(result).isNotNull();
        assertThat(result.change().before()).isEqualTo(model);
        assertThat(result.change().after()).isEqualTo(model);
        var conflict = new TapNewFieldEvent().field(new TapField("C", "NUMBER").tapType(new TapNumber()));
        assertThat(PdkDdlModels.apply(fill, PdkRowSchema.of(table), model, conflict)).isNull();
    }

    @Test void absentPrimaryKeysHaveTheSameMeaningAsZeroAndNegativePositionsWhenReplayingAnAdd() {
        for (Integer position : java.util.Arrays.asList(null, 0, -1)) {
            TapTable table = new TapTable("T").add(new TapField("ID", "NUMBER").primaryKeyPos(1).tapType(new TapNumber()))
                    .add(new TapField("C", "VARCHAR(16)").primaryKeyPos(position).tapType(new TapString().bytes(16L)));
            table.getNameFieldMap().get("C").setPrimaryKeyPos(position);
            var model = PdkSchemaDiscoverer.structuralModel(List.of(table)).tables().getFirst();
            var added = new TapNewFieldEvent().field(new TapField("C", "VARCHAR(16)").tapType(new TapString().bytes(16L)));
            assertThat(PdkDdlModels.apply(value -> {}, PdkRowSchema.of(table), model, added)).as("non-primary position " + position).isNotNull();
        }
    }

    @Test void aTypedPhysicalRenameKeepsThePreparedSelectedStreamAndRowShape() {
        TapTable table = new TapTable("T").add(new TapField("ID", "NUMBER").tapType(new TapNumber()));
        var model = PdkSchemaDiscoverer.structuralModel(List.of(table)).tables().getFirst();
        var rename = new io.tapdata.entity.event.ddl.table.TapRenameTableEvent();
        rename.setTableId("T"); rename.setNameChanges(new java.util.ArrayList<>(List.of(new ValueChange<>("T", "N"))));
        var result = PdkDdlModels.apply(value -> {}, PdkRowSchema.of(table), model, rename);
        assertThat(result).isNotNull();
        assertThat(result.change().before()).isEqualTo(model); assertThat(result.change().after()).isEqualTo(model);
        rename.setNameChanges(List.of(new ValueChange<>("N", null)));
        assertThat(PdkDdlModels.apply(value -> {}, PdkRowSchema.of(table), model, rename)).isNull();
    }

    @Test
    void rawSqlAndUnknownFieldChangesCannotClaimAnOrderedCompleteModel() {
        java.util.function.Consumer<TapTable> fillTypes = table -> { throw new AssertionError("unproved metadata must not reach type filling"); };
        TapTable table = new TapTable("T").add(new TapField("ID", "NUMBER").tapType(new TapNumber()));
        var model = PdkSchemaDiscoverer.structuralModel(List.of(table)).tables().getFirst();
        assertThat(PdkDdlModels.apply(fillTypes, PdkRowSchema.of(table), model, new TapDDLUnknownEvent())).isNull();
        assertThat(PdkDdlModels.apply(fillTypes, PdkRowSchema.of(table), model, new TapDropFieldEvent().fieldName("MISSING"))).isNull();
    }
}
