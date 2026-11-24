package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceSchemaChange;
import io.tapstate.spi.store.SourceTable;
import java.math.BigDecimal;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkDdlCommandTest {
    private static final TargetTable TARGET = new TargetTable("authored_target", List.of());
    private static Envelope event(Map<String,Object> command) {
        return Envelope.ddl(1,"source_table",Map.of(PdkDdlCommand.KEY,command));
    }
    @Test void capturedDefaultSurvivesPortableSerializationWithoutParsingOriginSql() {
        var before = new SourceTable("T",List.of(new SourceField("ID","INTEGER",TapstateType.INT64,null)),List.of("ID"),List.of());
        var after = new SourceTable("T",List.of(before.fields().getFirst(),new SourceField("C","NUMBER(10)",TapstateType.DECIMAL,null)),List.of("ID"),List.of());
        var column = new TapField("C","NUMBER(10)"); column.setDefaultValue(new BigDecimal("99"));
        var ddl = new TapNewFieldEvent().field(column);
        var command = PdkDdlCommand.encode(ddl,new SourceSchemaChange(before,after));
        @SuppressWarnings("unchecked") var transported = (Map<String,Object>) JsonReader.parse(JsonWriter.write(command));
        var plan = PdkMongoDdl.prepare("mongodb",event(transported),TARGET,true);
        assertThat(plan.table()).isEqualTo("authored_target");
        assertThat(plan.mutations()).hasSize(1);
        assertThat(plan.mutations().getFirst().filter()).isEqualTo(Map.of("C",Map.of("$exists",false)));
        assertThat(((Map<?,?>)plan.mutations().getFirst().update().get("$set")).get("C")).isEqualTo(new BigDecimal("99"));
    }
    @Test void sourceIntegerWidthAndDecimalScaleRemainExactForBackfilledValues() {
        assertThat(PdkMongoDdl.defaultValue(Map.of("kind","INT64","bit",32,"default","99"))).isInstanceOf(Long.class).isEqualTo(99L);
        assertThat(PdkMongoDdl.defaultValue(Map.of("kind","INT64","bit",64,"default","123456789012"))).isInstanceOf(Long.class);
        assertThat(PdkMongoDdl.defaultValue(Map.of("kind","DECIMAL","scale",4,"default","99"))).isEqualTo(new BigDecimal("99.0000"));
        assertThatThrownBy(() -> PdkMongoDdl.defaultValue(Map.of("kind","DECIMAL","scale",0,"default","99.1")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> PdkMongoDdl.defaultValue(Map.of("kind","DATETIME","function","SYSDATE")))
                .isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void decodedStringLiteralsAreDistinctFromSqlExpressions() {
        for (String literal : List.of("NULL", "'quoted'", "  spaced  ", "O'Brien"))
            assertThat(PdkMongoDdl.defaultValue(Map.of("kind","STRING","default",literal))).isEqualTo(literal);
        assertThat(PdkMongoDdl.defaultValue(Map.of("kind","STRING","function","NULL"))).isNull();
        assertThat(PdkMongoDdl.defaultValue(Map.of("kind","STRING","function","'NULL'"))).isEqualTo("NULL");
        assertThat(PdkMongoDdl.defaultValue(Map.of("kind","DOUBLE","default","1.25"))).isEqualTo(1.25d);
    }
    @Test void transformedColumnsPrimaryKeysAndPathExpressionsCannotCauseTargetMutation() {
        var rename = event(Map.of("format",1,"action","rename","before","V","after","W","primary",false));
        assertThatThrownBy(() -> PdkMongoDdl.prepare("mongodb",rename,TARGET,false)).isInstanceOf(UnsupportedOperationException.class);
        for (String column : List.of("_id","$operator","nested.path")) {
            var bad = event(Map.of("format",1,"action","drop","field",column,"primary",false));
            assertThatThrownBy(() -> PdkMongoDdl.prepare("mongodb",bad,TARGET,true)).isInstanceOf(UnsupportedOperationException.class);
        }
        var key = event(Map.of("format",1,"action","rename","before","ID","after","KEY","primary",true));
        assertThatThrownBy(() -> PdkMongoDdl.prepare("mongodb",key,TARGET,true)).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void unsupportedAndLegacyPayloadsRefuseInsteadOfPretendingApplySucceeded() {
        assertThatThrownBy(() -> PdkMongoDdl.prepare("mongodb",Envelope.ddl(1,"T",Map.of("origin","ALTER TABLE T DROP X")),TARGET,true))
                .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("typed captured event");
        assertThatThrownBy(() -> PdkMongoDdl.prepare("oracle",event(Map.of("format",1,"action","drop","field","V")),TARGET,true))
                .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("not implemented");
        var physical = PdkMongoDdl.prepare("mongodb",event(Map.of("format",1,"action","physical-table-rename")),TARGET,false);
        assertThat(physical.mutations()).isEmpty();
    }
}
