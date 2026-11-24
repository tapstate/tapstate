package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class PdkDdlWriteOrderTest {
    private static final TargetTable TARGET = new TargetTable("authored_target",List.of(
            new TargetField("ID","int",true),new TargetField("V","text",false)));
    private static PdkConnector connector(Path directory) {
        return PdkConnector.open("mongodb",new ConnectorRef(List.of(Synthetic.countingSink(directory)),
                "synthetic.CountingSink","2.0.8",null,null),Map.of());
    }
    private static Envelope rename() {
        return Envelope.ddl(2,"source",Map.of(PdkDdlCommand.KEY,Map.of("format",1,"action","rename",
                "before","V","after","W","primary",false)));
    }
    @Test void rowsBeforeAndAfterTheDdlAreFlushedOnTheirOwnSidesAndZeroModifiedIsAnAcknowledgement(@TempDir Path directory) throws Exception {
        var connector=connector(directory);var calls=new ArrayList<String>();
        connector.functions().supportExecuteCommandFunction((context,command,out) -> {
            assertThat(command.getParams()).containsEntry("collection","authored_target").containsEntry("multi",true)
                    .containsEntry("upsert",false);
            @SuppressWarnings("unchecked") var update=(Map<String,Object>)command.getParams().get("opObject");
            @SuppressWarnings("unchecked") var fields=(Map<String,Object>)update.get("$rename");
            assertThat(fields.put("V","W")).isEqualTo("W"); // Native connector may normalize nested maps in place.
            calls.add("ddl"); out.accept(new ExecuteResult<Long>().result(0L));
        });
        try(var writer=new PdkSinkWriter(connector,(context,events,table,out) -> {
            for(var event:events) {
                var row=((TapInsertRecordEvent)event).getAfter();
                calls.add(row.containsKey("V") ? "before" : "after");
                assertThat(event.getTableId()).isEqualTo("authored_target");
            }
            out.accept(new WriteListResult<io.tapdata.entity.event.dml.TapRecordEvent>().insertedCount(events.size()));
        },WriteMode.UPSERT,DdlPolicy.APPLY,Map.of("source",TARGET))) {
            var batch=List.of(Envelope.insert(1,"source",Map.of("ID",1,"V","before"),null),rename(),
                    Envelope.insert(3,"source",Map.of("ID",2,"W","after"),null));
            assertThat(writer.write(batch).toCompletableFuture().get().written()).isEqualTo(2);
            assertThat(calls).containsExactly("before","ddl","after");
            calls.clear();
            assertThat(writer.write(batch).toCompletableFuture().get().written()).isEqualTo(2);
            assertThat(calls).containsExactly("before","ddl","after");
        }
    }
    @Test void unsupportedDdlLaterInTheBatchRefusesBeforeAnEarlierRowCanBeWritten(@TempDir Path directory) throws Exception {
        var connector=connector(directory);var calls=new ArrayList<String>();
        try(var writer=new PdkSinkWriter(connector,(context,events,table,out) -> calls.add("row"),
                WriteMode.UPSERT,DdlPolicy.APPLY,Map.of("source",TARGET))) {
            assertThatThrownBy(() -> writer.write(List.of(Envelope.insert(1,"source",Map.of("ID",1),null),
                    Envelope.ddl(2,"source",Map.of("origin","ALTER TABLE secret DROP V")))).toCompletableFuture().get())
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class);
            assertThat(calls).isEmpty();
        }
    }
    @Test void aDdlFunctionThatReturnsWithoutAcknowledgingCannotAdvanceTheBatch(@TempDir Path directory) throws Exception {
        var connector=connector(directory);
        connector.functions().supportExecuteCommandFunction((context,command,out) -> {});
        try(var writer=new PdkSinkWriter(connector,(context,events,table,out) -> {throw new AssertionError("no rows");},
                WriteMode.UPSERT,DdlPolicy.APPLY,Map.of("source",TARGET))) {
            assertThatThrownBy(() -> writer.write(List.of(rename())).toCompletableFuture().get())
                    .hasRootCauseMessage("target DDL mutation returned no acknowledgement");
        }
    }
}
