package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class PdkWritePoliciesTest {
    private static final String SPEC="""
            {"configOptions":{"capabilities":[
              {"id":"dml_insert_policy","alternatives":["ignore_on_exists","update_on_exists","just_insert"]},
              {"id":"dml_update_policy","alternatives":["ignore_on_nonexists","insert_on_nonexists"]}
            ]}}
            """;
    private static final TargetTable TARGET=new TargetTable("rows",List.of(new TargetField("id","int",true)));

    @Test void upsertSelectsDeclaredPoliciesOnTheActualWriterThread(@TempDir Path directory)throws Exception {
        var connector=connector(directory,SPEC);
        var caller=Thread.currentThread();
        var rows=new HashMap<Object,Map<String,Object>>();
        var selected=new ArrayList<String>();
        try(var writer=new PdkSinkWriter(connector,(context,events,table,out) -> {
            assertThat(Thread.currentThread()).isNotSameAs(caller);
            var capabilities=context.getConnectorCapabilities();
            selected.add(capabilities.getCapabilityAlternative("dml_insert_policy"));
            selected.add(capabilities.getCapabilityAlternative("dml_update_policy"));
            for(var event:events) {
                assertThat(event).isInstanceOf(TapUpdateRecordEvent.class);
                var after=((TapUpdateRecordEvent)event).getAfter();
                if(rows.containsKey(after.get("id")) || "insert_on_nonexists".equals(capabilities.getCapabilityAlternative("dml_update_policy")))
                    rows.put(after.get("id"),after);
            }
            out.accept(new WriteListResult<TapRecordEvent>().modifiedCount(events.size()));
        },config(WriteMode.UPSERT),Map.of("input",TARGET),null)) {
            var after=Map.<String,Object>of("id",2,"status","active");
            writer.write(List.of(Envelope.update(1L,"input",Map.of("id",2,"status","inactive"),after,null)))
                    .toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertThat(selected).containsExactly("update_on_exists","insert_on_nonexists");
            assertThat(rows).containsEntry(2,after);
        }
    }

    @ParameterizedTest @ValueSource(strings={"dml_insert_policy","dml_update_policy"})
    void declaredPoliciesWithoutAnUpsertAlternativeRefuseBeforeAnyWrite(String policy,@TempDir Path directory) {
        var connector=connector(directory,"{\"configOptions\":{\"capabilities\":[{\"id\":\""+policy+"\",\"alternatives\":[\"ignore\"]}]}}");
        var writes=new AtomicInteger();
        try(connector) {
            assertThatThrownBy(() -> {
                try(var writer=new PdkSinkWriter(connector,(context,events,table,out) -> writes.incrementAndGet(),
                        config(WriteMode.UPSERT),Map.of("input",TARGET),null)) { }
            }).isInstanceOfSatisfying(TapstateException.class,error -> {
                assertThat(error.code()).isEqualTo(ConnectorError.WRITE_FAILED);
                assertThat(error.args().get("detail").toString()).contains(policy,"UPSERT");
            });
            assertThat(writes).hasValue(0);
        }
    }

    @ParameterizedTest @ValueSource(strings={"append","undeclared"})
    void unrelatedModesAndUndeclaredPoliciesKeepTheConnectorDefaults(String kind,@TempDir Path directory)throws Exception {
        var mode=kind.equals("append")?WriteMode.APPEND:WriteMode.UPSERT;
        var connector=connector(directory,kind.equals("undeclared")?null:SPEC);
        var calls=new AtomicInteger();
        try(var writer=new PdkSinkWriter(connector,(context,events,table,out) -> {
            assertThat(context.getConnectorCapabilities().getCapabilityAlternative("dml_insert_policy")).isNull();
            assertThat(context.getConnectorCapabilities().getCapabilityAlternative("dml_update_policy")).isNull();
            assertThat(events.getFirst()).isInstanceOf(mode==WriteMode.APPEND?TapInsertRecordEvent.class:TapUpdateRecordEvent.class);
            calls.incrementAndGet();out.accept(new WriteListResult<TapRecordEvent>().insertedCount(events.size()));
        },config(mode),Map.of("input",TARGET),null)) {
            var row=Map.<String,Object>of("id",2);
            writer.write(List.of(Envelope.update(1L,"input",row,row,null))).toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertThat(calls).hasValue(1);
        }
    }

    private static SinkConfig config(WriteMode mode){return new SinkConfig("demo",Map.of(),mode,DdlPolicy.FAIL);}
    private static PdkConnector connector(Path directory,String spec) {
        var ref=new ConnectorRef(List.of(Synthetic.countingSink(directory)),"synthetic.CountingSink","2.0.8",null,spec);
        return PdkConnector.open("demo",ref,Map.of());
    }
}
