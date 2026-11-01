package io.tapstate.adapters.pdk.fixture;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Isolated connector with observable verification and release calls, without a source database. */
public final class RetirementSource implements TapConnector {
    public static final String STATE="tapstate.test.retirement-source";
    @SuppressWarnings("unchecked")
    private static Map<String,Object> state(){return (Map<String,Object>)System.getProperties().get(STATE);}
    @SuppressWarnings("unchecked")
    private static void record(String event){((List<String>)state().get("events")).add(event);}
    @Override public void registerCapabilities(ConnectorFunctions functions,TapCodecsRegistry codecs) {
        if(!state().containsKey("noRelease"))functions.supportReleaseExternalFunction(context -> {
            record("release:"+context.getStateMap().get("tapdata_pg_slot"));
            if(state().containsKey("releaseFailure"))throw new IllegalStateException("fixture release refused");
            if(!state().containsKey("keepSlot"))state().put("count",0L);
        });
        if(!state().containsKey("noQuery"))functions.supportRunRawCommandFunction((context,query,table,size,consumer) -> {
            record(query);
            if(state().containsKey("queryFailure"))throw new IllegalStateException("fixture query refused");
            Object count=query.contains("slot_type=")?state().getOrDefault("metadataCount",1L):state().get("count");
            consumer.accept(List.of(TapInsertRecordEvent.create().table(table.getId()).after(Map.of("slot_count",count))));
        });
    }
    @Override public void init(TapConnectionContext context) {
        record("init");
        if(state().containsKey("initFailure"))throw new IllegalStateException("fixture init refused");
    }
    @Override public void stop(TapConnectionContext context) {
        record("stop");
        if(state().containsKey("stopFailure"))throw new IllegalStateException("fixture stop refused");
    }
    @Override public void discoverSchema(TapConnectionContext context,List<String> tables,int size,Consumer<List<TapTable>> consumer) {consumer.accept(List.of());}
    @Override public ConnectionOptions connectionTest(TapConnectionContext context,Consumer<TestItem> consumer){return ConnectionOptions.create();}
    @Override public int tableCount(TapConnectionContext context){return 0;}
}
