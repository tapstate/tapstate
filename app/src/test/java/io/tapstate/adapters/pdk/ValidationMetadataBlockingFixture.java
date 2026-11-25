package io.tapstate.adapters.pdk;

import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

/** Isolated connector fixture whose original stop can ignore interruption behind an explicit gate. */
public final class ValidationMetadataBlockingFixture {
    private ValidationMetadataBlockingFixture(){}
    public static ConnectorRef artifact(Path directory)throws Exception{
        var jar=SyntheticJar.compileToJar(directory,"synthetic.BlockingMetadata","""
            package synthetic;
            import io.tapdata.pdk.apis.TapConnector;
            import io.tapdata.pdk.apis.functions.ConnectorFunctions;
            import io.tapdata.entity.codec.TapCodecsRegistry;
            import io.tapdata.entity.schema.TapTable;
            import io.tapdata.entity.schema.TapField;
            import io.tapdata.entity.schema.type.TapNumber;
            import io.tapdata.pdk.apis.context.TapConnectionContext;
            import io.tapdata.pdk.apis.entity.ConnectionOptions;
            import io.tapdata.pdk.apis.entity.TestItem;
            import java.util.*;
            import java.util.concurrent.CountDownLatch;
            import java.util.function.Consumer;
            public class BlockingMetadata implements TapConnector {
                public void registerCapabilities(ConnectorFunctions f,TapCodecsRegistry codecs){
                    f.supportQueryByAdvanceFilter((c,q,t,s)->{throw new AssertionError("metadata cannot query business rows");});
                }
                public void init(TapConnectionContext c){}
                public void stop(TapConnectionContext c){
                    Map<String,CountDownLatch> gates=(Map)System.getProperties().get(c.getConnectionConfig().getString("gateKey"));
                    gates.get("entered").countDown();
                    while(gates.get("release").getCount()!=0)try{gates.get("release").await();}catch(InterruptedException ignored){}
                }
                public void discoverSchema(TapConnectionContext c,List<String> names,int limit,Consumer<List<TapTable>> sink){
                    var table=new TapTable(names.get(0));table.add(new TapField("id","BIGINT").tapType(new TapNumber().bit(64).scale(0)));
                    sink.accept(List.of(table));
                }
                public ConnectionOptions connectionTest(TapConnectionContext c,Consumer<TestItem> sink){return ConnectionOptions.create();}
                public int tableCount(TapConnectionContext c){return 1;}
            }
            """);
        var digest=MessageDigest.getInstance("SHA-256");try(var input=Files.newInputStream(jar)){var bytes=new byte[8192];for(int n;(n=input.read(bytes))!=-1;)digest.update(bytes,0,n);}
        return new ConnectorRef(List.of(jar),"synthetic.BlockingMetadata","2.0.8",null,null,HexFormat.of().formatHex(digest.digest()),false);
    }
}
