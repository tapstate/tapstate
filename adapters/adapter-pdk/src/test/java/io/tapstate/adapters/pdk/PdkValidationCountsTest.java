package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.ConnectionConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class PdkValidationCountsTest {
    @TempDir static Path directory;
    static ConnectorRef artifact;
    @BeforeAll static void fixture() {
        var jar = SyntheticJar.compileToJar(directory, "synthetic.NativeCount", """
            package synthetic;
            import io.tapdata.pdk.apis.TapConnector;
            import io.tapdata.pdk.apis.functions.ConnectorFunctions;
            import io.tapdata.entity.codec.TapCodecsRegistry;
            import io.tapdata.entity.schema.TapTable;
            import io.tapdata.pdk.apis.context.TapConnectionContext;
            import io.tapdata.pdk.apis.context.TapConnectorContext;
            import io.tapdata.pdk.apis.entity.ConnectionOptions;
            import io.tapdata.pdk.apis.entity.TestItem;
            import io.tapdata.pdk.apis.entity.ExecuteResult;
            import io.tapdata.pdk.apis.functions.connection.TableInfo;
            import java.util.*;
            import java.util.concurrent.*;
            import java.util.concurrent.atomic.AtomicInteger;
            import java.util.function.Consumer;
            public class NativeCount implements TapConnector {
                private void loader() {
                    if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader())
                        throw new AssertionError("wrong loader");
                }
                private String mode(TapConnectionContext c) { return c.getConnectionConfig().getString("mode"); }
                private void called(TapConnectionContext c, String name) {
                    ((AtomicInteger)c.getConnectionConfig().get(name)).incrementAndGet();
                }
                public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry codecs) {
                    loader();
                    f.supportGetTableNamesFunction((c,batch,sink) -> {
                        loader(); called(c,"metadata");
                        if (batch!=64) throw new AssertionError("metadata batch");
                        sink.accept(mode(c).equals("missing")?List.of():mode(c).equals("catalog")?List.of("records","other_records"):List.of("records"));
                    });
                    f.supportBatchCount((c,t) -> { throw new AssertionError("estimate fallback"); });
                    f.supportCountByPartitionFilterFunction((c,t,q) -> {
                        loader(); called(c,"counts");
                        if (!t.getId().equals("records") || q.getLimit()!=null || q.getSkip()!=null)
                            throw new AssertionError("wrong count scope");
                        if (mode(c).equals("missing")) throw new Exception("missing namespace");
                        if (mode(c).equals("fail")) throw new Exception("native count failed");
                        return mode(c).equals("zero") ? 0L : 9007199254740993L;
                    });
                    f.supportGetTableInfoFunction((c,t) -> {
                        loader(); called(c,"metadata");
                        if (mode(c).equals("missing")) throw new Exception("missing namespace");
                        throw new AssertionError("statistics cannot prove existence");
                    });
                    f.supportExecuteCommandFunction((c,command,sink) -> {
                        loader(); called(c,"counts");
                        if (mode(c).equals("protocol")) throw new AssertionError("a logical id cannot select a document protocol");
                        if (!command.getCommand().equals("count") || !command.getParams().get("database").equals("db")
                            || !command.getParams().get("collection").equals("records")
                            || !((Map)command.getParams().get("filter")).isEmpty()) throw new AssertionError("wrong count command");
                        sink.accept(new ExecuteResult<Long>().result(9007199254740993L));
                    });
                }
                public void init(TapConnectionContext c) throws Throwable {
                    loader(); called(c,"inits");
                    if (mode(c).equals("blocked")) {
                        ((CountDownLatch)c.getConnectionConfig().get("entered")).countDown();
                        ((CountDownLatch)c.getConnectionConfig().get("release")).await();
                    }
                }
                public void stop(TapConnectionContext c) throws Throwable {
                    loader(); called(c,"stops");
                    if (mode(c).equals("cleanup") && ((AtomicInteger)c.getConnectionConfig().get("stops")).get()==1)
                        throw new Exception("first cleanup failed");
                }
                public void discoverSchema(TapConnectionContext c,List<String> t,int n,Consumer<List<TapTable>> sink) {
                    throw new AssertionError("count must not discover or sample business values");
                }
                public ConnectionOptions connectionTest(TapConnectionContext c,Consumer<TestItem> sink) { return ConnectionOptions.create(); }
                public int tableCount(TapConnectionContext c) { return 1; }
            }
            """);
        artifact = new ConnectorRef(List.of(jar), "synthetic.NativeCount", "2.0.8", null);
    }
    static Map<String,Object> settings(String mode) {
        return Map.of("mode", mode,"database","db","counts",new AtomicInteger(),"metadata",new AtomicInteger(),
                "inits",new AtomicInteger(),"stops",new AtomicInteger(),"entered",new CountDownLatch(1),"release",new CountDownLatch(1));
    }
    static PdkValidationCounts read(Map<String,Object> settings, PdkValidationCounts.Operation operation) {
        return new PdkValidationCounts(new ConnectionConfig("source","fixture",settings), artifact,
                new PdkValidationCounts.Request("records", "db", operation), () -> {});
    }
    @Test void metadataInspectionRejectsMissingNativeMethodsWithoutTryingStatistics(){
        var jar=SyntheticJar.compileToJar(directory,"synthetic.NoNativeCount","""
            package synthetic;
            import io.tapdata.pdk.apis.TapConnector;
            import io.tapdata.pdk.apis.functions.ConnectorFunctions;
            import io.tapdata.entity.codec.TapCodecsRegistry;
            import io.tapdata.entity.schema.TapTable;
            import io.tapdata.pdk.apis.context.TapConnectionContext;
            import io.tapdata.pdk.apis.entity.ConnectionOptions;
            import io.tapdata.pdk.apis.entity.TestItem;
            import java.util.*;
            import java.util.function.Consumer;
            public class NoNativeCount implements TapConnector {
                public void registerCapabilities(ConnectorFunctions functions,TapCodecsRegistry codecs){
                    functions.supportGetTableNamesFunction((context,batch,sink)->sink.accept(List.of("records")));
                    functions.supportBatchCount((context,table)->{throw new AssertionError("estimated statistics are not native count capability");});
                }
                public void init(TapConnectionContext context){}
                public void stop(TapConnectionContext context){}
                public void discoverSchema(TapConnectionContext context,List<String> names,int size,Consumer<List<TapTable>> sink){throw new AssertionError("value discovery is not count capability");}
                public ConnectionOptions connectionTest(TapConnectionContext context,Consumer<TestItem> sink){return ConnectionOptions.create();}
                public int tableCount(TapConnectionContext context){return 1;}
            }
            """);
        var noCount=new ConnectorRef(List.of(jar),"synthetic.NoNativeCount","2.0.8",null);
        for(var operation:PdkValidationCounts.Operation.values()){
            try(var reader=new PdkValidationCounts(new ConnectionConfig("source","fixture",Map.of("database","db")),noCount,
                    new PdkValidationCounts.Request("records","db",operation),()->{})){
                assertThatThrownBy(reader::inspect).isInstanceOfSatisfying(TapstateException.class,
                        failed->assertThat(failed.code()).isEqualTo(ConnectorError.CAPABILITY_MISSING));
                assertThat(reader.drained()).isTrue();assertThat(reader.callExited().toCompletableFuture()).isCompletedExceptionally();
            }
        }
    }

    @Test void metadataInspectionNeverCountsOrDiscoversValueModelsAndRetainsOneOriginalAttempt(){
        for(var operation:PdkValidationCounts.Operation.values()){
            var values=settings("fail");
            try(var reader=read(values,operation)){
                assertThat(reader.inspect()).isEqualTo(operation==PdkValidationCounts.Operation.NATIVE_PROTOCOL
                        ?PdkValidationCounts.Operation.PARTITION_FILTER:operation);
                assertThat((AtomicInteger)values.get("counts")).hasValue(0);assertThat((AtomicInteger)values.get("metadata")).hasValue(1);
                assertThat((AtomicInteger)values.get("inits")).hasValue(1);assertThat(reader.drained()).isTrue();
                assertThat(reader.callExited().toCompletableFuture()).isCompleted();
                assertThatThrownBy(reader::count).isInstanceOf(TapstateException.class);
                assertThatThrownBy(reader::inspect).isInstanceOf(TapstateException.class);
            }
        }
        var missing=settings("missing");
        try(var reader=read(missing,PdkValidationCounts.Operation.NATIVE_PROTOCOL)){
            assertThatThrownBy(reader::inspect).isInstanceOfSatisfying(TapstateException.class,
                    failed->assertThat(failed.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
            assertThat((AtomicInteger)missing.get("counts")).hasValue(0);assertThat(reader.drained()).isTrue();
        }
    }

    @Test void exactNativeScalarHasNoBusinessRowsDiscoveryOrEstimateFallback() {
        for (var operation : PdkValidationCounts.Operation.values()) {
            var settings = settings("large");
            try (var reader = read(settings, operation)) {
                assertThat(reader.count()).isEqualTo(9007199254740993L);
                assertThat(reader.drained()).isTrue();
                assertThat(reader.callExited().toCompletableFuture()).isCompleted();
                assertThat((AtomicInteger)settings.get("counts")).hasValue(1);
                assertThat((AtomicInteger)settings.get("metadata")).hasValue(2);
                assertThatThrownBy(reader::count).isInstanceOf(TapstateException.class);
            }
        }
        try (var reader=read(settings("zero"),PdkValidationCounts.Operation.PARTITION_FILTER)) { assertThat(reader.count()).isZero(); }
    }
    @Test void missingNamespaceAndNativeFailureCannotBecomeZero() {
        for (var mode : List.of("missing","fail")) {
            var settings = settings(mode);
            try (var reader=read(settings,PdkValidationCounts.Operation.PARTITION_FILTER)) {
                assertThatThrownBy(reader::count).isInstanceOfSatisfying(TapstateException.class,
                        failed -> assertThat(failed.code()).isEqualTo(mode.equals("missing")?ValidationError.READ_PROTOCOL_VIOLATION:ValidationError.READ_FAILED));
                assertThat(reader.drained()).isTrue();
                assertThat(reader.callExited().toCompletableFuture()).isCompletedExceptionally();
            }
        }
        var settings=settings("missing");
        try(var reader=read(settings,PdkValidationCounts.Operation.DOCUMENT_COUNT)) {
            assertThatThrownBy(reader::count).isInstanceOf(TapstateException.class);
            assertThat((AtomicInteger)settings.get("counts")).hasValue(0);
        }
    }
    @Test void aLogicalConnectorNameDoesNotGrantOrSelectANativeCapability(){
        var values=settings("protocol");
        try(var reader=new PdkValidationCounts(new ConnectionConfig("source","mongodb",values),artifact,
                new PdkValidationCounts.Request("records","db",PdkValidationCounts.Operation.NATIVE_PROTOCOL),()->{})){
            assertThat(reader.count()).isEqualTo(9007199254740993L);
            assertThat((AtomicInteger)values.get("counts")).hasValue(1);
        }
    }
    @Test void sourceSelectionConsumesOnlyAVerifiedNativeNamespaceBeforeCounting(){
        var values=settings("large");var admissions=new AtomicInteger();
        try(var reader=new PdkValidationCounts(new ConnectionConfig("source","fixture",values),artifact,
                new PdkValidationCounts.Request("records","db",PdkValidationCounts.Operation.NATIVE_PROTOCOL),()->{},actual->{
                    assertThat(actual).isEqualTo("records");
                    assertThat((AtomicInteger)values.get("counts")).hasValue(admissions.getAndIncrement());
                })){
            assertThat(reader.count()).isEqualTo(9007199254740993L);assertThat(admissions).hasValue(2);
        }
        var refused=settings("large");var denied=new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","source-scope"),null);
        try(var reader=new PdkValidationCounts(new ConnectionConfig("source","fixture",refused),artifact,
                new PdkValidationCounts.Request("records","db",PdkValidationCounts.Operation.NATIVE_PROTOCOL),()->{},actual->{throw denied;})){
            assertThatThrownBy(reader::count).isSameAs(denied);
            assertThat((AtomicInteger)refused.get("counts")).hasValue(0);assertThat(reader.drained()).isTrue();
        }
    }
    @Test void scopeAdmissionCanSeeOtherNativeTablesBeforeTheScalarRead(){
        var values=settings("catalog");var seen=new java.util.ArrayList<String>();
        try(var reader=new PdkValidationCounts(new ConnectionConfig("source","fixture",values),artifact,
                new PdkValidationCounts.Request("records","db",PdkValidationCounts.Operation.NATIVE_PROTOCOL),()->{},seen::add)){
            assertThat(reader.count()).isEqualTo(9007199254740993L);
            assertThat(seen).containsExactly("records","other_records","records","other_records");
        }
    }
    @Test void cancellationBeforeDispatchOpensNothing() {
        var settings=settings("large");var reader=read(settings,PdkValidationCounts.Operation.PARTITION_FILTER);
        reader.close();assertThat(reader.drained()).isTrue();
        assertThatThrownBy(reader::count).isInstanceOfSatisfying(TapstateException.class,
                failed->assertThat(failed.code()).isEqualTo(ValidationError.READ_CANCELLED));
        assertThat((AtomicInteger)settings.get("inits")).hasValue(0);
    }
    @Test void preDispatchCloseDeliversExitCallbacksOutsideLifecycleLocks() {
        var reader=read(settings("large"),PdkValidationCounts.Operation.PARTITION_FILTER);
        try(var observer=Executors.newSingleThreadExecutor()) {
            var callback=reader.callExited().thenRun(()->{
                try { assertThat(observer.submit(reader::drained).get(250,TimeUnit.MILLISECONDS)).isTrue(); }
                catch(Exception blocked) { throw new AssertionError("exit callback cannot observe actual drain",blocked); }
            }).toCompletableFuture();
            reader.close();
            assertThat(callback.isDone()).isTrue();assertThat(callback.isCompletedExceptionally()).isFalse();
        }
    }
    @Test void cancellationDuringInitializationWaitsForActualReturnAndStopsLateResources()throws Exception {
        var settings=settings("blocked");var reader=read(settings,PdkValidationCounts.Operation.PARTITION_FILTER);
        var entered=(CountDownLatch)settings.get("entered");var release=(CountDownLatch)settings.get("release");
        try(var io=Executors.newSingleThreadExecutor()) {
            var result=io.submit(reader::count);
            try {
                assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();reader.close();
                assertThat(reader.drained()).isFalse();assertThat(reader.callExited().toCompletableFuture()).isNotDone();
            } finally { release.countDown(); }
            assertThatThrownBy(()->result.get(5,TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            assertThat(reader.drained()).isTrue();assertThat((AtomicInteger)settings.get("counts")).hasValue(0);
            assertThat((AtomicInteger)settings.get("stops")).hasValue(2);
        }finally{release.countDown();reader.close();}
    }
    @Test void failedCleanupKeepsTheOriginalHandleForRetry() {
        var settings=settings("cleanup");var reader=read(settings,PdkValidationCounts.Operation.PARTITION_FILTER);
        assertThatThrownBy(reader::count).isInstanceOf(TapstateException.class);
        assertThat(reader.callExited().toCompletableFuture()).isCompletedExceptionally();
        assertThat(reader.drained()).isFalse();reader.close();assertThat(reader.drained()).isTrue();
        assertThat((AtomicInteger)settings.get("stops")).hasValue(2);
    }
}
