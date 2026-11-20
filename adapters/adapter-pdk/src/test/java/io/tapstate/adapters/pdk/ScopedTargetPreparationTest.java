package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.common.StringType;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ScopedTargetPreparationTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","pipeline",7);
    private static final PipelineNode NODE=new PipelineNode("pipeline","sink");
    private static final WorkloadClaimFence CLAIM=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("node","boot"),3,4,1);
    private static final TargetTable TARGET=new TargetTable("orders",List.of(new TargetField("id","bigint",true)),List.of(new TargetIndex(List.of("id"),true)));
    private static final TargetTable DEFERRED = new TargetTable("orders", List.of(
            new TargetField("id", "bigint", true), TargetField.deferredDecimal("amount", decimalType())),
            List.of(new TargetIndex(List.of("id"), true)));

    @Test
    void aNewDeferredUnwindTargetVerifiesItsLogicalIndexAfterPreparation(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Throwable {
        var calls = new ArrayList<String>();
        var indexes = new ArrayList<io.tapdata.entity.schema.TapIndex>();
        var deferredUnwind = new TargetTable("orders", List.of(
                new TargetField("_id", "string", true, io.tapstate.core.common.TapstateType.STRING),
                new TargetField("position", "long", true, io.tapstate.core.common.TapstateType.INT64),
                TargetField.deferredDecimal("amount", decimalType())),
                List.of(new TargetIndex(List.of("_id", "position"), true)),
                new UnwindIdentity(List.of("_id"), "_unwind_source_id"));
        var targets = Map.of("expanded", deferredUnwind);
        var config = new SinkConfig("mongodb", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null,
                NODE, OnFullLoad.CLEAR, true);
        var connector = PdkConnector.open("mongodb", IdentityConnectorFixture.ref(directory), Map.of());
        connector.functions().supportGetTableNamesFunction((context, size, out) ->
                out.accept(indexes.isEmpty() ? List.of() : List.of("orders")));
        connector.functions().supportClearTable((context, event) -> calls.add("clear"));
        connector.functions().supportCreateIndex((context, model, event) -> indexes.addAll(event.getIndexList()));
        connector.functions().supportQueryIndexes((context, table, out) -> out.accept(List.copyOf(indexes)));
        var frozen = PdkTargetIdentityPlanner.assess(connector, config, targets);
        var store = new Receipts(calls, frozen.get("expanded").target());
        try (var writer = new PdkSinkWriter(connector, (context, events, table, result) -> {
            throw new AssertionError("the empty snapshot cannot write rows");
        }, config, targets, null, true, null, PipelineNode.Purpose.SYNC, binding(store), frozen)) {
            writer.prepareTargets();
            assertThat(indexes).isEmpty();
            assertThat(calls).isEmpty();

            writer.snapshotDelivered("expanded");

            assertThat(indexes).anySatisfy(index -> {
                assertThat(index.getUnique()).isTrue();
                assertThat(index.getIndexFields()).extracting(io.tapdata.entity.schema.TapIndexField::getName)
                        .containsExactly("_unwind_source_id", "position");
            });
            assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);
        }
    }

    @Test
    void emptyDeferredSnapshotPreparesAtTheDeliveredBoundaryWithoutWritingRows(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        var connector = PdkConnector.open("demo", new ConnectorRef(List.of(Synthetic.countingSink(directory)),
                "synthetic.CountingSink", "2.0.8", null), Map.of());
        var configured = functions(calls);
        connector.functions().supportCreateTableV2(configured.getCreateTableV2Function());
        connector.functions().supportClearTable(configured.getClearTableFunction());
        connector.functions().supportCreateIndex(configured.getCreateIndexFunction());
        var config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null,
                NODE, OnFullLoad.CLEAR, true);
        try (var writer = new PdkSinkWriter(connector,
                (context, events, table, result) -> { throw new AssertionError("an empty snapshot cannot write rows"); },
                config, Map.of("input", DEFERRED), null, true, null, PipelineNode.Purpose.SYNC, binding(store))) {
            writer.prepareTargets();
            assertThat(calls).isEmpty();
            writer.snapshotDelivered("input");
            writer.snapshotDelivered("input");
            assertThat(calls).containsExactly("issued", "create", "clear", "ready", "index");
            assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);
        }
    }

    @Test
    void invalidDeferredValuesCannotClearOrWriteTheTarget(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        var connector = PdkConnector.open("demo", new ConnectorRef(List.of(Synthetic.countingSink(directory)),
                "synthetic.CountingSink", "2.0.8", null), Map.of());
        connector.functions().supportClearTable((context, event) -> calls.add("clear"));
        var config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null,
                NODE, OnFullLoad.CLEAR, true);
        var event = io.tapstate.core.event.Envelope.insert(1L, "input", Map.of("id", 1L, "amount", new BigDecimal("1.25")), null);
        try (var writer = new PdkSinkWriter(connector,
                (context, events, table, result) -> { throw new AssertionError("unvalidated output cannot be written"); },
                config, Map.of("input", DEFERRED), null, true, null, PipelineNode.Purpose.SYNC, binding(store))) {
            assertThatThrownBy(() -> writer.write(List.of(event)).toCompletableFuture().get(3, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TapstateException.class)
                    .hasStackTraceContaining("matching source numeric descriptor");
            assertThat(calls).isEmpty();
            assertThat(store.held.revision()).isZero();
        }
    }

    @Test
    void deferredWritesPrepareOnceAfterBothImagesHaveBeenValidated(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        var connector = PdkConnector.open("demo", new ConnectorRef(List.of(Synthetic.countingSink(directory)),
                "synthetic.CountingSink", "2.0.8", null), Map.of());
        var configured = functions(calls);
        connector.functions().supportCreateTableV2(configured.getCreateTableV2Function());
        connector.functions().supportClearTable(configured.getClearTableFunction());
        connector.functions().supportCreateIndex(configured.getCreateIndexFunction());
        var number = decimalType();
        var type = new io.tapdata.entity.schema.type.TapNumber().fixed(number.fixed()).precision(number.precision())
                .scale(number.scale()).minValue(number.minValue()).maxValue(number.maxValue());
        var original = new io.tapstate.core.event.ConvertedValue(new BigDecimal("1.25"), "DECIMAL128",
                "TapNumberValue", PdkJsonProvider.parser().toJson(type), null);
        var event = io.tapstate.core.event.Envelope.update(1L, "input",
                Map.of("id", 1L, "amount", original), Map.of("id", 1L, "amount", "changed"), null);
        var config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null,
                NODE, OnFullLoad.CLEAR, true);
        try (var writer = new PdkSinkWriter(connector, (context, events, table, result) -> {
            assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);
            assertThat(table.getNameFieldMap()).containsOnlyKeys("id");
            var update = (io.tapdata.entity.event.dml.TapUpdateRecordEvent) events.getFirst();
            assertThat(update.getBefore()).containsEntry("amount", new BigDecimal("1.25"));
            assertThat(update.getAfter()).containsEntry("amount", "changed");
            calls.add("write");
            result.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>(0L, 1L, 0L));
        }, config, Map.of("input", DEFERRED), null, true, null, PipelineNode.Purpose.SYNC, binding(store))) {
            assertThat(writer.write(List.of(event)).toCompletableFuture().get(3, java.util.concurrent.TimeUnit.SECONDS).written())
                    .isEqualTo(1);
            assertThat(writer.write(List.of(event)).toCompletableFuture().get(3, java.util.concurrent.TimeUnit.SECONDS).written())
                    .isEqualTo(1);
        }
        assertThat(calls).containsExactly("issued", "create", "clear", "ready", "index", "write", "write");
    }

    @Test
    void theEmptySnapshotBoundaryRetainsCodedFailuresAndCodesConnectorFailures(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Throwable {
        var ref = new ConnectorRef(List.of(Synthetic.countingSink(directory)), "synthetic.CountingSink", "2.0.8", null);
        var coded = new TapstateException(ConnectorError.WRITE_FAILED, Map.of("connector", "demo"), null);
        for (var failure : List.of(coded, new IllegalStateException("clear failed"))) {
            var calls = new ArrayList<String>();
            var store = new Receipts(calls, DEFERRED);
            var connector = PdkConnector.open("demo", ref, Map.of());
            connector.functions().supportCreateTableV2((context, event) -> CreateTableOptions.create().tableExists(true));
            connector.functions().supportClearTable((context, event) -> { throw failure; });
            var config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null,
                    NODE, OnFullLoad.CLEAR, true);
            try (var writer = new PdkSinkWriter(connector,
                    (context, events, table, result) -> { throw new AssertionError("no rows expected"); },
                    config, Map.of("input", DEFERRED), null, true, null, PipelineNode.Purpose.SYNC, binding(store))) {
                if (failure == coded) {
                    assertThatThrownBy(() -> writer.snapshotDelivered("input")).isSameAs(coded);
                } else {
                    assertThatThrownBy(() -> writer.snapshotDelivered("input"))
                            .isInstanceOfSatisfying(TapstateException.class,
                                    error -> assertThat(error.code()).isEqualTo(ConnectorError.WRITE_FAILED))
                            .hasCause(failure);
                }
                assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.PREPARING);
            }
        }
    }

    @Test
    void aDeferredPreparedWriterClaimsClearOnceAndReusesTheConfirmedReceipt() throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        var functions = functions(calls);
        var first = deferredPreparation(functions, store, () -> {});

        first.prepareForWrite(DEFERRED, TargetTapTable.build(DEFERRED));
        int readsAfterPreparing = store.reads;
        first.prepareForWrite(DEFERRED, TargetTapTable.build(DEFERRED));
        assertThat(store.reads).isEqualTo(readsAfterPreparing);
        assertThat(calls).containsExactly("issued", "clear", "create", "ready", "index");
        assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);

        calls.clear();
        deferredPreparation(functions, store, () -> {}).prepareForWrite(DEFERRED, TargetTapTable.build(DEFERRED));
        assertThat(calls).containsExactly("index");
    }

    @Test
    void aDeferredWriterLosingTheClaimUsesTheOtherWritersReadyReceipt() throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        store.beforeBegin = () -> {
            store.held = new SinkPreparationStore.Snapshot(store.held.input(), store.held.operationId(), 2,
                    SinkPreparationStore.Phase.READY, CLAIM, new SinkPreparationStore.Result(false));
        };

        deferredPreparation(functions(calls), store, () -> {}).prepareForWrite(DEFERRED, TargetTapTable.build(DEFERRED));

        assertThat(calls).containsExactly("index");
        assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);
    }

    @Test
    void anInterruptedDeferredWaitDoesNotReplayAnUnknownClear() {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        store.begin(store.held.input());
        calls.clear();
        var preparation = deferredPreparation(functions(calls), store, () -> {});
        var table = TargetTapTable.build(DEFERRED);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> preparation.prepareForWrite(DEFERRED, table))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_STATE_REBUILD_REQUIRED));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(store.held.revision()).isEqualTo(1);
            assertThat(calls).isEmpty();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void waitingDeferredWritersObserveTheFirstWritersCompletion() throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls, DEFERRED);
        store.begin(store.held.input());
        calls.clear();
        var waitingReads = new java.util.concurrent.CountDownLatch(1);
        store.onLoad = () -> {
            if (store.reads >= 3) waitingReads.countDown();
        };
        try (var workers = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var waiting = workers.submit(() -> org.junit.jupiter.api.Assertions.assertDoesNotThrow(() ->
                    deferredPreparation(functions(calls), store, () -> {})
                            .prepareForWrite(DEFERRED, TargetTapTable.build(DEFERRED))));
            assertThat(waitingReads.await(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(waiting).isNotDone();
            store.complete(new Receipts.Original(store.held), new SinkPreparationStore.Result(false));
            waiting.get(3, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertThat(calls).containsExactly("ready", "index");
    }

    private static NumericType decimalType() {
        return new NumericType(null, true, null, null, new BigDecimal("-99999999.99"),
                new BigDecimal("99999999.99"), 10, 2);
    }

    private static PdkTargetPreparation deferredPreparation(ConnectorFunctions functions, Receipts store, Runnable active) {
        return new PdkTargetPreparation("mongodb", null, functions, OnFullLoad.CLEAR, true,
                NODE, null, true, PipelineNode.Purpose.SYNC, active, binding(store));
    }
    @Test void knownPreparationIsPublishedBeforeIndexAndRestartNeverClearsOrCreatesAgain()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var functions=functions(calls);
        functions.supportCreateIndex((context,table,event) -> {assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);calls.add("index-failed");throw new IllegalStateException("index failed");});
        assertThatThrownBy(() -> preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET))).hasMessage("index failed");
        assertThat(calls).containsExactly("issued","create","clear","ready","index-failed");calls.clear();
        functions.supportCreateIndex((context,table,event) -> calls.add("index"));
        preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET));assertThat(calls).containsExactly("index");
    }
    @Test void unknownExternalClearCannotBeIssuedAgainAfterReopening()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var functions=functions(calls);
        functions.supportClearTable((context,event) -> {calls.add("clear-unknown");throw new IllegalStateException("response lost");});
        assertThatThrownBy(() -> preparation("mongodb",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET))).hasMessage("response lost");
        assertThat(calls).containsExactly("issued","clear-unknown");calls.clear();
        assertThatThrownBy(() -> preparation("mongodb",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET))).isInstanceOf(TapstateException.class);
        assertThat(calls).isEmpty();assertThat(store.held.revision()).isEqualTo(1);
    }
    @Test void losingOriginalAuthorityAfterTargetClearCannotPublishReady()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var active=new AtomicBoolean(true);var functions=functions(calls);
        functions.supportClearTable((context,event) -> {calls.add("clear");active.set(false);});
        assertThatThrownBy(() -> preparation("mysql",functions,store,() -> {if(!active.get())throw refused();}).prepare(TARGET,TargetTapTable.build(TARGET))).isInstanceOf(TapstateException.class);
        assertThat(calls).containsExactly("issued","create","clear");assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.PREPARING);
    }
    @Test void preparedWriterRequiresReadyWithoutEverPreparingTheTarget() {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var binding=binding(store);
        assertThatThrownBy(() -> binding.requireReady(TARGET)).isInstanceOf(TapstateException.class);assertThat(calls).isEmpty();
        var attempt=store.begin(store.held.input());store.complete(attempt,new SinkPreparationStore.Result(true));calls.clear();binding.requireReady(TARGET);assertThat(calls).isEmpty();
    }
    @Test void preparedWritesReuseOnlyTheExactReadyModel() throws Throwable {
        var calls = new ArrayList<String>();
        var store = new Receipts(calls);
        store.complete(store.begin(store.held.input()), new SinkPreparationStore.Result(true));
        calls.clear();
        var preparation = new PdkTargetPreparation("demo", null, functions(calls), OnFullLoad.CLEAR, true,
                NODE, null, true, PipelineNode.Purpose.SYNC, () -> {}, binding(store));

        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        int readsAtOpen = store.reads;
        preparation.prepareForWrite(TARGET, TargetTapTable.build(TARGET));
        preparation.prepareForWrite(TARGET, TargetTapTable.build(TARGET));
        assertThat(store.reads).isEqualTo(readsAtOpen);
        assertThat(calls).isEmpty();

        var changed = new TargetTable(TARGET.name(), List.of(new TargetField("different", "bigint", true)));
        assertThatThrownBy(() -> preparation.prepareForWrite(changed, TargetTapTable.build(changed)))
                .isInstanceOf(TapstateException.class);
        assertThat(store.reads).isEqualTo(readsAtOpen + 1);
    }

    @Test void createdPrimaryKeyResultIsReusedWhenIndexRetryOpensAgain()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var functions=functions(calls);
        functions.supportCreateTableV2((context,event) -> {calls.add("create-new");return new CreateTableOptions().tableExists(false);});
        preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET));assertThat(calls).containsExactly("issued","create-new","ready");calls.clear();
        preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET));assertThat(calls).isEmpty();assertThat(store.held.result().created()).isTrue();
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"policy","node","purpose","mode"})
    void differentActualPreparationCannotConsumeTheOriginalReceipt(String change) {
        var calls=new ArrayList<String>();var store=new Receipts(calls);
        assertThatThrownBy(() -> new PdkTargetPreparation("mysql",null,functions(calls),
                change.equals("policy")?OnFullLoad.APPEND:OnFullLoad.CLEAR,!change.equals("mode"),
                change.equals("node")?new PipelineNode("pipeline","other"):NODE,null,false,
                change.equals("purpose")?PipelineNode.Purpose.VIEW:PipelineNode.Purpose.SYNC,() -> {},binding(store))
                .prepare(TARGET,TargetTapTable.build(TARGET))).isInstanceOf(TapstateException.class);
        assertThat(calls).isEmpty();assertThat(store.held.revision()).isZero();
    }
    @Test void sameNamedAliasWithAnotherModelCannotBypassThePreparedMemo()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var preparation=preparation("mysql",functions(calls),store,() -> {});
        preparation.prepare(TARGET,TargetTapTable.build(TARGET));calls.clear();
        var changed=new TargetTable(TARGET.name(),List.of(new TargetField("another","bigint",true)));
        assertThatThrownBy(() -> preparation.prepare(changed,TargetTapTable.build(changed))).isInstanceOf(TapstateException.class);assertThat(calls).isEmpty();
    }
    @Test void nativeWriterCannotWriteAnUnmappedInputStream(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory)throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var writes=new java.util.concurrent.atomic.AtomicInteger();
        var ref=new ConnectorRef(List.of(Synthetic.countingSink(directory)),"synthetic.CountingSink","2.0.8",null);
        var connector=PdkConnector.open("demo",ref,Map.of());
        var config=new SinkConfig("demo",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,null,NODE,OnFullLoad.CLEAR,true);
        try(var writer=new PdkSinkWriter(connector,(context,rows,table,result) -> {writes.incrementAndGet();result.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>(1L,0L,0L));},config,Map.of("known",TARGET),null,true,null,PipelineNode.Purpose.SYNC,binding(store))) {
            assertThatThrownBy(() -> writer.write(List.of(io.tapstate.core.event.Envelope.insert(1,"unmapped",Map.of("id",1),null))).toCompletableFuture().get(5,java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TapstateException.class);assertThat(writes).hasValue(0);
        }
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={true,false})
    void hostReceiptErrorKeepsItsIdentityThroughTheActualAsyncWriter(boolean fatal,@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory)throws Throwable {
        Throwable failure=fatal?new AssertionError("host invariant"):new IllegalStateException("host invariant");
        SinkPreparationStore broken=(SinkPreparationStore)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{SinkPreparationStore.class},(proxy,method,args) -> {throw failure;});
        var receipts=new PdkSinkPreparations(broken,SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.CLEAR,true);
        var connector=PdkConnector.open("demo",new ConnectorRef(List.of(Synthetic.countingSink(directory)),"synthetic.CountingSink","2.0.8",null),Map.of());
        var config=new SinkConfig("demo",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,null,NODE,OnFullLoad.CLEAR,true);
        try(var writer=new PdkSinkWriter(connector,(context,rows,table,result) -> {throw new AssertionError("no target write is allowed");},config,Map.of("known",TARGET),null,false,null,PipelineNode.Purpose.SYNC,receipts)) {
            assertThatThrownBy(() -> writer.write(List.of(io.tapstate.core.event.Envelope.insert(1,"known",Map.of("id",1),null))).toCompletableFuture().get(5,java.util.concurrent.TimeUnit.SECONDS))
                    .cause().isSameAs(failure);
        }
    }
    @Test void targetHashRetainsAllOrderedFieldsIndexesAttributesAndLosslessText() {
        String original=PdkSinkPreparations.modelHash(TARGET);assertThat(original).matches("[0-9a-f]{64}");
        assertThat(PdkSinkPreparations.modelHash(new TargetTable(TARGET.name(),TARGET.fields(),TARGET.indexes()))).isEqualTo(original);
        var variants=List.of(new TargetTable("orders2",TARGET.fields(),TARGET.indexes()),new TargetTable("orders",List.of(new TargetField("id","bigint",false)),TARGET.indexes()),
                new TargetTable("orders",TARGET.fields(),List.of(new TargetIndex(List.of("id"),false))),new TargetTable("orders",List.of(new TargetField("id",null,true)),TARGET.indexes()),
                new TargetTable("orders",List.of(new TargetField("id","bigint",true,null,new NumericType(64,true,false,false,BigDecimal.ZERO,BigDecimal.TEN,18,0),null)),TARGET.indexes()),
                new TargetTable("orders",List.of(new TargetField("id","bigint",true,null,null,new StringType(4L,false,false,1L,2))),TARGET.indexes()));
        for(var target:variants)assertThat(PdkSinkPreparations.modelHash(target)).isNotEqualTo(original);
        assertThat(PdkSinkPreparations.modelHash(new TargetTable("x\ud800",List.of()))).isNotEqualTo(PdkSinkPreparations.modelHash(new TargetTable("x\udc00",List.of())));
        assertThat(PdkSinkPreparations.modelHash(new TargetTable("a",List.of(new TargetField("b:c","d",false))))).isNotEqualTo(PdkSinkPreparations.modelHash(new TargetTable("a",List.of(new TargetField("b","c:d",false)))));
    }
    private static PdkSinkPreparations binding(Receipts store){return new PdkSinkPreparations(store,SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.CLEAR,true);}
    private static PdkTargetPreparation preparation(String id,ConnectorFunctions functions,Receipts store,Runnable active){return new PdkTargetPreparation(id,null,functions,OnFullLoad.CLEAR,true,NODE,null,false,PipelineNode.Purpose.SYNC,active,binding(store));}
    private static ConnectorFunctions functions(List<String> calls){var functions=new ConnectorFunctions();functions.supportCreateTableV2((context,event) -> {calls.add("create");return new CreateTableOptions().tableExists(true);});functions.supportClearTable((context,event) -> calls.add("clear"));functions.supportCreateIndex((context,table,event) -> calls.add("index"));return functions;}
    private static TapstateException refused(){return new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",SCOPE.pipelineId()),null);}
    private static final class Receipts implements SinkPreparationStore {
        private final List<String> calls;private volatile Snapshot held;private int reads;
        private Runnable beforeBegin = () -> {};
        private Runnable onLoad = () -> {};
        Receipts(List<String> calls){this(calls, TARGET);}
        Receipts(List<String> calls, TargetTable target){this.calls=calls;held=new Snapshot(PdkSinkPreparations.input(SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.CLEAR,true,target),UUID.randomUUID().toString(),0,Phase.PREPARING,null,null);}
        public void prepareTargets(List<Input> inputs){throw new AssertionError("plugin code cannot grant its own preparation roster");}
        public synchronized Snapshot load(Input input){reads++;if(!input.equals(held.input()))throw refused();onLoad.run();return held;}
        public synchronized Attempt begin(Input input){beforeBegin.run();load(input);if(held.revision()!=0)throw refused();held=new Snapshot(input,held.operationId(),1,Phase.PREPARING,CLAIM,null);calls.add("issued");return new Original(held);}
        public synchronized Snapshot complete(Attempt attempt,Result result){assertThat(attempt.original()).isEqualTo(held);held=new Snapshot(held.input(),held.operationId(),2,Phase.READY,CLAIM,result);calls.add("ready");return held;}
        private record Original(Snapshot original)implements Attempt{}
    }
}
