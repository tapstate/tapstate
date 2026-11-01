package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.mongodb.client.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.stream.Stream;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Strict decoding of detached durable samples; no mock supplies retirement or storage authority. */
class MongoCaptureOpeningShapesTest {
    private static final String ID="00000000-0000-0000-0000-000000000001";
    private static final CapturePreparation PREPARED=new CapturePreparation(new CacheBusinessScope("cluster",1,"cache"),CapturePreparation.Mode.SHARED_SRS,"chain","capture","mc-"+"a".repeat(64),ID);
    @Test void nativeAndLegacySamplesRemainUnresolvedWhenRead() {
        for(boolean legacy:List.of(false,true)) {
            var row=row(legacy);assertThat(read(row)).isSameAs(row);
            var journal=CaptureOpeningIntervals.read(row.get("openingJournal",Document.class));
            assertThat(journal.closedPrefixThrough()).isZero();assertThat(journal.unresolved()).hasSize(1);
        }
    }
    @ParameterizedTest
    @MethodSource("nativeDamage")
    void malformedNativeOpeningIsUnreadable(String path,Object invalid) {
        var row=row(false);damage(opening(row),path,invalid);assertUnreadable(row);
    }
    static Stream<Arguments> nativeDamage() {
        var args=new ArrayList<Arguments>();
        args.add(Arguments.of("extra",true));args.add(Arguments.of("kind","UNKNOWN"));
        args.add(Arguments.of("state","CLOSED"));args.add(Arguments.of("state",null));
        args.add(Arguments.of("resourceEpoch",1));args.add(Arguments.of("resourceEpoch",0L));args.add(Arguments.of("resourceEpoch",2L));
        args.add(Arguments.of("nodeId",7L));args.add(Arguments.of("nodeId",""));args.add(Arguments.of("bootId"," "));
        args.add(Arguments.of("openingId","not-a-uuid"));args.add(Arguments.of("openingId",null));
        args.add(Arguments.of("originalExecutionFence",null));args.add(Arguments.of("originalExecutionFence.extra",true));
        for(String field:List.of("pipeline","cacheIncarnation"))args.add(Arguments.of("originalExecutionFence."+field,""));
        for(String field:List.of("stateGeneration","claimGeneration","executionGeneration","topologyRevision")) {
            args.add(Arguments.of("originalExecutionFence."+field,"1"));args.add(Arguments.of("originalExecutionFence."+field,-1L));
        }
        args.add(Arguments.of("captureFence",null));args.add(Arguments.of("captureFence.extra",true));
        args.add(Arguments.of("captureFence.ownerNodeId",""));args.add(Arguments.of("captureFence.ownerBootId",7L));
        for(String field:List.of("claimGeneration","executionGeneration","topologyRevision")) {
            args.add(Arguments.of("captureFence."+field,1));args.add(Arguments.of("captureFence."+field,-1L));
        }
        args.add(Arguments.of("nodeId","x".repeat(1024)));
        return args.stream();
    }
    @ParameterizedTest
    @MethodSource("legacyDamage")
    void malformedLegacyCoverageCannotBeUsedAsAnOpeningProof(String path,Object invalid) {
        var row=row(true);damage(opening(row),path,invalid);assertUnreadable(row);
    }
    static Stream<Arguments> legacyDamage() {
        return Stream.of(Arguments.of("extra",true),Arguments.of("state","REGISTERED"),Arguments.of("resourceEpoch",1),Arguments.of("resourceEpoch",0L),Arguments.of("resourceEpoch",2L),
                Arguments.of("openingId",7L),Arguments.of("openingId","wrong-prefix"),Arguments.of("openingId","legacy-invalid"),Arguments.of("coverageDigest",7L),Arguments.of("coverageDigest","invalid"),
                Arguments.of("oldBusinessScope",null),Arguments.of("oldBusinessScope.extra",true),Arguments.of("oldBusinessScope.cacheIncarnation",""),Arguments.of("oldBusinessScope.clusterId","other"),Arguments.of("oldBusinessScope.bindingVersion",2L),Arguments.of("previousCaptureFence.ownerBootId",""));
    }
    @ParameterizedTest
    @MethodSource("controlDamage")
    void missingOrUnversionedControlIsPendingRatherThanOpeningAuthority(String path,Object value) {
        var row=row(false);row.put(path,value);
        assertThatThrownBy(() -> read(row)).isInstanceOfSatisfying(TapstateException.class,e -> {
            assertThat(e.code()).isEqualTo(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING);assertThat(e.args()).containsEntry("reason","control-unproven");
        });
    }
    static Stream<Arguments> controlDamage(){return Stream.of(Arguments.of("formatVersion",1L),Arguments.of("revision",1),Arguments.of("revision",0L),Arguments.of("revision",Long.MAX_VALUE),Arguments.of("resourceEpoch",1),Arguments.of("resourceEpoch",0L));}
    private static void assertUnreadable(Document row){assertThatThrownBy(() -> read(row)).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));}
    private static Document read(Document row) {
        var database=mock(MongoDatabase.class);var table=mock(MongoCollection.class);var query=mock(FindIterable.class);var session=mock(ClientSession.class);
        when(database.getCollection(SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.collectionName())).thenReturn(table);
        when(table.find(eq(session),any(Bson.class))).thenReturn(query);when(query.first()).thenReturn(row);
        var store=new MongoCaptureExternalRetirements(mock(MongoClient.class),database,"cluster",mock(MongoStorageBindingStore.class),mock(MongoRequiredNoteStore.class),() -> {});
        return store.control(session,PREPARED);
    }
    private static Document row(boolean legacy) {
        var entry=new Document("sequence",1L).append("openingId",legacy?"legacy-"+ID:ID).append("resourceEpoch",1L).append("kind",legacy?"LEGACY_UNTRACKED":"NATIVE_INSTANCE").append("state",legacy?"UNTRACKED":"REGISTERED");
        var fence=new Document("ownerNodeId","node").append("ownerBootId","boot").append("claimGeneration",1L).append("executionGeneration",1L).append("topologyRevision",1L);
        if(legacy)entry.append("oldBusinessScope",new Document("clusterId","cluster").append("bindingVersion",1L).append("cacheIncarnation","cache")).append("previousCaptureFence",fence).append("coverageDigest","a".repeat(64));
        else entry.append("nodeId","node").append("bootId","boot").append("originalExecutionFence",new Document("pipeline","flow").append("stateGeneration",1L).append("cacheIncarnation","cache").append("claimGeneration",1L).append("executionGeneration",1L).append("topologyRevision",1L)).append("captureFence",fence);
        return new Document("_id",MongoCaptureExternalRetirements.id(PREPARED)).append("formatVersion",1).append("revision",1L).append("resourceEpoch",1L).append("operationSeq",0L)
                .append("currentPreparation",MongoCapturePreparationFacts.row(PREPARED)).append("sharedNoteScope",null).append("originDigest",null)
                .append("openingJournal",CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(),0,entry).document()).append("aliasAuthorities",List.of()).append("operation",null).append("lastResult",null).append("gcPrefix",null);
    }
    private static Document opening(Document row){return row.get("openingJournal",Document.class).getList("unresolved",Document.class).getFirst();}
    private static void damage(Document row,String path,Object value){var parts=path.split("\\.");for(int i=0;i<parts.length-1;i++)row=row.get(parts[i],Document.class);row.put(parts[parts.length-1],value);}
}
