package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MongoCaptureClearControlsTest {
    private static final String OP="00000000-0000-0000-0000-000000000001";
    private static final String NEXT="00000000-0000-0000-0000-000000000002";
    private static final String HASH="a".repeat(64);
    private static final String CHAIN="srs-clear-"+"b".repeat(64);
    @Test void exactMarkerRoundTripsAndOnlyCancellationAllowsOrdinaryAccess() {
        var plain=new Document("captureId","capture").append("formatVersion",1);
        assertThat(MongoCaptureClearControls.read(plain)).isEmpty();assertThat(MongoCaptureClearControls.ordinary(plain)).isSameAs(plain);
        for(var phase:MongoCaptureClearControls.Phase.values()) {
            var original=authority(OP,1,HASH,CHAIN,NEXT);var root=MongoCaptureClearControls.candidate(plain,original,phase,HASH);
            var control=new MongoCaptureClearControls.Control(OP,1,HASH,CHAIN,NEXT,phase,HASH);
            assertThat(root).isEqualTo(new Document(plain).append("formatVersion",3).append("clearControl",new Document("operationId",OP).append("sequence",1L)
                    .append("requestHash",HASH).append("nextChain",CHAIN).append("nextPreparationId",NEXT).append("phase",phase.name()).append("receiptHash",HASH)));
            assertThat(MongoCaptureClearControls.read(root)).contains(control);assertThat(MongoCaptureClearControls.same(control,original)).isTrue();
            if(phase==MongoCaptureClearControls.Phase.CANCELLED)assertThat(MongoCaptureClearControls.ordinary(root)).isEqualTo(plain);
            else assertThatThrownBy(() -> MongoCaptureClearControls.ordinary(root)).isInstanceOfSatisfying(TapstateException.class,e -> {
                assertThat(e.code()).isEqualTo(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING);assertThat(e.args()).containsEntry("reason","cache-frozen");
            });
            assertThat(root.get("clearControl")).isEqualTo(MongoCaptureClearControls.encode(control));
        }
    }
    @ParameterizedTest
    @ValueSource(strings={"null-root","tag","recovery","missing-control","extra","sequence-type","sequence-zero","sequence-max","operation","preparation","request","receipt","chain","phase","null-phase","wrong-type"})
    void malformedControlNeverBecomesOrdinaryAccessOrAuthority(String damage) {
        var row=new Document("formatVersion",3).append("clearControl",MongoCaptureClearControls.encode(control(MongoCaptureClearControls.Phase.FROZEN)));
        var marker=row.get("clearControl",Document.class);
        switch(damage) {
            case "tag" -> row.put("formatVersion",3L);
            case "recovery" -> row.put("recoveryControl",new Document());
            case "missing-control" -> row.put("clearControl",null);
            case "extra" -> marker.put("extra",true);
            case "sequence-type" -> marker.put("sequence",1);
            case "sequence-zero" -> marker.put("sequence",0L);
            case "sequence-max" -> marker.put("sequence",Long.MAX_VALUE);
            case "operation" -> marker.put("operationId","invalid");
            case "preparation" -> marker.put("nextPreparationId","invalid");
            case "request" -> marker.put("requestHash","invalid");
            case "receipt" -> marker.put("receiptHash","invalid");
            case "chain" -> marker.put("nextChain","different");
            case "phase" -> marker.put("phase","READY");
            case "null-phase" -> marker.put("phase",null);
            case "wrong-type" -> marker.put("requestHash",7L);
            default -> { }
        }
        var input=damage.equals("null-root")?null:row;
        assertThatThrownBy(() -> MongoCaptureClearControls.read(input)).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    @Test void sameRequiresEveryOriginalCoordinateAndANewerOperationMayOnlyReplaceCancellation() {
        var control=control(MongoCaptureClearControls.Phase.FROZEN);var exact=authority(OP,1,HASH,CHAIN,NEXT);
        assertThat(MongoCaptureClearControls.candidate(root(control),exact,MongoCaptureClearControls.Phase.RETIRED,HASH).get("clearControl",Document.class).getString("phase")).isEqualTo("RETIRED");
        for(var changed:List.of(authority(NEXT,1,HASH,CHAIN,NEXT),authority(OP,2,HASH,CHAIN,NEXT),authority(OP,1,"c".repeat(64),CHAIN,NEXT),authority(OP,1,HASH,"srs-clear-"+"d".repeat(64),NEXT),authority(OP,1,HASH,CHAIN,OP))) {
            assertThat(MongoCaptureClearControls.same(control,changed)).isFalse();
            assertThatThrownBy(() -> MongoCaptureClearControls.candidate(root(control),changed,MongoCaptureClearControls.Phase.FROZEN,HASH)).isInstanceOf(TapstateException.class);
        }
        var cancelled=root(control(MongoCaptureClearControls.Phase.CANCELLED));var later=authority(NEXT,2,HASH,CHAIN,OP);
        assertThat(MongoCaptureClearControls.read(MongoCaptureClearControls.candidate(cancelled,later,MongoCaptureClearControls.Phase.FROZEN,HASH)).orElseThrow().sequence()).isEqualTo(2);
        assertThatThrownBy(() -> MongoCaptureClearControls.candidate(cancelled,authority(NEXT,1,HASH,CHAIN,OP),MongoCaptureClearControls.Phase.FROZEN,HASH)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> MongoCaptureClearControls.candidate(root(control(MongoCaptureClearControls.Phase.RETIRED)),exact,MongoCaptureClearControls.Phase.FROZEN,HASH)).isInstanceOf(TapstateException.class);
    }
    private static MongoCaptureClearControls.Control control(MongoCaptureClearControls.Phase phase){return new MongoCaptureClearControls.Control(OP,1,HASH,CHAIN,NEXT,phase,HASH);}
    private static Document root(MongoCaptureClearControls.Control control){return new Document("formatVersion",3).append("clearControl",MongoCaptureClearControls.encode(control));}
    private static CaptureClearPreparation authority(String operation,long sequence,String hash,String chain,String next) {
        var authority=mock(CaptureClearPreparation.class);when(authority.operationId()).thenReturn(operation);when(authority.sequence()).thenReturn(sequence);when(authority.requestHash()).thenReturn(hash);
        var replacement=mock(CapturePreparation.class);when(replacement.chainId()).thenReturn(chain);when(replacement.preparationId()).thenReturn(next);when(authority.replacement()).thenReturn(replacement);return authority;
    }
}
