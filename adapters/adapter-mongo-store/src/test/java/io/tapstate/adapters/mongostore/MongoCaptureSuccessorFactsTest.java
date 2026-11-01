package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/** Golden existing recovery shapes; accepting a new predecessor requires its own format contract. */
class MongoCaptureSuccessorFactsTest {
    private static final String BIRTH="00000000-0000-0000-0000-000000000010";
    private static final String CLEAR="00000000-0000-0000-0000-000000000020";
    private static final String RECOVERY="00000000-0000-0000-0000-000000000030";
    @Test void birthAndExistingRecoveryFormatsRoundTripWithoutChangingTheirIdentity() {
        var original=birth();var encoded=MongoCapturePreparationFacts.row(original);
        assertThat(encoded.getInteger("formatVersion")).isEqualTo(1);
        assertThat(MongoCapturePreparationFacts.available(encoded)).isEqualTo(original);
        var next=recovery(original);
        var record=new MongoCapturePreparationFacts.Recovery(original,next,"f".repeat(64),MongoCapturePreparationFacts.RecoveryPhase.RESERVED);
        var stored=MongoCapturePreparationFacts.configurationRow(record);
        assertThat(stored.getInteger("formatVersion")).isEqualTo(2);
        assertThat(MongoCapturePreparationFacts.recovery(stored)).contains(record);
    }
    @Test void theExistingV2RecoveryShapeCannotRepresentAClearSuccessorPredecessor() {
        var original=birth();
        var cleared=new CapturePreparation(original.scope(),original.mode(),MongoCaptureExternalRetirements.clearChain(original,CLEAR),
                original.captureId(),original.sourceIdentityHash(),CLEAR,original.canonicalChainId(),original.birthPreparationId());
        assertThat(MongoCapturePreparationFacts.annex(MongoCapturePreparationFacts.row(cleared))).isEqualTo(cleared);
        // This is the existing writer's exact v2 shape, captured without granting or persisting it.
        var incompatible=new Document(MongoCapturePreparationFacts.row(original)).append("formatVersion",2)
                .append("currentRecovery",new Document("formatVersion",1).append("operationId",RECOVERY).append("planHash","f".repeat(64))
                        .append("previousPhysicalChain",cleared.chainId()).append("previousPreparationId",CLEAR)
                        .append("physicalChain",MongoCapturePreparationFacts.replacementChain(cleared,RECOVERY)).append("phase","RESERVED"));
        assertThat(MongoCapturePreparationFacts.legacyConfigurationRow(new MongoCapturePreparationFacts.Recovery(cleared,recovery(cleared),
                "f".repeat(64),MongoCapturePreparationFacts.RecoveryPhase.RESERVED))).isEqualTo(incompatible);
        assertThatThrownBy(() -> MongoCapturePreparationFacts.recovery(incompatible)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    @Test void theTaggedWrapperKeepsOneClearPredecessorAndTheExistingRecoveryAnnex() {
        var previous=cleared();var next=recovery(previous);
        for(var phase:MongoCapturePreparationFacts.RecoveryPhase.values()) {
            var fact=new MongoCapturePreparationFacts.Recovery(previous,next,"a".repeat(64),phase);
            var encoded=MongoCapturePreparationFacts.configurationRow(fact);
            assertThat(encoded).isEqualTo(new Document("_id",MongoCapturePreparationFacts.id(previous.scope(),previous.captureId()))
                    .append("formatVersion",4).append("original",MongoCapturePreparationFacts.row(previous.birth()))
                    .append("currentRecovery",new Document("formatVersion",2).append("operationId",RECOVERY).append("planHash","a".repeat(64))
                            .append("previous",MongoCapturePreparationFacts.row(previous)).append("replacement",MongoCapturePreparationFacts.row(next)).append("phase",phase.name())));
            assertThat(MongoCapturePreparationFacts.recovery(encoded)).contains(fact);
            assertThat(MongoCapturePreparationFacts.decode(encoded)).isEqualTo(next);
            if(phase==MongoCapturePreparationFacts.RecoveryPhase.READY)assertThat(MongoCapturePreparationFacts.available(encoded)).isEqualTo(next);
            else assertThatThrownBy(() -> MongoCapturePreparationFacts.available(encoded)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CAPTURE_STATE_REBUILD_REQUIRED));
            assertThat(MongoCaptureExternalRetirements.bytes(encoded)).isLessThanOrEqualTo(8192);
        }
        String again="00000000-0000-0000-0000-000000000040";
        var successor=new CapturePreparation(next.scope(),next.mode(),MongoCapturePreparationFacts.replacementChain(next,again),next.captureId(),
                next.sourceIdentityHash(),again,next.canonicalChainId(),next.birthPreparationId());
        assertThat(MongoCapturePreparationFacts.configurationRow(new MongoCapturePreparationFacts.Recovery(next,successor,"b".repeat(64),MongoCapturePreparationFacts.RecoveryPhase.READY))
                .getInteger("formatVersion")).isEqualTo(2);
    }
    @Test void aClearReservationHasACodedPendingAnswerUntilItsReadyReceiptIsPublished() {
        var original=birth();var next=cleared();
        var marker=new Document("operationId",CLEAR).append("sequence",1L).append("previousChain",original.chainId())
                .append("previousPreparationId",original.preparationId()).append("nextChain",next.chainId()).append("nextPreparationId",CLEAR)
                .append("phase","RESERVED").append("receiptHash",null);
        var row=new Document("_id",MongoCapturePreparationFacts.id(original.scope(),original.captureId())).append("formatVersion",3)
                .append("original",MongoCapturePreparationFacts.row(original)).append("currentClear",marker);
        assertThatThrownBy(() -> MongoCapturePreparationFacts.available(row)).isInstanceOfSatisfying(TapstateException.class,failure -> {
            assertThat(failure.code()).isEqualTo(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING);
            assertThat(failure.args()).containsEntry("capture",original.captureId()).containsEntry("operation",CLEAR).containsEntry("reason","clear-successor-not-ready");
        });
        marker.put("phase","READY");marker.put("receiptHash","c".repeat(64));
        assertThat(MongoCapturePreparationFacts.available(row)).isEqualTo(next);
    }
    @Test void malformedTagsAndBudgetsCannotBeReadAsAnotherSupportedShape() {
        for(String damage:List.of("tag","inner-tag","extra","previous-birth","previous-recovery","nested","different-birth","next-clear","operation","hash","phase","id","bytes")) {
            var original=cleared();var row=MongoCapturePreparationFacts.configurationRow(new MongoCapturePreparationFacts.Recovery(original,recovery(original),"a".repeat(64),MongoCapturePreparationFacts.RecoveryPhase.READY));
            var current=row.get("currentRecovery",Document.class);
            switch(damage) {
                case "tag" -> row.put("formatVersion",4L);
                case "inner-tag" -> current.put("formatVersion",2L);
                case "extra" -> current.put("unexpected",true);
                case "previous-birth" -> current.put("previous",MongoCapturePreparationFacts.row(original.birth()));
                case "previous-recovery" -> current.put("previous",MongoCapturePreparationFacts.row(recovery(original)));
                case "nested" -> current.put("previous",Document.parse(row.toJson()));
                case "different-birth" -> current.get("previous",Document.class).get("original",Document.class).put("preparationId",RECOVERY);
                case "next-clear" -> current.put("replacement",MongoCapturePreparationFacts.row(original));
                case "operation" -> current.put("operationId",CLEAR);
                case "hash" -> current.put("planHash","bad");
                case "phase" -> current.put("phase","UNKNOWN");
                case "id" -> row.get("_id",Document.class).put("capture","foreign");
                case "bytes" -> current.put("unexpected","x".repeat(8192));
                default -> throw new IllegalArgumentException(damage);
            }
            assertThatThrownBy(() -> MongoCapturePreparationFacts.recovery(row)).as(damage).isInstanceOf(TapstateException.class);
        }
    }
    private static CapturePreparation cleared() {
        var original=birth();return new CapturePreparation(original.scope(),original.mode(),MongoCaptureExternalRetirements.clearChain(original,CLEAR),
                original.captureId(),original.sourceIdentityHash(),CLEAR,original.canonicalChainId(),original.birthPreparationId());
    }
    private static CapturePreparation birth() {
        var scope=new CacheBusinessScope("cluster",1,"00000000-0000-0000-0000-000000000001");
        String chain=SourceCaptureIdentity.physical("postgres",Map.of("host","original"));
        return new CapturePreparation(scope,CapturePreparation.Mode.SHARED_SRS,chain,SourceCaptureIdentity.sharedCapture(chain),chain,BIRTH);
    }
    private static CapturePreparation recovery(CapturePreparation previous) {
        return new CapturePreparation(previous.scope(),previous.mode(),MongoCapturePreparationFacts.replacementChain(previous,RECOVERY),
                previous.captureId(),previous.sourceIdentityHash(),RECOVERY,previous.canonicalChainId(),previous.birthPreparationId());
    }
}
