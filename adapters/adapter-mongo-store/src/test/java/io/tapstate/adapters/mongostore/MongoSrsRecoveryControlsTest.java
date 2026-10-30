package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoSrsRecoveryControlsTest {
    private static final CacheBusinessScope SCOPE=new CacheBusinessScope("cluster-a",1,"cache-a");
    private static final WorkloadClaimFence CAPTURE=new WorkloadClaimFence(new WorkloadClaimKey("cluster-a",WorkloadClaimType.CAPTURE,"capture-a"),
            new WorkloadOwner("node-a","boot-a"),1,0,1);
    private static final String OPERATION="11111111-1111-4111-8111-111111111111";
    private static final String PHYSICAL="srs-recovery-"+"a".repeat(64);

    @Test void equivalentDocumentOrderingHasTheSameRestartStableControlDigest() {
        var original=MongoSrsChainStore.initialRow(SCOPE,"chain-a",null,CAPTURE);
        var reversed=new Document();var keys=new ArrayList<>(original.keySet());Collections.reverse(keys);
        keys.forEach(key -> reversed.put(key,original.get(key)));
        var first=MongoSrsRecoveryControls.candidate(original,OPERATION,"b".repeat(64),PHYSICAL,MongoSrsRecoveryControls.Phase.CANCELLED,List.of(),List.of());
        var second=MongoSrsRecoveryControls.candidate(reversed,OPERATION,"b".repeat(64),PHYSICAL,MongoSrsRecoveryControls.Phase.CANCELLED,List.of(),List.of());
        assertThat(MongoSrsRecoveryControls.read(first)).isEqualTo(MongoSrsRecoveryControls.read(second));
        MongoSrsRecoveryControls.verify(second,List.of(),List.of());
        assertThat(original.getInteger("formatVersion")).isEqualTo(1);assertThat(original).doesNotContainKey("recoveryControl");
    }

    @ParameterizedTest @ValueSource(strings={"operation","phase","root","membership","cursor","digest"})
    void aReceiptCannotValidateDifferentControlFacts(String change) {
        var root=MongoSrsChainStore.initialRow(SCOPE,"chain-a",null,CAPTURE);
        var memberships=new ArrayList<>(List.of(new Document("_id",new Document("consumer","A")).append("revision",1L)));
        var cursors=new ArrayList<>(List.of(new Document("_id",new Document("consumer","A")).append("progressRevision",1L)));
        var held=MongoSrsRecoveryControls.candidate(root,OPERATION,"b".repeat(64),PHYSICAL,MongoSrsRecoveryControls.Phase.CANCELLED,memberships,cursors);
        MongoSrsRecoveryControls.verify(held,memberships,cursors);
        switch(change) {
            case "operation" -> held.get("recoveryControl",Document.class).put("operationId","22222222-2222-4222-8222-222222222222");
            case "phase" -> held.get("recoveryControl",Document.class).put("phase","RETIRED");
            case "root" -> held.put("modelWriteFence",held.getLong("modelWriteFence")+1);
            case "membership" -> memberships.clear();
            case "cursor" -> cursors.clear();
            case "digest" -> held.get("recoveryControl",Document.class).put("controlHash","f".repeat(64));
            default -> throw new AssertionError(change);
        }
        assertThatThrownBy(() -> MongoSrsRecoveryControls.verify(held,memberships,cursors)).isInstanceOf(TapstateException.class);
    }

    @Test void cancellationKeepsItsOperationAndNamespaceAndRetirementIsTerminal() {
        var root=MongoSrsChainStore.initialRow(SCOPE,"chain-a",null,CAPTURE);
        var cancelled=MongoSrsRecoveryControls.candidate(root,OPERATION,"b".repeat(64),PHYSICAL,MongoSrsRecoveryControls.Phase.CANCELLED,List.of(),List.of());
        assertThatThrownBy(() -> MongoSrsRecoveryControls.candidate(cancelled,"22222222-2222-4222-8222-222222222222",
                "c".repeat(64),PHYSICAL,MongoSrsRecoveryControls.Phase.RETIRED,List.of(),List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> MongoSrsRecoveryControls.candidate(cancelled,OPERATION,"c".repeat(64),"srs-recovery-"+"d".repeat(64),
                MongoSrsRecoveryControls.Phase.RETIRED,List.of(),List.of())).isInstanceOf(TapstateException.class);
        var retired=MongoSrsRecoveryControls.candidate(cancelled,OPERATION,"c".repeat(64),PHYSICAL,MongoSrsRecoveryControls.Phase.RETIRED,List.of(),List.of());
        MongoSrsRecoveryControls.verify(retired,List.of(),List.of());
        assertThatThrownBy(() -> MongoSrsRecoveryControls.candidate(retired,OPERATION,"c".repeat(64),PHYSICAL,
                MongoSrsRecoveryControls.Phase.CANCELLED,List.of(),List.of())).isInstanceOf(TapstateException.class);
    }

    @ParameterizedTest @ValueSource(strings={"missing","extra","type","phase","version","identity"})
    void malformedQuarantineNeverBecomesAnOrdinaryRoot(String damage) {
        var held=MongoSrsRecoveryControls.candidate(MongoSrsChainStore.initialRow(SCOPE,"chain-a",null,CAPTURE),OPERATION,"b".repeat(64),PHYSICAL,
                MongoSrsRecoveryControls.Phase.CANCELLED,List.of(),List.of());
        var control=held.get("recoveryControl",Document.class);
        switch(damage) {
            case "missing" -> control.remove("controlHash");
            case "extra" -> control.put("extra",true);
            case "type" -> control.put("formatVersion",1L);
            case "phase" -> control.put("phase","ACTIVE");
            case "version" -> held.put("formatVersion",1);
            case "identity" -> control.put("replacementChain","unknown");
            default -> throw new AssertionError(damage);
        }
        assertThatThrownBy(() -> MongoSrsRecoveryControls.requireOrdinary(held)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
