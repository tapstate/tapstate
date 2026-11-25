package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.StorageRecoveryCommands.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/** These are persisted control codecs, not CACHE replacement or source recovery evidence. */
class MongoSharedSrsRecoveryFactsTest {
    private static final String OPERATION="11111111-1111-4111-8111-111111111111";
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");

    @Test void theExactVersionTwoPendingRequestRetainsItsOriginalObservedControlClosure() {
        var expected=manifest("orders");var row=pending(expected);
        var plan=MongoStorageRecoveryCommands.pending(root(row)).orElseThrow();
        assertThat(plan.operationId()).isEqualTo(OPERATION);
        assertThat(plan.manifest()).isEqualTo(expected);
        assertThat(plan.manifest().sharedSrs().previous().replacement()).isFalse();
        assertThat(plan.manifest().sharedSrs().pipelines().get("flow").scope().stateGeneration()).isEqualTo(1);
        assertThat(row.get("sharedSrs",Document.class).toJson()).doesNotContain("checkpoint","password","resumeOverride");
    }

    @Test void aFlatReplacementPredecessorRoundTripsWithoutAddingARecursiveMapping() {
        var birth=manifest("orders").sharedSrs();String previousOperation="22222222-2222-4222-8222-222222222222";
        var previous=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,
                MongoCapturePreparationFacts.replacementChain(birth.previous(),previousOperation),birth.previous().captureId(),
                birth.previous().sourceIdentityHash(),previousOperation,birth.previous().canonicalChainId(),birth.previous().birthPreparationId());
        var retained=new SharedSrsRecoverySnapshot(previous,birth.captureFence(),birth.controlHash(),
                birth.consumers().stream().map(member -> new SrsConsumerMembership(new SrsConsumerContext(
                        new SrsConsumerKey(previous.chainId(),"flow","input"),1),member.revision(),false,member.tables(),member.commandFence())).toList(),birth.pipelines());
        var encoded=MongoSharedSrsRecoveryFacts.encode(retained);
        assertThat(MongoSharedSrsRecoveryFacts.decode(encoded)).isEqualTo(retained);
        assertThat(encoded.get("previous",Document.class)).containsKeys("original","physicalChain","preparationId").doesNotContainKey("currentRecovery");
        assertThat(encoded.get("previous",Document.class).get("original",Document.class).getInteger("formatVersion")).isOne();
    }

    @ParameterizedTest @ValueSource(strings={"missing","extra","version","type","scope","duplicate","detached","generation","claim","membership-hash","capture","plan-hash"})
    void malformedOrChangedFactsCannotBecomeTheOriginalConfirmedScope(String fault) {
        var row=pending(manifest("orders"));var annex=row.get("sharedSrs",Document.class);
        var consumers=annex.getList("consumers",Document.class);var pipelines=annex.getList("pipelines",Document.class);
        switch(fault) {
            case "missing" -> annex.remove("captureFence");
            case "extra" -> annex.put("replacementPermission",true);
            case "version" -> annex.put("formatVersion",2);
            case "type" -> annex.put("formatVersion",1L);
            case "scope" -> pipelines.getFirst().get("scope",Document.class).put("cacheIncarnation","different");
            case "duplicate" -> annex.put("consumers",List.of(consumers.getFirst(),consumers.getFirst()));
            case "detached" -> consumers.getFirst().put("detached",true);
            case "generation" -> pipelines.getFirst().get("scope",Document.class).put("stateGeneration",2L);
            case "claim" -> pipelines.getFirst().get("claim",Document.class).put("resourceId","other");
            case "membership-hash" -> consumers.getFirst().put("contentHash","b".repeat(64));
            case "capture" -> annex.get("captureFence",Document.class).put("resourceId","other");
            case "plan-hash" -> annex.put("controlHash","b".repeat(64));
            default -> throw new AssertionError(fault);
        }
        assertCode(() -> MongoStorageRecoveryCommands.pending(root(row)),IoError.DOCUMENT_UNREADABLE);
    }

    @Test void theWholePendingRequestBudgetIncludesDefinitionsOutsideItsControlAnnex() {
        var original=manifest("orders");var definitions=new java.util.TreeMap<>(original.definitionHashes());
        definitions.put("dependency-"+"x".repeat(262144),"b".repeat(64));
        var large=new Manifest(original.selection(),original.affectedPipelineIds(),definitions,original.dependencyHash(),0,original.sharedSrs());
        var row=pending(large);
        assertCode(() -> MongoStorageRecoveryCommands.pending(root(row)),IoError.DOCUMENT_TOO_LARGE);
    }

    @Test void theControlAnnexBudgetDoesNotTruncateAnOverLimitConsumer() {
        var row=pending(manifest("table-"+"x".repeat(262144)));
        assertCode(() -> MongoStorageRecoveryCommands.pending(root(row)),IoError.DOCUMENT_TOO_LARGE);
    }

    @Test void ordinaryVersionOneCannotCarryAnUnrecognizedSharedAnnexOrSharedExtent() {
        var row=pending(manifest("orders"));row.put("formatVersion",1);
        assertCode(() -> MongoStorageRecoveryCommands.pending(root(row)),IoError.DOCUMENT_UNREADABLE);
        row.remove("sharedSrs");
        row.put("selection",new Document("extent","SHARED_SRS").append("pipelineIds",List.of()));
        assertCode(() -> MongoStorageRecoveryCommands.pending(root(row)),IoError.DOCUMENT_UNREADABLE);
    }

    @Test void anOrdinaryVersionOnePlanKeepsItsOriginalShapeAndHash() {
        var manifest=new Manifest(new Selection(Extent.PIPELINE,Set.of("flow")),Set.of("flow"),Map.of("flow","a".repeat(64)),"a".repeat(64),0);
        var plan=new Plan(OPERATION,null,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),manifest,
                "8a158800cd58553846f873553423c40ee0c3e2da5393156f7ddd1cbe45b5a317",1,Phase.PLANNED);
        var encoded=MongoStorageRecoveryCommands.encode(plan);
        assertThat(encoded.getInteger("formatVersion")).isOne();
        assertThat(encoded).doesNotContainKey("sharedSrs");
        assertThat(encoded.get("selection",Document.class)).isEqualTo(new Document("extent","PIPELINE").append("pipelineIds",List.of("flow")));
        assertThat(MongoStorageRecoveryCommands.pending(root(encoded))).contains(plan);
    }

    private static Manifest manifest(String table) {
        var previous=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"chain","capture","mc-"+"a".repeat(64),
                "33333333-3333-4333-8333-333333333333");
        var pipeline=new PipelineIntentScope("cluster",1,"cache","flow",1);
        var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),1,1,0);
        var capture=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("node","boot"),1,1,0);
        var consumer=new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("chain","flow","input"),1),1,false,Set.of(table),claim);
        var snapshot=new SharedSrsRecoverySnapshot(previous,capture,"a".repeat(64),List.of(consumer),Map.of("flow",new SharedSrsRecoverySnapshot.Pipeline(pipeline,claim)));
        return new Manifest(new Selection(Extent.SHARED_SRS,Set.of(),"chain","chain"),Set.of("flow"),Map.of("flow","a".repeat(64)),"a".repeat(64),0,snapshot);
    }
    private static Document pending(Manifest manifest) {
        return new Document("formatVersion",2).append("operationId",OPERATION).append("previousOperationId",null)
                .append("scope",new Document("clusterId","cluster").append("bindingVersion",1L).append("mode","OP")
                        .append("cacheIncarnation","cache").append("authGeneration",1L))
                .append("selection",new Document("extent","SHARED_SRS").append("pipelineIds",List.of()).append("canonicalChain","chain").append("physicalChain","chain"))
                .append("affectedPipelineIds",List.of("flow")).append("definitionHashes",new Document(manifest.definitionHashes()))
                .append("dependencyHash",manifest.dependencyHash()).append("configurationRevision",manifest.configurationRevision())
                .append("planHash",MongoSharedSrsRecoveryFacts.hash(manifest)).append("controlRevision",1L).append("phase","PLANNED")
                .append("sharedSrs",MongoSharedSrsRecoveryFacts.encode(manifest.sharedSrs()));
    }
    private static Document root(Document row) {
        return new Document("clusterId","cluster").append("bindingVersion",1L).append("pendingRecovery",row);
    }
    private static void assertCode(Runnable action,IoError code) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(code));
    }
}
