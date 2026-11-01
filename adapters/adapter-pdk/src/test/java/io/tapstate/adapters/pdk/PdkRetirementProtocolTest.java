package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.adapters.pdk.fixture.RetirementSource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PdkRetirementProtocolTest {
    private static final String HASH="a".repeat(64);
    private static final String POLICY="{\"properties\":{\"autoClearSlot\":{\"type\":\"boolean\"}}}";
    private final Map<String,Object> state=new ConcurrentHashMap<>();
    private final List<String> events=new CopyOnWriteArrayList<>();
    private final Map<RequiredNoteScope,RequiredNoteValue> values=new HashMap<>();
    private List<RequiredNoteCarry> carries=List.of();
    private boolean blocked;
    private RequiredNoteOrigin origin;
    private ConnectorRef reference;
    private RequiredNoteStore notes;
    private PdkExternalResourceRetirement retirement;
    private final RequiredNoteScope shared=new RequiredNoteScope(new RequiredNoteOwner(1,"pdk.chain.chain",RequiredNoteOwner.Kind.CHAIN,"chain",0),1);
    private final RequiredNoteScope privateScope=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1),1);
    @BeforeEach void setup() throws Exception {
        state.put("events",events);state.put("count",1L);System.getProperties().put(RetirementSource.STATE,state);
        var classes=Path.of(RetirementSource.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        reference=new ConnectorRef(List.of(classes),RetirementSource.class.getName(),"2.0.8",null,POLICY,HASH,false);
        origin=origin(Map.of("database","original-db"));
        values.put(shared,note("original-slot"));
        notes=(RequiredNoteStore)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RequiredNoteStore.class},(proxy,method,args) -> switch(method.getName()) {
            case "guardedBy" -> proxy;
            case "origin" -> origin;
            case "open" -> new RequiredNoteScope((RequiredNoteOwner)args[0],1);
            case "read" -> values.getOrDefault((RequiredNoteScope)args[0],new RequiredNoteValue(0,null));
            case "carries" -> carries;
            case "readCarried" -> values.getOrDefault(((RequiredNoteCarry)args[0]).source(),new RequiredNoteValue(0,null));
            case "carryBlocked","allCarryBlocked" -> blocked;
            case "compareAndSet" -> true;
            default -> throw new AssertionError("unexpected note call: "+method.getName());
        });
        retirement=new PdkExternalResourceRetirement(id -> reference,notes);
    }
    @AfterEach void cleanup(){System.getProperties().remove(RetirementSource.STATE);}
    @Test void verifiesTheOriginalMetadataBeforeReleaseAndChecksAbsenceAfterIt() throws Exception {
        var permit=new Permit();var result=retirement.begin(preparation(),permit).get(10,TimeUnit.SECONDS);
        assertThat(result).isEqualTo(new PdkExternalResourceRetirement.Result(true,"confirmed-absent"));
        assertThat(events).containsExactly("init",PdkExternalResourceRetirement.query("original-slot"),
                PdkExternalResourceRetirement.query("original-slot")+" AND slot_type='logical' AND database='original-db'","stop","release:original-slot","stop","init",
                PdkExternalResourceRetirement.query("original-slot"),"stop");
        assertThat(permit.calls).containsExactly("consume","ended");
    }
    @Test void physicalSlotVerificationUsesNullDatabaseAndCustomNameIsOnlyAFallback() throws Exception {
        origin=origin(Map.of("logPluginName","physical","customSlotName","custom-slot"));values.clear();
        var result=retirement.begin(preparation(),new Permit()).get(10,TimeUnit.SECONDS);
        assertThat(result.absent()).isTrue();
        assertThat(events).contains(PdkExternalResourceRetirement.query("custom-slot")+" AND slot_type='physical' AND database IS NULL");
    }
    @Test void alreadyAbsentNeedsPositiveVerificationAndDoesNotInvokeRelease() throws Exception {
        state.put("count",0L);var permit=new Permit();
        assertThat(retirement.begin(preparation(),permit).get(10,TimeUnit.SECONDS).reason()).isEqualTo("confirmed-absent-before-call");
        assertThat(events).noneMatch(e -> e.startsWith("release:"));assertThat(permit.calls).containsExactly("consume","ended");
    }
    @ParameterizedTest
    @ValueSource(strings={"noRelease","noQuery","queryFailure","metadata","ambiguous","missingName","missingDatabase","stopFailure","releaseFailure","keepSlot"})
    void uncertainEvidenceNeverAuthorizesSuccessfulRetirement(String damage) throws Exception {
        String reason=switch(damage) {
            case "metadata" -> {state.put("metadataCount",0L);yield "slot-metadata-mismatch";}
            case "ambiguous" -> {state.put("count",2L);yield "slot-identity-not-unique";}
            case "missingName" -> {values.clear();yield "slot-name-unproven";}
            case "missingDatabase" -> {origin=origin(Map.of());yield "original-database-unproven";}
            case "noRelease" -> "release-function-missing";
            case "noQuery" -> "verification-function-missing";
            case "queryFailure" -> "verification-query-refused";
            case "stopFailure" -> "source-stop-unproven";
            case "releaseFailure" -> "release-or-cleanup-unproven";
            case "keepSlot" -> "original-slot-remains";
            default -> throw new AssertionError(damage);
        };
        state.put(damage,true);var permit=new Permit();
        var result=retirement.begin(preparation(),permit).get(10,TimeUnit.SECONDS);
        assertThat(result).isEqualTo(new PdkExternalResourceRetirement.Result(false,reason));
        assertThat(permit.calls).startsWith("consume");
        assertThat(permit.calls).contains(damage.equals("stopFailure") || damage.equals("releaseFailure")?"unknown:"+reason:"ended");
        if(!Set.of("keepSlot","releaseFailure").contains(damage))assertThat(events).noneMatch(e -> e.startsWith("release:"));
    }
    @Test void unexpectedInitializationFailureRemainsExceptional() throws Exception {
        state.put("initFailure",true);var permit=new Permit();
        assertThatThrownBy(() -> retirement.begin(preparation(),permit).get(10,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        assertThat(events).contains("stop");assertThat(permit.calls).containsExactly("consume");
    }
    @Test void policyUsesOnlyThePinnedArtifactAndSavedSettings() {
        assertThat(retirement.automatic(origin)).isTrue();
        assertThat(retirement.automatic(origin(Map.of("autoClearSlot",false)))).isFalse();
        assertThat(retirement.automatic(origin(Map.of("autoClearSlot",true)))).isTrue();
        assertThat(retirement.declaresPolicy(origin)).isTrue();assertThat(retirement.supported(origin,shared)).isTrue();
        reference=new ConnectorRef(reference.classpath(),reference.className(),"2.0.8",null,"{\"nested\":[{}]}",HASH,false);
        assertThat(retirement.declaresPolicy(origin)).isFalse();assertThat(retirement.supported(origin,shared)).isTrue();
        values.clear();values.put(privateScope,note("carried"));carries=List.of(new RequiredNoteCarry(shared,privateScope));
        assertThat(retirement.supported(origin,shared)).isTrue();values.clear();assertThat(retirement.supported(origin,shared)).isFalse();
        reference=new ConnectorRef(reference.classpath(),reference.className(),"2.0.8",null,null,HASH,false);
        assertThat(retirement.declaresPolicy(origin)).isFalse();assertThat(retirement.resourceNameHash(origin,shared)).isNull();
        assertThatThrownBy(() -> retirement.automatic(origin(Map.of("autoClearSlot",List.of("invalid")))))
                .isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.args()).containsEntry("reason","original-policy-unreadable"));
        assertThatThrownBy(() -> retirement.automatic(ConnectorNoteOrigin.capture("postgres","b".repeat(64),Map.of())))
                .isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(ConnectorError.LOAD_FAILED));
    }
    @Test void slotResolutionHonorsDeletionAndCarryBarriersAndRefusesDisagreement() {
        assertThat(retirement.resourceNameHash(origin,shared)).isEqualTo(CaptureResourceNotes.nameHash("original-slot"));
        values.clear();values.put(privateScope,note("carried"));carries=List.of(new RequiredNoteCarry(shared,privateScope));
        assertThat(retirement.resourceNameHash(origin,shared)).isEqualTo(CaptureResourceNotes.nameHash("carried"));
        blocked=true;assertThat(retirement.resourceNameHash(origin,shared)).isEqualTo(CaptureResourceNotes.nameHash(null));
        blocked=false;values.put(shared,new RequiredNoteValue(2,null));
        assertThat(retirement.resourceNameHash(origin,shared)).isEqualTo(CaptureResourceNotes.nameHash(null));
        assertThat(retirement.resourceNameHash(origin(Map.of("customSlotName","custom")),shared)).isEqualTo(CaptureResourceNotes.nameHash("custom"));
        values.put(shared,note(7));assertReason(() -> retirement.resourceNameHash(origin,shared),"slot-note-unreadable");
        values.put(shared,note(" "));assertReason(() -> retirement.resourceNameHash(origin,shared),"slot-note-unreadable");
        values.remove(shared);
        var other=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","other"),PipelineNode.Purpose.CAPTURE,1),1);
        values.put(other,note("different"));carries=List.of(new RequiredNoteCarry(shared,privateScope),new RequiredNoteCarry(shared,other));
        assertReason(() -> retirement.resourceNameHash(origin,shared),"carry-slot-disagreement");
    }
    @Test void observedCarryResolutionRequiresTheExactPhysicalGeneration() {
        var own=observed(shared,null,true,List.of(privateScope));var earlier=observed(privateScope,"carried",false,List.of());
        assertThat(retirement.savedSlot(own,List.of(own,earlier))).isEqualTo("carried");
        assertReason(() -> retirement.savedSlot(own,List.of(own)),"carry-source-not-observed");
        var changed=observed(new RequiredNoteScope(privateScope.owner(),2),"carried",false,List.of());
        assertReason(() -> retirement.savedSlot(own,List.of(own,changed)),"carry-source-not-observed");
        assertThat(retirement.savedSlot(observed(shared,null,false,List.of(privateScope)),List.of(earlier))).isNull();
        assertThat(retirement.savedSlot(observed(shared,"own",true,List.of(privateScope)),List.of(earlier))).isEqualTo("own");
        assertThat(retirement.slot(observed(shared,null,false,List.of()),List.of())).isNull();
        origin=origin(Map.of("customSlotName","custom"));assertThat(retirement.slot(observed(shared,null,false,List.of()),List.of())).isEqualTo("custom");
    }
    @Test void observedSourcesMustAgreeAndCannotCrossBindingsOrNamespaces() {
        var other=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","other"),PipelineNode.Purpose.CAPTURE,1),1);
        var own=observed(shared,null,true,List.of(privateScope,other));
        var first=observed(privateScope,"first",false,List.of());var second=observed(other,"second",false,List.of());
        assertReason(() -> retirement.savedSlot(own,List.of(first,second)),"carry-slot-disagreement");
        assertThat(retirement.savedSlot(own,List.of(observed(privateScope,null,false,List.of()),second))).isEqualTo("second");
        var changedBinding=new RequiredNoteScope(RequiredNoteOwner.forNode(2,new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1),1);
        assertReason(() -> retirement.savedSlot(observed(shared,null,true,List.of(privateScope)),List.of(observed(changedBinding,"first",false,List.of()),second)),"carry-source-not-observed");
        var foreign=ConnectorNoteOrigin.capture("postgres",HASH,Map.of("database","different"));
        assertReason(() -> retirement.requireOriginal(preparation().observation().previous(),foreign),"original-capture-context-mismatch");
        reference=new ConnectorRef(reference.classpath(),reference.className(),"2.0.8",null,"{\"nested\":[{\"autoClearSlot\":{\"type\":\"string\"}}]}",HASH,false);
        assertThat(retirement.declaresPolicy(origin)).isFalse();
        values.put(shared,note(7));assertThat(retirement.supported(origin,shared)).isFalse();
    }
    private CaptureResourceNotes observed(RequiredNoteScope scope,Object value,boolean mayCarry,List<RequiredNoteScope> sources) {
        var savedOrigin=origin;
        return new CaptureResourceNotes() {
            public RequiredNoteScope scope(){return scope;} public RequiredNoteOrigin origin(){return savedOrigin;}
            public RequiredNoteValue slot(){return value==null?new RequiredNoteValue(0,null):note(value);}
            public boolean mayCarrySlot(){return mayCarry;} public List<RequiredNoteScope> carries(){return sources;}
            public boolean resumable(){return false;}
        };
    }
    private CaptureClearPreparation preparation() {
        var business=new CacheBusinessScope("cluster",1,"cache");
        var previous=new CapturePreparation(business,CapturePreparation.Mode.SHARED_SRS,"chain","capture",SourceCaptureIdentity.physical("postgres",ConnectorNoteOrigin.settings(origin)),"00000000-0000-0000-0000-000000000001");
        var fence=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("node","boot"),1,1,1);
        return new CaptureClearPreparation() {
            public String operationId(){return "clear";} public long sequence(){return 1;}
            public PipelineIntentScope originalScope(){return new PipelineIntentScope("cluster",1,"cache","flow",1);}
            public String originalOperationId(){return "original";} public SharedSrsRecoverySnapshot observation(){return new SharedSrsRecoverySnapshot(previous,fence,HASH,List.of(),Map.of());}
            public CapturePreparation replacement(){return previous;} public RequiredNoteScope notes(){return shared;}
            public String requestHash(){return HASH;} public List<CaptureRetirementMember> members(){return List.of();}
            public void requireCurrent(){}
        };
    }
    private static RequiredNoteOrigin origin(Map<String,Object> settings){return ConnectorNoteOrigin.capture("postgres",HASH,settings);}
    private static RequiredNoteValue note(Object value){return new RequiredNoteValue(1,ConnectorStateCodec.encode(value));}
    private static void assertReason(Runnable action,String reason){assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.args()).containsEntry("reason",reason));}
    private static final class Permit implements CaptureExternalRetirements.Attempt {
        final List<String> calls=new CopyOnWriteArrayList<>();
        public String id(){return "attempt";}public void consume(){calls.add("consume");}public void ended(){calls.add("ended");}public void unknown(String reason){calls.add("unknown:"+reason);}
    }
}
