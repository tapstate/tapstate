package io.tapstate.spi.store;

import io.tapstate.core.alerting.*;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;

/** CONFIG-only alert persistence. No callback exposes a driver session or performs external delivery. */
public interface AlertStore {
    enum Outcome { STAGED, UNCHANGED, CONFLICT }
    enum CommitKind { STATE_AND_PLAN, PLAN, PERMIT, RESULT, END }
    record Stored<T>(long revision,T value) { public Stored { if(revision<1)throw new IllegalArgumentException("positive stored revision required"); Objects.requireNonNull(value); } }
    /** Created by the assembly from original installation/managed-Cluster facts, never deserialized from a request. */
    record Host(AlarmKey.Scope scope, StorageSetupScope setup, AlertChannelPolicy.Deployment deployment, Runnable locallyActive, AlertIntent.SystemRoute managedRoute, AlertIntent.SdkTarget managedTarget,
            java.util.function.Function<AlarmKey.Resource,Optional<ResourceEvidence>> resourceEvidence, Set<AlertChannelPolicy.Capability> availableChannels) {
        public Host(AlarmKey.Scope scope,StorageSetupScope setup,AlertChannelPolicy.Deployment deployment,Runnable active){this(scope,setup,deployment,active,null,null,r->Optional.empty(),Set.of());}
        public Host withChannels(Set<AlertChannelPolicy.Capability> channels){return new Host(scope,setup,deployment,locallyActive,managedRoute,managedTarget,resourceEvidence,channels);}
        public Host { availableChannels=Set.copyOf(availableChannels);if(availableChannels.size()>64)throw new IllegalArgumentException("too many channels");Objects.requireNonNull(resourceEvidence);Objects.requireNonNull(scope);Objects.requireNonNull(setup);Objects.requireNonNull(deployment);Objects.requireNonNull(locallyActive);
            if(!scope.clusterId().equals(setup.clusterId())||deployment==AlertChannelPolicy.Deployment.UNRESOLVED)throw new IllegalArgumentException("unresolved host authority"); }
    }
    sealed interface ResourceEvidence permits NodeEvidence,StorageEvidence { AlarmKey.Resource resource(); }
    /** Captured from the actual membership/lease event; the guard is pure and must refuse expired or foreign producer evidence. */
    record NodeEvidence(AlarmKey.Resource resource,String nodeId,String bootId,long expectedMembershipRevision,
            String producerIncarnation,Runnable requireCurrent) implements ResourceEvidence {
        public NodeEvidence { Objects.requireNonNull(resource);Objects.requireNonNull(nodeId);Objects.requireNonNull(bootId);Objects.requireNonNull(producerIncarnation);Objects.requireNonNull(requireCurrent);
            if(expectedMembershipRevision<1||!resource.kind().equals("cluster-node")||!resource.id().equals(nodeId)||!resource.incarnation().equals(bootId))throw new IllegalArgumentException("foreign member evidence"); }
    }
    enum StorageRole { CACHE,MASTER }
    /** Original CONFIG-backed identity only; unavailable CACHE is never queried to infer a replacement resource. */
    record StorageEvidence(AlarmKey.Resource resource,StorageRole role,CacheAuthScope cache,SourceBindingIdentity master) implements ResourceEvidence {
        public StorageEvidence { Objects.requireNonNull(resource);Objects.requireNonNull(role);if(!resource.kind().equals("cluster-storage")||(role==StorageRole.CACHE)!=(cache!=null)||(role==StorageRole.MASTER)!=(master!=null))throw new IllegalArgumentException("foreign storage evidence"); }
    }
    /** Original verified request; revalidate may do IO before a transaction, locallyActive must remain pure. */
    record Access(AlertTestAuthority actor,Runnable revalidate,Runnable locallyActive,Supplier<String> currentVerifierBinding,
            String cloudSubject,String cloudOrganization,String cloudGrantedScope) {
        public Access(AlertTestAuthority actor,Runnable revalidate,Runnable active,Supplier<String> verifier){this(actor,revalidate,active,verifier,null,null,null);}
        public static Access cloud(String subject,String organization,String scope,Runnable revalidate,Runnable active){return new Access(null,revalidate,active,()->"",subject,organization,scope);}
        public String subject(){return actor==null?cloudSubject:actor.subject();}
        public Access { if((actor==null)!=(cloudSubject!=null)||actor==null&&(cloudSubject.isBlank()||cloudOrganization==null||cloudOrganization.isBlank()||cloudGrantedScope==null))throw new IllegalArgumentException("missing verified caller");Objects.requireNonNull(revalidate);Objects.requireNonNull(locallyActive);Objects.requireNonNull(currentVerifierBinding); }
    }
    /** Ephemeral producer proof. The guard is pure; its lease was read before entering CONFIG. */
    record EvaluationProof(PipelineIntentSnapshot pipeline,Runnable requireCurrent) {
        public EvaluationProof { Objects.requireNonNull(pipeline);Objects.requireNonNull(requireCurrent);if(pipeline.deleted())throw new IllegalArgumentException("deleted producer intent"); }
    }
    record StateChange(long revision,AlertStateRecord.Head before,AlertStateRecord.Head after,List<AlertStateRecord.Transition> transitions,AlertSignal diagnosticSignal,EvaluationProof proof) {
        public StateChange(long revision,AlertStateRecord.Head before,AlertStateRecord.Head after,List<AlertStateRecord.Transition> transitions,AlertSignal diagnosticSignal){this(revision,before,after,transitions,diagnosticSignal,null);}
        public StateChange { Objects.requireNonNull(after);transitions=List.copyOf(transitions);if(revision<0||(revision==0)!=(before==null))throw new IllegalArgumentException("invalid state baseline"); }
    }
    record IntentChange(Stored<AlertIntent.Record> before,AlertIntent.Record after) { public IntentChange { Objects.requireNonNull(after); } }
    record ScheduleChange(Stored<AlertSchedule.Record> before,AlertSchedule.Record after) { public ScheduleChange { Objects.requireNonNull(after); } }
    record PlanVersion(long configurationRevision,long alertFence) { }
    record Dependency(AlarmKey root,AlarmKey dependent) { }
    /** One bounded CONFIG read snapshot. The version must accompany every derived write. */
    record Planning(PlanVersion version,Host host,List<AlertConfiguration.Definition> definitions,
            List<Stored<AlertStateRecord.Head>> states,List<Stored<AlertSchedule.Record>> schedules,
            Map<DeliveryKey,Stored<AlertIntent.Record>> intents,
            Map<FrozenGroupPolicy.Member,AlertStateRecord.Episode> episodes,
            Map<AlertTargetLimit.Domain,AlertTargetLimit> limits,Set<Dependency> dependencies,boolean isolatedRecords) {
        public Planning(PlanVersion version,Host host,List<AlertConfiguration.Definition> definitions,List<Stored<AlertStateRecord.Head>> states,
                List<Stored<AlertSchedule.Record>> schedules,Map<DeliveryKey,Stored<AlertIntent.Record>> intents,
                Map<FrozenGroupPolicy.Member,AlertStateRecord.Episode> episodes,Map<AlertTargetLimit.Domain,AlertTargetLimit> limits,Set<Dependency> dependencies){
            this(version,host,definitions,states,schedules,intents,episodes,limits,dependencies,false);
        }
        public Planning { definitions=List.copyOf(definitions);states=List.copyOf(states);schedules=List.copyOf(schedules);
            intents=Map.copyOf(intents);episodes=Map.copyOf(episodes);limits=Map.copyOf(limits);dependencies=Set.copyOf(dependencies); }
    }
    Planning planning();
    record Commit(CommitKind kind,StateChange state,List<IntentChange> intents,List<ScheduleChange> schedules,PlanVersion version) {
        public Commit(CommitKind kind,StateChange state,List<IntentChange> intents,List<ScheduleChange> schedules){this(kind,state,intents,schedules,null);}
        public Commit { Objects.requireNonNull(kind);intents=List.copyOf(intents);schedules=List.copyOf(schedules);
            if(intents.size()>100||schedules.size()>100||state==null&&intents.isEmpty()&&schedules.isEmpty())throw new IllegalArgumentException("empty or oversized alert commit"); }
    }
    record PreparedCandidate(AlertTestCommand.Parameters parameters,AlertIntent.CandidateTarget target,AlertIntent.EncryptedCandidate encrypted) {
        @Override public String toString(){return "PreparedAlertCandidate[redacted]";}
    }
    /** Safe query capabilities and typed keyset cursors. Guards remain process-local inside CONFIG; request IO revalidation belongs outside. */
    record ReadAccess(boolean allResources,Set<AlarmKey.Resource> resources,Runnable requireActive) {
        public ReadAccess { resources=Set.copyOf(resources);Objects.requireNonNull(requireActive);if(resources.size()>1000||allResources&&!resources.isEmpty())throw new IllegalArgumentException("invalid authorized resources"); }
        public boolean permits(AlarmKey key){return allResources||resources.contains(key.resource());}
    }
    record ActiveCursor(int severityRank,long secondOrder,int nanoOrder,AlarmKey key) { }
    record AlarmFilter(AlarmSeverity severity,String search,AlertConfiguration.Match match) {
        public AlarmFilter { if(search!=null&&(search.length()>128||search.codePoints().anyMatch(Character::isISOControl)))throw new io.tapstate.core.common.TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","search","reason","outside-query-budget"),null); }
    }
    record ActivePage(long total,List<Stored<AlertStateRecord.Head>> rows,ActiveCursor next,boolean isolatedRecords) {public ActivePage{rows=List.copyOf(rows);}}
    record HistoryPage(List<AlertStateRecord.Episode> rows,Long nextEpisode,boolean isolatedRecords){public HistoryPage{rows=List.copyOf(rows);}}
    record TransitionCursor(long revision,int ordinal){ }
    record TransitionPage(List<AlertStateRecord.Transition> rows,TransitionCursor next,boolean isolatedRecords){public TransitionPage{rows=List.copyOf(rows);}}
    record DueCursor(Instant candidateAt,String purpose,String deliveryId){ }
    record DuePage(List<Stored<AlertIntent.Record>> rows,DueCursor next,boolean isolatedRecords){public DuePage{rows=List.copyOf(rows);}}
    record ScheduleCursor(Instant candidateAt,AlertSchedule.Key key){ }
    record SchedulePage(List<Stored<AlertSchedule.Record>> rows,ScheduleCursor next,boolean isolatedRecords){public SchedulePage{rows=List.copyOf(rows);}}
    record DefinitionPage(List<AlertConfiguration.Definition> rows,String nextId){public DefinitionPage{rows=List.copyOf(rows);}}
    record ResourcePage(List<AlarmKey.Resource> rows,String nextId){public ResourcePage{rows=List.copyOf(rows);}}
    default ResourcePage resources(String kind,String after,int limit,Access access){throw new UnsupportedOperationException();}
    record EpisodeCursor(AlarmKey key,long number,long secondOrder,int nanoOrder) { }
    record EndedPage(List<AlertStateRecord.Episode> rows,EpisodeCursor next,boolean isolatedRecords){public EndedPage{rows=List.copyOf(rows);}}
    record DeliveryPage(List<Stored<AlertIntent.Record>> rows,String nextId,boolean isolatedRecords){public DeliveryPage{rows=List.copyOf(rows);}}
    record EvaluationPage(List<Stored<AlertStateRecord.Head>> rows,AlarmKey next,Set<AlarmKey.Resource> removedResources){
        public EvaluationPage(List<Stored<AlertStateRecord.Head>> rows,AlarmKey next){this(rows,next,Set.of());}
        public EvaluationPage{rows=List.copyOf(rows);removedResources=Set.copyOf(removedResources);}
    }

    Optional<Stored<AlertStateRecord.Head>> state(AlarmKey key,ReadAccess access);
    /** Returns parameters only when the current definition is exactly the original evaluation revision. */
    default Optional<AlertConfiguration.Rule> originalRule(AlertRule binding,ReadAccess access){return Optional.empty();}
    Optional<Stored<AlertIntent.Record>> test(AlertTestCommand.Key key,Access access);
    Optional<Stored<AlertIntent.Record>> intent(AlertIntent.Identity identity,Access access);
    Optional<AlertConfiguration.Definition> definition(AlertConfiguration.Kind kind,String id,Access access);
    DefinitionPage definitions(AlertConfiguration.Kind kind,String after,int limit,Access access);
    /** Fixed-size index windows; filtered empty pages may still carry a continuation. */
    default EndedPage ended(EpisodeCursor after,int limit,ReadAccess access,AlertSnapshot.EndReason reason){throw new UnsupportedOperationException();}
    default DeliveryPage deliveries(String after,int limit,String channel,DeliveryEvidence.Outcome outcome,ReadAccess access){throw new UnsupportedOperationException();}
    default Optional<Stored<AlertIntent.Record>> delivery(String id,ReadAccess access){throw new UnsupportedOperationException();}
    /** Background evaluation only, scoped by the original Host; unknown records fail the whole page. */
    DefinitionPage evaluationRules(String after,int limit);
    EvaluationPage evaluationStates(AlarmKey after,int limit);
    Outcome saveDefinition(AlertConfiguration.Definition before,AlertConfiguration.Definition after,Access access);
    Outcome saveReceiver(AlertReceiver before,AlertReceiver after,PrivateMaterial replacement,boolean retainOriginalMaterial,Access access);
    enum PrivateField { AUTHENTICATION,PRIVATE_CA,ENDPOINT }
    default PreparedCandidate prepareCandidate(AlertTestCommand.Key key,AlertTestCommand.Parameters form,AlertReceiver.Transport transport,
            AlertConfiguration.Identity existingReceiver,long bindingRevision,PrivateMaterial material,Access access){
        return prepareCandidate(key,form,transport,existingReceiver,bindingRevision,material,Set.of(),access);
    }
    PreparedCandidate prepareCandidate(AlertTestCommand.Key key,AlertTestCommand.Parameters form,AlertReceiver.Transport transport,
            AlertConfiguration.Identity existingReceiver,long bindingRevision,PrivateMaterial material,Set<PrivateField> retainOriginal,Access access);
    Stored<AlertIntent.Record> admitTest(AlertIntent.Record candidate,Access access);
    Outcome commit(Commit candidate);
    Outcome commitAsCaller(Commit candidate,Access access);
    PrivateMaterial resolvePermittedMaterial(AlertIntent.Identity identity,DeliveryAttempt.Permit permit,Access access);
    ActivePage active(ActiveCursor after,int limit,ReadAccess access);
    default ActivePage activeFiltered(ActiveCursor after,int limit,ReadAccess access,AlarmFilter filter){throw new UnsupportedOperationException();}
    HistoryPage history(AlarmKey key,Long after,int limit,ReadAccess access);
    TransitionPage transitions(AlarmKey key,TransitionCursor after,int limit,ReadAccess access);
    DuePage dueIntents(Instant through,DueCursor after,int limit);
    SchedulePage dueSchedules(Instant through,ScheduleCursor after,int limit);

    /** Caller-owned private data, erased on close and never returned by public configuration queries. */
    final class PrivateMaterial implements AutoCloseable {
        private byte[] authentication,privateCa;private URI endpoint;private boolean closed;
        public PrivateMaterial(byte[] authentication,byte[] privateCa,URI endpoint){this.authentication=authentication==null?null:authentication.clone();this.privateCa=privateCa==null?null:privateCa.clone();this.endpoint=endpoint;}
        public byte[] authentication(){requireOpen();return authentication==null?null:authentication.clone();}
        public byte[] privateCa(){requireOpen();return privateCa==null?null:privateCa.clone();}
        public URI endpoint(){requireOpen();return endpoint;}
        private void requireOpen(){if(closed)throw new IllegalStateException("private material is closed");}
        @Override public void close(){if(authentication!=null)Arrays.fill(authentication,(byte)0);if(privateCa!=null)Arrays.fill(privateCa,(byte)0);authentication=null;privateCa=null;endpoint=null;closed=true;}
        @Override public String toString(){return "AlertPrivateMaterial[redacted]";}
    }
}
