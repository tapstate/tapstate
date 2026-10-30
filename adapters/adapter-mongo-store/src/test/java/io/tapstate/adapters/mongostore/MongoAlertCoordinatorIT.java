package io.tapstate.adapters.mongostore;

import com.mongodb.client.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertStore.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertCoordinatorIT {
    @Test void unsupportedBuiltinFormatThreeCannotBlockCompatibleDeliveryPages(){
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var unknown=MongoAlertDeliveryCodec.write(facts.intent(),1);
            unknown.get("_id",Document.class).put("deliveryId","future-builtin-format-three");unknown.put("formatVersion",3);
            SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).insertOne(unknown);
            var due=f.store.dueIntents(T.plusSeconds(1),null,10);assertThat(due.rows()).hasSize(1);assertThat(due.isolatedRecords()).isTrue();
            var page=f.store.deliveries(null,10,null,null,readAll());assertThat(page.rows()).hasSize(1);assertThat(page.isolatedRecords()).isTrue();
        });
    }
    @Test void unsupportedBuiltinFormatThreeStillProtectsUnknownReferencesDuringDefinitionCleanup(){
        withStore(f->{var facts=businessFacts(f);var unknown=MongoAlertDeliveryCodec.write(facts.intent(),1);
            unknown.get("_id",Document.class).put("deliveryId","future-opaque-reference");unknown.put("formatVersion",3);
            SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).insertOne(unknown);
            var receiver=new AlertReceiver(header(Kind.RECEIVER,"unreferenced",f.access.subject()),"Retired",true,1,email("other@example.test"),null);
            f.store.saveReceiver(null,receiver,null,false,f.access);f.store.saveReceiver(receiver,receiver.retire(T,f.access.subject()),null,true,f.access);
            f.time.set(T.plus(Duration.ofDays(8)));clean(f,6,null);
            assertThat(f.store.definition(Kind.RECEIVER,"unreferenced",f.access)).isPresent();
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).find(new Document("_id",unknown.get("_id"))).first()).isEqualTo(unknown);
        });
    }
    @Test void evaluationRecoveryUsesOriginalIdKeysetWithoutMissingOrRepeatingHeads() {
        withStore(f->{
            var facts=businessFacts(f);accept(f,facts);
            var original=facts.head.snapshot().rule();var secondIdentity=new Identity(Kind.RULE,SCOPE,"rule-b","22222222-2222-4222-8222-222222222222");
            var definition=(Rule)f.store.definition(Kind.RULE,"rule-a",f.access).orElseThrow();
            f.store.saveDefinition(null,new Rule(Header.create(secondIdentity,POLICY.deployment(),T,f.access.subject()),"Second",definition.typeId(),1,definition.resources(),definition.parameters(),null,true,null),f.access);
            var key=new AlarmKey(original.key().scope(),original.key().resource(),original.key().typeId(),secondIdentity.incarnation());
            var rule=new AlertRule(key,original.revision(),original.generation(),original.producerIncarnation(),original.triggerFor(),original.resolveFor(),original.maxAge(),original.maxGap());
            var signal=new AlertSignal(key,rule.revision(),rule.generation(),rule.producerIncarnation(),1,T,AlertSignal.Condition.BAD,AlarmSeverity.MAJOR);
            var snapshot=AlertLifecycle.evaluate(AlertSnapshot.initial(rule),signal,T).snapshot();
            var episode=new AlertStateRecord.Episode(rule,snapshot.episode(),POLICY,AlertStateRecord.Diagnostics.empty(true),false);
            var binding=new AlertIntent.Member(rule,1,secondIdentity,facts.head.binding().sourceBindingRevision());
            var head=new AlertStateRecord.Head(snapshot,POLICY.deployment(),AlertOwnership.unclaimed(),null,false,episode,binding);
            f.store.commit(new Commit(CommitKind.STATE_AND_PLAN,new StateChange(0,null,head,List.of(),null),List.of(),List.of()));
            var first=f.store.evaluationStates(null,1);assertThat(first.rows()).hasSize(1);assertThat(first.next()).isNotNull();
            var second=f.store.evaluationStates(first.next(),1);assertThat(second.rows()).hasSize(1);assertThat(second.next()).isNull();
            assertThat(first.rows().getFirst().value().snapshot().rule().key()).isNotEqualTo(second.rows().getFirst().value().snapshot().rule().key());
            assertThat(List.of(first.rows().getFirst().value().snapshot().rule().key(),second.rows().getFirst().value().snapshot().rule().key()))
                    .containsExactlyInAnyOrder(original.key(),key);
        });
    }
    @Test void recoveryPageDiscoversOriginalResourceRemovalAndRetirementRechecksIt() {
        withStore(f->{
            var facts=businessFacts(f);accept(f,facts);var resource=facts.head.snapshot().rule().key().resource();
            assertThat(f.store.evaluationStates(null,10).removedResources()).isEmpty();
            f.domain.artifacts().delete("orders",f.domain.artifacts().sourceBinding("orders").orElseThrow().contentHash());
            assertThat(f.store.evaluationStates(null,10).removedResources()).containsExactly(resource);
            f.domain.artifacts().create(source("orders"));
            assertThat(f.store.evaluationStates(null,10).removedResources()).containsExactly(resource);
            f.time.set(T.plusSeconds(1));var old=f.store.state(facts.head.snapshot().rule().key(),readAll()).orElseThrow();
            var ended=AlertLifecycle.terminate(old.value().snapshot(),AlertSnapshot.EndReason.RESOURCE_REMOVED,f.time.get());
            var episode=new AlertStateRecord.Episode(old.value().episode().originalBinding(),ended.snapshot().episode(),POLICY,old.value().episode().diagnostics(),false);
            var head=new AlertStateRecord.Head(ended.snapshot(),POLICY.deployment(),old.value().owner(),f.time.get(),false,episode,old.value().binding());
            assertThat(f.store.commit(new Commit(CommitKind.END,new StateChange(old.revision(),old.value(),head,List.of(),null),List.of(),List.of()))).isEqualTo(Outcome.STAGED);
            assertThat(f.store.state(ended.snapshot().rule().key(),readAll()).orElseThrow().value().cleanupAt()).isNotNull();
        });
    }
    @Test void staleDisabledSnapshotCannotTerminateACurrentlyReenabledRule() {
        withStore(f -> {
            var facts = businessFacts(f); accept(f, facts);
            var original = (Rule) f.store.definition(Kind.RULE, "rule-a", f.access).orElseThrow();
            var disabled = new Rule(original.header().next(T.plusSeconds(1), f.access.subject()), original.name(),
                    original.typeId(), original.typeVersion(), original.resources(), original.parameters(), null, false, null);
            f.time.set(T.plusSeconds(1)); f.store.saveDefinition(original, disabled, f.access);
            var enabled = new Rule(disabled.header().next(T.plusSeconds(2), f.access.subject()), disabled.name(),
                    disabled.typeId(), disabled.typeVersion(), disabled.resources(), disabled.parameters(), null, true, null);
            f.time.set(T.plusSeconds(2)); f.store.saveDefinition(disabled, enabled, f.access);
            var before = f.store.state(facts.head.snapshot().rule().key(), readAll()).orElseThrow();
            var ended = AlertLifecycle.terminate(before.value().snapshot(), AlertSnapshot.EndReason.DISABLED, f.time.get());
            var oldEpisode = before.value().episode();
            var episode = new AlertStateRecord.Episode(oldEpisode.originalBinding(), ended.snapshot().episode(), oldEpisode.policy(), oldEpisode.diagnostics(), false);
            var after = new AlertStateRecord.Head(ended.snapshot(), POLICY.deployment(), before.value().owner(), null, false, episode, before.value().binding());
            var change = new StateChange(before.revision(), before.value(), after, List.of(), null);
            assertThatThrownBy(() -> f.store.commit(new Commit(CommitKind.END, change, List.of(), List.of())))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.store.state(facts.head.snapshot().rule().key(), readAll()).orElseThrow()).isEqualTo(before);
        });
    }
    @Container static final MongoDBContainer MONGO=new MongoDBContainer(DockerImageName.parse("mongo:7.0")).withCommand("--replSet","docker-rs","--setParameter","enableTestCommands=1");
    static final Instant T=Instant.parse("2026-10-28T00:00:00Z");static final AlarmKey.Scope SCOPE=new AlarmKey.Scope("installation-a","cluster-a");
    static final AlertIntent.Payload PAYLOAD=new AlertIntent.Payload("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    static final AlertIntent.Policy POLICY=AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(AlertChannelPolicy.Deployment.ON_PREM));
    static final AlertChannelPolicy.Capability EMAIL=new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL);
    static final String UUID_A="11111111-1111-4111-8111-111111111111";

    @Test void stateScheduleAndIntentAbortTogetherOnALaterCasConflictAndReplayOriginalKeys() {
        withStore(f->{var facts=businessFacts(f);var change=new StateChange(0,null,facts.head,List.of(),null);
            var intent=new IntentChange(null,facts.intent);var bad=new ScheduleChange(new Stored<>(99,facts.schedule),facts.schedule);
            long originalRevision=revision(f);
            assertThat(f.store.commit(new Commit(CommitKind.STATE_AND_PLAN,change,List.of(intent),List.of(bad)))).isEqualTo(Outcome.CONFLICT);
            assertThat(SystemCollections.ALERT_INSTANCES.on(f.db).countDocuments()).isZero();assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isZero();
            assertThat(revision(f)).isEqualTo(originalRevision);
            var command=new Commit(CommitKind.STATE_AND_PLAN,change,List.of(intent),List.of(new ScheduleChange(null,facts.schedule)));
            assertThat(f.store.commit(command)).isEqualTo(Outcome.STAGED);assertThat(f.store.commit(command)).isEqualTo(Outcome.UNCHANGED);
            assertThat(f.store.state(facts.head.snapshot().rule().key(),readAll()).orElseThrow().revision()).isOne();
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isOne();assertThat(SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).countDocuments()).isOne();
            assertThat(revision(f)).isEqualTo(originalRevision);
        });
    }
    @Test void currentRevocationAndSourceRecreationRefuseNewPermitsWithoutAdvancingBudget() {
        withStore(f->{var facts=businessFacts(f);assertThat(accept(f,facts)).isEqualTo(Outcome.STAGED);
            var original=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var pending=AlertDeliveryFixtures.pending(original.value(),"worker-a","attempt-a");
            f.domain.artifacts().delete("orders",f.domain.artifacts().sourceBinding("orders").orElseThrow().contentHash());
            f.domain.artifacts().create(source("orders"));
            assertThatThrownBy(()->f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(original,pending)),List.of()))).isInstanceOf(TapstateException.class);
            assertThat(f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow().value().state().attemptsConsumed()).isZero();
            f.domain.tokens().revoke(f.access.actor().subject());
            assertThatThrownBy(()->f.store.definition(Kind.RULE,"rule-a",f.access)).isInstanceOf(TapstateException.class);
        });
    }
    @Test void fixedPermitResultAndTargetRestrictionRemainOneTransactionAndSurviveReopen() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var original=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var pending=AlertDeliveryFixtures.pending(original.value(),"worker-a","attempt-a");
            assertThat(f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(original,pending)),List.of()))).isEqualTo(Outcome.STAGED);
            var permit=pending.state().attempts().getFirst().permit();
            try(var material=f.store.resolvePermittedMaterial(pending.frozen().identity(),permit,null)){assertThat(material.authentication()).isNull();}
            f.time.set(T.plusSeconds(1));var accepted=AlertDeliveryFixtures.accepted(pending,T.plusSeconds(1));
            var before=f.store.intent(pending.frozen().identity(),f.access).orElseThrow();
            assertThat(f.store.commit(new Commit(CommitKind.RESULT,null,List.of(new IntentChange(before,accepted)),List.of()))).isEqualTo(Outcome.STAGED);
            try(var reopened=MongoConfigurationDomain.open(f.settings,SCOPE.clusterId())){
                var view=reopened.alerts(f.host,BuiltinAlarmTypes.catalog(),f.clock);
                assertThat(view.intent(pending.frozen().identity(),f.access).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            }
            assertThatThrownBy(()->f.store.resolvePermittedMaterial(pending.frozen().identity(),permit,null)).isInstanceOf(TapstateException.class);
        });
    }
    @Test void testAdmissionIsAtomicWithQuotaAndSameKeyReplayPrecedesExpiredOrMissingMaterial() {
        withStore(f->{var candidate=test(f,"1",email("one@example.test"));var admitted=f.store.admitTest(candidate,f.access);
            assertThat(admitted.revision()).isOne();assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).countDocuments()).isOne();
            var row=SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).find().first();assertThat(row.getInteger("formatVersion")).isEqualTo(2);
            var pending=AlertDeliveryFixtures.pending(admitted.value(),"worker-a","test-attempt");
            assertThat(f.store.commitAsCaller(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(admitted,pending)),List.of()),f.access)).isEqualTo(Outcome.STAGED);
            try(var material=f.store.resolvePermittedMaterial(pending.frozen().identity(),pending.state().attempts().getFirst().permit(),f.access)){assertThat(material.authentication()).isNull();}
            f.time.set(T.plusSeconds(600));assertThat(f.store.admitTest(candidate,f.access).value().frozen()).isEqualTo(admitted.value().frozen());
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isOne();assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).find().first().getLong("recordRevision")).isOne();
        });
    }
    @Test void canonicalTargetBusyAndPerActorQuotaCannotBeBypassedByNewCandidateIds() {
        withStore(f->{var first=test(f,"1",email("one@example.test"));f.store.admitTest(first,f.access);
            assertThatThrownBy(()->f.store.admitTest(test(f,"2",email("one@example.test")),f.access)).isInstanceOf(TapstateException.class);
            f.store.admitTest(test(f,"3",email("two@example.test")),f.access);f.store.admitTest(test(f,"4",email("three@example.test")),f.access);
            assertThatThrownBy(()->f.store.admitTest(test(f,"5",email("four@example.test")),f.access)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isEqualTo(3);
        });
    }
    @Test void lostQuotaDoesNotResetExistingAcceptedTestsAndFailedAdmissionLeavesNoIntent() {
        withStore(f->{f.store.admitTest(test(f,"1",email("one@example.test")),f.access);
            SystemCollections.ALERT_TEST_QUOTAS.on(f.db).deleteMany(new Document());
            assertThatThrownBy(()->f.store.admitTest(test(f,"2",email("two@example.test")),f.access)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isOne();assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).countDocuments()).isZero();
        });
    }
    @Test void boundedQueriesUseTheSameResourcePredicateAndNeverWriteOrTreatUnknownAsHealthy() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var key=facts.head.snapshot().rule().key();
            long before=revision(f),fence=SystemCollections.CONFIGURATION_VERSION.on(f.db).find().first().getLong("alertWriteFence");
            var allowed=new ReadAccess(false,Set.of(key.resource()),()->{});var page=f.store.active(null,1,allowed);
            assertThat(page.total()).isOne();assertThat(page.rows()).hasSize(1);assertThat(page.isolatedRecords()).isFalse();
            var hidden=new ReadAccess(false,Set.of(),()->{});assertThat(f.store.active(null,5,hidden).total()).isZero();
            assertThat(f.store.history(key,null,1,allowed).rows()).hasSize(1);
            assertThat(f.store.dueIntents(T.plusSeconds(1),null,1).rows()).hasSize(1);
            assertThat(f.store.dueSchedules(T.plusSeconds(121),null,1).rows()).hasSize(1);
            assertThatThrownBy(()->f.store.active(null,101,readAll())).isInstanceOf(TapstateException.class);
            assertThat(revision(f)).isEqualTo(before);assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.db).find().first().getLong("alertWriteFence")).isEqualTo(fence);
            var unknown=new Document("_id",MongoAlertStateCodec.key(new AlarmKey(SCOPE,key.resource(),"future.type","future"))).append("formatVersion",99).append("status","FIRING");
            SystemCollections.ALERT_INSTANCES.on(f.db).insertOne(unknown);assertThat(f.store.active(null,5,allowed).isolatedRecords()).isTrue();assertThat(f.store.active(null,5,hidden).isolatedRecords()).isFalse();
        });
    }
    @Test void downgradedHumanRoleCannotAuthorizeNewAlertManagement() {
        withStore(f->{
            var actor=new AlertTestAuthority(AlertTestAuthority.Mechanism.HUMAN_JWT,"installation-a","alice","ADMIN","a".repeat(64),T.plusSeconds(900));
            var access=new Access(actor,()->{},()->{},()->"a".repeat(64));
            var users=SystemCollections.CONFIG_USERS.on(f.db);users.insertOne(new Document("_id","alice").append("passwordHash","isolated-test-hash").append("role","admin"));
            assertThat(f.store.definition(Kind.RULE,"missing",access)).isEmpty();
            users.updateOne(new Document("_id","alice"),new Document("$set",new Document("role","reader")));
            assertThatThrownBy(()->f.store.definition(Kind.RULE,"missing",access)).isInstanceOf(TapstateException.class);
        });
    }
    @Test void matchingSilencePreventsANewPermitEvenWhenAllFrozenBindingsStillMatch() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);
            var silence=new Silence(header(Kind.SILENCE,"maintenance",f.access.actor().subject()),"Maintenance",new AlertSilenceWindow(SCOPE,true,Set.of(),Set.of(),T,Duration.ofMinutes(5),null,null));
            f.store.saveDefinition(null,silence,f.access);var old=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var pending=AlertDeliveryFixtures.pending(old.value(),"worker-a","attempt-a");
            assertThatThrownBy(()->f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(old,pending)),List.of()))).isInstanceOf(TapstateException.class);
            assertThat(f.store.intent(old.value().frozen().identity(),f.access).orElseThrow().value().state().attemptsConsumed()).isZero();
        });
    }
    @Test void faultPermitCannotContinueAfterItsEpisodeHasRecovered() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var oldState=f.store.state(facts.head.snapshot().rule().key(),readAll()).orElseThrow();
            var r=oldState.value().snapshot().rule();var signal=new AlertSignal(r.key(),r.revision(),r.generation(),r.producerIncarnation(),2,T.plusSeconds(1),AlertSignal.Condition.GOOD,AlarmSeverity.MAJOR);
            f.time.set(T.plusSeconds(1));var resolved=AlertLifecycle.evaluate(oldState.value().snapshot(),signal,f.time.get()).snapshot();
            var ep=new AlertStateRecord.Episode(r,resolved.episode(),POLICY,facts.head.episode().diagnostics(),false);
            var head=new AlertStateRecord.Head(resolved,POLICY.deployment(),oldState.value().owner(),null,false,ep,facts.head.binding());
            f.store.commit(new Commit(CommitKind.STATE_AND_PLAN,new StateChange(1,oldState.value(),head,List.of(),null),List.of(),List.of()));
            var old=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var permit=DeliveryAttempt.grant(SCOPE,old.value().frozen().identity().deliveryId(),"late-attempt",1,1,"worker-a",old.value().frozen().retry(),T,old.value().frozen().deadline(),T.plusSeconds(1)).orElseThrow();
            var st=old.value().state();var next=new AlertIntent.Record(old.value().frozen(),new AlertIntent.State(new AlertOwnership(1,"worker-a",T.plusSeconds(120)),0,false,null,null,1,List.of(AlertIntent.Attempt.pending(permit)),DeliveryEvidence.Outcome.UNKNOWN,st.retained(),st.payload(),st.candidate(),null,null,null));
            assertThatThrownBy(()->f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(old,next)),List.of()))).isInstanceOf(TapstateException.class);
        });
    }
    @Test void materialResolutionCannotCrossTheOriginalAttemptDeadlineDuringPreparation() {
        withStore(f->{var transport=new AlertReceiver.Email("smtp.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.PLAIN,"account-a","alerts@example.test",List.of("one@example.test"),AlertReceiver.Trust.SYSTEM);
            var candidate=test(f,"1",transport);var old=f.store.admitTest(candidate,f.access);var pending=AlertDeliveryFixtures.pending(old.value(),"worker-a","attempt-a");
            f.store.commitAsCaller(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(old,pending)),List.of()),f.access);
            var calls=new java.util.concurrent.atomic.AtomicInteger();var host=new Host(SCOPE,f.host.setup(),f.host.deployment(),()->{if(calls.incrementAndGet()==5)f.time.set(T.plusSeconds(61));}).withChannels(Set.of(EMAIL));
            var view=f.domain.alerts(host,BuiltinAlarmTypes.catalog(),f.clock);var permit=pending.state().attempts().getFirst().permit();
            assertThatThrownBy(()->view.resolvePermittedMaterial(pending.frozen().identity(),permit,f.access)).isInstanceOf(TapstateException.class);
        });
    }
    @Test void cloudReadCredentialCannotAdministrativelyEndState() {
        withStore(f->{var scope=new AlarmKey.Scope("managed-org",SCOPE.clusterId());var key=new AlarmKey(scope,new AlarmKey.Resource("cluster","cluster-a","installation-a"),"cluster.storage-unavailable","rule-a");
            var r=new AlertRule(key,1,1,"producer-a",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
            var signal=new AlertSignal(key,1,1,"producer-a",1,T,AlertSignal.Condition.BAD,AlarmSeverity.CRITICAL);var snapshot=AlertLifecycle.evaluate(AlertSnapshot.initial(r),signal,T).snapshot();
            var policy=AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(AlertChannelPolicy.Deployment.CLOUD_MANAGED));
            var binding=new AlertIntent.Member(r,1,new Identity(Kind.RULE,scope,"removed-rule",UUID_A),0);
            var old=new AlertStateRecord.Head(snapshot,policy.deployment(),AlertOwnership.unclaimed(),null,false,new AlertStateRecord.Episode(r,snapshot.episode(),policy,AlertStateRecord.Diagnostics.empty(false),false),binding);
            SystemCollections.ALERT_INSTANCES.on(f.db).insertOne(MongoAlertStateEnvelope.head(old,1));SystemCollections.ALERT_EPISODES.on(f.db).insertOne(MongoAlertStateEnvelope.episode(old.episode()));
            var ended=AlertLifecycle.terminate(snapshot,AlertSnapshot.EndReason.DISABLED,T).snapshot();var after=new AlertStateRecord.Head(ended,policy.deployment(),old.owner(),null,false,new AlertStateRecord.Episode(r,ended.episode(),policy,old.episode().diagnostics(),false),binding);
            var view=f.domain.alerts(new Host(scope,f.host.setup(),policy.deployment(),()->{}),BuiltinAlarmTypes.catalog(),f.clock);
            var access=Access.cloud("cloud-user","managed-org","READ",()->{},()->{});var change=new Commit(CommitKind.END,new StateChange(1,old,after,List.of(),null),List.of(),List.of());
            assertThatThrownBy(()->view.commitAsCaller(change,access)).isInstanceOf(TapstateException.class);
            assertThat(view.commitAsCaller(change,Access.cloud("cloud-writer","managed-org","WRITE",()->{},()->{}))).isEqualTo(Outcome.STAGED);
        });
    }

    @Test void sourceAbaAndRuleReplacementCanCloseTheOriginalEpisodeUnderItsOriginalBinding() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var key=facts.head.snapshot().rule().key();
            f.domain.artifacts().save(new SourceResource("orders",null,"mongodb",Map.of("database","changed"),null,null,null,null));f.domain.artifacts().save(source("orders"));
            var binding=f.domain.artifacts().sourceBinding("orders").orElseThrow().identity();assertThat(binding.bindingRevision()).isEqualTo(3);
            f.time.set(T.plusSeconds(1));var original=(Rule)f.store.definition(Kind.RULE,"rule-a",f.access).orElseThrow();
            var parameters=new java.util.HashMap<>(original.parameters());parameters.put("trigger-for",java.math.BigDecimal.ONE);
            var changed=new Rule(original.header().next(f.time.get(),f.access.actor().subject()),original.name(),original.typeId(),original.typeVersion(),original.resources(),parameters,original.severityOverride(),true,original.metric());
            f.store.saveDefinition(original,changed,f.access);var current=(Rule)f.store.definition(Kind.RULE,"rule-a",f.access).orElseThrow();assertThat(current.evaluationRevision()).isEqualTo(2);
            var rule=new AlertRule(key,current.evaluationRevision(),2,"producer-b",Duration.ofSeconds(1),Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
            var update=AlertLifecycle.replace(facts.head.snapshot(),rule,f.time.get());var e=new AlertStateRecord.Episode(facts.head.episode().originalBinding(),update.snapshot().episode(),POLICY,facts.head.episode().diagnostics(),false);
            var after=new AlertStateRecord.Head(update.snapshot(),POLICY.deployment(),facts.head.owner(),null,false,e,new AlertIntent.Member(rule,1,changed.header().identity(),3));
            assertThat(f.store.commit(new Commit(CommitKind.STATE_AND_PLAN,new StateChange(1,facts.head,after,List.of(),null),List.of(),List.of()))).isEqualTo(Outcome.STAGED);
            var saved=f.store.state(key,readAll()).orElseThrow().value();assertThat(saved.episode().originalBinding()).isEqualTo(facts.head.snapshot().rule());assertThat(saved.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.SUPERSEDED);
        });
    }
    @Test void missingOriginalTargetHeadCannotBeRecreatedByANewDeliveryForTheSameTarget() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var frozen=facts.intent.frozen();var target=((AlertIntent.ReceiverTarget)frozen.target()).transport();var d=AlertTargetIdentity.domains(SCOPE,target).get(frozen.identity().targetId());
            var limits=SystemCollections.ALERT_TARGET_LIMITS.on(f.db);limits.deleteMany(new Document());limits.insertOne(MongoAlertNotificationCodec.limit(new AlertTargetLimit(d,T.plusSeconds(600)),1));limits.deleteMany(new Document());
            var previous=((AlertIntent.Business)frozen.identity()).key();var n=previous.notification();var key=new DeliveryKey(new DeliveryKey.NotificationRound(n.scope(),n.subjectId(),n.subjectIncarnation(),n.routeId(),n.routeIncarnation(),DeliveryKey.Kind.REMINDER,2),previous.channelType(),previous.receiverIncarnation(),previous.targetId());
            var next=new AlertIntent.Frozen(new AlertIntent.Business(key),frozen.channel(),frozen.route(),frozen.target(),frozen.content(),frozen.members(),frozen.retry(),frozen.retention(),T,frozen.deadline());var candidate=AlertDeliveryFixtures.initial(next,PAYLOAD,null);
            assertThatThrownBy(()->f.store.commit(new Commit(CommitKind.PLAN,null,List.of(new IntentChange(null,candidate)),List.of()))).isInstanceOf(TapstateException.class);
            assertThat(limits.countDocuments()).isZero();assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isOne();
        });
    }

    @Test void duplicateCommandsRaceWithoutDoubleQuotaAndExpiredResultLookupNeedsNoCandidateMaterial() {
        withStore(f->{var candidate=test(f,"1",email("one@example.test"));var start=new java.util.concurrent.CountDownLatch(1);
            try(var executor=java.util.concurrent.Executors.newFixedThreadPool(2)){
                var a=executor.submit(()->{start.await();return f.store.admitTest(candidate,f.access);});var b=executor.submit(()->{start.await();return f.store.admitTest(candidate,f.access);});start.countDown();
                assertThat(a.get(20,java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(b.get(20,java.util.concurrent.TimeUnit.SECONDS));
            }catch(Exception e){throw new AssertionError(e);}
            f.time.set(T.plusSeconds(600));assertThat(f.store.test(((AlertIntent.Test)candidate.frozen().identity()).command().key(),f.access)).isPresent();
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isOne();assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).find().first().getLong("recordRevision")).isOne();
        });
    }
    @Test void scopeQuotaRaceAdmitsOnlyOneOfTheLastTwoActors() {
        withStore(f->{var actors=new ArrayList<Fixture>();actors.add(f);
            for(int i=1;i<5;i++){var token=new TokenRecord("token-"+i,"ADMIN","isolated-"+i,false,T);f.domain.tokens().save(token);
                var actor=new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a",token.tokenId(),"ADMIN",token.authenticationBinding(),null);
                actors.add(new Fixture(f.domain,f.db,f.settings,f.store,f.host,new Access(actor,()->{},()->{},()->""),f.time,f.clock));}
            for(int i=0;i<9;i++){var actor=actors.get(i/3);actor.store.admitTest(test(actor,Integer.toHexString(i+1),email("recipient"+i+"@example.test")),actor.access);}
            var a=actors.get(3);var b=actors.get(4);var ca=test(a,"a",email("last-a@example.test"));var cb=test(b,"b",email("last-b@example.test"));var start=new java.util.concurrent.CountDownLatch(1);
            try(var executor=java.util.concurrent.Executors.newFixedThreadPool(2)){
                var futures=List.of(executor.submit(()->{start.await();try{a.store.admitTest(ca,a.access);return true;}catch(TapstateException refused){return false;}}),
                        executor.submit(()->{start.await();try{b.store.admitTest(cb,b.access);return true;}catch(TapstateException refused){return false;}}));start.countDown();
                int accepted=0;for(var future:futures)if(future.get(20,java.util.concurrent.TimeUnit.SECONDS))accepted++;assertThat(accepted).isOne();
            }catch(Exception e){throw new AssertionError(e);}
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isEqualTo(10);
            var row=SystemCollections.ALERT_TEST_QUOTAS.on(f.db).find().first();assertThat(MongoAlertNotificationCodec.readQuota(SCOPE,AlertTestQuota.minute(T),row).value().scopeAccepted()).isEqualTo(10);
        });
    }
    @Test void explicitRetainedCredentialsAreSealedFromTheOriginalReceiverAndStaleEditingBaselineIsRejected() {
        withStore(f->{var h=header(Kind.RECEIVER,"existing",f.access.actor().subject());var transport=new AlertReceiver.Email("smtp.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.PLAIN,"account-a","alerts@example.test",List.of("one@example.test"),AlertReceiver.Trust.SYSTEM);
            var reference=new AlertReceiver.CredentialReference(h.identity(),"22222222-2222-4222-8222-222222222222",AlertReceiver.authenticationIdentity(transport));
            var receiver=new AlertReceiver(h,"Original",true,1,transport,reference);
            try(var secret=new PrivateMaterial("original-test-password".getBytes(java.nio.charset.StandardCharsets.UTF_8),null,null)){f.store.saveReceiver(null,receiver,secret,false,f.access);}
            var key=new AlertTestCommand.Key(SCOPE,f.access.actor().principalKey(),new AlertTestCommand.ClientId(T,"1".repeat(32)));
            var parameters=new AlertTestCommand.Parameters("email",new AlertTestCommand.Candidate("current-form","a".repeat(64)),new AlertTestCommand.ExistingReceiver(UUID_A,1),"one@example.test","binding-a",null,PAYLOAD.sha256());
            var prepared=f.store.prepareCandidate(key,parameters,transport,h.identity(),1,null,Set.of(PrivateField.AUTHENTICATION),f.access);
            var accepted=new AlertTestCommand.Accepted(key,prepared.parameters(),T,T.plusSeconds(300));
            var frozen=new AlertIntent.Frozen(new AlertIntent.Test(accepted),EMAIL,null,prepared.target(),content(),List.of(),ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5),Duration.ofSeconds(60)),POLICY,T,T.plusSeconds(300),f.access.actor());
            var candidate=AlertDeliveryFixtures.initial(frozen,PAYLOAD,prepared.encrypted());var admitted=f.store.admitTest(candidate,f.access);var pending=AlertDeliveryFixtures.pending(admitted.value(),"worker-a","attempt-a");
            f.store.commitAsCaller(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(admitted,pending)),List.of()),f.access);
            try(var restored=f.store.resolvePermittedMaterial(frozen.identity(),pending.state().attempts().getFirst().permit(),f.access)){assertThat(restored.authentication()).isEqualTo("original-test-password".getBytes(java.nio.charset.StandardCharsets.UTF_8));}
            var changed=receiver.edit("Renamed",true,transport,reference,T,f.access.actor().subject());f.store.saveReceiver(receiver,changed,null,true,f.access);
            assertThatThrownBy(()->f.store.prepareCandidate(new AlertTestCommand.Key(SCOPE,key.actorPrincipal(),new AlertTestCommand.ClientId(T,"2".repeat(32))),parameters,transport,h.identity(),1,null,Set.of(PrivateField.AUTHENTICATION),f.access)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.ALERT_RECEIVERS.on(f.db).countDocuments()).isOne();
        });
    }
    @Test void activeKeysetPagesKeepTiesStableAndExcludeHigherSeverityHiddenResources() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var old=facts.head;
            for(int i=0;i<3;i++){var resource=i==2?new AlarmKey.Resource("source","hidden","hidden-incarnation"):old.snapshot().rule().key().resource();
                var key=new AlarmKey(SCOPE,resource,"source.connection-failed","other-"+i);var rule=new AlertRule(key,1,1,"producer-a",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
                var severity=i==2?AlarmSeverity.CRITICAL:AlarmSeverity.MAJOR;var signal=new AlertSignal(key,1,1,"producer-a",1,T,AlertSignal.Condition.BAD,severity);var snapshot=AlertLifecycle.evaluate(AlertSnapshot.initial(rule),signal,T).snapshot();
                var ep=new AlertStateRecord.Episode(rule,snapshot.episode(),POLICY,AlertStateRecord.Diagnostics.empty(true),false);var h=new AlertStateRecord.Head(snapshot,POLICY.deployment(),AlertOwnership.unclaimed(),null,false,ep);
                SystemCollections.ALERT_INSTANCES.on(f.db).insertOne(MongoAlertStateEnvelope.head(h,1));SystemCollections.ALERT_EPISODES.on(f.db).insertOne(MongoAlertStateEnvelope.episode(ep));
            }
            var access=new ReadAccess(false,Set.of(old.snapshot().rule().key().resource()),()->{});var seen=new HashSet<AlarmKey>();ActiveCursor cursor=null;
            do{var page=f.store.active(cursor,1,access);assertThat(page.total()).isEqualTo(3);assertThat(page.rows()).hasSize(1);var key=page.rows().getFirst().value().snapshot().rule().key();assertThat(seen.add(key)).isTrue();cursor=page.next();}while(cursor!=null);
            assertThat(seen).hasSize(3);assertThat(seen).allMatch(k->!k.resource().id().equals("hidden"));
        });
    }
    @Test void absentChannelCapabilityPausesAStoredTaskBeforeAnyPermitIsWritten() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var view=f.domain.alerts(new Host(SCOPE,f.host.setup(),POLICY.deployment(),()->{}),BuiltinAlarmTypes.catalog(),f.clock);
            var old=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();var pending=AlertDeliveryFixtures.pending(old.value(),"worker-a","attempt-a");
            assertThatThrownBy(()->view.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(old,pending)),List.of()))).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(AlertError.CHANNEL_UNAVAILABLE));
            assertThat(f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow().value().state().attemptsConsumed()).isZero();
        });
    }

    @Test void managedSdkStatePlanPermitAndAcceptedEventDoNotUseOnPremControlRows() {
        withStore(f->{
            var scope=new AlarmKey.Scope("managed-org",SCOPE.clusterId());var route=new AlertIntent.SystemRoute("managed-route","managed-route-binding",1);var target=new AlertIntent.SdkTarget("managed-target",SCOPE.clusterId(),"managed-sdk-binding");
            var channel=new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK);
            var host=new Host(scope,f.host.setup(),AlertChannelPolicy.Deployment.CLOUD_MANAGED,()->{},route,target,r->Optional.empty(),Set.of(channel));
            var view=f.domain.alerts(host,BuiltinAlarmTypes.catalog(),f.clock);var access=Access.cloud("cloud-writer","managed-org","WRITE",()->{},()->{});
            f.domain.artifacts().create(source("cloud-orders"));var binding=f.domain.artifacts().sourceBinding("cloud-orders").orElseThrow().identity();var resource=new AlarmKey.Resource("source","cloud-orders",binding.incarnation());
            var identity=new Identity(Kind.RULE,scope,"cloud-rule",UUID_A);var header=Header.create(identity,host.deployment(),T,"cloud-writer");
            var rule=new Rule(header,"Connection","source.connection-failed",1,new Resources(Set.of("source"),false,Set.of(resource)),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());view.saveDefinition(null,rule,access);
            var key=new AlarmKey(scope,resource,rule.typeId(),UUID_A);var evaluation=new AlertRule(key,1,1,"producer-a",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
            var signal=new AlertSignal(key,1,1,"producer-a",1,T,AlertSignal.Condition.BAD,AlarmSeverity.MAJOR);var snapshot=AlertLifecycle.evaluate(AlertSnapshot.initial(evaluation),signal,T).snapshot();
            var policy=AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(host.deployment()));var member=new AlertIntent.Member(evaluation,1,identity,binding.bindingRevision());
            var head=new AlertStateRecord.Head(snapshot,host.deployment(),AlertOwnership.unclaimed(),null,false,new AlertStateRecord.Episode(evaluation,snapshot.episode(),policy,AlertStateRecord.Diagnostics.empty(true),false),member);
            var delivery=new DeliveryKey(DeliveryKey.NotificationRound.forEpisode(key,1,route.id(),route.incarnation(),DeliveryKey.Kind.FAULT,1),"cloud-sdk",target.incarnation(),target.logicalTargetId());
            var frozen=new AlertIntent.Frozen(new AlertIntent.Business(delivery),channel,route,target,new AlertIntent.Content("application/json",PAYLOAD.sha256(),new AlertIntent.Template("fault",1,1,"en",MessageSettings.empty()),false),List.of(member),new ChannelRetryPolicy(1,0,List.of(),Duration.ofDays(1),Duration.ofSeconds(30)),policy,T,T.plusSeconds(86400));
            var initial=AlertDeliveryFixtures.initial(frozen,PAYLOAD,null);var grouping=new AlertGroupingClock.Policy(Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(120));
            var schedule=new AlertSchedule.Record(AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.EPISODE,delivery),route,policy,T,AlertOwnership.unclaimed(),grouping,new AlertGroupingClock(T,T,false,1,null),0,List.of(),null,null,List.of(),List.of(delivery),null,false,List.of(member));
            assertThat(view.commit(new Commit(CommitKind.STATE_AND_PLAN,new StateChange(0,null,head,List.of(),null),List.of(new IntentChange(null,initial)),List.of(new ScheduleChange(null,schedule))))).isEqualTo(Outcome.STAGED);
            var old=view.intent(frozen.identity(),access).orElseThrow();var pending=AlertDeliveryFixtures.pending(initial,"worker-a","sdk-attempt");assertThat(view.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(old,pending)),List.of()))).isEqualTo(Outcome.STAGED);
            f.time.set(T.plusSeconds(1));var accepted=AlertDeliveryFixtures.accepted(pending,T.plusSeconds(1));var prior=view.intent(frozen.identity(),access).orElseThrow();
            assertThat(view.commit(new Commit(CommitKind.RESULT,null,List.of(new IntentChange(prior,accepted)),List.of()))).isEqualTo(Outcome.STAGED);
            var saved=view.intent(frozen.identity(),access).orElseThrow().value();assertThat(saved.state().acceptedReceipt().source()).isEqualTo("sdk-event-center");assertThat(saved.state().acceptedReceipt().remoteId()).isEqualTo("event-a");
            assertThat(SystemCollections.ALERT_TARGET_LIMITS.on(f.db).countDocuments()).isZero();assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).countDocuments()).isZero();
            f.time.set(T.plus(Duration.ofHours(2)));view.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
            var pruned=view.intent(frozen.identity(),access).orElseThrow().value();assertThat(pruned.state().payload()).isNull();assertThat(pruned.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(pruned.state().attempts()).hasSize(1);
        });
    }

    @Test void maintenanceSettlesOriginalDeadlinePrunesPayloadAndKeepsActiveAlarmAndTargetLimit() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var port=f.store.maintenance().orElseThrow();
            var before=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var pending=AlertDeliveryFixtures.pending(before.value(),"worker-a","maintenance-attempt");
            f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(before,pending)),List.of()));
            f.time.set(T.plusSeconds(61));var batch=clean(f,0,null);
            assertThat(f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow().value().state().retained().stoppedAt()).isNull();
            assertThat(f.store.state(facts.head.snapshot().rule().key(),readAll()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
            f.time.set(T.plus(Duration.ofDays(5)));clean(f,0,null);var stopped=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow().value();
            assertThat(stopped.state().retained().stoppedAt()).isEqualTo(facts.intent.frozen().deadline());
            assertThat(stopped.state().attempts().getFirst().outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
            assertThat(stopped.state().payload()).isNull();assertThat(stopped.state().payloadPrunedAt()).isEqualTo(f.time.get());
            clean(f,4,null);assertThat(f.store.active(null,5,readAll()).total()).isOne();
            var inventory=port.inventory();assertThat(inventory.logicalBytes()).isPositive();assertThat(inventory.resources()).hasSize(12);
        });
    }
    @Test void acceptedPayloadExpiresEarlyAndResultHandoffPreservesRecoveryEvidence() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var before=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var pending=AlertDeliveryFixtures.pending(before.value(),"worker-a","maintenance-accepted");f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(before,pending)),List.of()));
            f.time.set(T.plusSeconds(1));var accepted=AlertDeliveryFixtures.accepted(pending,f.time.get());
            f.store.commit(new Commit(CommitKind.RESULT,null,List.of(new IntentChange(f.store.intent(pending.frozen().identity(),f.access).orElseThrow(),accepted)),List.of()));
            f.time.set(T.plus(Duration.ofDays(2)));clean(f,0,null);
            var pruned=f.store.intent(accepted.frozen().identity(),f.access).orElseThrow().value();assertThat(pruned.state().payload()).isNull();assertThat(pruned.state().attempts()).hasSize(1);
            f.time.set(T.plus(Duration.ofDays(8)));for(int i=0;i<3;i++)clean(f,0,null);
            assertThat(f.store.intent(accepted.frozen().identity(),f.access)).isEmpty();
            var schedule=SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).find().first();var row=MongoAlertScheduleCodec.read(facts.schedule.key(),schedule).value();
            assertThat(row.summaries()).singleElement().satisfies(summary->assertThat(summary.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED));
            assertThat(row.protectedIntents()).isEmpty();assertThat(row.summariesHandedOff()).isFalse();
            assertThat(f.store.active(null,5,readAll()).total()).isOne();
        });
    }
    @Test void expiredHistoryReleasesOnlyItsOriginalPointerAndPreservesNextEpisodeSequence() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).deleteMany(new Document());SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).deleteMany(new Document());
            var key=facts.head.snapshot().rule().key();var original=facts.head.snapshot();f.time.set(T.plusSeconds(1));
            var result=AlertLifecycle.evaluate(original,new AlertSignal(key,1,1,"producer-a",2,f.time.get(),AlertSignal.Condition.GOOD,AlarmSeverity.MAJOR),f.time.get());
            var episode=new AlertStateRecord.Episode(original.rule(),result.snapshot().episode(),POLICY,AlertStateRecord.Diagnostics.empty(true),false);
            var head=new AlertStateRecord.Head(result.snapshot(),POLICY.deployment(),AlertOwnership.unclaimed(),null,false,episode,facts.head.binding());
            var headRow=MongoAlertStateEnvelope.head(head,2);SystemCollections.ALERT_INSTANCES.on(f.db).replaceOne(new Document("_id",headRow.get("_id")),headRow);
            var epRow=MongoAlertStateEnvelope.episode(episode);SystemCollections.ALERT_EPISODES.on(f.db).replaceOne(new Document("_id",epRow.get("_id")),epRow);
            f.time.set(T.plus(Duration.ofDays(31)));clean(f,2,null);
            var after=f.store.state(key,readAll()).orElseThrow().value();assertThat(after.snapshot().episode()).isNull();assertThat(after.snapshot().episodeSequence()).isOne();
            assertThat(f.store.history(key,null,10,readAll()).rows()).isEmpty();
            var next=AlertLifecycle.evaluate(after.snapshot(),new AlertSignal(key,1,1,"producer-a",3,f.time.get(),AlertSignal.Condition.BAD,AlarmSeverity.MAJOR),f.time.get());assertThat(next.snapshot().episode().number()).isEqualTo(2);
        });
    }
    @Test void maintenanceCursorAdvancesAcrossUnknownRowsAndObeysIoBudget() {
        withStore(f->{var unknown=new Document("_id",new Document("authorityId",SCOPE.authorityId()).append("clusterId",SCOPE.clusterId()).append("purpose","business").append("deliveryId","a-unknown"))
                    .append("formatVersion",999).append("cleanupAt",Date.from(T.minusSeconds(100)));
            var candidate=test(f,"1",email("one@example.test"));f.store.admitTest(candidate,f.access);
            SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).insertOne(unknown);f.time.set(T.plus(Duration.ofDays(5)));
            var first=f.store.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),100,1024*1024,Duration.ofSeconds(2));
            assertThat(first.scanned()).isLessThanOrEqualTo(100);assertThat(first.bytes()).isLessThanOrEqualTo(1024*1024);assertThat(first.next()).isNotNull();
            var second=clean(f,0,first.next());clean(f,0,null);clean(f,0,null);assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).find(new Document("_id",unknown.get("_id"))).first()).isEqualTo(unknown);
            assertThat(f.store.test(((AlertIntent.Test)candidate.frozen().identity()).command().key(),f.access)).isEmpty();
            assertThat(second.skipped()).containsKey("unreadable");
        });
    }
    @Test void maintenanceExcludesTestsFromRealPendingAndFailureCountsAndExpiredReplayCannotResend() {
        withStore(f->{var candidate=test(f,"1",email("one@example.test"));f.store.admitTest(candidate,f.access);var port=f.store.maintenance().orElseThrow();
            assertThat(port.inventory().pending()).isZero();f.time.set(T.plus(Duration.ofDays(5)));clean(f,0,null);clean(f,0,null);
            assertThat(port.inventory().failed()).isZero();assertThatThrownBy(()->f.store.admitTest(candidate,f.access)).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(AlertError.COMMAND_EXPIRED));
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).countDocuments()).isZero();
        });
    }
    @Test void maintenanceRetainsRetiredReceiverUntilItsSubscriptionReleasesTheExactReference() {
        withStore(f->{var facts=businessFacts(f);var receiver=(AlertReceiver)f.store.definition(Kind.RECEIVER,"receiver-a",f.access).orElseThrow();
            f.time.set(T.plusSeconds(1));f.store.saveReceiver(receiver,receiver.retire(f.time.get(),f.access.subject()),null,true,f.access);
            f.time.set(T.plus(Duration.ofDays(8)));clean(f,6,null);
            assertThat(f.store.definition(Kind.RECEIVER,"receiver-a",f.access)).isPresent();
            var route=(Subscription)f.store.definition(Kind.SUBSCRIPTION,"route-a",f.access).orElseThrow();
            f.store.saveDefinition(route,route.retire(f.time.get(),f.access.subject()),f.access);
            f.time.set(T.plus(Duration.ofDays(16)));clean(f,7,null);clean(f,6,null);
            assertThat(f.store.definition(Kind.RECEIVER,"receiver-a",f.access)).isEmpty();
        });
    }
    @Test void expiredTargetLimitIsNotPinnedByUnrelatedEmailTraffic() {
        withStore(f->{var candidate=test(f,"1",email("one@example.test"));f.store.admitTest(candidate,f.access);
            var other=AlertTargetIdentity.domains(SCOPE,email("other@example.test")).values().iterator().next();
            SystemCollections.ALERT_TARGET_LIMITS.on(f.db).insertOne(MongoAlertNotificationCodec.limit(new AlertTargetLimit(other,T.minusSeconds(4000)),1));
            var batch=clean(f,9,null);assertThat(SystemCollections.ALERT_TARGET_LIMITS.on(f.db).find(new Document("_id",MongoAlertNotificationCodec.limitKey(other))).first()).as("%s",batch).isNull();
            var original=AlertTargetIdentity.domains(SCOPE,email("one@example.test")).values().iterator().next();
            var old=SystemCollections.ALERT_TARGET_LIMITS.on(f.db).find(new Document("_id",MongoAlertNotificationCodec.limitKey(original))).first();
            assertThat(old).isNotNull();
        });
    }
    @Test void projectedPagesBoundActualCommandsAndBytesEvenWhenANonNodeRowHasAnOversizedNodeField(){
        withStore(f->{
            var poisoned=MongoAlertNotificationCodec.quota(new AlertTestQuota.Bucket(SCOPE,T.minusSeconds(9000),Map.of("original-actor",1)),1);
            poisoned.append("nodeId","x".repeat(4*1024*1024-16*1024));SystemCollections.ALERT_TEST_QUOTAS.on(f.db).insertOne(poisoned);
            var arrayPoison=MongoAlertNotificationCodec.quota(new AlertTestQuota.Bucket(SCOPE,T.minusSeconds(9060),Map.of("original-actor",1)),1);
            arrayPoison.put("cleanupAt",List.of(Date.from(T.minusSeconds(5400)),"x".repeat(4*1024*1024-16*1024)));SystemCollections.ALERT_TEST_QUOTAS.on(f.db).insertOne(arrayPoison);
            for(int i=0;i<8;i++)SystemCollections.ALERT_TEST_QUOTAS.on(f.db).insertOne(MongoAlertNotificationCodec.quota(
                    new AlertTestQuota.Bucket(SCOPE,T.minusSeconds(7200+i*60L),Map.of("original-actor",1)),1));
            AlertMaintenance.Cursor cursor=null;long changed=0,isolated=0;
            for(int i=0;i<4;i++){
                var commands=new ArrayList<String>();var observed=new java.util.concurrent.atomic.AtomicLong();
                MongoCommandObservation.CURRENT.set(commands::add);MongoCommandObservation.BYTES.set(observed::addAndGet);AlertMaintenance.Batch batch;
                try{batch=clean(f,10,cursor);}finally{MongoCommandObservation.CURRENT.remove();MongoCommandObservation.BYTES.remove();}
                assertThat(commands.size()).isLessThanOrEqualTo(batch.scanned());assertThat(batch.scanned()).isLessThanOrEqualTo(200);
                assertThat(observed.get()).isLessThanOrEqualTo(batch.bytes());assertThat(batch.bytes()).isLessThanOrEqualTo(4L*1024*1024);
                changed+=batch.changed();isolated+=batch.skipped().getOrDefault("unreadable",0L);cursor=batch.next();if(cursor==null)break;
            }
            assertThat(changed).isEqualTo(8);assertThat(isolated).isPositive();assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).countDocuments()).isEqualTo(2);
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).find(new Document("_id",poisoned.get("_id"))).first()).isEqualTo(poisoned);
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).find(new Document("_id",arrayPoison.get("_id"))).first()).isEqualTo(arrayPoison);
        });
    }
    @Test void nodeDiscoveryDoesNotFetchAnUnboundedForeignCleanupTimestampField(){
        withStore(f->{var poisoned=new Document("_id","e".repeat(64)).append("formatVersion",1).append("authorityId",SCOPE.authorityId()).append("clusterId",SCOPE.clusterId())
                    .append("phase","EXPECTED").append("nodeId","poisoned-node").append("bootId","original-boot").append("cleanupAt","x".repeat(4*1024*1024-16*1024));
            SystemCollections.ALERT_NODE_LIFECYCLES.on(f.db).insertOne(poisoned);
            var commands=new ArrayList<String>();var observed=new java.util.concurrent.atomic.AtomicLong();MongoCommandObservation.CURRENT.set(commands::add);MongoCommandObservation.BYTES.set(observed::addAndGet);AlertMaintenance.Batch batch;
            try{batch=clean(f,11,null);}finally{MongoCommandObservation.CURRENT.remove();MongoCommandObservation.BYTES.remove();}
            assertThat(batch.changed()).isZero();assertThat(batch.skipped().getOrDefault("unreadable",0L)).isPositive();
            assertThat(commands.size()).isLessThanOrEqualTo(batch.scanned());assertThat(observed.get()).isLessThanOrEqualTo(batch.bytes());
            assertThat(batch.scanned()).isLessThanOrEqualTo(200);assertThat(batch.bytes()).isLessThanOrEqualTo(4L*1024*1024);
            assertThat(SystemCollections.ALERT_NODE_LIFECYCLES.on(f.db).find(new Document("_id",poisoned.get("_id"))).first()).isEqualTo(poisoned);
        });
    }
    @Test void validActiveHeadsAndSchedulesWithNullCleanupTimestampsAreNotIsolated(){
        withStore(f->{var facts=businessFacts(f);assertThat(accept(f,facts)).isEqualTo(Outcome.STAGED);
            var head=SystemCollections.ALERT_INSTANCES.on(f.db).find().first();var schedule=SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).find().first();
            assertThat(head.get("cleanupAt")).isNull();assertThat(schedule.get("cleanupAt")).isNull();
            for(int collection:List.of(1,4)){
                var batch=clean(f,collection,null);assertThat(batch.changed()).isZero();assertThat(batch.skipped()).doesNotContainKey("unreadable");
            }
            assertThat(SystemCollections.ALERT_INSTANCES.on(f.db).find().first()).isEqualTo(head);assertThat(SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).find().first()).isEqualTo(schedule);
        });
    }
    @Test void boundAndRecoveredAuthorityCommandsFitEveryWarmMaintenancePage(){
        for(boolean recovered:List.of(false,true))withStore(f->{
            String suffix=java.util.UUID.randomUUID().toString().replace("-","");String cacheName="alert_budget_cache_"+suffix;
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(cacheName),cacheName,null);
            var masterUri=MONGO.getReplicaSetUrl("alert_budget_master_"+suffix);
            var master=new SourceResource("views",null,"mongodb",Map.of("isUri",true,"uri",masterUri),null,null,null,null);
            try(var probe=com.mongodb.client.MongoClients.create(masterUri)){probe.getDatabase("admin").runCommand(new Document("ping",1));}
            try{
                var committed=f.domain.bindings().commitWithFirstCache(f.host.setup(),StorageBinding.Mode.OP,connection,master);
                try(var cache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"alert-budget-authority")){
                    cache.prepareFirstAuth(committed.firstCreation().orElseThrow());cache.prepareInitialBusiness(committed.preparation(),()->{});
                    assertThat(cache.loadAuthScope(committed.preparation().authScope())).isEqualTo(committed.preparation().authScope());
                }
                f.domain.bindings().complete(f.host.setup());
                f.domain.firstAdmin().create(f.domain.firstAdmin().load().orElseThrow().scope(),new User("budget-admin","isolated-test-hash","admin"));
                if(recovered){
                    try(var client=com.mongodb.client.MongoClients.create(connection.uri())){client.getDatabase(cacheName).drop();}
                    try(var cache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"alert-budget-recovery")){
                        var prepared=f.domain.prepareAuthenticationRecovery(cache,()->{});f.domain.requireAuthenticationPublished(prepared);
                        assertThat(prepared.authScope().authGeneration()).isEqualTo(2);assertThat(prepared.authScope().cacheIncarnation()).isNotEqualTo(committed.preparation().authScope().cacheIncarnation());
                    }
                }
                var setup=SystemCollections.STORAGE_SETUP.on(f.db).find(new Document("_id","cluster")).first();
                var scope=new StorageSetupScope(SCOPE.clusterId(),setup.getString("installationId"),setup.getString("operationId"),setup.getLong("generation"));
                var host=new Host(SCOPE,scope,AlertChannelPolicy.Deployment.ON_PREM,()->{}).withChannels(Set.of(EMAIL));
                var store=f.domain.alerts(host,BuiltinAlarmTypes.catalog(),f.clock);var port=store.maintenance().orElseThrow();
                port.maintain(new AlertMaintenance.Cursor(4,null,null),200,4L*1024*1024,Duration.ofSeconds(10));
                for(int i=0;i<12;i++)SystemCollections.ALERT_TEST_QUOTAS.on(f.db).insertOne(MongoAlertNotificationCodec.quota(
                        new AlertTestQuota.Bucket(SCOPE,T.minusSeconds(7200+i*60L),Map.of("original-actor",1)),1));
                AlertMaintenance.Cursor cursor=new AlertMaintenance.Cursor(10,null,null);long changed=0;
                for(int i=0;i<4;i++){
                    var commands=new ArrayList<String>();var bytes=new java.util.concurrent.atomic.AtomicLong();MongoCommandObservation.CURRENT.set(commands::add);MongoCommandObservation.BYTES.set(bytes::addAndGet);AlertMaintenance.Batch batch;
                    try{batch=port.maintain(cursor,200,4L*1024*1024,Duration.ofSeconds(10));}finally{MongoCommandObservation.CURRENT.remove();MongoCommandObservation.BYTES.remove();}
                    assertThat(commands.size()).as("recovered=%s; charged=%s; commands=%s",recovered,batch.scanned(),commands).isLessThanOrEqualTo(batch.scanned());
                    assertThat(bytes.get()).isLessThanOrEqualTo(batch.bytes());assertThat(batch.scanned()).isLessThanOrEqualTo(200);assertThat(batch.bytes()).isLessThanOrEqualTo(4L*1024*1024);
                    changed+=batch.changed();cursor=batch.next();if(cursor==null)break;
                }
                assertThat(changed).isEqualTo(12);
            }finally{try(var client=com.mongodb.client.MongoClients.create(connection.uri())){client.getDatabase(cacheName).drop();}}
        });
    }
    @Test void maintenanceConservativeBudgetBoundsObservedMongoCommandsIncludingAuthorityAndCommit() {
        withStore(f->{var candidate=test(f,"1",email("one@example.test"));f.store.admitTest(candidate,f.access);f.time.set(T.plus(Duration.ofDays(5)));
            var commands=new ArrayList<String>();MongoCommandObservation.CURRENT.set(commands::add);
            AlertMaintenance.Batch batch;
            try{batch=clean(f,0,null);}finally{MongoCommandObservation.CURRENT.remove();}
            assertThat(commands).contains("find","update","commitTransaction");
            assertThat(commands.size()).isLessThanOrEqualTo(batch.scanned()).isLessThanOrEqualTo(200);
        });
    }
    @Test void maximalScheduleReferencesMakeBoundedProgressWithoutStarvingTheFollowingCandidate() {
        withStore(f->{var facts=businessFacts(f);var members=new ArrayList<AlertIntent.Member>();var summaries=new ArrayList<AlertSchedule.Previous>();
            for(int i=0;i<100;i++) {
                var key=new AlarmKey(SCOPE,new AlarmKey.Resource("source","historical-"+i,"creation-"+i),"source.connection-failed",UUID_A);
                var binding=new AlertRule(key,1,1,"original",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
                members.add(new AlertIntent.Member(binding,1,facts.head.binding().ruleIdentity(),1));
                summaries.add(new AlertSchedule.Previous(key,2,DeliveryEvidence.Outcome.ACCEPTED,false));
                for(long episode=1;episode<=2;episode++) {
                    var ended=new AlertSnapshot.Episode(episode,T,AlarmSeverity.MAJOR,T.plusSeconds(1),T.plusSeconds(1),AlertSnapshot.EndReason.RECOVERED);
                    SystemCollections.ALERT_EPISODES.on(f.db).insertOne(MongoAlertStateEnvelope.episode(new AlertStateRecord.Episode(binding,ended,POLICY,AlertStateRecord.Diagnostics.empty(false),false)));
                }
            }
            var key=facts.schedule.key();var schedule=new AlertSchedule.Record(key,facts.schedule.route(),POLICY,T,AlertOwnership.unclaimed(),facts.schedule.grouping(),facts.schedule.clock(),0,List.of(),null,null,summaries,List.of(),T.plusSeconds(2),true,members);
            var row=MongoAlertScheduleCodec.write(schedule,1);SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).insertOne(row);f.time.set(T.plus(Duration.ofDays(40)));
            boolean removed=false;int visits=0;
            for(int i=0;i<8&&!removed;i++){var page=clean(f,1,null);visits++;assertThat(page.scanned()).isLessThanOrEqualTo(200);removed=SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).find(new Document("_id",row.get("_id"))).first()==null;}
            assertThat(removed).isTrue();assertThat(visits).isLessThanOrEqualTo(3);
        });
    }

    @Test void pressureRefusesFreshTestsButPreservesAcceptedResultsAndRequiresTwoFreshReliefSamples() {
        withStore(f->{var facts=businessFacts(f);accept(f,facts);var original=f.store.intent(facts.intent.frozen().identity(),f.access).orElseThrow();
            var pending=AlertDeliveryFixtures.pending(original.value(),"worker-a","pressure-result");f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(original,pending)),List.of()));
            var port=f.store.maintenance().orElseThrow();port.enableCapacityProtection();
            f.time.set(T.plusSeconds(60));capacity(f,AlertMaintenancePolicy.CAPACITY_BYTES);
            assertThatThrownBy(()->f.store.admitTest(test(f,"1",email("other@example.test")),f.access)).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.args()).containsEntry("reason","storage-pressure"));
            var accepted=AlertDeliveryFixtures.accepted(pending,f.time.get());var before=f.store.intent(pending.frozen().identity(),f.access).orElseThrow();
            assertThat(f.store.commit(new Commit(CommitKind.RESULT,null,List.of(new IntentChange(before,accepted)),List.of()))).isEqualTo(Outcome.STAGED);
            f.time.set(T.plusSeconds(120));capacity(f,0);
            assertThatThrownBy(()->f.store.admitTest(test(f,"1",email("other@example.test")),f.access)).isInstanceOf(TapstateException.class);
            f.time.set(T.plusSeconds(180));capacity(f,0);assertThat(f.store.admitTest(test(f,"1",email("other@example.test")),f.access).revision()).isOne();
            f.time.set(T.plusSeconds(301));assertThatThrownBy(()->f.store.admitTest(test(f,"2",email("third@example.test")),f.access)).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.args()).containsEntry("reason","capacity-unavailable"));
        });
    }
    private static void capacity(Fixture f,long bytes) {
        try{var method=f.store.getClass().getDeclaredMethod("observedCapacity",AlertMaintenance.Inventory.class);method.setAccessible(true);
            method.invoke(f.store,new AlertMaintenance.Inventory(f.time.get(),List.of(new AlertMaintenance.Usage("alert_instances",1,bytes,0,0,null)),0,null,0,0));
        }catch(ReflectiveOperationException failure){throw new AssertionError(failure);}
    }
    @Test void oneTimeExpiredSilenceIsReclaimedButAnUnboundedPeriodicWindowRemains() {
        withStore(f->{var once=new Silence(header(Kind.SILENCE,"one-time",f.access.subject()),"Once",new AlertSilenceWindow(SCOPE,true,Set.of(),Set.of(),T,Duration.ofMinutes(1),null,null));
            var periodic=new Silence(header(Kind.SILENCE,"periodic",f.access.subject()),"Periodic",new AlertSilenceWindow(SCOPE,true,Set.of(),Set.of(),T,Duration.ofMinutes(1),Duration.ofDays(1),null));
            f.store.saveDefinition(null,once,f.access);f.store.saveDefinition(null,periodic,f.access);f.time.set(T.plus(Duration.ofDays(8)));
            clean(f,8,null);assertThat(f.store.definition(Kind.SILENCE,"one-time",f.access)).isEmpty();assertThat(f.store.definition(Kind.SILENCE,"periodic",f.access)).isPresent();
        });
    }

    @Test void moreLargeSchedulesThanProofCacheSlotsStillFinishTheirOriginalCandidates() {
        withStore(f->{var facts=businessFacts(f);var members=new ArrayList<AlertIntent.Member>();var summaries=new ArrayList<AlertSchedule.Previous>();
            for(int i=0;i<100;i++){
                var key=new AlarmKey(SCOPE,new AlarmKey.Resource("source","retained-"+i,"creation-"+i),"source.connection-failed",UUID_A);
                var rule=new AlertRule(key,1,1,"original",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
                members.add(new AlertIntent.Member(rule,1,facts.head.binding().ruleIdentity(),1));
                summaries.add(new AlertSchedule.Previous(key,2,DeliveryEvidence.Outcome.ACCEPTED,false));
                for(long number=1;number<=2;number++){var ended=new AlertSnapshot.Episode(number,T,AlarmSeverity.MAJOR,T.plusSeconds(1),T.plusSeconds(1),AlertSnapshot.EndReason.RECOVERED);
                    SystemCollections.ALERT_EPISODES.on(f.db).insertOne(MongoAlertStateEnvelope.episode(new AlertStateRecord.Episode(rule,ended,POLICY,AlertStateRecord.Diagnostics.empty(false),false)));}
            }
            for(int i=0;i<17;i++){
                var old=facts.schedule.key();var key=new AlertSchedule.Key(SCOPE,old.kind(),old.subjectId()+"-"+i,old.subjectIncarnation(),old.routeId(),old.routeIncarnation(),old.channelType(),old.receiverIncarnation(),old.targetId());
                var schedule=new AlertSchedule.Record(key,facts.schedule.route(),POLICY,T,AlertOwnership.unclaimed(),facts.schedule.grouping(),facts.schedule.clock(),0,List.of(),null,null,summaries,List.of(),T.plusSeconds(2),true,members);
                SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).insertOne(MongoAlertScheduleCodec.write(schedule,1));
            }
            f.time.set(T.plus(Duration.ofDays(40)));AlertMaintenance.Cursor cursor=null;int batches=0;
            while(SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).countDocuments()>0&&batches++<60){var batch=clean(f,1,cursor);cursor=batch.next();assertThat(batch.scanned()).isLessThanOrEqualTo(200);}
            assertThat(SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.db).countDocuments()).isZero();
        });
    }
    @Test void manyLimitCandidatesCannotEvictTheirInProgressNegativeTargetScans() {
        withStore(f->{var original=test(f,"1",email("one@example.test"));f.store.admitTest(original,f.access);
            for(int i=0;i<100;i++){
                var at=T.minusSeconds((i/10+1)*60L);var old=(AlertIntent.Test)original.frozen().identity();
                var command=new AlertTestCommand.Accepted(new AlertTestCommand.Key(SCOPE,old.command().key().actorPrincipal(),new AlertTestCommand.ClientId(at,String.format("%032x",i+10))),old.command().parameters(),at,at.plusSeconds(300));
                var frozen=original.frozen();var copy=new AlertIntent.Frozen(new AlertIntent.Test(command),frozen.channel(),null,frozen.target(),frozen.content(),List.of(),frozen.retry(),frozen.retention(),at,command.deadline(),frozen.testAuthority());
                SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).insertOne(MongoAlertDeliveryCodec.write(AlertDeliveryFixtures.initial(copy,PAYLOAD,null),1));
            }
            for(int i=0;i<17;i++){
                var domain=AlertTargetIdentity.domains(SCOPE,email("other-"+i+"@example.test")).values().iterator().next();
                SystemCollections.ALERT_TARGET_LIMITS.on(f.db).insertOne(MongoAlertNotificationCodec.limit(new AlertTargetLimit(domain,T.minusSeconds(4000)),1));
            }
            f.time.set(T.plusSeconds(4000));AlertMaintenance.Cursor cursor=null;int batches=0;
            while(SystemCollections.ALERT_TARGET_LIMITS.on(f.db).countDocuments()>1&&batches++<60){var batch=clean(f,9,cursor);cursor=batch.next();assertThat(batch.scanned()).isLessThanOrEqualTo(200);}
            assertThat(SystemCollections.ALERT_TARGET_LIMITS.on(f.db).countDocuments()).isOne();
        });
    }
    @Test void actualLargeConditionalWriteBytesStayInsideTheSameBatchAllowance() {
        withStore(f->{var facts=businessFacts(f);byte[] bytes=new byte[256*1024];Arrays.fill(bytes,(byte)'a');var payload=new AlertIntent.Payload(bytes);var frozen=facts.intent.frozen();
            var large=new AlertIntent.Frozen(frozen.identity(),frozen.channel(),frozen.route(),frozen.target(),new AlertIntent.Content(frozen.content().contentType(),payload.sha256(),frozen.content().template(),false),frozen.members(),frozen.retry(),frozen.retention(),T,frozen.deadline());
            var initial=AlertDeliveryFixtures.initial(large,payload,null);f.store.commit(new Commit(CommitKind.STATE_AND_PLAN,new StateChange(0,null,facts.head,List.of(),null),List.of(new IntentChange(null,initial)),List.of(new ScheduleChange(null,facts.schedule))));
            var pending=AlertDeliveryFixtures.pending(initial,"worker","large-attempt");var before=f.store.intent(large.identity(),f.access).orElseThrow();f.store.commit(new Commit(CommitKind.PERMIT,null,List.of(new IntentChange(before,pending)),List.of()));
            f.time.set(T.plusSeconds(1));var accepted=AlertDeliveryFixtures.accepted(pending,f.time.get());before=f.store.intent(large.identity(),f.access).orElseThrow();f.store.commit(new Commit(CommitKind.RESULT,null,List.of(new IntentChange(before,accepted)),List.of()));
            f.time.set(T.plus(Duration.ofDays(2)));var observed=new java.util.concurrent.atomic.AtomicLong();MongoCommandObservation.BYTES.set(observed::addAndGet);AlertMaintenance.Batch batch;
            try{batch=clean(f,0,null);}finally{MongoCommandObservation.BYTES.remove();}
            assertThat(batch.changed()).isPositive();assertThat(observed.get()).isGreaterThan(payload.size()).isLessThanOrEqualTo(batch.bytes());assertThat(batch.bytes()).isLessThanOrEqualTo(4L*1024*1024);
        });
    }
    @Test void transientSchemaFailureDoesNotRunUnbilledVerificationCallbacks() {
        withStore(f->{var admin=f.db.getName();try(var client=MongoClients.create(MONGO.getReplicaSetUrl())){
            var control=client.getDatabase("admin");control.runCommand(new Document("configureFailPoint","failCommand").append("mode",new Document("times",100)).append("data",new Document("failCommands",List.of("find")).append("namespace",admin+".schema_migrations").append("errorCode",112).append("errorLabels",List.of("TransientTransactionError"))));
            var count=new java.util.concurrent.atomic.AtomicInteger();MongoCommandObservation.COLLECTIONS.set((name,collection)->{if(name.equals("find")&&collection.equals("schema_migrations"))count.incrementAndGet();});
            try{assertThatThrownBy(()->f.store.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),200,4L*1024*1024,Duration.ofSeconds(2))).isInstanceOf(TapstateException.class);assertThat(count.get()).isOne();}
            finally{MongoCommandObservation.COLLECTIONS.remove();control.runCommand(new Document("configureFailPoint","failCommand").append("mode","off"));}
        }});
    }

    @Test void unsampledAdmissionReservationsBoundBurstsAndClearOnlyAfterFreshObservations() {
        withStore(f->{var port=f.store.maintenance().orElseThrow();port.inventory();port.enableCapacityProtection();
            try{var method=f.store.getClass().getDeclaredMethod("admission",String.class,int.class);method.setAccessible(true);method.invoke(f.store,"alert_notification_schedules",320);}
            catch(ReflectiveOperationException failure){throw new AssertionError(failure);}
            assertThat(port.diagnostics().get("admissionReservedBytes")).isEqualTo(320L*(AlertSchedule.MAX_BYTES+AlertIntent.MAX_BYTES));
            assertThatThrownBy(()->f.store.admitTest(test(f,"1",email("other@example.test")),f.access)).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.args()).containsEntry("reason","storage-pressure"));
            var facts=businessFacts(f);assertThat(accept(f,facts)).isEqualTo(Outcome.STAGED);
            f.time.set(T.plusSeconds(60));port.inventory();assertThatThrownBy(()->f.store.admitTest(test(f,"1",email("other@example.test")),f.access)).isInstanceOf(TapstateException.class);
            f.time.set(T.plusSeconds(120));port.inventory();assertThat(f.store.admitTest(test(f,"1",email("other@example.test")),f.access).revision()).isOne();
        });
    }

    @Test void aTransientCandidateWriteRetriesOnlyInsideItsBilledBatch() {
        withStore(f->{var minute=T.minusSeconds(7200);var row=MongoAlertNotificationCodec.quota(new AlertTestQuota.Bucket(SCOPE,minute,Map.of("original-actor",1)),1);
            SystemCollections.ALERT_TEST_QUOTAS.on(f.db).insertOne(row);
            try(var client=MongoClients.create(MONGO.getReplicaSetUrl())){var admin=client.getDatabase("admin");
                admin.runCommand(new Document("configureFailPoint","failCommand").append("mode",new Document("times",1)).append("data",new Document("failCommands",List.of("update")).append("namespace",f.db.getName()+".configuration_version").append("errorCode",112).append("errorLabels",List.of("TransientTransactionError"))));
                try{var batch=clean(f,10,null);assertThat(batch.skipped()).containsEntry("conflict",1L);assertThat(batch.scanned()).isLessThanOrEqualTo(200);assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.db).countDocuments()).isZero();}
                finally{admin.runCommand(new Document("configureFailPoint","failCommand").append("mode","off"));}
            }
        });
    }

    @Test void anOversizedUnknownIdentityIsRetainedWithoutBlockingOrdinaryCleanup() {
        withStore(f->{var candidate=test(f,"1",email("one@example.test"));f.store.admitTest(candidate,f.access);
            var bad=new Document("_id",new Document("authorityId",SCOPE.authorityId()).append("clusterId",SCOPE.clusterId()).append("purpose","business").append("deliveryId","a".repeat(10000)))
                    .append("formatVersion",999).append("cleanupAt",Date.from(T.minusSeconds(3600)));
            SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).insertOne(bad);f.time.set(T.plus(Duration.ofDays(5)));
            for(int i=0;i<3;i++){var batch=clean(f,0,null);assertThat(batch.skipped()).containsKey("unreadable");}
            assertThat(f.store.test(((AlertIntent.Test)candidate.frozen().identity()).command().key(),f.access)).isEmpty();
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.db).find(new Document("_id",bad.get("_id"))).first()).isEqualTo(bad);
        });
    }
    @Test void fullOrStaleCapacityStillAllowsOriginalRecoveryStateIntentAndRequiredSchedule() {
        for(boolean stale:List.of(false,true))withStore(f->{var facts=businessFacts(f);accept(f,facts);var port=f.store.maintenance().orElseThrow();port.enableCapacityProtection();
            f.time.set(T.plusSeconds(1));capacity(f,AlertMaintenancePolicy.CAPACITY_BYTES);if(stale)f.time.set(T.plusSeconds(122));
            var before=f.store.state(facts.head.snapshot().rule().key(),readAll()).orElseThrow();var old=before.value();var binding=old.snapshot().rule();
            var signal=new AlertSignal(binding.key(),binding.revision(),binding.generation(),binding.producerIncarnation(),2,f.time.get(),AlertSignal.Condition.GOOD,AlarmSeverity.MAJOR);
            var resolved=AlertLifecycle.evaluate(old.snapshot(),signal,f.time.get());var ep=new AlertStateRecord.Episode(old.episode().originalBinding(),resolved.snapshot().episode(),POLICY,old.episode().diagnostics(),false);
            var head=new AlertStateRecord.Head(resolved.snapshot(),POLICY.deployment(),old.owner(),null,false,ep,old.binding());
            var original=((AlertIntent.Business)facts.intent.frozen().identity()).key();var round=DeliveryKey.NotificationRound.forEpisode(binding.key(),1,original.notification().routeId(),original.notification().routeIncarnation(),DeliveryKey.Kind.RECOVERY,1);
            var key=new DeliveryKey(round,original.channelType(),original.receiverIncarnation(),original.targetId());var frozen=facts.intent.frozen();
            var recoveryFrozen=new AlertIntent.Frozen(new AlertIntent.Business(key),frozen.channel(),frozen.route(),frozen.target(),frozen.content(),frozen.members(),frozen.retry(),POLICY,f.time.get(),f.time.get().plusSeconds(86400));
            var recovery=AlertDeliveryFixtures.initial(recoveryFrozen,PAYLOAD,null);
            var schedule=new AlertSchedule.Record(AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,key),frozen.route(),POLICY,f.time.get(),AlertOwnership.unclaimed(),facts.schedule.grouping(),AlertGroupingClock.empty(),0,List.of(),null,null,List.of(),List.of(key),null,false,frozen.members());
            var commit=new Commit(CommitKind.STATE_AND_PLAN,new StateChange(before.revision(),old,head,List.of(),signal),List.of(new IntentChange(null,recovery)),List.of(new ScheduleChange(null,schedule)));
            assertThat(f.store.commit(commit)).isEqualTo(Outcome.STAGED);assertThat(f.store.state(binding.key(),readAll()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
            assertThat(f.store.intent(recoveryFrozen.identity(),f.access)).isPresent();
        });
    }

    private static AlertMaintenance.Batch clean(Fixture f,int collection,AlertMaintenance.Cursor cursor) {
        return f.store.maintenance().orElseThrow().maintain(cursor==null?new AlertMaintenance.Cursor(collection,null,null):cursor,200,4L*1024*1024,Duration.ofSeconds(5));
    }

    private static Outcome accept(Fixture f,Facts facts){return f.store.commit(new Commit(CommitKind.STATE_AND_PLAN,new StateChange(0,null,facts.head,List.of(),null),List.of(new IntentChange(null,facts.intent)),List.of(new ScheduleChange(null,facts.schedule))));}
    private record Facts(AlertStateRecord.Head head,AlertIntent.Record intent,AlertSchedule.Record schedule){ }
    private static Facts businessFacts(Fixture f){
        f.domain.artifacts().create(source("orders"));var binding=f.domain.artifacts().sourceBinding("orders").orElseThrow().identity();
        var resource=new AlarmKey.Resource("source","orders",binding.incarnation());var resources=new Resources(Set.of("source"),false,Set.of(resource));
        var rule=new Rule(header(Kind.RULE,"rule-a",f.access.actor().subject()),"Connection","source.connection-failed",1,resources,Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
        assertThat(f.store.saveDefinition(null,rule,f.access)).isEqualTo(Outcome.STAGED);
        var receiver=new AlertReceiver(header(Kind.RECEIVER,"receiver-a",f.access.actor().subject()),"Mail",true,1,email("one@example.test"),null);
        assertThat(f.store.saveReceiver(null,receiver,null,false,f.access)).isEqualTo(Outcome.STAGED);
        var route=new Subscription(header(Kind.SUBSCRIPTION,"route-a",f.access.actor().subject()),"Route",new Match(resources,Set.of(),Set.of()),true,List.of(new RouteChannel("email",List.of(receiver.header().identity()),MessageSettings.empty())),Set.of(),new AlertGroupingClock.Policy(Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(120)),List.of());
        assertThat(f.store.saveDefinition(null,route,f.access)).isEqualTo(Outcome.STAGED);
        var key=new AlarmKey(SCOPE,resource,"source.connection-failed",UUID_A);var eval=new AlertRule(key,1,1,"producer-a",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(30));
        var signal=new AlertSignal(key,1,1,"producer-a",1,T,AlertSignal.Condition.BAD,AlarmSeverity.MAJOR);var snapshot=AlertLifecycle.evaluate(AlertSnapshot.initial(eval),signal,T).snapshot();
        var member=new AlertIntent.Member(eval,1,rule.header().identity(),binding.bindingRevision());
        var ep=new AlertStateRecord.Episode(eval,snapshot.episode(),POLICY,AlertStateRecord.Diagnostics.empty(true),false);
        var head=new AlertStateRecord.Head(snapshot,POLICY.deployment(),AlertOwnership.unclaimed(),null,false,ep,member);
        var target=AlertTargetIdentity.domains(SCOPE,receiver.transport()).keySet().iterator().next();
        var delivery=new DeliveryKey(DeliveryKey.NotificationRound.forEpisode(key,1,"route-a",UUID_A,DeliveryKey.Kind.FAULT,1),"email",UUID_A,target);
        var frozen=new AlertIntent.Frozen(new AlertIntent.Business(delivery),EMAIL,new AlertIntent.ConfiguredRoute(route.header().identity(),1),AlertIntent.ReceiverTarget.capture(receiver),content(),List.of(member),ChannelRetryPolicy.emailDefaults(),POLICY,T,T.plusSeconds(86400));
        var intent=AlertDeliveryFixtures.initial(frozen,PAYLOAD,null);
        var schedule=new AlertSchedule.Record(AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.EPISODE,delivery),frozen.route(),POLICY,T,AlertOwnership.unclaimed(),route.grouping(),new AlertGroupingClock(T,T,false,1,null),0,List.of(),null,null,List.of(),List.of(delivery),null,false,List.of(member));
        return new Facts(head,intent,schedule);
    }
    private static AlertIntent.Record test(Fixture f,String nonce,AlertReceiver.Email transport){
        Instant created=f.time.get();
        var key=new AlertTestCommand.Key(SCOPE,f.access.actor().principalKey(),new AlertTestCommand.ClientId(created,nonce.repeat(32)));
        String target=AlertTargetIdentity.domains(SCOPE,transport).keySet().iterator().next();
        var base=new AlertTestCommand.Parameters("email",new AlertTestCommand.Candidate("candidate-"+nonce,"a".repeat(64)),null,target,"binding-a",null,PAYLOAD.sha256());
        PreparedCandidate p;
        try(var material=transport.authentication()==AlertReceiver.Authentication.NONE?null:new PrivateMaterial("isolated-test-password".getBytes(java.nio.charset.StandardCharsets.UTF_8),null,null)){
            p=f.store.prepareCandidate(key,base,transport,null,0,material,f.access);
        }
        var accepted=new AlertTestCommand.Accepted(key,p.parameters(),created,created.plusSeconds(300));
        var frozen=new AlertIntent.Frozen(new AlertIntent.Test(accepted),EMAIL,null,p.target(),content(),List.of(),ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5),Duration.ofSeconds(60)),POLICY,created,created.plusSeconds(300),f.access.actor());
        return AlertDeliveryFixtures.initial(frozen,PAYLOAD,p.encrypted());
    }
    private static AlertIntent.Content content(){return new AlertIntent.Content("message/rfc822",PAYLOAD.sha256(),new AlertIntent.Template("fault",1,1,"en",MessageSettings.empty()),false);}
    private static AlertReceiver.Email email(String recipient){return new AlertReceiver.Email("smtp.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of(recipient),AlertReceiver.Trust.SYSTEM);}
    private static Header header(Kind kind,String id,String actor){return Header.create(new Identity(kind,SCOPE,id,UUID_A),AlertChannelPolicy.Deployment.ON_PREM,T,actor);}
    private static SourceResource source(String id){return new SourceResource(id,null,"mongodb",Map.of("database","isolated"),null,null,null,null);}
    private static ReadAccess readAll(){return new ReadAccess(true,Set.of(),()->{});}
    private static long revision(Fixture f){return SystemCollections.CONFIGURATION_VERSION.on(f.db).find().first().getLong("revision");}
    private record Fixture(MongoConfigurationDomain domain,MongoDatabase db,MongoConnectionSettings settings,AlertStore store,Host host,Access access,AtomicReference<Instant> time,Clock clock){ }
    private static void withStore(java.util.function.Consumer<Fixture> work){
        try(var client=MongoClients.create(MONGO.getReplicaSetUrl())){var db=client.getDatabase("alert_coordinator_"+java.util.UUID.randomUUID().toString().replace("-",""));var settings=new MongoConnectionSettings(MONGO.getReplicaSetUrl(db.getName()),null,Duration.ofSeconds(5));
            try(var domain=MongoConfigurationBootstrap.prepare(settings,SCOPE.clusterId(),"installation-a","setup-a",1)){
                var token=new TokenRecord("operator-token","ADMIN","isolated-test-hash",false,T);domain.tokens().save(token);
                var actor=new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a",token.tokenId(),"ADMIN",token.authenticationBinding(),null);var access=new Access(actor,()->{},()->{},()->"");
                var time=new AtomicReference<>(T);Clock clock=new Clock(){public java.time.ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(java.time.ZoneId zone){return this;}public Instant instant(){return time.get();}};
                var host=new Host(SCOPE,new StorageSetupScope(SCOPE.clusterId(),"installation-a","setup-a",1),AlertChannelPolicy.Deployment.ON_PREM,()->{}).withChannels(Set.of(EMAIL));
                work.accept(new Fixture(domain,db,settings,domain.alerts(host,BuiltinAlarmTypes.catalog(),clock),host,access,time,clock));
            }finally{db.drop();}
        }
    }
}
