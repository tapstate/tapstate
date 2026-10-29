package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class AlertNotificationPlannerIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static final Instant T=AlertEvaluationHostIT.T;
    static final Runnable ACTIVE=()->{};
    static AlertStore.Access access(){var token=new TokenRecord("operator","ADMIN","isolated-test-hash",false,T);
        return new AlertStore.Access(new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a","operator","ADMIN",token.authenticationBinding(),null),ACTIVE,ACTIVE,()->"");}
    static AlertNotificationPlanner planner(AlertEvaluationHostIT.Fixture f,String worker){return new AlertNotificationPlanner(f.store(),f.clock(),worker,null,ACTIVE);}
    static AlertConfiguration.Subscription route(AlertEvaluationHostIT.Fixture f,Duration wait){
        var scope=f.storeHost().scope();var dep=AlertChannelPolicy.Deployment.ON_PREM;
        var receiver=new AlertReceiver(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RECEIVER,scope,"receiver"),dep,T,"operator"),"Email",true,1,
                new AlertReceiver.Email("smtp.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test","b@example.test"),AlertReceiver.Trust.SYSTEM),null);
        assertThat(f.store().saveReceiver(null,receiver,null,false,access())).isEqualTo(AlertStore.Outcome.STAGED);
        var route=new AlertConfiguration.Subscription(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,scope,"route"),dep,T,"operator"),"Route",
                new AlertConfiguration.Match(new AlertConfiguration.Resources(Set.of("pipeline"),true,Set.of()),Set.of(),Set.of()),true,
                List.of(new AlertConfiguration.RouteChannel("email",List.of(receiver.header().identity()),MessageSettings.empty())),Set.of(AlertConfiguration.GroupBy.TYPE),
                new AlertGroupingClock.Policy(wait,Duration.ofSeconds(1),Duration.ofHours(1)),List.of());
        assertThat(f.store().saveDefinition(null,route,access())).isEqualTo(AlertStore.Outcome.STAGED);return route;
    }
    static void bad(AlertEvaluationHostIT.Fixture f){assertThat(f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);}
    static List<AlertStore.Stored<AlertIntent.Record>> intents(AlertEvaluationHostIT.Fixture f){return f.store().dueIntents(T.plus(Duration.ofDays(60)),null,100).rows();}
    @Test void stateAndPendingMembershipCommitTogetherAndRestartCreatesOneIntentPerTarget(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);
            assertThat(f.store().planning().schedules()).hasSize(2);assertThat(intents(f)).isEmpty();
            p.tick();assertThat(intents(f)).hasSize(2);var ids=intents(f).stream().map(v->v.value().frozen().identity().deliveryId()).toList();
            planner(f,"planner-b").tick();p.tick();assertThat(intents(f).stream().map(v->v.value().frozen().identity().deliveryId())).containsExactlyInAnyOrderElementsOf(ids);
            assertThat(f.store().planning().schedules().stream().filter(s->s.value().batch()==null)).allMatch(s->s.value().clock().round()==1);
        });
    }
    @Test void groupWaitPersistsAcrossReopenAndOldPlanningVersionCannotCommit(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ofSeconds(10));var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);var old=f.store().planning();
            p.tick();assertThat(intents(f)).isEmpty();
            var root=old.schedules().getFirst();assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(),List.of(new AlertStore.ScheduleChange(root,root.value())),old.version()))).isEqualTo(AlertStore.Outcome.CONFLICT);
            f.now().set(T.plusSeconds(10));p.tick();assertThat(intents(f)).hasSize(2);
        });
    }
    @Test void recoveryBeforeAnyGrantCancelsFaultWithoutSendingOrInventingRecovery(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var originals=intents(f);
            f.now().set(T.plusSeconds(1));assertThat(f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            p.tick();var retained=originals.stream().map(v->f.store().intent(v.value().frozen().identity(),access()).orElseThrow()).toList();assertThat(retained).hasSize(2).allMatch(v->v.value().state().permitsStopped());
            assertThat(intents(f)).noneMatch(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY);
        });
    }
    @Test void twoNodesRaceWithoutDuplicateRoundsAndExpiredOwnerIsTakenOver(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var a=planner(f,"planner-a");var b=planner(f,"planner-b");f.evaluator().withPlanner(a);bad(f);
            try(var workers=Executors.newFixedThreadPool(2)){var barrier=new CyclicBarrier(2);var tasks=List.of(a,b).stream().map(p->workers.submit(()->{try{barrier.await();p.tick();}catch(io.tapstate.core.common.TapstateException contention){assertThat(contention.code()).isEqualTo(AlertError.COMMAND_CONFLICT);}catch(Exception failure){throw new AssertionError(failure);}})).toList();for(var task:tasks)try{task.get(10,TimeUnit.SECONDS);}catch(Exception failure){throw new AssertionError(failure);}}
            assertThat(intents(f)).hasSize(2);f.now().set(T.plusSeconds(31));b.tick();
            assertThat(f.store().planning().schedules().stream().filter(s->s.value().batch()==null)).allMatch(s->s.value().owner().ownerId().equals("planner-b"));
            assertThat(intents(f)).hasSize(2);
        });
    }
    @Test void clearEvaluationWithoutAnEpisodeDoesNotBlockAnotherRulesNotification(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"mixed-heads");f.evaluator().withPlanner(p);
            var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"clear-rule"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");
            var clear=new AlertConfiguration.Rule(h,"No confirmed fault","pipeline.failed",1,f.rule().resources(),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
            assertThat(f.store().saveDefinition(null,clear,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var binding=new AlertEvaluationHost.Binding(clear,new AlarmKey.Resource("pipeline","orders","1"),"clear-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            assertThat(f.evaluator().accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.UNKNOWN,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            assertThat(f.store().state(binding.key(),f.reads()).orElseThrow().value().episode()).isNull();
            bad(f);p.tick();assertThat(intents(f)).hasSize(2);
        });
    }
    static AlertStore sendingStore(AlertEvaluationHostIT.Fixture f){return f.configuration().alerts(f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL),new AlertChannelPolicy.Capability("webhook",1,AlertChannelPolicy.Family.OP_EXTERNAL))),BuiltinAlarmTypes.catalog(),f.clock());}
    static AlertIntent.Record grant(AlertEvaluationHostIT.Fixture f,AlertStore.Stored<AlertIntent.Record> before){
        var record=before.value();var original=record.state();var now=f.clock().instant();
        var attempt=DeliveryAttempt.grant(record.frozen().identity().scope(),record.frozen().identity().deliveryId(),"attempt-1",1,1,"sender",record.frozen().retry(),record.frozen().createdAt(),record.frozen().deadline(),now).orElseThrow();
        var pending=new AlertIntent.Record(record.frozen(),new AlertIntent.State(new AlertOwnership(1,"sender",now.plusSeconds(120)),0,false,null,null,1,List.of(AlertIntent.Attempt.pending(attempt)),DeliveryEvidence.Outcome.UNKNOWN,
                original.retained(),original.payload(),null,null,null,null));
        assertThat(sendingStore(f).commit(new AlertStore.Commit(AlertStore.CommitKind.PERMIT,null,List.of(new AlertStore.IntentChange(before,pending)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);return pending;
    }
    static void accepted(AlertEvaluationHostIT.Fixture f,AlertIntent.Identity identity){
        var before=f.store().intent(identity,access()).orElseThrow();var old=before.value();var state=old.state();var attempt=state.attempts().getFirst();var now=f.clock().instant();
        var receipt=attempt.value().receipt(attempt.permit(),DeliveryEvidence.Outcome.ACCEPTED,now).attempt();
        var result=new AlertIntent.Attempt(attempt.permit(),receipt.outcome(),receipt.endedAt(),now,new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.STOP,identity.channelType().equals("webhook")?200:250,identity.channelType().equals("webhook")?"webhook":"smtp",null),new AlertIntent.Receipt("channel-protocol","provider-accepted",null));
        var retention=state.retained().value(old.frozen()).accept(now,attempt.permit().deadline(),now).orElseThrow();
        Instant stopped=state.permitsStopped()?state.permitsStoppedAt():now;Instant fixed=stopped.isAfter(receipt.endedAt())?stopped:receipt.endedAt();
        if(retention.stoppedAt()==null)retention=retention.stop(fixed,attempt.permit().deadline());
        var after=new AlertIntent.Record(old.frozen(),new AlertIntent.State(state.owner(),state.manualCycles(),true,state.permitsStopped()?state.stopReason():AlertIntent.StopReason.ACCEPTED,null,state.attemptsConsumed(),List.of(result),DeliveryEvidence.Outcome.ACCEPTED,
                AlertIntent.Retained.capture(retention),state.payload(),state.candidate(),null,null,null,stopped,receipt.endedAt(),state.cycleStarts(),new AlertIntent.Receipt("channel-protocol","provider-accepted",null)));
        assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(before,after)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
    }
    static void recover(AlertEvaluationHostIT.Fixture f){assertThat(f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);}
    @Test void acceptedTargetGetsOneRecoveryWhileNeverGrantedTargetOnlyCancels(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var original=intents(f);var identity=original.getFirst().value().frozen().identity();grant(f,original.getFirst());
            f.now().set(T.plusSeconds(1));accepted(f,identity);recover(f);p.tick();
            assertThat(intents(f)).hasSize(1).allMatch(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY);
            var recovered=intents(f).getFirst().value();assertThat(recovered.frozen().identity().targetId()).isEqualTo(identity.targetId());
            var id=recovered.frozen().identity().deliveryId();p.tick();assertThat(intents(f)).hasSize(1);assertThat(intents(f).getFirst().value().frozen().identity().deliveryId()).isEqualTo(id);
        });
    }
    @Test void unknownOriginalWaitsOnlyToItsFixedAttemptDeadlineAndLateAcceptedDoesNotReplanRecovery(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var original=intents(f).getFirst();var pending=grant(f,original);var identity=pending.frozen().identity();
            f.now().set(T.plusSeconds(1));recover(f);p.tick();assertThat(intents(f)).noneMatch(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY);
            Instant deadline=pending.state().attempts().getFirst().permit().deadline();f.now().set(deadline);p.tick();var recovery=intents(f).stream().filter(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY).toList();assertThat(recovery).hasSize(1);
            assertThat(new String(recovery.getFirst().value().state().payload().bytes(),java.nio.charset.StandardCharsets.US_ASCII)).contains("MIME-Version");
            f.now().set(deadline.plusSeconds(1));accepted(f,identity);p.tick();assertThat(intents(f).stream().filter(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY)).hasSize(1);
        });
    }
    @Test void silenceReleaseUsesDurableMembershipWithoutClearingTheAlarm(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);
            var silence=new AlertConfiguration.Silence(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SILENCE,f.storeHost().scope(),"silence"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator"),"Maintenance",
                    new AlertSilenceWindow(f.storeHost().scope(),true,Set.of(),Set.of(),T,Duration.ofSeconds(5),null,null));
            assertThat(f.store().saveDefinition(null,silence,access())).isEqualTo(AlertStore.Outcome.STAGED);bad(f);p.tick();assertThat(intents(f)).isEmpty();assertThat(f.store().active(null,5,f.reads()).total()).isOne();
            f.now().set(T.plusSeconds(5));p.tick();assertThat(intents(f)).hasSize(2);planner(f,"planner-b").tick();assertThat(intents(f)).hasSize(2);
        });
    }
    @Test void partialFrozenBatchKeepsOriginalContentAndHasOneBoundedSuccessorPerTarget(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);
            var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"rule-b"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");
            var rule=new AlertConfiguration.Rule(h,"Second failure","pipeline.failed",1,f.rule().resources(),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
            assertThat(f.store().saveDefinition(null,rule,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var binding=new AlertEvaluationHost.Binding(rule,f.binding(()->null).resource(),"original-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            assertThat(f.evaluator().accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);p.tick();var originals=intents(f);assertThat(originals).hasSize(2).allMatch(v->v.value().frozen().members().size()==2);
            f.now().set(T.plusSeconds(1));recover(f);p.tick();var successors=intents(f);assertThat(successors).hasSize(2).allMatch(v->v.value().frozen().members().size()==1&&v.value().frozen().members().getFirst().ruleIdentity().equals(h.identity()));
            for(var original:originals){var retained=f.store().intent(original.value().frozen().identity(),access()).orElseThrow().value();assertThat(retained.frozen()).isEqualTo(original.value().frozen());assertThat(retained.state().permitsStopped()).isTrue();assertThat(successors.stream().filter(v->v.value().frozen().identity().targetId().equals(retained.frozen().identity().targetId())).findFirst().orElseThrow().value().frozen().deadline()).isEqualTo(retained.frozen().deadline());}
            p.tick();assertThat(intents(f).stream().map(v->v.value().frozen().identity().deliveryId())).containsExactlyInAnyOrderElementsOf(successors.stream().map(v->v.value().frozen().identity().deliveryId()).toList());
        });
    }
    @Test void anUnfinishedPredecessorCoalescesMissedRepeatsAndResumesAfterOneInterval(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var originals=intents(f);
            f.now().set(T.plusSeconds(7200));p.tick();assertThat(intents(f)).hasSize(2);
            for(var original:originals)grant(f,original);f.now().set(T.plusSeconds(7201));for(var original:originals)accepted(f,original.value().frozen().identity());p.tick();assertThat(intents(f)).isEmpty();
            f.now().set(T.plusSeconds(10800));p.tick();assertThat(intents(f)).isEmpty();f.now().set(T.plusSeconds(10801));p.tick();assertThat(intents(f)).hasSize(2).allMatch(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.REMINDER);
        });
    }

    @Test void aNewEpisodeIsRegisteredBeforeItsPredecessorRecoveryCanFinish(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();grant(f,intents(f).getFirst());
            f.now().set(T.plusSeconds(1));recover(f);p.tick();
            f.now().set(T.plusSeconds(2));assertThat(f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            assertThat(f.store().planning().schedules().stream().filter(v->v.value().batch()==null).flatMap(v->v.value().members().stream()).filter(m->m.episode()==2).count()).isEqualTo(2);
        });
    }
    @Test void endingAllOriginalMembersAfterASuccessorWasFrozenDoesNotEraseItsPointer(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);
            var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"rule-b"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");
            var rule=new AlertConfiguration.Rule(h,"Second failure","pipeline.failed",1,f.rule().resources(),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());f.store().saveDefinition(null,rule,access());
            var binding=new AlertEvaluationHost.Binding(rule,f.binding(()->null).resource(),"original-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            f.evaluator().accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));p.tick();var pending=grant(f,intents(f).getFirst());
            f.now().set(T.plusSeconds(1));recover(f);p.tick();f.now().set(pending.state().attempts().getFirst().permit().deadline());p.tick();p.tick();
            f.now().set(f.clock().instant().plusSeconds(1));assertThat(f.evaluator().accept(binding,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            assertThatCode(p::tick).doesNotThrowAnyException();
        });
    }

    @Test void initialWaitStartsAtSilenceReleaseRatherThanAtFaultAcceptance(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ofSeconds(10));var p=planner(f,"planner-a");f.evaluator().withPlanner(p);
            var silence=new AlertConfiguration.Silence(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SILENCE,f.storeHost().scope(),"silence"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator"),"Maintenance",new AlertSilenceWindow(f.storeHost().scope(),true,Set.of(),Set.of(),T,Duration.ofSeconds(20),null,null));
            f.store().saveDefinition(null,silence,access());bad(f);p.tick();f.now().set(T.plusSeconds(20));p.tick();assertThat(intents(f)).isEmpty();f.now().set(T.plusSeconds(30));p.tick();assertThat(intents(f)).hasSize(2);
        });
    }
    @Test void revokingRuleAfterGenuineRecoveryTerminatesNotificationWorkWithoutStalling(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var first=intents(f).getFirst();grant(f,first);f.now().set(T.plusSeconds(1));accepted(f,first.value().frozen().identity());recover(f);
            var r=f.rule();var disabled=new AlertConfiguration.Rule(r.header().next(f.clock().instant(),"operator"),r.name(),r.typeId(),r.typeVersion(),r.resources(),r.parameters(),r.severityOverride(),false,r.metric());
            assertThat(f.store().saveDefinition(r,disabled,access())).isEqualTo(AlertStore.Outcome.STAGED);assertThatCode(p::tick).doesNotThrowAnyException();assertThat(intents(f)).isEmpty();
        });
    }
    @Test void terminalUnknownWaitsOneRepeatIntervalEvenWhenItsEvidenceRemainsProtected(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var original=intents(f).getFirst();grant(f,original);
            f.now().set(T.plus(Duration.ofDays(1)));p.tick();assertThat(intents(f)).noneMatch(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.REMINDER);
        });
    }

    static void webhookRoute(AlertEvaluationHostIT.Fixture f){
        var scope=f.storeHost().scope();var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RECEIVER,scope,"webhook"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");
        var endpoint=java.net.URI.create("https://hooks.example.test/alerts");var transport=AlertReceiver.Webhook.fromEndpoint(endpoint,AlertReceiver.Authentication.NONE,null,AlertReceiver.Trust.SYSTEM);
        var receiver=new AlertReceiver(h,"Webhook",true,1,transport,AlertReceiver.CredentialReference.create(h.identity(),AlertReceiver.authenticationIdentity(transport)));
        try(var material=new AlertStore.PrivateMaterial(null,null,endpoint)){assertThat(f.store().saveReceiver(null,receiver,material,false,access())).isEqualTo(AlertStore.Outcome.STAGED);}
        var route=new AlertConfiguration.Subscription(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,scope,"route"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator"),"Webhook route",new AlertConfiguration.Match(f.rule().resources(),Set.of(),Set.of()),true,
                List.of(new AlertConfiguration.RouteChannel("webhook",List.of(h.identity()),MessageSettings.empty())),Set.of(AlertConfiguration.GroupBy.TYPE),new AlertGroupingClock.Policy(Duration.ZERO,Duration.ofSeconds(1),Duration.ofHours(1)),List.of());
        f.store().saveDefinition(null,route,access());
    }
    @Test void longRetryAfterPreservesUnplannedRecoveryUntilTheRealTargetRestrictionEnds(){
        AlertEvaluationHostIT.withFixture(f->{webhookRoute(f);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var first=intents(f).getFirst();grant(f,first);f.now().set(T.plusSeconds(1));accepted(f,first.value().frozen().identity());p.tick();
            f.now().set(T.plusSeconds(3601));p.tick();var reminder=intents(f).getFirst();grant(f,reminder);f.now().set(T.plusSeconds(3602));
            var before=f.store().intent(reminder.value().frozen().identity(),access()).orElseThrow();var old=before.value();var state=old.state();var permit=state.attempts().getFirst().permit();var now=f.clock().instant();Instant restriction=T.plus(Duration.ofDays(2));
            var result=new AlertIntent.Attempt(permit,DeliveryEvidence.Outcome.NOT_ACCEPTED,now,null,new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.RETRY,429,"webhook",restriction),null);
            var retained=state.retained().value(old.frozen()).stop(now,permit.deadline());
            var stopped=new AlertIntent.Record(old.frozen(),new AlertIntent.State(state.owner(),0,true,AlertIntent.StopReason.AGE_LIMIT,null,1,List.of(result),DeliveryEvidence.Outcome.NOT_ACCEPTED,AlertIntent.Retained.capture(retained),state.payload(),null,null,null,null,now,now));
            assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(before,stopped)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
            f.now().set(T.plusSeconds(3603));recover(f);p.tick();f.now().set(T.plus(Duration.ofDays(1)).plusSeconds(3604));p.tick();assertThat(f.store().planning().schedules().stream().filter(v->v.value().batch()==null).flatMap(v->v.value().members().stream()).count()).isOne();
            f.now().set(restriction);p.tick();assertThat(intents(f)).hasSize(1).allMatch(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY);
        });
    }
    @Test void aRealReopenedConfigurationResumesOriginalGroupWithoutRecreatingTheRound(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ofSeconds(10));var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();f.now().set(T.plusSeconds(31));
            try(var reopened=io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(f.settings(),f.storeHost().scope().clusterId())){
                var view=reopened.alerts(f.storeHost(),BuiltinAlarmTypes.catalog(),f.clock());var recovered=new AlertNotificationPlanner(view,f.clock(),"planner-b",null,ACTIVE);recovered.tick();var ids=intents(f).stream().map(v->v.value().frozen().identity().deliveryId()).toList();assertThat(ids).hasSize(2);recovered.tick();assertThat(intents(f).stream().map(v->v.value().frozen().identity().deliveryId())).containsExactlyInAnyOrderElementsOf(ids);
            }
        });
    }
    @Test void deliveryClaimTakeoverPreservesOriginalAttemptsAndRejectsOldOwnerWrites(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var original=intents(f).getFirst();var identity=(AlertIntent.Business)original.value().frozen().identity();
            var a=new AlertDeliveryClaim(f.store(),"sender-a",f.clock(),ACTIVE);var b=new AlertDeliveryClaim(f.store(),"sender-b",f.clock(),ACTIVE);var first=a.acquire(identity).orElseThrow();assertThat(first.value().state().owner().generation()).isOne();assertThat(b.acquire(identity)).isEmpty();
            f.now().set(T.plusSeconds(31));var second=b.acquire(identity).orElseThrow();assertThat(second.value().state().owner().generation()).isEqualTo(2);assertThat(second.value().frozen()).isEqualTo(original.value().frozen());
            var stale=AlertIntentControl.claim(first.value(),"sender-a",T.plusSeconds(1),T.plusSeconds(60));
            assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(first,stale)),List.of()))).isEqualTo(AlertStore.Outcome.CONFLICT);
        });
    }

    static void resourceGrouping(AlertEvaluationHostIT.Fixture f,Duration change){var r=route(f,Duration.ZERO);var updated=r.edit(r.name(),r.match(),true,r.channels(),Set.of(AlertConfiguration.GroupBy.RESOURCE),new AlertGroupingClock.Policy(Duration.ZERO,change,Duration.ofHours(1)),r.inhibition(),T,"operator");f.store().saveDefinition(r,updated,access());}
    static void staleFault(AlertEvaluationHostIT.Fixture f){var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"stale-rule"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");var r=new AlertConfiguration.Rule(h,"Stale observation","pipeline.observation-stale",1,f.rule().resources(),Map.of("max-age",java.math.BigDecimal.valueOf(5),"startup-grace",java.math.BigDecimal.ZERO),null,true,null).validated(BuiltinAlarmTypes.catalog());f.store().saveDefinition(null,r,access());var binding=new AlertEvaluationHost.Binding(r,f.binding(()->null).resource(),"original-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));assertThat(f.evaluator().accept(binding,1,new AlertInput.Freshness(T,true,T.minusSeconds(120),T.minusSeconds(120),Duration.ZERO,Duration.ofSeconds(5)))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);}
    static AlertConfiguration.Silence silence(AlertEvaluationHostIT.Fixture f,String id,String type,Instant start,Duration duration){var value=new AlertConfiguration.Silence(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SILENCE,f.storeHost().scope(),id),AlertChannelPolicy.Deployment.ON_PREM,f.clock().instant(),"operator"),"Maintenance",new AlertSilenceWindow(f.storeHost().scope(),true,Set.of(),Set.of(type),start,duration,null,null));f.store().saveDefinition(null,value,access());return value;}
    @Test void alternatingSilencedSurvivorsUseNewGroupWorkWithoutExpandingAnOldDisposition(){
        AlertEvaluationHostIT.withFixture(f->{resourceGrouping(f,Duration.ofSeconds(30));var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);staleFault(f);p.tick();
            f.now().set(T.plusSeconds(1));silence(f,"failure-window","pipeline.failed",f.clock().instant(),Duration.ofSeconds(1));p.tick();
            f.now().set(T.plusSeconds(2));silence(f,"stale-window","pipeline.observation-stale",f.clock().instant(),Duration.ofSeconds(60));assertThatCode(p::tick).doesNotThrowAnyException();
        });
    }
    @Test void handedOffAcceptedSummaryDoesNotSendTheSameRemainingMemberAsANewFault(){
        AlertEvaluationHostIT.withFixture(f->{resourceGrouping(f,Duration.ofSeconds(1));var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);staleFault(f);p.tick();var originals=intents(f);for(var original:originals)grant(f,original);f.now().set(T.plusSeconds(1));for(var original:originals)accepted(f,original.value().frozen().identity());p.tick();
            f.now().set(T.plusSeconds(2));silence(f,"failure-window","pipeline.failed",f.clock().instant(),Duration.ofSeconds(60));p.tick();assertThat(intents(f)).isEmpty();
        });
    }
    @Test void fullRequalificationStillStopsAnUnsentSuccessorAfterLateOriginalAccepted(){
        AlertEvaluationHostIT.withFixture(f->{resourceGrouping(f,Duration.ofSeconds(1));var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);staleFault(f);p.tick();var original=intents(f).getFirst();var pending=grant(f,original);
            f.now().set(T.plusSeconds(1));silence(f,"failure-window","pipeline.failed",f.clock().instant(),Duration.ofSeconds(60));p.tick();
            f.now().set(pending.state().attempts().getFirst().permit().deadline());p.tick();var successor=intents(f).stream().filter(v->v.value().frozen().identity().targetId().equals(original.value().frozen().identity().targetId())).findFirst().orElseThrow();assertThat(successor.value().frozen().members()).hasSize(1);
            f.now().set(T.plusSeconds(62));accepted(f,original.value().frozen().identity());p.tick();assertThat(f.store().intent(successor.value().frozen().identity(),access()).orElseThrow().value().state().permitsStopped()).isTrue();
        });
    }

    @Test void laterDiscoveryDoesNotRegisterAnExistingEpisodeInAnOlderSurvivingRoot(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var oldRoots=new HashSet<AlertSchedule.Key>();
            var ordered=org.mockito.Mockito.mock(AlertStore.class,org.mockito.AdditionalAnswers.delegatesTo(f.store()));
            org.mockito.Mockito.doAnswer(call->{var view=f.store().planning();var rows=new ArrayList<>(view.schedules());rows.sort(Comparator.comparingInt(v->oldRoots.contains(v.value().key())?0:1));return new AlertStore.Planning(view.version(),view.host(),view.definitions(),view.states(),rows,view.intents(),view.episodes(),view.limits(),view.dependencies());}).when(ordered).planning();
            var p=new AlertNotificationPlanner(ordered,f.clock(),"planner-a",null,ACTIVE);f.evaluator().withPlanner(p);bad(f);
            var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"rule-b"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");var rule=new AlertConfiguration.Rule(h,"Second failure","pipeline.failed",1,f.rule().resources(),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());f.store().saveDefinition(null,rule,access());
            var binding=new AlertEvaluationHost.Binding(rule,f.binding(()->null).resource(),"original-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));f.evaluator().accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));
            f.store().planning().schedules().stream().filter(v->v.value().batch()==null).forEach(v->oldRoots.add(v.value().key()));p.tick();var pending=grant(f,intents(f).getFirst());
            f.now().set(T.plusSeconds(1));recover(f);p.tick();f.now().set(T.plusSeconds(2));f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null));p.tick();
            f.now().set(pending.state().attempts().getFirst().permit().deadline());p.tick();p.tick();p.tick();
            assertThat(f.store().planning().schedules().stream().filter(v->v.value().batch()==null).flatMap(v->v.value().members().stream()).filter(m->m.episode()==2&&m.ruleIdentity().equals(f.rule().header().identity())).count()).isEqualTo(2);
        });
    }

    @Test void anIsolatedScheduleRemainsVisibleWhileCompatibleExistingRootsContinue(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);
            try(var client=com.mongodb.client.MongoClients.create(f.settings().uri())){var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());var collection=db.getCollection("alert_notification_schedules");
                var original=collection.find().first();var isolated=org.bson.Document.parse(original.toJson());isolated.get("_id",org.bson.Document.class).put("subjectId","unknown-root");isolated.put("formatVersion",999);collection.insertOne(isolated);
                assertThatThrownBy(p::tick).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,e->assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(intents(f)).hasSize(2);assertThat(collection.find(new org.bson.Document("_id",isolated.get("_id"))).first()).isEqualTo(isolated);assertThat(f.store().planning().isolatedRecords()).isTrue();f.now().set(T.plusSeconds(1));recover(f);assertThat(f.store().active(null,5,f.reads()).total()).isZero();
            }
        });
    }
    @Test void boundedRootPassesRotateToTargetsBeyondTheFirstPass(){
        AlertEvaluationHostIT.withFixture(f->{var first=route(f,Duration.ZERO);for(int index=1;index<17;index++){var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,f.storeHost().scope(),"route-"+index),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");var route=new AlertConfiguration.Subscription(header,"Route "+index,first.match(),true,first.channels(),first.groupBy(),first.grouping(),List.of());assertThat(f.store().saveDefinition(null,route,access())).isEqualTo(AlertStore.Outcome.STAGED);}
            var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();assertThat(intents(f)).hasSize(32);p.tick();assertThat(intents(f)).hasSize(34);
        });
    }

    static void rejectionWithRestriction(AlertEvaluationHostIT.Fixture f,AlertIntent.Identity identity,Instant restriction){
        var before=f.store().intent(identity,access()).orElseThrow();var old=before.value();var state=old.state();var permit=state.attempts().getFirst().permit();var now=f.clock().instant();
        var result=new AlertIntent.Attempt(permit,DeliveryEvidence.Outcome.NOT_ACCEPTED,now,null,new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.RETRY,429,"webhook",restriction),null);
        var retained=state.retained().value(old.frozen()).stop(now,permit.deadline());var after=new AlertIntent.Record(old.frozen(),new AlertIntent.State(state.owner(),0,true,AlertIntent.StopReason.AGE_LIMIT,null,1,List.of(result),DeliveryEvidence.Outcome.NOT_ACCEPTED,AlertIntent.Retained.capture(retained),state.payload(),null,null,null,null,now,now));
        assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(before,after)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
    }
    @Test void exactOriginalDeadlineClosesUncreatedSuccessorWorkWithoutAHiddenRetiredObligation(){
        AlertEvaluationHostIT.withFixture(f->{webhookRoute(f);var route=(AlertConfiguration.Subscription)f.store().definition(AlertConfiguration.Kind.SUBSCRIPTION,"route",access()).orElseThrow();f.store().saveDefinition(route,route.edit(route.name(),route.match(),true,route.channels(),Set.of(AlertConfiguration.GroupBy.RESOURCE),route.grouping(),route.inhibition(),T,"operator"),access());
            var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);staleFault(f);p.tick();var first=intents(f).getFirst();grant(f,first);f.now().set(T.plusSeconds(1));rejectionWithRestriction(f,first.value().frozen().identity(),T.plus(Duration.ofDays(2)));recover(f);p.tick();
            f.now().set(first.value().frozen().deadline());p.tick();var key=((AlertIntent.Business)first.value().frozen().identity()).key();
            try(var client=com.mongodb.client.MongoClients.create(f.settings().uri())){var collection=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("alert_notification_schedules");var query=new org.bson.Document("_id.subjectId",key.notification().subjectId()).append("_id.subjectIncarnation",key.notification().subjectIncarnation()).append("_id.targetId",key.targetId());var batch=collection.find(query).first();
                assertThat(batch.get("retiredAt")).isNotNull();assertThat(batch.get("disposition",org.bson.Document.class).getString("action")).isEqualTo("STOP_NO_SUCCESSOR");assertThat(batch.getList("pending",org.bson.Document.class)).isEmpty();assertThat(batch.get("disposition",org.bson.Document.class).get("successor")).isNull();
                f.now().set(f.clock().instant().plusSeconds(1));p.tick();assertThat(collection.find(query).first()).isEqualTo(batch);
            }
        });
    }
    @Test void missingRequiredBatchIsIsolatedAndDoesNotStarveAHealthyLaterRoot(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);p.tick();var roots=f.store().planning().schedules().stream().filter(v->v.value().batch()==null).toList();var broken=roots.getFirst().value().protectedIntents().getFirst();var healthy=roots.getLast().value().protectedIntents().getFirst();grant(f,f.store().intent(new AlertIntent.Business(healthy),access()).orElseThrow());f.now().set(T.plusSeconds(1));accepted(f,new AlertIntent.Business(healthy));
            try(var client=com.mongodb.client.MongoClients.create(f.settings().uri())){var collection=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("alert_notification_schedules");var query=new org.bson.Document("_id.subjectId",broken.notification().subjectId()).append("_id.subjectIncarnation",broken.notification().subjectIncarnation()).append("_id.targetId",broken.targetId());assertThat(collection.deleteOne(query).getDeletedCount()).isOne();
                assertThat(f.store().planning().isolatedRecords()).isTrue();assertThatThrownBy(p::tick).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,e->assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                f.now().set(T.plusSeconds(3601));assertThatThrownBy(p::tick).isInstanceOf(io.tapstate.core.common.TapstateException.class);assertThat(intents(f).stream().filter(v->((AlertIntent.Business)v.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.REMINDER)).hasSize(1).allMatch(v->v.value().frozen().identity().targetId().equals(healthy.targetId()));assertThat(collection.countDocuments(query)).isZero();
            }
        });
    }

    @Test void sourceDependencyPlanningReadsDoNotWriteTheOriginalSourceFence(){
        AlertEvaluationHostIT.withFixture(true,f->{var route=route(f,Duration.ZERO);var edge=new AlertInhibitionPolicy.Edge("source.connection-failed","pipeline.failed");f.store().saveDefinition(route,route.edit(route.name(),route.match(),true,route.channels(),route.groupBy(),route.grouping(),List.of(edge),T,"operator"),access());var p=planner(f,"planner-a");f.evaluator().withPlanner(p);bad(f);
            var identity=f.configuration().artifacts().sourceBinding("views").orElseThrow().identity();var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"source-rule"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator");var definition=new AlertConfiguration.Rule(header,"Source failure","source.connection-failed",1,new AlertConfiguration.Resources(Set.of("source"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());f.store().saveDefinition(null,definition,access());
            var binding=new AlertEvaluationHost.Binding(definition,new AlarmKey.Resource("source","views",identity.incarnation()),"source-operation",identity.bindingRevision(),ACTIVE);assertThat(f.evaluator().accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            try(var client=com.mongodb.client.MongoClients.create(f.settings().uri())){var sources=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("sources");long before=sources.find(new org.bson.Document("_id","views")).first().get("sourceIdentity",org.bson.Document.class).getLong("writeFence");var snapshot=f.store().planning();long after=sources.find(new org.bson.Document("_id","views")).first().get("sourceIdentity",org.bson.Document.class).getLong("writeFence");assertThat(snapshot.dependencies()).isNotEmpty();assertThat(after).isEqualTo(before);p.tick();assertThat(intents(f)).isEmpty();assertThat(f.store().active(null,5,f.reads()).total()).isEqualTo(2);}
        });
    }

    @Test void managedPlanningUsesOriginalSystemBindingSingleAttemptAndBusinessAgeBudget(){
        AlertEvaluationHostIT.withFixture(false,true,f->{var p=new AlertNotificationPlanner(f.store(),f.clock(),"planner-a",java.net.URI.create("https://cloud.example.test"),ACTIVE);f.evaluator().withPlanner(p);bad(f);p.tick();var rows=intents(f);assertThat(rows).hasSize(1);var frozen=rows.getFirst().value().frozen();assertThat(frozen.route()).isEqualTo(f.storeHost().managedRoute());assertThat(frozen.target()).isEqualTo(f.storeHost().managedTarget());assertThat(frozen.retry().maximumTotalAttempts()).isOne();assertThat(frozen.deadline()).isEqualTo(T.plus(Duration.ofDays(1)));assertThat(frozen.retry().attemptDeadline(T.plusSeconds(600),frozen.deadline())).contains(T.plusSeconds(630));
            try{var payload=new com.fasterxml.jackson.databind.ObjectMapper().readTree(rows.getFirst().value().state().payload().bytes());assertThat(payload.path("payload").path("episode").asLong()).isOne();assertThat(payload.path("payload").path("resourceId").asText()).isEqualTo("orders");assertThat(payload.path("severity").asText()).isEqualTo("CRITICAL");}catch(java.io.IOException bad){throw new AssertionError(bad);}
            p.tick();assertThat(intents(f)).hasSize(1);
        });
    }

    @Test void fullCapacityDoesNotBlockARealGoodInputOrItsOriginalRecoveryPlan() {
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var planner=planner(f,"pressure-recovery");f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var original=intents(f).getFirst();grant(f,original);f.now().set(T.plusSeconds(1));accepted(f,original.value().frozen().identity());
            f.store().maintenance().orElseThrow().enableCapacityProtection();
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(f.store(),"observedCapacity",new AlertMaintenance.Inventory(f.clock().instant(),List.of(new AlertMaintenance.Usage("alert_instances",1,AlertMaintenancePolicy.CAPACITY_BYTES,0,0,null)),0,null,0,0));
            recover(f);planner.tick();assertThat(intents(f)).hasSize(1).allMatch(row->((AlertIntent.Business)row.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY);
            assertThat(f.store().state(f.store().evaluationStates(null,100).rows().getFirst().value().snapshot().rule().key(),f.reads()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
        });
    }

    @Test void businessFaultAndRecoveryInheritTheOriginallyAcceptedProfileAcrossHostChanges() {
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var properties=new AlertRetentionSettings();properties.setRetention(Map.of("accepted-payload","PT15M","accepted-result","PT6H"));
            var configured=properties.resolve(f.storeHost().deployment());var planner=planner(f,"profile-planner");
            var creator=new AlertEvaluationHost(f.store(),BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"profile-creator",ACTIVE,configured).withPlanner(planner);
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));assertThat(creator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);planner.tick();
            var originals=intents(f);assertThat(originals).hasSize(2).allMatch(row->row.value().frozen().retention().value().acceptedPayload().equals(Duration.ofMinutes(15)));
            var first=originals.getFirst();grant(f,first);f.now().set(T.plusSeconds(1));accepted(f,first.value().frozen().identity());
            var receipt=f.store().intent(first.value().frozen().identity(),access()).orElseThrow().value();assertThat(receipt.state().retained().payloadUntil()).isEqualTo(f.clock().instant().plus(Duration.ofMinutes(15)));
            assertThat(receipt.state().retained().resultUntil()).isEqualTo(f.clock().instant().plus(Duration.ofHours(6)));
            f.now().set(T.plus(Duration.ofHours(7)));f.store().maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
            assertThat(f.store().intent(first.value().frozen().identity(),access()).orElseThrow().value().state().detailsPrunedAt()).isNotNull();var newHost=new AlertEvaluationHost(f.store(),BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"profile-reopened",ACTIVE).withPlanner(planner);
            assertThat(newHost.accept(binding,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);planner.tick();
            assertThat(intents(f)).hasSize(1).allMatch(row->((AlertIntent.Business)row.value().frozen().identity()).key().notification().kind()==DeliveryKey.Kind.RECOVERY
                    && row.value().frozen().retention().value().acceptedPayload().equals(Duration.ofMinutes(15)));
        });
    }

}
