package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import io.tapstate.app.AlertEvaluationHostIT.Fixture;
import static io.tapstate.app.AlertEvaluationHostIT.withFixture;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** Metadata CAS and evaluator authority remain distinct across real CONFIG transactions and reopening. */
@RequiresDocker
class AlertRuleBindingIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void metadataKeepsOriginalPlanningAndFrozenPayloadWhileOldProducersAreFenced(){
        withFixture(f->{route(f,Duration.ZERO);var planner=planner(f,"semantic-planner");f.evaluator().withPlanner(planner);
            var initial=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));bad(f);planner.tick();
            var frozen=intents(f).stream().map(row->row.value().frozen()).toList();assertThat(frozen).isNotEmpty();
            var original=f.store().state(initial.key(),f.reads()).orElseThrow();var rule=saved(f);
            f.now().set(T.plusSeconds(1));var rename=edit(rule,"Renamed",rule.parameters(),rule.severityOverride(),true,f.clock().instant());
            f.store().saveDefinition(rule,rename,access());var renamed=saved(f);assertThat(renamed.evaluationRevision()).isEqualTo(rule.evaluationRevision());
            assertThat(f.evaluator().tick(binding(f,renamed))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
            assertThat(f.store().state(initial.key(),f.reads()).orElseThrow()).isEqualTo(original);
            f.now().set(T.plusSeconds(2));var severity=edit(renamed,renamed.name(),renamed.parameters(),AlarmSeverity.CRITICAL,true,f.clock().instant());
            f.store().saveDefinition(renamed,severity,access());var current=saved(f);f.evaluator().tick(binding(f,current));planner.tick();
            var after=f.store().state(initial.key(),f.reads()).orElseThrow();
            assertThat(after.value().snapshot().rule()).isEqualTo(original.value().snapshot().rule());
            assertThat(after.value().snapshot().lastSequence()).isEqualTo(original.value().snapshot().lastSequence());
            assertThat(after.value().snapshot().lastObservedAt()).isEqualTo(original.value().snapshot().lastObservedAt());
            assertThat(after.value().episode().value().severity()).isEqualTo(AlarmSeverity.CRITICAL);
            assertThat(intents(f).stream().map(row->row.value().frozen()).toList()).containsExactlyInAnyOrderElementsOf(frozen);
            assertThat(f.store().originalRule(after.value().snapshot().rule(),f.reads()).orElseThrow().header()).isEqualTo(current.header());
            f.now().set(T.plusSeconds(3));
            assertThatThrownBy(()->f.evaluator().accept(initial,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isInstanceOf(TapstateException.class);
            var legacy=new AlertStore.StateChange(after.revision(),after.value(),after.value(),List.of(),null);
            assertThatThrownBy(()->f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.STATE_AND_PLAN,legacy,List.of(),List.of()))).isInstanceOf(TapstateException.class);
            assertThat(f.store().state(initial.key(),f.reads()).orElseThrow()).isEqualTo(after);
            try(var reopened=MongoConfigurationDomain.open(f.settings(),f.storeHost().scope().clusterId())){
                var store=reopened.alerts(f.storeHost(),BuiltinAlarmTypes.catalog(),f.clock());
                assertThat(store.state(initial.key(),f.reads()).orElseThrow()).isEqualTo(after);
                assertThat(((AlertConfiguration.Rule)store.definition(AlertConfiguration.Kind.RULE,"rule",access()).orElseThrow()).bindingRevision()).isEqualTo(1);
            }
        });
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(ints={-1,5})
    void rollbackPublishesClockInvalidWithoutAcceptingFutureEvidenceOrRewritingTheEpisode(int rollback){
        withFixture(f->{bad(f);var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            f.now().set(T.plusSeconds(10));f.evaluator().accept(binding,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null));
            var original=f.store().state(binding.key(),f.reads()).orElseThrow();var rule=saved(f);
            f.now().set(T.plusSeconds(rollback<0?11:rollback));f.store().saveDefinition(rule,edit(rule,rule.name(),rule.parameters(),AlarmSeverity.CRITICAL,true,f.clock().instant()),access());
            var current=saved(f);f.now().set(T.plusSeconds(rollback));
            assertThat(f.evaluator().tick(binding(f,current))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            var invalid=f.store().state(binding.key(),f.reads()).orElseThrow();var snapshot=invalid.value().snapshot();
            assertThat(snapshot.quality()).isEqualTo(AlertSnapshot.Quality.CLOCK_INVALID);assertThat(snapshot.lastObservedAt()).isEqualTo(T.plusSeconds(10));assertThat(snapshot.lastSequence()).isEqualTo(2);
            assertThat(invalid.value().episode()).isEqualTo(original.value().episode());
            var advanced=new AlertSnapshot(snapshot.rule(),snapshot.enabled(),snapshot.status(),snapshot.quality(),3,T.plusSeconds(11),snapshot.pendingSince(),snapshot.recoverySince(),snapshot.episodeSequence(),snapshot.episode());
            var old=invalid.value();var forged=new AlertStateRecord.Head(advanced,old.deployment(),old.owner(),old.retiredAt(),old.summariesHandedOff(),old.episode(),old.binding(),old.operationalEvidence());
            var change=new AlertStore.StateChange(invalid.revision(),old,forged,List.of(),null,new AlertStore.EvaluationProof(f.intent(),ACTIVE),current.header().revision());
            assertThatThrownBy(()->f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.STATE_AND_PLAN,change,List.of(),List.of()))).isInstanceOf(IllegalArgumentException.class);
            assertThat(f.store().state(binding.key(),f.reads()).orElseThrow()).isEqualTo(invalid);
            f.now().set(T.plusSeconds(12));f.evaluator().tick(binding(f,current));var later=f.store().state(binding.key(),f.reads()).orElseThrow().value();
            assertThat(later.snapshot().quality()).isEqualTo(AlertSnapshot.Quality.CLOCK_INVALID);assertThat(later.episode().value().severity()).isEqualTo(AlarmSeverity.CRITICAL);
            assertThat(later.snapshot().lastObservedAt()).isEqualTo(T.plusSeconds(10));
            f.now().set(T.plusSeconds(13));f.evaluator().accept(binding(f,current),3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null));
            var recovered=f.store().state(binding.key(),f.reads()).orElseThrow().value();
            assertThat(recovered.snapshot().quality()).isEqualTo(AlertSnapshot.Quality.VALID);assertThat(recovered.snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
            assertThat(recovered.episode().value().number()).isOne();
        });
    }
    @Test void aPreparedOldEvaluationCannotCommitAfterTheMetadataCasWins() throws Exception {
        withFixture(f->{bad(f);var before=f.store().state(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)).key(),f.reads()).orElseThrow();
            var prepared=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
            var boundary=org.mockito.Mockito.mock(AlertStore.class,org.mockito.AdditionalAnswers.delegatesTo(f.store()));
            org.mockito.Mockito.doAnswer(call->{prepared.countDown();if(!release.await(10,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("evaluation barrier timed out");return f.store().commit(call.getArgument(0));}).when(boundary).commit(org.mockito.ArgumentMatchers.any());
            var old=new AlertEvaluationHost(boundary,BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"host-a",ACTIVE);
            var original=saved(f);var binding=binding(f,original);f.now().set(T.plusSeconds(1));
            try(var executor=java.util.concurrent.Executors.newSingleThreadExecutor()){
                var result=executor.submit(()->old.accept(binding,2,new AlertInput.Conditions(T.plusSeconds(1),AlertSignal.Condition.BAD,null)));
                try{
                    assertThat(prepared.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    f.store().saveDefinition(original,edit(original,original.name(),original.parameters(),AlarmSeverity.CRITICAL,true,f.clock().instant()),access());
                    release.countDown();assertThatThrownBy(()->result.get(10,java.util.concurrent.TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                    assertThat(f.store().state(binding.key(),f.reads()).orElseThrow()).isEqualTo(before);
                    f.evaluator().tick(binding(f,saved(f)));assertThat(f.store().state(binding.key(),f.reads()).orElseThrow().value().episode().value().severity()).isEqualTo(AlarmSeverity.CRITICAL);
                }catch(InterruptedException e){Thread.currentThread().interrupt();throw new AssertionError(e);}finally{release.countDown();}
            }
        });
    }
    @Test void numericEqualityKeepsBindingButCriteriaAbaAndDisableEnableAdvanceIt(){
        withFixture(f->{bad(f);var first=saved(f);var key=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)).key();
            var values=new HashMap<String,BigDecimal>();first.parameters().forEach((k,v)->values.put(k,v.setScale(2)));
            f.now().set(T.plusSeconds(1));var equal=edit(first,first.name(),values,first.severityOverride(),true,f.clock().instant());
            f.store().saveDefinition(first,equal,access());var current=saved(f);f.evaluator().tick(binding(f,current));
            assertThat(current.evaluationRevision()).isEqualTo(1);assertThat(f.store().state(key,f.reads()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
            values.put("trigger-for",BigDecimal.ONE);f.now().set(T.plusSeconds(2));f.store().saveDefinition(current,edit(current,current.name(),values,null,true,f.clock().instant()),access());
            current=saved(f);assertThat(current.evaluationRevision()).isEqualTo(2);f.evaluator().tick(binding(f,current));
            var ended=f.store().state(key,f.reads()).orElseThrow().value();assertThat(ended.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.SUPERSEDED);
            f.now().set(T.plusSeconds(3));f.store().saveDefinition(current,edit(current,current.name(),first.parameters(),null,true,f.clock().instant()),access());
            current=saved(f);assertThat(current.evaluationRevision()).isEqualTo(3);f.evaluator().tick(binding(f,current));
            f.now().set(T.plusSeconds(4));f.store().saveDefinition(current,edit(current,current.name(),current.parameters(),null,false,f.clock().instant()),access());
            current=saved(f);assertThat(current.evaluationRevision()).isEqualTo(4);f.evaluator().tick(binding(f,current));
            f.now().set(T.plusSeconds(5));f.store().saveDefinition(current,edit(current,current.name(),current.parameters(),null,true,f.clock().instant()),access());
            current=saved(f);assertThat(current.evaluationRevision()).isEqualTo(5);f.evaluator().tick(binding(f,current));
            assertThat(f.store().state(key,f.reads()).orElseThrow().value().episode().value()).isEqualTo(ended.episode().value());
        });
    }
    @Test void retiredVersionTwoRuleIsCollectedWhileAFutureRuleIsProtected(){
        withFixture(f->{var original=saved(f);f.now().set(T.plusSeconds(1));
            var retired=new AlertConfiguration.Rule(original.header().retire(f.clock().instant(),"operator"),original.name(),original.typeId(),1,original.resources(),original.parameters(),null,true,null);
            f.store().saveDefinition(original,retired,access());
            try(var client=MongoClients.create(f.settings().uri())){
                var rows=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("alert_rules");
                var known=rows.find().first();assertThat(known.getInteger("formatVersion")).isEqualTo(2);
                var future=Document.parse(known.toJson());future.get("_id",Document.class).put("id","future-rule");future.put("formatVersion",3);rows.insertOne(future);
                f.now().set(T.plus(Duration.ofDays(8)));
                f.store().maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(5,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
                assertThat(rows.find(new Document("_id",known.get("_id"))).first()).isNull();
                assertThat(rows.find(new Document("_id",future.get("_id"))).first()).isEqualTo(future);
            }
        });
    }
    @Test void legacyRuleEnrollsOnlyAtExplicitCasAndMissingVersionTwoBindingCannotBeReconstructed(){
        withFixture(f->{try(var client=MongoClients.create(f.settings().uri())){
            var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());var rules=db.getCollection("alert_rules");
            var original=rules.find().first();var legacy=new Document(original);legacy.put("formatVersion",1);legacy.put("recordRevision",4L);
            legacy.get("body",Document.class).remove("bindingRevision");rules.replaceOne(new Document("_id",legacy.get("_id")),legacy);
            var old=saved(f);assertThat(old.bindingRevision()).isZero();assertThat(old.evaluationRevision()).isEqualTo(4);
            assertThat(f.store().definition(AlertConfiguration.Kind.RULE,"rule",access())).isPresent();assertThat(rules.find().first()).isEqualTo(legacy);
            f.evaluator().accept(binding(f,old),1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));
            f.now().set(T.plusSeconds(1));var rename=edit(old,"Metadata",old.parameters(),null,true,f.clock().instant());
            assertThat(f.store().saveDefinition(old,rename,access())).isEqualTo(AlertStore.Outcome.STAGED);
            assertThat(f.store().saveDefinition(old,rename,access())).isEqualTo(AlertStore.Outcome.UNCHANGED);
            var enrolled=saved(f);assertThat(enrolled.header().revision()).isEqualTo(5);assertThat(enrolled.bindingRevision()).isEqualTo(4);
            f.evaluator().tick(binding(f,enrolled));assertThat(f.store().active(null,10,f.reads()).total()).isOne();
            var row=rules.find().first();assertThat(row.getInteger("formatVersion")).isEqualTo(2);
            rules.updateOne(new Document("_id",row.get("_id")),new Document("$unset",new Document("body.bindingRevision","")));
            assertThatThrownBy(()->saved(f)).isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(rules.find().first().get("body",Document.class)).doesNotContainKey("bindingRevision");
        }});
    }
    static AlertConfiguration.Rule saved(Fixture f){return (AlertConfiguration.Rule)f.store().definition(AlertConfiguration.Kind.RULE,"rule",access()).orElseThrow();}
    static AlertConfiguration.Rule edit(AlertConfiguration.Rule r,String name,Map<String,BigDecimal> parameters,AlarmSeverity severity,boolean enabled,Instant now){
        return new AlertConfiguration.Rule(r.header().next(now,"operator"),name,r.typeId(),r.typeVersion(),r.resources(),parameters,severity,enabled,r.metric());
    }
    static AlertEvaluationHost.Binding binding(Fixture f,AlertConfiguration.Rule r){var initial=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
        return new AlertEvaluationHost.Binding(r,initial.resource(),initial.producer(),initial.sourceBindingRevision(),ACTIVE,initial.proof());}
}
