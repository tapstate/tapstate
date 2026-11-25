package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** Original receipts and traces remain bound to their episode while visible history and physical references expire. */
@RequiresDocker
class AlertOldEpisodeReplayIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void lateOriginalEvidenceAfterVisibleHistoryExpiryCannotChangeTheRecurrenceOrRecreateDeletedHistory(){
        AlertEvaluationHostIT.withFixture(f->{var route=route(f,Duration.ZERO);var receiver=(AlertReceiver)f.store().definition(AlertConfiguration.Kind.RECEIVER,"receiver",access()).orElseThrow();
            var transport=(AlertReceiver.Email)receiver.transport();var single=new AlertReceiver.Email(transport.host(),transport.port(),transport.encryption(),transport.authentication(),transport.username(),transport.from(),List.of("a@example.test"),transport.trust());
            f.store().saveReceiver(receiver,receiver.edit(receiver.name(),true,single,null,T,"operator"),null,false,access());
            var values=new EnumMap<AlertRetentionPolicy.Setting,Duration>(AlertRetentionPolicy.Setting.class);values.putAll(AlertRetentionPolicy.defaults(f.storeHost().deployment()).values());
            values.put(AlertRetentionPolicy.Setting.ENDED_EPISODE,Duration.ofDays(1));var policy=AlertRetentionPolicy.configure(f.storeHost().deployment(),values);
            var evaluator=new AlertEvaluationHost(f.store(),BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"cross-episode",ACTIVE,policy);
            var planner=planner(f,"cross-episode-planner");evaluator.withPlanner(planner);var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,new AlarmDiagnostics.SafeTrace("ORIGINAL_EPISODE_TRACE",1,false,false)));planner.tick();
            var original=intents(f).getFirst();var identity=original.value().frozen().identity();var pending=grant(f,original);var permit=pending.state().attempts().getFirst().permit();
            f.now().set(T.plusSeconds(1));evaluator.accept(binding,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null));planner.tick();
            f.now().set(permit.deadline().plusSeconds(1));clean(f.store());planner.tick();
            f.now().set(permit.deadline().plusSeconds(2));evaluator.accept(binding,3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,new AlarmDiagnostics.SafeTrace("CURRENT_EPISODE_TRACE",1,false,false)));planner.tick();
            assertThat(f.store().state(binding.key(),f.reads()).orElseThrow().value().snapshot().episode().number()).isEqualTo(2);
            f.now().set(T.plus(Duration.ofDays(4)));f.store().saveDefinition(route,route.edit(route.name(),route.match(),false,route.channels(),route.groupBy(),route.grouping(),route.inhibition(),f.clock().instant(),"operator"),access());planner.tick();clean(f.store());
            f.now().set(T.plus(Duration.ofDays(8)));clean(f.store());
            assertThat(f.store().history(binding.key(),null,10,f.reads()).rows()).noneMatch(episode->episode.value().number()==1);
            var before=f.store().state(binding.key(),f.reads()).orElseThrow();var old=f.store().intent(identity,null).orElseThrow();assertThat(old.value().state().payload()).isNull();
            var evidence=AlertDeliveryProgress.result(old.value(),permit,DeliveryEvidence.Outcome.ACCEPTED,
                    new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.STOP,250,"smtp",null),new AlertIntent.Receipt("channel-protocol","late-original-acceptance",null),f.clock().instant(),0);
            assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(old,evidence)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
            assertThat(evidence.state().stopReason()).isEqualTo(old.value().state().stopReason());assertThat(evidence.state().payload()).isNull();
            var accepted=f.store().intent(identity,null).orElseThrow();var laterFailure=AlertDeliveryProgress.result(accepted.value(),permit,DeliveryEvidence.Outcome.NOT_ACCEPTED,
                    new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.STOP,550,"smtp",null),null,f.clock().instant(),0);
            f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(accepted,laterFailure)),List.of()));
            assertThat(f.store().intent(identity,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            planner.tick();assertThat(f.store().state(binding.key(),f.reads()).orElseThrow()).isEqualTo(before);
            assertThat(f.store().history(binding.key(),null,10,f.reads()).rows()).noneMatch(episode->episode.value().number()==1);
            f.now().set(T.plus(Duration.ofDays(20)));planner.tick();for(int pass=0;pass<3;pass++)clean(f.store());
            try(var mongo=MongoClients.create(f.settings().uri())){
                var database=mongo.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                assertThat(database.getCollection("alert_episodes").find(new Document("_id.number",1L)).first()).isNull();
            }
            assertThat(f.store().intent(identity,null)).isEmpty();
            var current=f.store().state(binding.key(),f.reads()).orElseThrow().value();
            assertThat(evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,new AlarmDiagnostics.SafeTrace("LATE_OLD_TRACE",1,false,false)))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
            var after=f.store().state(binding.key(),f.reads()).orElseThrow().value();assertThat(after.snapshot()).isEqualTo(current.snapshot());
            assertThat(after.episode().diagnostics()).isEqualTo(current.episode().diagnostics());assertThat(after.snapshot().episode().number()).isEqualTo(2);assertThat(after.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
            assertThat(f.store().commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(accepted,accepted.value())),List.of()))).isEqualTo(AlertStore.Outcome.CONFLICT);
            assertThat(f.store().intent(identity,null)).isEmpty();
        });
    }
    private static void clean(AlertStore store){for(int collection=0;collection<12;collection++)store.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(collection,null,null),200,4L*1024*1024,Duration.ofSeconds(5));}
}
