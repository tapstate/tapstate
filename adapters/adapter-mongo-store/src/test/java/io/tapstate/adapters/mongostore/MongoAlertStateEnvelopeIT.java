package io.tapstate.adapters.mongostore;

import com.mongodb.*;
import com.mongodb.client.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertStateRecord.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static io.tapstate.adapters.mongostore.AlertStateEnvelopeFixtures.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertStateEnvelopeIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final TransactionOptions TX = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    @Test void diagnosticStateAndTransitionSurviveReopenAndAbortTogether() {
        withStore(f -> {
            var head = fired(); var transition = new Transition(new AlertUpdate.Transition(AlertUpdate.Kind.FIRING, T, 1, AlarmSeverity.MAJOR), null,
                    DiagnosticStatus.CAPTURED, head.episode().diagnostics().latest(), POLICY);
            assertThatThrownBy(() -> tx(f, s -> { f.store.stageEnvelope(s, 0, null, head, List.of(transition), bad(1,T), clock(T)); throw new Abort(); })).isInstanceOf(Abort.class);
            assertThat(SystemCollections.ALERT_TRANSITIONS.on(f.db).countDocuments()).isZero();
            assertThat((Object)tx(f, s -> f.store.stageEnvelope(s, 0, null, head, List.of(transition), bad(1,T), clock(T)))).isEqualTo(MongoAlertStateStore.Stage.STAGED);
            try(var other = MongoClients.create(MONGO.getReplicaSetUrl()); var session = other.startSession()) {
                var reopened = new MongoAlertStateStore(other.getDatabase(f.db.getName()), KEY.scope(), () -> {});
                assertThat(session.withTransaction(() -> reopened.loadEnvelope(session, KEY).orElseThrow(), TX).value()).isEqualTo(head);
            }
            assertThat((Object)tx(f, s -> f.store.stageEnvelope(s, 0, null, head, List.of(transition), bad(1,T), clock(T.plusSeconds(3))))).isEqualTo(MongoAlertStateStore.Stage.UNCHANGED);
            assertThat(SystemCollections.ALERT_TRANSITIONS.on(f.db).countDocuments()).isOne();
        });
    }
    @Test void staleCaptureCannotRewriteFirstOrUseTheCurrentBindingForAnOldEpisode() {
        withStore(f -> {
            var old = fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var sig = bad(2,T.plusSeconds(1)); var next = AlertLifecycle.evaluate(old.snapshot(),sig,T.plusSeconds(1)).snapshot();
            var d = Diagnostics.captured(AlarmDiagnostics.attach(next,sig,new AlarmDiagnostics.SafeTrace("second",1,true,false),old.episode().diagnostics().history()).orElseThrow());
            var good = head(next,new Episode(RULE,next.episode(),POLICY,d,false),old.owner());
            assertThat((Object)tx(f,s -> f.store.stageEnvelope(s,1,old,good,List.of(),sig,clock(T.plusSeconds(1))))).isEqualTo(MongoAlertStateStore.Stage.STAGED);
            var fake = head(next,new Episode(RULE,next.episode(),POLICY,Diagnostics.captured(new AlarmDiagnostics.History(d.history().latest(),d.history().latest())),false),old.owner());
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,2,good,fake,List.of(),sig,clock(T.plusSeconds(1))))).isInstanceOf(IllegalArgumentException.class);
            var ended = AlertLifecycle.terminate(next,AlertSnapshot.EndReason.DISABLED,T.plusSeconds(2)).snapshot();
            var changed = head(ended,new Episode(RULE,ended.episode(),POLICY,d,false),old.owner());
            tx(f,s -> f.store.stageEnvelope(s,2,good,changed,List.of(),null,clock(T.plusSeconds(2))));
            assertThat((Object)tx(f,s -> f.store.episodeEnvelope(s,KEY,1).orElseThrow().diagnostics().first())).isEqualTo(old.episode().diagnostics().first());
        });
    }
    @Test void workerOwnershipDoesNotReplaceEvaluationGenerationAndCannotRenewAfterExpiry() {
        withStore(f -> {
            var old = fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var claim = head(old.snapshot(),old.episode(),new AlertOwnership(1,"worker-a",T.plusSeconds(5)));
            tx(f,s -> f.store.stageEnvelope(s,1,old,claim,List.of(),null,clock(T)));
            assertThat(claim.snapshot().rule().generation()).isEqualTo(2);
            var renew = head(old.snapshot(),old.episode(),new AlertOwnership(1,"worker-a",T.plusSeconds(15)));
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,2,claim,renew,List.of(),null,clock(T.plusSeconds(6))))).isInstanceOf(IllegalArgumentException.class);
            var takeover = head(old.snapshot(),old.episode(),new AlertOwnership(2,"worker-b",T.plusSeconds(15)));
            tx(f,s -> f.store.stageEnvelope(s,2,claim,takeover,List.of(),null,clock(T.plusSeconds(6))));
            assertThat((Object)tx(f,s -> f.store.stageEnvelope(s,2,claim,renew,List.of(),null,clock(T.plusSeconds(7))))).isEqualTo(MongoAlertStateStore.Stage.CONFLICT);
        });
    }
    @Test void frozenPolicyAndPrunedOccurrenceSurviveSettingsChangesAndNoCaptureResurrection() {
        withStore(f -> {
            var old = fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var ended = AlertLifecycle.terminate(old.snapshot(),AlertSnapshot.EndReason.DISABLED,T.plusSeconds(1)).snapshot();
            var e = new Episode(RULE,ended.episode(),POLICY,old.episode().diagnostics(),false); var h = head(ended,e,old.owner());
            tx(f,s -> f.store.stageEnvelope(s,1,old,h,List.of(),null,clock(T.plusSeconds(1))));
            var pruned = head(ended,new Episode(RULE,e.value(),POLICY,e.diagnostics().prune(e.diagnosticUntil()),false),old.owner());
            tx(f,s -> f.store.stageEnvelope(s,2,h,pruned,List.of(),null,clock(e.diagnosticUntil())));
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,3,pruned,h,List.of(),bad(1,T),clock(e.diagnosticUntil())))).isInstanceOf(IllegalArgumentException.class);
            assertThat((Object)tx(f,s -> f.store.loadEnvelope(s,KEY).orElseThrow().value())).isEqualTo(pruned);
        });
    }
    @Test void missingEpisodeAndLegacyHeadRefuseMutationRatherThanGuessingPolicy() {
        withStore(f -> {
            var old=fired();
            tx(f,s -> f.store.stage(s,0,AlertSnapshot.initial(RULE),new AlertUpdate(old.snapshot(),List.of(),false)));
            assertThatThrownBy(() -> tx(f,s -> f.store.loadEnvelope(s,KEY))).isInstanceOf(TapstateException.class);
            assertThat((Object)tx(f,s -> f.store.load(s,KEY).orElseThrow().snapshot())).isEqualTo(old.snapshot());
            SystemCollections.ALERT_EPISODES.on(f.db).deleteMany(new Document());
            assertThatThrownBy(() -> tx(f,s -> f.store.loadEnvelope(s,KEY))).isInstanceOf(TapstateException.class);
        });
    }
    @Test void unchangedExpiredOwnerCannotMutateTheSnapshotOrDiagnosticFrontier() {
        withStore(f -> {
            var old = fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var claim = head(old.snapshot(),old.episode(),new AlertOwnership(1,"worker-a",T.plusSeconds(5)));
            tx(f,s -> f.store.stageEnvelope(s,1,old,claim,List.of(),null,clock(T)));
            var next = AlertLifecycle.evaluate(claim.snapshot(),bad(2,T.plusSeconds(6)),T.plusSeconds(6)).snapshot();
            var illegal = head(next,new Episode(RULE,next.episode(),POLICY,old.episode().diagnostics(),false),claim.owner());
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,2,claim,illegal,List.of(),null,clock(T.plusSeconds(6))))).isInstanceOf(IllegalArgumentException.class);
        });
    }
    @Test void increasingSequenceCannotRewindTimeWithinTheSameEvaluationBinding() {
        withStore(f -> {
            var old=fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var next=AlertLifecycle.evaluate(old.snapshot(),bad(2,T.plusSeconds(10)),T.plusSeconds(10)).snapshot();
            var h=head(next,new Episode(RULE,next.episode(),POLICY,old.episode().diagnostics(),false),old.owner());
            tx(f,s -> f.store.stageEnvelope(s,1,old,h,List.of(),null,clock(T.plusSeconds(10))));
            var rewind=new AlertSnapshot(RULE,true,next.status(),next.quality(),3,T.plusSeconds(1),null,null,1,next.episode());
            var illegal=head(rewind,h.episode(),h.owner());
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,2,h,illegal,List.of(),null,clock(T.plusSeconds(11))))).isInstanceOf(IllegalArgumentException.class);
        });
    }

    @Test void releasedHistoryPointerCannotOverwriteTheOriginalEpisodeNumber() {
        withStore(f -> {
            var old=fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var goodSignal=new AlertSignal(KEY,1,2,"producer-1",2,T.plusSeconds(1),AlertSignal.Condition.GOOD,AlarmSeverity.MAJOR);
            var ended=AlertLifecycle.evaluate(old.snapshot(),goodSignal,T.plusSeconds(1)).snapshot();
            var endHead=head(ended,new Episode(RULE,ended.episode(),POLICY,old.episode().diagnostics(),false),old.owner());
            tx(f,s -> f.store.stageEnvelope(s,1,old,endHead,List.of(),null,clock(T.plusSeconds(1))));
            var released=new AlertSnapshot(RULE,true,AlertSnapshot.Status.RESOLVED,AlertSnapshot.Quality.VALID,2,T.plusSeconds(1),null,null,1,null);
            var release=head(released,null,old.owner()); tx(f,s -> f.store.stageEnvelope(s,2,endHead,release,List.of(),null,clock(T.plusSeconds(1))));
            var fakeEpisode=new AlertSnapshot.Episode(1,T.plusSeconds(2),AlarmSeverity.MAJOR,null,null,null);
            var fake=new AlertSnapshot(RULE,true,AlertSnapshot.Status.FIRING,AlertSnapshot.Quality.VALID,3,T.plusSeconds(2),null,null,1,fakeEpisode);
            var bad=head(fake,new Episode(RULE,fakeEpisode,POLICY,Diagnostics.empty(true),false),old.owner());
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,3,release,bad,List.of(),null,clock(T.plusSeconds(2))))).isInstanceOf(IllegalArgumentException.class);
            assertThat((Object)tx(f,s -> f.store.episodeEnvelope(s,KEY,1).orElseThrow())).isEqualTo(endHead.episode());
        });
    }

    @Test void expiredIdenticalHeadCannotAppendAHistoryTransition() {
        withStore(f -> {
            var old=fired(); tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var claim=head(old.snapshot(),old.episode(),new AlertOwnership(1,"worker-a",T.plusSeconds(5)));
            tx(f,s -> f.store.stageEnvelope(s,1,old,claim,List.of(),null,clock(T)));
            var change=new Transition(new AlertUpdate.Transition(AlertUpdate.Kind.QUALITY_CHANGED,T.plusSeconds(6),1,AlarmSeverity.MAJOR),null,
                    DiagnosticStatus.CAPTURED,old.episode().diagnostics().latest(),POLICY);
            assertThatThrownBy(() -> tx(f,s -> f.store.stageEnvelope(s,2,claim,claim,List.of(change),null,clock(T.plusSeconds(6))))).isInstanceOf(IllegalArgumentException.class);
            assertThat((Object)tx(f,s -> f.store.stageEnvelope(s,2,claim,claim,List.of(),null,clock(T.plusSeconds(6))))).isEqualTo(MongoAlertStateStore.Stage.UNCHANGED);
        });
    }

    @Test void sameEvaluationCannotSwitchItsConfigurationIdentityOrResourceBinding() {
        withStore(f -> {
            var old=fired();var ruleA=new AlertConfiguration.Identity(AlertConfiguration.Kind.RULE,KEY.scope(),"rule-a","11111111-1111-4111-8111-111111111111");
            var a=new io.tapstate.core.alerting.AlertIntent.Member(RULE,1,ruleA,0);
            var bound=new Head(old.snapshot(),old.deployment(),old.owner(),null,false,old.episode(),a);
            tx(f,s -> f.store.stageEnvelope(s,0,null,bound,List.of(),bad(1,T),clock(T)));
            var ruleB=new AlertConfiguration.Identity(AlertConfiguration.Kind.RULE,KEY.scope(),"rule-a","22222222-2222-4222-8222-222222222222");
            var b=new io.tapstate.core.alerting.AlertIntent.Member(RULE,1,ruleB,0);
            var relabelled=new Head(old.snapshot(),old.deployment(),old.owner(),null,false,old.episode(),b);
            assertThatThrownBy(()->tx(f,s -> f.store.stageEnvelope(s,1,bound,relabelled,List.of(),null,clock(T)))).isInstanceOf(IllegalArgumentException.class);
        });
    }
    @Test void newEvaluationBindingMustEndTheOriginalOpenEpisode() {
        withStore(f -> {
            var old=fired();tx(f,s -> f.store.stageEnvelope(s,0,null,old,List.of(),bad(1,T),clock(T)));
            var rule=new AlertRule(KEY,2,3,"producer-2",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(15),Duration.ofSeconds(10));
            var changed=new AlertSnapshot(rule,true,old.snapshot().status(),old.snapshot().quality(),1,T.plusSeconds(1),null,null,1,old.snapshot().episode());
            var illegal=head(changed,old.episode(),old.owner());
            assertThatThrownBy(()->tx(f,s -> f.store.stageEnvelope(s,1,old,illegal,List.of(),null,clock(T.plusSeconds(1))))).isInstanceOf(IllegalArgumentException.class);
        });
    }

    private record Fixture(MongoClient client,MongoDatabase db,MongoAlertStateStore store) { }
    private static <T> T tx(Fixture f,Function<ClientSession,T> work) { try(var s=f.client.startSession()){ return s.withTransaction(()->work.apply(s),TX); } }
    private static void withStore(java.util.function.Consumer<Fixture> work) {
        try(var client=MongoClients.create(MONGO.getReplicaSetUrl())) {
            var db=client.getDatabase("alert_envelope_"+UUID.randomUUID().toString().replace("-",""));
            var settings=new MongoConnectionSettings(MONGO.getReplicaSetUrl(db.getName()),null,Duration.ofSeconds(5));
            try(var original=MongoConfigurationBootstrap.prepare(settings,KEY.scope().clusterId(),"installation-a","setup-a",1)) {
                work.accept(new Fixture(client,db,new MongoAlertStateStore(db,KEY.scope(),()->{})));
            } finally { db.drop(); }
        }
    }
    private static final class Abort extends RuntimeException { }
}
