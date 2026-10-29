package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.spi.alerting.NotificationChannel;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** Real CONFIG pruning uses each original deployment and result window, independently of physical history expiry. */
@RequiresDocker
class AlertRetentionMatrixIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static Stream<Arguments> outcomes(){return Stream.of(false,true).flatMap(cloud->Stream.of("accepted","rejected","unknown","retry").map(outcome->Arguments.of(cloud,outcome)));}
    @ParameterizedTest @MethodSource("outcomes")
    void payloadAndResultWindowsRemainFrozenAndNeverDeleteTheActiveEpisode(boolean cloud,String outcome){
        AlertEvaluationHostIT.withFixture(false,cloud,f->{
            if(!cloud)route(f,Duration.ZERO);
            var planner=new AlertNotificationPlanner(f.store(),f.clock(),"retention-matrix",cloud?URI.create("https://cloud.example.test"):null,ACTIVE);
            f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var capability=cloud?new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK):new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL);
            var store=f.configuration().alerts(f.storeHost().withChannels(Set.of(capability)),BuiltinAlarmTypes.catalog(),f.clock());
            var channels=new AlertDeliveryRuntimeIT.Channels(store);
            channels.answer=request->switch(outcome){
                case "accepted"->cloud?new NotificationChannel.Result(DeliveryEvidence.Outcome.ACCEPTED,NotificationChannel.Action.STOP,null,NotificationChannel.Phase.FINAL_RESPONSE,200,"cloud-sdk",new NotificationChannel.Receipt("012345678901234567890123","fixture-request")):AlertDeliveryRuntimeIT.accepted();
                case "rejected"->new NotificationChannel.Result(DeliveryEvidence.Outcome.NOT_ACCEPTED,NotificationChannel.Action.STOP,null,NotificationChannel.Phase.FINAL_RESPONSE,cloud?403:550,"fixture-rejected");
                default->new NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,outcome.equals("retry")?NotificationChannel.Action.RETRY:NotificationChannel.Action.STOP,null,NotificationChannel.Phase.DEADLINE,0,"fixture-unknown");
            };
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,store,channels,"retention-sender")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
            var original=store.deliveries(null,100,null,null,f.reads()).rows().getFirst().value();var identity=original.frozen().identity();
            assertThat(original.state().attemptsConsumed()).isOne();
            var permit=original.state().attempts().getFirst().permit();
            if(!outcome.equals("accepted")&&!cloud){
                if(outcome.equals("retry"))assertThat(original.state().nextAttemptAt()).isNotNull();
                else assertThat(original.state().nextAttemptAt()).isNull();
                f.now().set(original.frozen().deadline().minusNanos(1));clean(store);
                assertThat(store.intent(identity,null).orElseThrow().value().state().payload()).isNotNull();
                f.now().set(original.frozen().deadline().plusNanos(1));clean(store);
            }
            var stopped=store.intent(identity,null).orElseThrow().value();var retained=stopped.state().retained();
            assertThat(stopped.state().retained().stoppedAt()).isNotNull();
            var expected=outcome.equals("accepted")?DeliveryEvidence.Outcome.ACCEPTED:outcome.equals("rejected")?DeliveryEvidence.Outcome.NOT_ACCEPTED:DeliveryEvidence.Outcome.UNKNOWN;
            assertThat(stopped.state().summary()).isEqualTo(expected);
            f.now().set(retained.payloadUntil().minusNanos(1));clean(store);
            assertThat(store.intent(identity,null).orElseThrow().value().state().payload()).isNotNull();
            f.now().set(retained.payloadUntil());clean(store);
            var pruned=store.intent(identity,null).orElseThrow().value();
            assertThat(pruned.state().payload()).isNull();assertThat(pruned.state().summary()).isEqualTo(expected);
            assertThat(pruned.frozen()).isEqualTo(original.frozen());
            assertThat(pruned.state().attemptsConsumed()).isOne();
            assertThat(store.active(null,10,f.reads()).total()).isOne();
            f.now().set(retained.resultUntil());clean(store);
            var closed=store.intent(identity,null).orElseThrow().value();
            assertThat(closed.state().detailsPrunedAt()).isNotNull();assertThat(closed.state().attempts()).isEmpty();
            assertThat(closed.state().summary()).isEqualTo(expected);assertThat(closed.state().lastAttemptEnd()).isNotNull();
            assertThat(AlertDeliveryProgress.result(closed,permit,DeliveryEvidence.Outcome.ACCEPTED,
                    new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.STOP,200,"fixture-late",null),
                    new AlertIntent.Receipt(cloud?"sdk-event-center":"channel-protocol","012345678901234567890123",null),f.clock().instant(),0)).isEqualTo(closed);
            assertThat(store.active(null,10,f.reads()).total()).isOne();
        });
    }
    private static void clean(AlertStore store){store.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),200,4L*1024*1024,Duration.ofSeconds(5));}
}
