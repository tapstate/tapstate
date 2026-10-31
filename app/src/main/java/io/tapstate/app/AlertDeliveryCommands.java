package io.tapstate.app;

import io.tapstate.adapters.alerting.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;

/** Application commands shared by configuration tests and the eventual authenticated HTTP surface. */
final class AlertDeliveryCommands {
    private final AlertStore store;private final AlarmKey.Scope scope;private final AlertChannelPolicy.Deployment deployment;private final Clock clock;private final AlertRetentionPolicy retentionPolicy;
    AlertDeliveryCommands(AlertStore store,AlarmKey.Scope scope,AlertChannelPolicy.Deployment deployment,Clock clock){this(store,scope,deployment,clock,AlertRetentionPolicy.defaults(deployment));}
    AlertDeliveryCommands(AlertStore store,AlarmKey.Scope scope,AlertChannelPolicy.Deployment deployment,Clock clock,AlertRetentionPolicy retention){this.store=store;this.scope=scope;this.deployment=deployment;this.clock=clock;
        if(retention.deployment()!=deployment)throw new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","retention","reason","foreign-deployment"),null);this.retentionPolicy=retention;}
    AlertStore.Stored<AlertIntent.Record> test(AlertTestCommand.ClientId command,String candidateId,AlertReceiver.Transport transport,
            String target,AlertReceiver existing,AlertStore.PrivateMaterial material,Set<AlertStore.PrivateField> retain,
            AlertConfiguration.Subscription messageRoute,AlertStore.Access access){
        return test(command,candidateId,transport,target,existing,material,retain,messageRoute,access,true);
    }
    private AlertStore.Stored<AlertIntent.Record> test(AlertTestCommand.ClientId command,String candidateId,AlertReceiver.Transport transport,
            String target,AlertReceiver existing,AlertStore.PrivateMaterial material,Set<AlertStore.PrivateField> retain,
            AlertConfiguration.Subscription messageRoute,AlertStore.Access access,boolean recoverRace){
        if(deployment!=AlertChannelPolicy.Deployment.ON_PREM||access==null||access.actor()==null)throw stale("test-deployment-or-actor");
        access.revalidate().run();var now=clock.instant();var key=new AlertTestCommand.Key(scope,access.actor().principalKey(),command);
        var previous=store.test(key,access);
        MessageSettings settings=MessageSettings.empty();AlertTestCommand.Route route=null;long revision=1;
        if(messageRoute!=null){
            if(!store.definition(AlertConfiguration.Kind.SUBSCRIPTION,messageRoute.header().identity().id(),access).filter(messageRoute::equals).isPresent())throw stale("message-route");
            settings=messageRoute.channels().stream().filter(c->c.channelType().equals(transport.channelType())).findFirst().orElseThrow(()->stale("route-channel")).messages();
            revision=messageRoute.header().revision();route=new AlertTestCommand.Route(messageRoute.header().identity().incarnation(),revision,revision);
        }
        if(previous.isPresent()){
            var old=((AlertIntent.Test)previous.get().value().frozen().identity()).command();
            var form=new AlertTestCommand.Parameters(transport.channelType(),new AlertTestCommand.Candidate(candidateId,"0".repeat(64)),
                    existing==null?null:new AlertTestCommand.ExistingReceiver(existing.header().identity().incarnation(),existing.header().revision()),
                    target,"candidate",route,old.parameters().payloadSha256());
            var prepared=store.prepareCandidate(key,form,transport,existing==null?null:existing.header().identity(),
                    existing==null?0:existing.bindingRevision(),material,retain,access);
            if(!old.parameters().equals(prepared.parameters()))throw new TapstateException(AlertError.COMMAND_CONFLICT,Map.of(),null);
            return previous.get();
        }
        var renderedAt=now;
        var rendered=new NotificationRenderer().render(new NotificationRenderer.Facts(NotificationRenderer.Kind.CHANNEL_TEST,key.testId(),0,
                renderedAt,List.of(),0,null,List.of(),key.testId(),transport.channelType()),settings);
        if(!rendered.successful())throw stale("test-rendering");
        byte[] bytes;
        if(transport instanceof AlertReceiver.Email email){var mime=EmailMime.freeze(key.testId(),renderedAt,email.from(),target,rendered.snapshot());if(!mime.successful())throw stale("test-mime");bytes=mime.bytes();}
        else bytes=rendered.snapshot().webhook();
        var payload=new AlertIntent.Payload(bytes);
        var form=new AlertTestCommand.Parameters(transport.channelType(),new AlertTestCommand.Candidate(candidateId,"0".repeat(64)),
                existing==null?null:new AlertTestCommand.ExistingReceiver(existing.header().identity().incarnation(),existing.header().revision()),
                target,"candidate",route,payload.sha256());
        var prepared=store.prepareCandidate(key,form,transport,existing==null?null:existing.header().identity(),existing==null?0:existing.bindingRevision(),material,retain,access);
        var admitted=AlertTestCommand.admit(deployment,new AlertChannelPolicy.Capability(transport.channelType(),1,AlertChannelPolicy.Family.OP_EXTERNAL),key,prepared.parameters(),null,now).command();
        var retention=AlertIntent.Policy.capture(retentionPolicy);
        var frozen=new AlertIntent.Frozen(new AlertIntent.Test(admitted),new AlertChannelPolicy.Capability(transport.channelType(),1,AlertChannelPolicy.Family.OP_EXTERNAL),null,prepared.target(),
                new AlertIntent.Content(transport.channelType().equals("email")?"message/rfc822":"application/json",payload.sha256(),new AlertIntent.Template("channel-test",1,revision,"en",settings),false),
                List.of(),ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5),Duration.ofSeconds(transport.channelType().equals("email")?60:30)),retention,now,admitted.deadline(),access.actor());
        var retained=AlertIntent.Retained.capture(DeliveryRetention.begin(retention.value(),DeliveryRetention.Kind.CHANNEL_TEST,now,admitted.deadline()));
        try {
            return store.admitTest(new AlertIntent.Record(frozen,new AlertIntent.State(AlertOwnership.unclaimed(),0,false,null,now,0,List.of(),DeliveryEvidence.Outcome.NOT_ATTEMPTED,
                    retained,payload,prepared.encrypted(),null,null,null)),access);
        } catch(TapstateException conflict) {
            if(!recoverRace || conflict.code()!=AlertError.COMMAND_CONFLICT || store.test(key,access).isEmpty())throw conflict;
            return test(command,candidateId,transport,target,existing,material,retain,messageRoute,access,false);
        }
    }
    AlertStore.Stored<AlertIntent.Record> result(AlertTestCommand.Key key,AlertStore.Access access){return store.test(key,access).orElseThrow(()->stale("test-not-found"));}
    AlertStore.Outcome retry(AlertIntent.Identity identity,long expectedRevision,AlertStore.Access access){
        if(deployment!=AlertChannelPolicy.Deployment.ON_PREM||!(identity instanceof AlertIntent.Business)||access==null)throw stale("retry-deployment-or-purpose");
        var before=store.intent(identity,access).orElseThrow(()->stale("original-intent"));
        if(before.revision()!=expectedRevision)throw new TapstateException(AlertError.COMMAND_CONFLICT,Map.of(),null);
        var after=AlertDeliveryProgress.manual(before.value(),clock.instant());
        return store.commitAsCaller(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(before,after)),List.of()),access);
    }
    private static TapstateException stale(String action){return new TapstateException(AlertError.STALE_ACTION,Map.of("action",action),null);}
}
