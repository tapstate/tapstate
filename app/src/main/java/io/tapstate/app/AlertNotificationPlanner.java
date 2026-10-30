package io.tapstate.app;

import io.tapstate.adapters.alerting.EmailMime;
import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AlertStore;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;

/** Bounded planning over one CONFIG snapshot. Every result is a candidate until its versioned commit succeeds. */
final class AlertNotificationPlanner {
    private static final int MAX_CHANGES = 100;
    private final AlertStore store;
    private final Clock clock;
    private final String worker;
    private final URI cloudOrigin;
    private final NotificationRenderer renderer = new NotificationRenderer();
    private final Runnable active;
    private int offset;
    record Destination(AlertIntent.Route route, AlertIntent.Target target, String channel, String receiver,
            String targetId, MessageSettings messages, long settingsRevision, AlertGroupingClock.Policy grouping,
            Set<AlertConfiguration.GroupBy> groupBy, AlertConfiguration.Subscription subscription) { }
    record Changes(List<AlertStore.IntentChange> intents,List<AlertStore.ScheduleChange> schedules) { }

    AlertNotificationPlanner(AlertStore store,Clock clock,String worker,URI cloudOrigin,Runnable active) {
        this.store=Objects.requireNonNull(store);this.clock=Objects.requireNonNull(clock);this.worker=Objects.requireNonNull(worker);
        this.cloudOrigin=cloudOrigin;this.active=Objects.requireNonNull(active);
    }
    /** State acceptance registers notification membership in the same commit, without sending or advancing a round. */
    AlertStore.Outcome accept(AlertStore.Commit state) {
        active.run();var view=store.planning();var changes=seed(view,state.state()==null?null:state.state().after());
        if(state.state()!=null&&state.state().after().binding()!=null&&view.unavailableRules().containsKey(state.state().after().binding().ruleIdentity().id()))throw unavailableRule();
        if(view.isolatedRecords()&&!changes.schedules().isEmpty())throw isolated();
        return store.commit(new AlertStore.Commit(state.kind(),state.state(),changes.intents(),changes.schedules(),view.version()));
    }
    void tick() {
        active.run();
        // Discovery starts from durable state and schedules on every process; no process-local known set is authoritative.
        var initial=store.planning();boolean isolated=initial.isolatedRecords(),unavailable=!initial.unavailableRules().isEmpty();var seed=isolated?new Changes(List.of(),List.of()):seed(initial,null);
        if(!seed.schedules().isEmpty())require(store.commit(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(),seed.schedules(),initial.version())));
        var keys=store.planning().schedules().stream().map(v->v.value()).filter(v->v.batch()==null&&v.retiredAt()==null).map(AlertSchedule.Record::key).toList();
        int count=Math.min(32,keys.size());
        for(int index=0;index<count;index++){var key=keys.get(Math.floorMod(offset+index,keys.size()));active.run();
            boolean done=false;for(int retry=0;retry<3;retry++){
                var view=store.planning();isolated|=view.isolatedRecords();unavailable|=!view.unavailableRules().isEmpty();var row=view.schedules().stream().filter(v->v.value().key().equals(key)).findFirst();if(row.isEmpty()||held(view,row.orElseThrow().value())){done=true;break;}
                var changes=advance(view,row.get());if(changes.intents().isEmpty()&&changes.schedules().isEmpty()){done=true;break;}
                if(store.commit(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,changes.intents(),changes.schedules(),view.version()))!=AlertStore.Outcome.CONFLICT){done=true;break;}
            }if(!done)throw new TapstateException(AlertError.COMMAND_CONFLICT,Map.of(),null);
        }
        offset=keys.isEmpty()?0:Math.floorMod(offset+count,keys.size());
        if(isolated)throw isolated();
        if(unavailable)throw unavailableRule();
    }
    private Changes seed(AlertStore.Planning view,AlertStateRecord.Head replacement) {
        var heads=new LinkedHashMap<AlarmKey,AlertStateRecord.Head>();
        if(replacement==null)view.states().forEach(v->heads.put(v.value().snapshot().rule().key(),v.value()));
        else heads.put(replacement.snapshot().rule().key(),replacement);
        var roots=new LinkedHashMap<AlertSchedule.Key,AlertSchedule.Record>();var original=new HashMap<AlertSchedule.Key,AlertStore.Stored<AlertSchedule.Record>>();
        view.schedules().stream().filter(v->v.value().batch()==null&&v.value().retiredAt()==null).forEach(v->{roots.put(v.value().key(),v.value());original.put(v.value().key(),v);});
        var changed=new LinkedHashSet<AlertSchedule.Key>();
        for(var head:heads.values()){
            if(!firing(head)||head.binding()==null||view.unavailableRules().containsKey(head.binding().ruleIdentity().id()))continue;
            for(var destination:destinations(view,head)){
                if(!installed(view.host(),destination))continue;
                String group=group(destination,head.binding());
                if(roots.values().stream().anyMatch(r->sameDestination(r,destination)&&r.members().stream().anyMatch(m->m.episodeMember().equals(head.binding().episodeMember()))))continue;
                var existing=roots.values().stream().filter(r->!held(view,r)&&sameDestination(r,destination)&&r.retention().equals(head.episode().policy())&&r.members().stream().anyMatch(m->group(destination,m).equals(group))
                        &&r.members().stream().noneMatch(m->m.evaluation().key().equals(head.binding().evaluation().key())&&m.episode()!=head.binding().episode())).findFirst();
                AlertSchedule.Record root;
                if(existing.isPresent())root=existing.get();
                else {
                    String subject="group-"+group+"-"+occurrence(head.binding());
                    var key=new AlertSchedule.Key(head.snapshot().rule().key().scope(),AlertSchedule.SubjectKind.GROUP,subject,"root",
                            destination.route().id(),destination.route().incarnation(),destination.channel(),destination.receiver(),destination.targetId());
                    root=new AlertSchedule.Record(key,destination.route(),head.episode().policy(),clock.instant(),AlertOwnership.unclaimed(),destination.grouping(),
                            AlertGroupingClock.empty(),0,List.of(),null,null,List.of(),List.of(),null,false,List.of());
                }
                var member=head.binding();
                if(root.members().stream().anyMatch(m->m.evaluation().key().equals(member.evaluation().key())))continue;
                var members=new ArrayList<>(root.members());members.add(member);members.sort(Comparator.comparing(AlertNotificationPlanner::occurrence));
                var eligibleNow=!muted(view,destination,head,clock.instant());
                var candidateClock=eligibleNow?root.clock().changed(clock.instant()):root.clock();
                root=copy(root,root.owner(),candidateClock,root.batchSequence(),members,eligibleNow?members:root.pending(),root.disposition(),root.summaries(),root.protectedIntents(),null);
                roots.put(root.key(),root);changed.add(root.key());
                if(changed.size()>MAX_CHANGES)throw invalid("planning-registration-budget");
                if(replacement==null&&changed.size()==MAX_CHANGES)return new Changes(List.of(),changed.stream().map(k->new AlertStore.ScheduleChange(original.get(k),roots.get(k))).toList());
            }
        }
        return new Changes(List.of(),changed.stream().map(k->new AlertStore.ScheduleChange(original.get(k),roots.get(k))).toList());
    }
    private Changes advance(AlertStore.Planning view,AlertStore.Stored<AlertSchedule.Record> stored) {
        var root=stored.value();Instant now=clock.instant();var ownership=claim(root.owner(),now);
        if(ownership==null)return new Changes(List.of(),List.of());
        var destinations=destinations(view,null);var destination=destinations.stream().filter(d->sameDestination(root,d)).findFirst().orElse(null);
        boolean canCreate=destination!=null&&installed(view.host(),destination);
        var edits=new LinkedHashMap<DeliveryKey,AlertStore.IntentChange>();var schedules=new ArrayList<AlertStore.ScheduleChange>();
        var refs=new ArrayList<>(root.protectedIntents());var summaries=new LinkedHashMap<FrozenGroupPolicy.Member,AlertSchedule.Previous>();
        root.summaries().forEach(s->summaries.put(new FrozenGroupPolicy.Member(s.alarm(),s.episode()),s));
        var current=new HashMap<FrozenGroupPolicy.Member,AlertStateRecord.Head>();for(var row:view.states())if(row.value().binding()!=null&&row.value().episode()!=null)current.put(row.value().binding().episodeMember(),row.value());
        var eligible=new ArrayList<AlertIntent.Member>();var remaining=new ArrayList<AlertIntent.Member>();var ended=new ArrayList<AlertIntent.Member>();
        for(var member:root.members()){
            var episode=episode(view,member);var head=current.get(member.episodeMember());
            if(episode.value().endedAt()!=null)ended.add(member);
            else if(destination!=null&&bindingCurrent(view,member)&&head!=null&&firing(head)){
                remaining.add(member);if(matches(destination,head)&&!muted(view,destination,head,now))eligible.add(member);
            }
        }
        boolean membershipChanged=!sameMembers(root.members(),remaining);
        var clockValue=!eligible.isEmpty()&&(membershipChanged||!sameMembers(root.pending(),eligible))?root.clock().changed(now):root.clock();
        if(clockValue.lastPlannedAt()!=null&&!eligible.isEmpty()&&eligible.stream().allMatch(m->covered(view,root,m)))
            clockValue=new AlertGroupingClock(clockValue.firstCandidateAt(),clockValue.lastPlannedAt(),false,clockValue.round(),clockValue.reminderEligibleAt());
        boolean planned=false;boolean waitingSuccessor=false;long batchSequence=root.batchSequence();
        // Reconcile every frozen predecessor before recovery or any successor gets a new identity.
        for(var key:List.copyOf(refs)){
            var retained=view.intents().get(key);if(retained==null)throw invalid("missing-original-intent");var intent=retained.value();boolean pendingSuccessor=false;
            if(key.notification().kind()==DeliveryKey.Kind.FAULT||key.notification().kind()==DeliveryKey.Kind.REMINDER){
                var historicalKey=AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,key);
                var historical=view.schedules().stream().filter(v->v.value().key().equals(historicalKey)).findFirst().orElseThrow(()->invalid("missing-frozen-batch"));
                var survivors=historical.value().members().stream().filter(m->now.isBefore(retained.value().frozen().deadline())).filter(m->eligible.stream().anyMatch(e->e.episodeMember().equals(m.episodeMember()))).toList();
                if(survivors.size()!=intent.frozen().members().size()||historical.value().disposition()!=null&&historical.value().disposition().successor()!=null){
                    if(historical.value().disposition()!=null&&historical.value().disposition().successor()!=null){
                        var recorded=historical.value().disposition();
                        if(intent.state().summary()==DeliveryEvidence.Outcome.ACCEPTED){
                            var successor=view.intents().get(recorded.successor());
                            if(successor!=null){var stopped=AlertIntentControl.stop(successor.value(),AlertIntent.StopReason.CANCELLED,now);if(!stopped.equals(successor.value()))edits.put(recorded.successor(),new AlertStore.IntentChange(successor,stopped));}
                            var covered=new AlertSchedule.Disposition(key,recorded.originalRevision(),FrozenGroupPolicy.Action.STOP_COVERED,recorded.pending(),recorded.successor(),null,DeliveryEvidence.Outcome.ACCEPTED);
                            var before=historical.value();var after=copy(before,claimOrOriginal(before.owner(),now),before.clock(),before.batchSequence(),before.members(),List.of(),covered,before.summaries(),before.protectedIntents(),before.retiredAt());
                            if(!after.equals(before))putSchedule(schedules,new AlertStore.ScheduleChange(historical,after));
                        }
                    }else{
                    var stopped=AlertIntentControl.stop(intent,AlertIntent.StopReason.CANCELLED,now);if(!stopped.equals(intent)){edits.put(key,new AlertStore.IntentChange(retained,stopped));intent=stopped;}
                    var batchKey=AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,key);
                    var batch=view.schedules().stream().filter(v->v.value().key().equals(batchKey)).findFirst().orElseThrow(()->invalid("missing-frozen-batch"));
                    var before=batch.value();var decision=FrozenGroupPolicy.reconcile(new FrozenGroupPolicy.Batch(key,before.batch().revision(),intent.frozen().members().stream().map(AlertIntent.Member::episodeMember).toList()),
                            survivors.stream().map(AlertIntent.Member::episodeMember).collect(java.util.stream.Collectors.toSet()),List.of(evidence(intent)),now);
                    var pending=decision.pendingMembers().stream().map(m->survivors.stream().filter(v->v.episodeMember().equals(m)).findFirst().orElseThrow()).toList();
                    DeliveryKey successor=before.disposition()==null?null:before.disposition().successor();
                    if(successor!=null&&decision.action()==FrozenGroupPolicy.Action.STOP_COVERED){
                        pending=before.disposition().pending();var oldSuccessor=view.intents().get(successor);
                        if(oldSuccessor!=null){var stoppedSuccessor=AlertIntentControl.stop(oldSuccessor.value(),AlertIntent.StopReason.CANCELLED,now);if(!stoppedSuccessor.equals(oldSuccessor.value()))edits.put(successor,new AlertStore.IntentChange(oldSuccessor,stoppedSuccessor));}
                    }
                    if(successor==null&&decision.action()==FrozenGroupPolicy.Action.STOP_PENDING){
                        pendingSuccessor=now.isBefore(intent.frozen().deadline());
                        Instant qualification=root.clock().lastPlannedAt()==null?root.createdAt():root.clock().lastPlannedAt().plus(root.grouping().changeInterval());
                        if(!planned&&pendingSuccessor&&!now.isBefore(qualification)&&canCreate&&targetReady(view,destination,now)){
                            var candidate=new AlertGroupingClock.Candidate(AlertGroupingClock.Reason.CHANGE,Math.incrementExact(clockValue.round()),qualification);
                            successor=delivery(root,DeliveryKey.Kind.FAULT,candidate.round(),Math.incrementExact(batchSequence));
                            var next=create(view,destination,successor,pending,null,now,intent.frozen().deadline(),intent.frozen().retry(),intent.frozen().retention());
                            edits.put(successor,new AlertStore.IntentChange(null,next));refs.add(successor);
                            clockValue=clockValue.planned(candidate,now);batchSequence++;planned=true;pendingSuccessor=false;
                            var nextKey=AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,successor);
                            schedules.add(new AlertStore.ScheduleChange(null,new AlertSchedule.Record(nextKey,root.route(),root.retention(),now,ownership,root.grouping(),clockValue,batchSequence,List.of(),new AlertSchedule.Batch(successor,1,pending),null,List.of(),List.of(successor),null,false,pending)));
                        }
                    }
                    if(decision.action()==FrozenGroupPolicy.Action.STOP_COVERED&&eligible.stream().allMatch(survivors::contains))clockValue=new AlertGroupingClock(clockValue.firstCandidateAt(),clockValue.lastPlannedAt(),false,clockValue.round(),clockValue.reminderEligibleAt());
                    var disposition=new AlertSchedule.Disposition(key,before.batch().revision(),decision.action(),pending,successor,decision.recheckAt(),intent.state().summary());
                    var after=copy(before,claimOrOriginal(before.owner(),now),before.clock(),before.batchSequence(),survivors,
                            disposition.pending(),disposition,before.summaries(),before.protectedIntents(),before.retiredAt());
                    if(!after.equals(before))putSchedule(schedules,new AlertStore.ScheduleChange(batch,after));
                    waitingSuccessor|=pendingSuccessor||decision.action()==FrozenGroupPolicy.Action.STOP_WAIT;
                    // The stable root retains survivors as unique pending work; the original frozen row retains its disposition identity.
                    }
                }
            }
            if(!now.isBefore(intent.frozen().deadline())&&!intent.state().permitsStopped()){
                intent=AlertIntentControl.stop(intent,AlertIntent.StopReason.EXPIRED,now);edits.put(key,new AlertStore.IntentChange(retained,intent));
            }else if(intent.state().permitsStopped()){
                var settled=AlertIntentControl.stop(intent,intent.state().stopReason(),now);if(!settled.equals(intent)){intent=settled;edits.put(key,new AlertStore.IntentChange(retained,intent));}
            }
            if(!pendingSuccessor&&intent.state().retained().stoppedAt()!=null&&intent.state().attempts().stream().noneMatch(a->a.endedAt()==null)
                    &&(intent.state().summary()!=DeliveryEvidence.Outcome.UNKNOWN||!now.isBefore(intent.state().retained().resultUntil()))){
                if(key.notification().kind()==DeliveryKey.Kind.FAULT||key.notification().kind()==DeliveryKey.Kind.REMINDER)for(var member:intent.frozen().members()){
                    var id=member.episodeMember();var old=summaries.get(id);summaries.put(id,new AlertSchedule.Previous(id.key(),id.episode(),
                            old==null?intent.state().summary():DeliveryEvidence.aggregate(List.of(old.outcome(),intent.state().summary())),
                            intent.frozen().content().renderingFailed()||old!=null&&old.renderingFailed()));
                }
                refs.remove(key);
                if(key.notification().kind()==DeliveryKey.Kind.FAULT||key.notification().kind()==DeliveryKey.Kind.REMINDER){
                    var batchKey=AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,key);
                    var batch=view.schedules().stream().filter(v->v.value().key().equals(batchKey)).findFirst().orElseThrow(()->invalid("missing-frozen-batch"));
                    var staged=schedules.stream().filter(c->c.after().key().equals(batchKey)).findFirst().map(AlertStore.ScheduleChange::after).orElse(batch.value());
                    var local=intent.frozen().members().stream().map(m->new AlertSchedule.Previous(m.evaluation().key(),m.episode(),intentOutcome(edits,view,key),changedIntent(view,edits,key).frozen().content().renderingFailed())).toList();
                    putSchedule(schedules,new AlertStore.ScheduleChange(batch,copy(staged,staged.owner(),staged.clock(),staged.batchSequence(),staged.members(),List.of(),staged.disposition(),local,List.of(),now)));
                }
            }
            if(intent.state().retained().stoppedAt()!=null)clockValue=clockValue.coalescedPredecessorEnded(intent.state().retained().stoppedAt(),root.grouping());
        }
        // At most one new notification round per root command; ended members retain their identity until this decision commits.
        var retainedMembers=new ArrayList<>(remaining);var waiting=new ArrayList<AlertIntent.Member>();
        for(var member:ended){
            var previous=new ArrayList<DeliveryEvidence.Previous>();var summary=summaries.get(member.episodeMember());if(summary!=null)previous.add(summary.evidence());
            for(var key:refs)if(key.notification().kind()==DeliveryKey.Kind.FAULT||key.notification().kind()==DeliveryKey.Kind.REMINDER){var intent=changedIntent(view,edits,key);if(intent.frozen().members().stream().anyMatch(m->m.episodeMember().equals(member.episodeMember())))previous.add(evidence(intent));}
            var ep=episode(view,member);boolean revoked=destination==null||!bindingCurrent(view,member)||ep.value().endReason()!=AlertSnapshot.EndReason.RECOVERED;
            boolean silenced=view.definitions().stream().filter(d->d instanceof AlertConfiguration.Silence).map(d->(AlertConfiguration.Silence)d).anyMatch(s->s.matches(member.evaluation().key(),now));
            var recovery=DeliveryEvidence.recovery(previous,now,revoked,silenced);
            if(recovery.kind()!=DeliveryEvidence.RecoveryKind.NONE&&(!canCreate||!targetReady(view,destination,now))||recovery.kind()==DeliveryEvidence.RecoveryKind.WAIT||planned&&recovery.kind()!=DeliveryEvidence.RecoveryKind.NONE){waiting.add(member);continue;}
            if(recovery.kind()!=DeliveryEvidence.RecoveryKind.NONE){
                var candidate=new AlertGroupingClock.Candidate(AlertGroupingClock.Reason.CHANGE,Math.incrementExact(clockValue.round()),now);
                var key=delivery(root,DeliveryKey.Kind.RECOVERY,candidate.round(),Math.incrementExact(batchSequence));
                var intent=create(view,destination,key,List.of(member),recovery.kind(),now,null,null,root.retention());edits.put(key,new AlertStore.IntentChange(null,intent));refs.add(key);
                clockValue=clockValue.planned(candidate,now);batchSequence++;planned=true;
            }
        }
        retainedMembers.addAll(waiting);
        if(!planned&&!waitingSuccessor&&canCreate&&!eligible.isEmpty()){
            boolean unfinished=refs.stream().anyMatch(k->k.notification().kind()!=DeliveryKey.Kind.RECOVERY&&changedIntent(view,edits,k).state().retained().stoppedAt()==null);
            var due=clockValue.due(root.grouping(),now,true,unfinished);
            if(due.isPresent()&&targetReady(view,destination,now)){
                var candidate=due.get();DeliveryKey.Kind kind=candidate.reason()==AlertGroupingClock.Reason.REPEAT?DeliveryKey.Kind.REMINDER:DeliveryKey.Kind.FAULT;
                var key=delivery(root,kind,candidate.round(),Math.incrementExact(batchSequence));
                var intent=create(view,destination,key,eligible,null,now,null,null,root.retention());edits.put(key,new AlertStore.IntentChange(null,intent));refs.add(key);
                clockValue=clockValue.planned(candidate,now);batchSequence++;
                var batchKey=AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,key);
                var batch=new AlertSchedule.Batch(key,1,eligible);
                var batchRow=new AlertSchedule.Record(batchKey,root.route(),root.retention(),now,ownership,root.grouping(),clockValue,batchSequence,List.of(),batch,null,List.of(),List.of(key),null,false,eligible);
                schedules.add(new AlertStore.ScheduleChange(null,batchRow));
            }
        }
        // A finished empty root may retire only after the original references have been handed to summaries.
        Instant retired=retainedMembers.isEmpty()&&refs.isEmpty()?now:null;
        var after=copy(root,ownership,clockValue,batchSequence,retainedMembers,eligible,null,List.copyOf(summaries.values()),refs,retired);
        if(!after.equals(root))schedules.add(new AlertStore.ScheduleChange(stored,after));
        if(edits.size()>MAX_CHANGES||schedules.size()>MAX_CHANGES)throw invalid("planning-commit-budget");
        return new Changes(List.copyOf(edits.values()),List.copyOf(schedules));
    }
    private static boolean held(AlertStore.Planning view,AlertSchedule.Record root){
        if(root.members().stream().anyMatch(member->view.unavailableRules().containsKey(member.ruleIdentity().id()))
                ||root.pending().stream().anyMatch(member->view.unavailableRules().containsKey(member.ruleIdentity().id()))
                ||root.disposition()!=null&&root.disposition().pending().stream().anyMatch(member->view.unavailableRules().containsKey(member.ruleIdentity().id())))return true;
        for(var key:root.protectedIntents()){var intent=view.intents().get(key);if(intent!=null&&intent.value().frozen().members().stream().anyMatch(member->view.unavailableRules().containsKey(member.ruleIdentity().id())))return true;}
        return false;
    }
    private static TapstateException unavailableRule(){return new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-planning","field","rule-capability"),null);}
    private List<Destination> destinations(AlertStore.Planning view,AlertStateRecord.Head head) {
        var host=view.host();var result=new ArrayList<Destination>();
        if(host.deployment()==AlertChannelPolicy.Deployment.CLOUD_MANAGED){
            if(host.managedRoute()==null||host.managedTarget()==null||cloudOrigin==null)throw invalid("managed-planning-binding");
            result.add(new Destination(host.managedRoute(),host.managedTarget(),"cloud-sdk",host.managedTarget().incarnation(),host.managedTarget().logicalTargetId(),
                    MessageSettings.empty(),1,new AlertGroupingClock.Policy(Duration.ZERO,Duration.ofSeconds(1),Duration.ofHours(1)),Set.of(),null));return result;
        }
        for(var definition:view.definitions())if(definition instanceof AlertConfiguration.Subscription route&&route.header().active()&&route.enabled()&&route.bindingRevision()>0){
            if(head!=null&&!route.matches(head.snapshot().rule().key(),head.episode().value().severity()))continue;
            for(var channel:route.channels())for(var receiverId:channel.receivers()){
                var receiver=view.definitions().stream().filter(d->d instanceof AlertReceiver&&d.header().identity().equals(receiverId)).map(d->(AlertReceiver)d)
                        .filter(r->r.header().active()&&r.enabled()&&r.transport().channelType().equals(channel.channelType())).findFirst();
                if(receiver.isEmpty())continue;
                var target=AlertIntent.ReceiverTarget.capture(receiver.get());
                var targets=AlertTargetIdentity.domains(host.scope(),target.transport()).keySet();
                for(var id:targets)result.add(new Destination(new AlertIntent.ConfiguredRoute(route.header().identity(),route.bindingRevision()),target,
                        channel.channelType(),receiverId.incarnation(),id,channel.messages(),route.header().revision(),route.grouping(),route.groupBy(),route));
                if(result.size()>1000)throw invalid("planning-destinations-budget");
            }
        }
        return result;
    }
    private static boolean covered(AlertStore.Planning view,AlertSchedule.Record root,AlertIntent.Member member){
        if(root.summaries().stream().anyMatch(v->v.alarm().equals(member.evaluation().key())&&v.episode()==member.episode()&&v.outcome()==DeliveryEvidence.Outcome.ACCEPTED))return true;
        return root.protectedIntents().stream().filter(k->k.notification().kind()==DeliveryKey.Kind.FAULT||k.notification().kind()==DeliveryKey.Kind.REMINDER).map(view.intents()::get)
                .filter(Objects::nonNull).anyMatch(v->v.value().state().summary()==DeliveryEvidence.Outcome.ACCEPTED&&v.value().frozen().members().stream().anyMatch(m->m.episodeMember().equals(member.episodeMember())));
    }
    private static boolean bindingCurrent(AlertStore.Planning view,AlertIntent.Member member){return view.definitions().stream().filter(d->d instanceof AlertConfiguration.Rule).map(d->(AlertConfiguration.Rule)d).anyMatch(r->r.header().identity().equals(member.ruleIdentity())&&r.evaluationRevision()==member.evaluation().revision()&&r.matches(member.evaluation().key()));}
    private boolean matches(Destination d,AlertStateRecord.Head head){return d.subscription()==null||d.subscription().matches(head.snapshot().rule().key(),head.episode().value().severity());}
    private boolean muted(AlertStore.Planning view,Destination d,AlertStateRecord.Head head,Instant now){
        for(var def:view.definitions())if(def instanceof AlertConfiguration.Silence silence&&silence.matches(head.snapshot().rule().key(),now))return true;
        if(d.subscription()!=null){var policy=new AlertInhibitionPolicy(view.host().scope(),d.subscription().inhibition());
            for(var row:view.states())if(policy.suppresses(row.value().snapshot(),head.snapshot(),view.dependencies().contains(new AlertStore.Dependency(row.value().snapshot().rule().key(),head.snapshot().rule().key()))))return true;
        }return false;
    }
    private static boolean firing(AlertStateRecord.Head head){return head.retiredAt()==null&&head.snapshot().enabled()&&head.snapshot().status()==AlertSnapshot.Status.FIRING&&head.episode()!=null&&head.episode().value().endedAt()==null;}
    private static boolean installed(AlertStore.Host host,Destination destination){
        return !(destination.target() instanceof AlertIntent.ReceiverTarget receiver&&receiver.transport() instanceof AlertReceiver.HttpsJson generic)
                ||host.availableChannels().contains(new AlertChannelPolicy.Capability(generic.channelType(),1,AlertChannelPolicy.Family.OP_EXTERNAL));
    }
    private static String occurrence(AlertIntent.Member m){var k=m.evaluation().key();return hash(List.of(k.scope().authorityId(),k.scope().clusterId(),k.resource().kind(),k.resource().id(),k.resource().incarnation(),k.typeId(),k.qualifier(),Long.toString(m.episode())));}
    private static String group(Destination d,AlertIntent.Member m){
        var key=m.evaluation().key();var fields=new ArrayList<String>(List.of(d.route().id(),d.route().incarnation(),d.channel(),d.receiver(),d.targetId()));
        fields.add(d.route() instanceof AlertIntent.ConfiguredRoute route?Long.toString(route.bindingRevision()):Integer.toString(((AlertIntent.SystemRoute)d.route()).profileVersion()));
        fields.add(d.target() instanceof AlertIntent.ReceiverTarget receiver?Long.toString(receiver.bindingRevision()):((AlertIntent.SdkTarget)d.target()).clusterBinding());
        if(d.groupBy().isEmpty())fields.add(occurrence(m));
        for(var by:AlertConfiguration.GroupBy.values())if(d.groupBy().contains(by))switch(by){
            case RESOURCE->{fields.add(key.resource().kind());fields.add(key.resource().id());fields.add(key.resource().incarnation());}
            case TYPE->fields.add(key.typeId());case RULE->fields.add(m.ruleIdentity().incarnation());
        }
        return hash(fields);
    }
    private static boolean sameDestination(AlertSchedule.Record root,Destination d){
        return root.route().equals(d.route())&&root.key().channelType().equals(d.channel())&&root.key().receiverIncarnation().equals(d.receiver())&&root.key().targetId().equals(d.targetId())
                &&!root.members().isEmpty()&&root.key().subjectId().startsWith("group-"+group(d,root.members().getFirst())+"-");
    }
    private boolean targetReady(AlertStore.Planning view,Destination d,Instant now){
        if(!(d.target() instanceof AlertIntent.ReceiverTarget receiver))return true;
        var domain=AlertTargetIdentity.domains(view.host().scope(),receiver.transport()).get(d.targetId());var limit=view.limits().get(domain);
        return limit==null||limit.restrictionElapsed(now);
    }
    private AlertIntent.Record create(AlertStore.Planning view,Destination d,DeliveryKey key,List<AlertIntent.Member> members,
            DeliveryEvidence.RecoveryKind recovery,Instant now,Instant originalDeadline,ChannelRetryPolicy originalPolicy,AlertIntent.Policy policy){
        if(policy.deployment()!=view.host().deployment())throw invalid("foreign-retention-profile");
        var identity=new AlertIntent.Business(key);var capability=new AlertChannelPolicy.Capability(d.channel(),d.channel().equals("cloud-sdk")?2:1,
                d.channel().equals("cloud-sdk")?AlertChannelPolicy.Family.CLOUD_SDK:AlertChannelPolicy.Family.OP_EXTERNAL);
        var retry=originalPolicy!=null?originalPolicy:d.channel().equals("email")?ChannelRetryPolicy.emailDefaults():d.target() instanceof AlertIntent.SdkTarget?new ChannelRetryPolicy(1,0,List.of(),Duration.ofDays(1),Duration.ofSeconds(30)):ChannelRetryPolicy.webhookDefaults();
        Instant deadline=originalDeadline==null?now.plus(retry.maximumAge()):originalDeadline;
        var template=new AlertIntent.Template("fixed-alert",1,d.settingsRevision(),"en",d.messages());byte[] bytes=null;
        boolean recoveryMessage=key.notification().kind()==DeliveryKey.Kind.RECOVERY;
        if(d.channel().equals("cloud-sdk")){
            var member=members.getFirst();var ep=episode(view,member).value();var alarm=member.evaluation().key();
            var destination=new io.tapstate.spi.alerting.NotificationChannel.Destination(alarm.scope(),d.channel(),d.receiver(),d.targetId(),((AlertIntent.SdkTarget)d.target()).clusterBinding());
            bytes=CloudAlertEventWire.encode(cloudOrigin,destination,key,alarm,recoveryMessage?ep.recoveredAt():ep.firedAt(),ep.severity(),alarm.typeId(),recoveryMessage&&recovery!=DeliveryEvidence.RecoveryKind.RECOVERY,member.episode());
        }else{
            var items=members.stream().map(m->{var ep=episode(view,m).value();return new NotificationRenderer.Item(m.evaluation().key(),m.episode(),
                    recoveryMessage?NotificationRenderer.State.RESOLVED:NotificationRenderer.State.FIRING,ep.severity(),m.evaluation().key().typeId(),null,ep.firedAt(),null,ep.recoveredAt());}).toList();
            var kind=recoveryMessage?(recovery==DeliveryEvidence.RecoveryKind.RECOVERY?NotificationRenderer.Kind.RECOVERY:NotificationRenderer.Kind.RECOVERY_SUMMARY)
                    :key.notification().kind()==DeliveryKey.Kind.REMINDER?NotificationRenderer.Kind.REMINDER:NotificationRenderer.Kind.FAULT;
            var prior=new ArrayList<NotificationRenderer.Previous>();
            for(var row:view.schedules())if(row.value().batch()==null&&row.value().key().subjectId().equals(key.notification().subjectId())){
                var outcomes=new ArrayList<DeliveryEvidence.Outcome>();for(var summary:row.value().summaries())if(members.stream().anyMatch(m->m.episodeMember().equals(new FrozenGroupPolicy.Member(summary.alarm(),summary.episode()))))outcomes.add(summary.outcome());
                for(var ref:row.value().protectedIntents())if(ref.notification().kind()!=DeliveryKey.Kind.RECOVERY&&view.intents().get(ref).value().frozen().members().stream().anyMatch(m->members.stream().anyMatch(n->n.episodeMember().equals(m.episodeMember()))))outcomes.add(view.intents().get(ref).value().state().summary());
                if(!outcomes.isEmpty())prior.add(new NotificationRenderer.Previous("previous-summary",DeliveryEvidence.aggregate(outcomes)));
            }
            var rendered=renderer.render(new NotificationRenderer.Facts(kind,identity.deliveryId(),key.notification().round(),now,items,items.size(),null,prior,null,null),d.messages());
            if(rendered.successful()){
                if(d.target() instanceof AlertIntent.ReceiverTarget receiver&&receiver.transport() instanceof AlertReceiver.Email email){var mime=EmailMime.freeze(identity.deliveryId(),now,email.from(),d.targetId(),rendered.snapshot());if(mime.successful())bytes=mime.bytes();}
                else bytes=rendered.snapshot().webhook();
            }
        }
        var payload=bytes==null?null:new AlertIntent.Payload(bytes);
        var frozen=new AlertIntent.Frozen(identity,capability,d.route(),d.target(),new AlertIntent.Content(d.channel().equals("email")?"message/rfc822":"application/json",payload==null?null:payload.sha256(),template,payload==null),members,retry,policy,now,deadline);
        var retained=DeliveryRetention.begin(policy.value(),view.host().deployment()==AlertChannelPolicy.Deployment.CLOUD_MANAGED?DeliveryRetention.Kind.CLOUD_SDK:DeliveryRetention.Kind.OP_EXTERNAL,now,deadline);
        if(payload==null)retained=retained.stop(now,now);
        return new AlertIntent.Record(frozen,new AlertIntent.State(AlertOwnership.unclaimed(),0,payload==null,payload==null?AlertIntent.StopReason.RENDER_FAILED:null,
                payload==null?null:now,0,List.of(),DeliveryEvidence.Outcome.NOT_ATTEMPTED,AlertIntent.Retained.capture(retained),payload,null,null,null,null));
    }
    private static AlertStateRecord.Episode episode(AlertStore.Planning view,AlertIntent.Member m){var value=view.episodes().get(m.episodeMember());if(value==null)throw invalid("missing-episode-evidence");return value;}
    private static DeliveryEvidence.Previous evidence(AlertIntent.Record intent){return new DeliveryEvidence.Previous(intent.state().summary(),intent.frozen().content().renderingFailed(),intent.state().attempts().stream().filter(a->a.endedAt()==null).map(a->a.permit().deadline()).min(Instant::compareTo).orElse(null));}
    private static AlertIntent.Record changedIntent(AlertStore.Planning view,Map<DeliveryKey,AlertStore.IntentChange> edits,DeliveryKey key){return edits.containsKey(key)?edits.get(key).after():view.intents().get(key).value();}
    private static DeliveryEvidence.Outcome intentOutcome(Map<DeliveryKey,AlertStore.IntentChange> edits,AlertStore.Planning view,DeliveryKey key){return changedIntent(view,edits,key).state().summary();}
    private static void putSchedule(List<AlertStore.ScheduleChange> changes,AlertStore.ScheduleChange value){changes.removeIf(v->v.after().key().equals(value.after().key()));changes.add(value);}
    private static DeliveryKey delivery(AlertSchedule.Record root,DeliveryKey.Kind kind,long round,long batch){return new DeliveryKey(new DeliveryKey.NotificationRound(root.key().scope(),root.key().subjectId(),Long.toString(batch),root.key().routeId(),root.key().routeIncarnation(),kind,round),root.key().channelType(),root.key().receiverIncarnation(),root.key().targetId());}
    private AlertOwnership claim(AlertOwnership before,Instant now){if(before.activeAt(now)&&!before.ownerId().equals(worker))return null;return claimOrOriginal(before,now);}
    private AlertOwnership claimOrOriginal(AlertOwnership before,Instant now){if(before.activeAt(now)&&before.ownerId().equals(worker)&&before.leaseUntil().isAfter(now.plusSeconds(10)))return before;return new AlertOwnership(before.activeAt(now)?before.generation():Math.incrementExact(before.generation()),worker,before.activeAt(now)&&before.leaseUntil().isAfter(now.plusSeconds(30))?before.leaseUntil():now.plusSeconds(30));}
    private static boolean sameMembers(List<AlertIntent.Member> a,List<AlertIntent.Member> b){return a.stream().map(AlertIntent.Member::episodeMember).collect(java.util.stream.Collectors.toSet()).equals(b.stream().map(AlertIntent.Member::episodeMember).collect(java.util.stream.Collectors.toSet()));}
    private static AlertSchedule.Record copy(AlertSchedule.Record r,AlertOwnership owner,AlertGroupingClock clock,long sequence,List<AlertIntent.Member> members,List<AlertIntent.Member> pending,
            AlertSchedule.Disposition disposition,List<AlertSchedule.Previous> summaries,List<DeliveryKey> refs,Instant retired){return new AlertSchedule.Record(r.key(),r.route(),r.retention(),r.createdAt(),owner,r.grouping(),clock,sequence,pending,r.batch(),disposition,summaries,refs,retired,r.summariesHandedOff(),members);}
    private static void require(AlertStore.Outcome result){if(result==AlertStore.Outcome.CONFLICT)throw new TapstateException(AlertError.COMMAND_CONFLICT,Map.of(),null);}
    static String managedBinding(URI origin,AlarmKey.Scope scope,io.tapstate.spi.store.StorageSetupScope setup){
        return hash(List.of("managed-alert-v1",origin.normalize().toASCIIString(),scope.authorityId(),scope.clusterId(),setup.installationId(),setup.operationId(),Long.toString(setup.generation())));
    }
    private static String hash(List<String> fields){try{var digest=MessageDigest.getInstance("SHA-256");for(var field:fields){var b=field.getBytes(StandardCharsets.UTF_8);digest.update(ByteBuffer.allocate(4).putInt(b.length).array());digest.update(b);}return HexFormat.of().formatHex(digest.digest());}catch(NoSuchAlgorithmException bug){throw new IllegalStateException(bug);}}
    private static TapstateException isolated(){return new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-planning","field","isolated-records"),null);}
    private static TapstateException invalid(String reason){return new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","notificationPlanning","reason",reason),null);}
}
