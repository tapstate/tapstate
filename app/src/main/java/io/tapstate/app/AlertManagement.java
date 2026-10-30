package io.tapstate.app;

import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.control.core.*;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;
import static io.tapstate.app.AlertHttpModel.*;

/** Scoped management over the admitted store. It never starts an adapter or changes an alarm on a read. */
final class AlertManagement implements AlertControlDispatcher {
    private final java.util.function.Supplier<PipelineAlertRuntime.Services> services;
    private final java.util.function.Function<CredentialAuthentication,AlertStore.Access> access;
    private final java.util.function.Supplier<PipelineAlertRuntime.Health> health;
    private final AlarmKey.Scope scope;private final AlertChannelPolicy.Deployment deployment;private final Clock clock;private final AuditGate audit;
    AlertManagement(java.util.function.Supplier<PipelineAlertRuntime.Services> services,java.util.function.Function<CredentialAuthentication,AlertStore.Access> access,
            java.util.function.Supplier<PipelineAlertRuntime.Health> health,AlarmKey.Scope scope,AlertChannelPolicy.Deployment deployment,Clock clock,AuditGate audit){
        this.services=services;this.access=access;this.health=health;this.scope=scope;this.deployment=deployment;this.clock=clock;this.audit=audit;
    }
    @Override public Map<String,Object> dispatch(String operation,CredentialAuthentication caller,Map<String,Object> input){
        var op=ControlOperations.registry().resolve(operation);
        if(!operation.startsWith("alert.")||!caller.token().scope().permits(op.scope())
                ||deployment==AlertChannelPolicy.Deployment.ON_PREM&&op.scope()==Scope.WRITE&&!caller.token().scope().permits(Scope.ADMIN))throw stale("permission");
        caller.requireActive().run();var resolved=services.get();var a=access.apply(caller);
        var result=audit.dispatch(op,new AuditContext(caller.token().subject(),"alerts",optional(input,"etag")),()->invoke(operation,input,resolved,a,caller));
        caller.requireActive().run();a.locallyActive().run();return result;
    }
    private Map<String,Object> invoke(String op,Map<String,Object> input,PipelineAlertRuntime.Services resolved,AlertStore.Access access,CredentialAuthentication caller){
        var store=resolved.store();var reads=new AlertStore.ReadAccess(true,Set.of(),access.locallyActive());
        var capabilityTypes=new DeliveryTypes(store,reads);
        var author=new AlertAuthoring(store,scope,deployment,resolved.catalog(),clock,resolved.channels());var commands=new AlertDeliveryCommands(store,scope,deployment,clock,resolved.evaluator().retentionPolicy());
        int limit=integer(input,"limit",25);if(limit<1||limit>100)throw invalid("limit");
        switch(op){
            case "alert.catalog": {
                fields(input);return map("deployment",deployment.name(),"clusterId",scope.clusterId(),"refreshedAt",instant(clock.instant()),
                        "channelPolicies",deployment==AlertChannelPolicy.Deployment.ON_PREM?channelPolicies(resolved.channels()):Map.of(),
                        "channels",deployment==AlertChannelPolicy.Deployment.ON_PREM?resolved.channels().descriptors().stream().map(AlertManagement::channelDescriptor).toList():List.of(),
                        "types",resolved.catalog().types().stream().map(t->type(t,resolved.providers(),resolved.descriptions())).toList());
            }
            case "alert.resources": {
                fields(input,"kind","cursor","limit");var p=store.resources(text(input,"kind"),optional(input,"cursor"),limit,access);return map("rows",p.rows().stream().map(AlertHttpModel::resource).toList(),"nextCursor",p.nextId());
            }
            case "alert.center": {
                fields(input,"view","cursor","limit","severity","search","endReason");String view=optional(input,"view");String token=optional(input,"cursor");String severity=optional(input,"severity"), search=optional(input,"search");var query=new AlertStore.AlarmFilter(severity==null?null:value(AlarmSeverity.class,severity),search,null);var reason=input.get("endReason")==null?AlertSnapshot.EndReason.RECOVERED:value(AlertSnapshot.EndReason.class,text(input,"endReason"));String queryContext=JsonWriter.write(java.util.Arrays.asList(severity,search,reason.name()));
                if(view==null||view.equals("active")){
                    AlertStore.ActiveCursor cursor=null;if(token!=null){var c=cursor(token,"active:"+queryContext);cursor=new AlertStore.ActiveCursor(integer(c,"rank",-1),number(c,"second",0),integer(c,"nano",0),key(text(c,"key"),scope));}
                    var page=store.activeFiltered(cursor,limit,reads,query);var next=page.next();return map("rows",page.rows().stream().map(r->head(r.value())).toList(),"total",page.total(),"isolated",page.isolatedRecords(),"nextCursor",next==null?null:cursorToken("active:"+queryContext,map("rank",next.severityRank(),"second",next.secondOrder(),"nano",next.nanoOrder(),"key",key(next.key()))),"refreshedAt",instant(clock.instant()));
                }
                if(!view.equals("history"))throw invalid("view");AlertStore.EpisodeCursor cursor=null;if(token!=null){var c=cursor(token,"history:"+queryContext);cursor=new AlertStore.EpisodeCursor(key(text(c,"key"),scope),number(c,"episode",0),number(c,"second",0),integer(c,"nano",0));}
                var p=store.ended(cursor,limit,reads,reason);return map("rows",p.rows().stream().filter(e->(query.severity()==null||e.value().severity()==query.severity())&&(search==null||e.originalBinding().key().resource().id().toLowerCase(Locale.ROOT).contains(search.toLowerCase(Locale.ROOT))||e.originalBinding().key().typeId().toLowerCase(Locale.ROOT).contains(search.toLowerCase(Locale.ROOT)))).map(AlertHttpModel::episode).toList(),"isolated",p.isolatedRecords(),"nextCursor",p.next()==null?null:cursorToken("history:"+queryContext,map("key",key(p.next().key()),"episode",p.next().number(),"second",p.next().secondOrder(),"nano",p.next().nanoOrder())),"refreshedAt",instant(clock.instant()));
            }
            case "alert.detail": {
                fields(input,"key","episode");var key=key(text(input,"key"),scope);long number=number(input,"episode",0);
                var retained=store.state(key,reads);var h=retained.orElse(null);
                long selected=number==0&&h!=null&&h.value().episode()!=null?h.value().episode().value().number():number;
                AlertStateRecord.Episode e=selected==0?null:store.history(key,selected-1,1,reads).rows().stream().filter(v->v.value().number()==selected).findFirst().orElseThrow(()->stale("episode-not-found"));
                if(e==null)throw stale("alarm-not-found");
                boolean current=e.value().endedAt()==null&&h!=null&&e.equals(h.value().episode());
                var m=current?head(h.value()):episode(e);m.put("diagnostics",diagnostics(e.diagnostics()));
                var rule=current?h.value().snapshot().rule():e.originalBinding();
                var observed=current?h.value().snapshot().lastObservedAt():e.diagnostics().latest()==null?null:e.diagnostics().latest().observedAt();
                var descriptor=resolved.catalog().types().stream().filter(t->t.id().equals(key.typeId())).findFirst();
                descriptor.ifPresent(t->{var description=resolved.descriptions().describe(t);m.put("condition",description.condition());m.put("recovery",description.recovery());m.put("guidance",description.guidance());});
                m.put("thresholds",store.originalRule(rule,reads).map(AlertConfiguration.Rule::parameters).orElse(null));
                m.put("durationSeconds",Duration.between(e.value().firedAt(),e.value().endedAt()==null?clock.instant():e.value().endedAt()).toSeconds());
                m.put("basis",map("typeId",key.typeId(),"quality",current?h.value().snapshot().quality().name():"HISTORICAL", "observedAt",instant(observed),"triggerForSeconds",rule.triggerFor().toSeconds(),"recoverForSeconds",rule.resolveFor().toSeconds()));return m;
            }
            case "alert.deliveries": {
                fields(input,"cursor","limit","channel","result","id","key","episode");var id=optional(input,"id");if(id!=null)return projectDelivery(store.delivery(id,reads).orElseThrow(()->stale("delivery-not-found")),true,resolved,capabilityTypes);
                var channel=optional(input,"channel");if(channel!=null&&!"cloud-sdk".equals(channel))resolved.channels().require(channel);
                if(deployment==AlertChannelPolicy.Deployment.CLOUD_MANAGED&&channel!=null&&!channel.equals("cloud-sdk"))throw stale("managed-channel");
                var outcome=optional(input,"result");if(outcome!=null&&!Set.of("NOT_ATTEMPTED","NOT_ACCEPTED","UNKNOWN","ACCEPTED","WAITING_RETRY","QUEUED","IN_FLIGHT","PAUSED","CANCELLED","EXHAUSTED","EXPIRED").contains(outcome))throw invalid("result");var alarm=input.get("key")==null?null:key(text(input,"key"),scope);long episode=number(input,"episode",0);String filter=JsonWriter.write(java.util.Arrays.asList(channel,outcome,input.get("key"),episode));String token=optional(input,"cursor");String after=token==null?null:text(cursor(token,"deliveries:"+filter),"after");
                var p=store.deliveries(after,limit,channel,null,reads);var rows=p.rows().stream().filter(r->alarm==null||r.value().frozen().members().stream().anyMatch(m->m.evaluation().key().equals(alarm)&&(episode==0||m.episode()==episode))).map(r->projectDelivery(r,false,resolved,capabilityTypes)).filter(r->outcome==null||outcome.equals(r.get("outcome"))||outcome.equals(r.get("displayState"))).toList();
                return map("rows",rows,"isolated",p.isolatedRecords(),"nextCursor",p.nextId()==null?null:cursorToken("deliveries:"+filter,map("after",p.nextId())));
            }
            case "alert.health": {
                fields(input);var h=health.get();return map("deployment",deployment.name(),"degraded",h.degraded(),"closed",h.closed(),"rejected",h.rejected(),"code",h.code(),"parameters",h.parameters(),"queuedInputs",h.queuedInputs(),"coverage",resolved.catalog().types().stream().filter(t->resolved.providers().contains(t.providerId())).map(AlarmTypeCatalog.Type::id).toList(),"queuedBytes",h.queuedBytes(),"evaluationLagMillis",h.evaluationLagMillis(),"lastEvaluatedAt",instant(h.lastEvaluatedAt()),"maintenance",h.maintenance(),"cleanupAvailable",Boolean.TRUE.equals(h.maintenance().get("available")),"refreshedAt",instant(clock.instant()));
            }
            case "alert.configuration.list": {
                fields(input,"kind","cursor","limit");var kind=value(AlertConfiguration.Kind.class,text(input,"kind"));author.external(kind);var p=store.definitions(kind,optional(input,"cursor"),limit,access);return map("rows",p.rows().stream().filter(d->d.header().active()).map(AlertAuthoring::project).toList(),"nextCursor",p.nextId());
            }
            case "alert.configuration.save", "alert.configuration.retire":return AlertAuthoring.project(author.save(input,access,op.endsWith("retire")));
            case "alert.rule-preview":return AlertRulePreview.evaluate(input,scope,resolved.catalog(),deployment,caller.token().subject(),clock.instant());
            case "alert.route-preview": {
                requireOp();fields(input,"form","cursor","limit");var form=object(input.get("form"));var match=new AlertConfiguration.Match(AlertAuthoring.resources(form),strings(form,"typeIds"),AlertAuthoring.severities(form));
                var contextFields=map("types",match.typeIds().stream().sorted().toList(),"severities",match.severities().stream().map(Enum::name).sorted().toList(),"kinds",match.resources().kinds().stream().sorted().toList(),"all",match.resources().all(),"resources",match.resources().selected().stream().sorted(Comparator.comparing(AlarmKey.Resource::kind).thenComparing(AlarmKey.Resource::id).thenComparing(AlarmKey.Resource::incarnation)).map(AlertHttpModel::resource).toList());String context="route:"+JsonWriter.write(contextFields);AlertStore.ActiveCursor cursor=null;if(input.get("cursor")!=null){var c=cursor(text(input,"cursor"),context);cursor=new AlertStore.ActiveCursor(integer(c,"rank",-1),number(c,"second",0),integer(c,"nano",0),key(text(c,"key"),scope));}
                var p=store.activeFiltered(cursor,limit,reads,new AlertStore.AlarmFilter(null,null,match));var snapshot=store.planning();var now=clock.instant();var edges=new ArrayList<AlertInhibitionPolicy.Edge>();for(var raw:list(form,"inhibition")){var edge=object(raw);edges.add(new AlertInhibitionPolicy.Edge(text(edge,"rootType"),text(edge,"dependentType")));}var policy=new AlertInhibitionPolicy(scope,edges);
                var rows=p.rows().stream().map(r->{var row=head(r.value());var key=r.value().snapshot().rule().key();boolean muted=snapshot.definitions().stream().filter(AlertConfiguration.Silence.class::isInstance).map(AlertConfiguration.Silence.class::cast).anyMatch(s->s.matches(key,now));boolean inhibited=snapshot.states().stream().map(v->v.value().snapshot()).anyMatch(root->policy.suppresses(root,r.value().snapshot(),snapshot.dependencies().contains(new AlertStore.Dependency(root.rule().key(),key))));row.put("silenced",muted);row.put("inhibited",inhibited);return row;}).toList();var n=p.next();
                return map("rows",rows,"total",p.total(),"nextCursor",n==null?null:cursorToken(context,map("rank",n.severityRank(),"second",n.secondOrder(),"nano",n.nanoOrder(),"key",key(n.key()))),"isolated",p.isolatedRecords(),"previewOnly",true,"refreshedAt",instant(now));
            }
            case "alert.preview":resolved.channels().require(text(input,"channel"));return preview(input);
            case "alert.test": {
                fields(input,"commandId","candidateId","form","existingId","etag","target","routeId");requireOp();var form=object(input.get("form"));AlertReceiver existing=null;
                if(input.get("existingId")!=null){existing=(AlertReceiver)store.definition(AlertConfiguration.Kind.RECEIVER,text(input,"existingId"),access).orElseThrow(()->stale("receiver-not-found"));if(existing.header().revision()!=number(input,"etag",0))throw stale("receiver-etag");}
                var transport=AlertAuthoring.transport(form,existing,resolved.channels());var retain=new LinkedHashSet<AlertStore.PrivateField>();if(bool(form,"retainMaterial",false)&&existing!=null){if(transport.authentication()!=AlertReceiver.Authentication.NONE)retain.add(AlertStore.PrivateField.AUTHENTICATION);if(transport.trust()==AlertReceiver.Trust.PRIVATE_CA)retain.add(AlertStore.PrivateField.PRIVATE_CA);if(!(transport instanceof AlertReceiver.Email)&&privateInput(form,"endpoint")==null)retain.add(AlertStore.PrivateField.ENDPOINT);}
                AlertConfiguration.Subscription route=null;if(input.get("routeId")!=null)route=(AlertConfiguration.Subscription)store.definition(AlertConfiguration.Kind.SUBSCRIPTION,text(input,"routeId"),access).orElseThrow(()->stale("route-not-found"));
                try(var material=AlertAuthoring.material(form)){return projectDelivery(commands.test(AlertTestCommand.ClientId.parse(text(input,"commandId")),text(input,"candidateId"),transport,transport instanceof AlertReceiver.Email?emailTarget(text(input,"target")):AlertReceiver.httpsTargetId(transport),existing,material,retain,route,access),false,resolved,capabilityTypes);}
            }
            case "alert.test-result": {fields(input,"commandId");requireOp();return projectDelivery(commands.result(new AlertTestCommand.Key(scope,access.actor().principalKey(),AlertTestCommand.ClientId.parse(text(input,"commandId"))),access),false,resolved,capabilityTypes);}
            case "alert.retry": {fields(input,"id","etag");requireOp();var current=store.delivery(text(input,"id"),reads).orElseThrow(()->stale("delivery-not-found"));AlertAuthoring.check(commands.retry(current.value().frozen().identity(),number(input,"etag",0),access));return map("queued",true);}
            default:throw new IllegalStateException("unregistered alert operation");
        }
    }
    private static String emailTarget(String value){int at=value.lastIndexOf('@');return at<0?value:value.substring(0,at+1)+value.substring(at+1).toLowerCase(Locale.ROOT);}
    private Map<String,Object> preview(Map<String,Object> input){
        fields(input,"messages","kind","channel");requireOp();var kind=value(NotificationRenderer.Kind.class,text(input,"kind"));var channel=text(input,"channel");var now=clock.instant();
        var key=new AlarmKey(scope,new AlarmKey.Resource("pipeline","preview","preview"),"pipeline.failed","preview");boolean recovered=kind==NotificationRenderer.Kind.RECOVERY||kind==NotificationRenderer.Kind.RECOVERY_SUMMARY;
        var item=new NotificationRenderer.Item(key,1L,recovered?NotificationRenderer.State.RESOLVED:NotificationRenderer.State.FIRING,AlarmSeverity.MAJOR,"Example Pipeline","Example failure evidence",now,now,recovered?now:null);
        var result=new NotificationRenderer().render(new NotificationRenderer.Facts(kind,"preview",kind==NotificationRenderer.Kind.CHANNEL_TEST?0:1,now,kind==NotificationRenderer.Kind.CHANNEL_TEST?List.of():List.of(item),kind==NotificationRenderer.Kind.CHANNEL_TEST?0:1,null,List.of(),kind==NotificationRenderer.Kind.CHANNEL_TEST?"preview":null,kind==NotificationRenderer.Kind.CHANNEL_TEST?channel:null),messages(object(input.get("messages"))));
        if(!result.successful())throw invalid("preview-budget");var s=result.snapshot();return map("example",true,"subject",s.subject(),"text",s.text(),"html",s.html(),"webhook",new String(s.webhook(),java.nio.charset.StandardCharsets.UTF_8));
    }
    private Map<String,Object> projectDelivery(AlertStore.Stored<AlertIntent.Record> stored,boolean detail,PipelineAlertRuntime.Services services,
            DeliveryTypes types){
        var row=delivery(stored,detail);if(!stored.value().state().permitsStopped()&&stored.value().state().summary()!=DeliveryEvidence.Outcome.ACCEPTED
                &&(services.delivery()==null||!services.delivery().supportsForManagement(stored.value().frozen())))row.put("displayState","PAUSED");
        var capability=stored.value().state().summary()==DeliveryEvidence.Outcome.ACCEPTED?null:types.unavailable(stored.value().frozen());
        if(capability!=null){row.put("capability",capability);if(!stored.value().state().permitsStopped())row.put("displayState","PAUSED");}
        boolean retry=false;if(deployment==AlertChannelPolicy.Deployment.ON_PREM&&stored.value().frozen().identity() instanceof AlertIntent.Business
                &&capability==null&&services.delivery()!=null&&services.delivery().supportsForManagement(stored.value().frozen())){
            try{AlertDeliveryProgress.manual(stored.value(),clock.instant());retry=true;}catch(io.tapstate.core.common.TapstateException refused){}}
        row.put("canRetry",retry);return row;
    }
    private static final class DeliveryTypes {
        private final AlertStore store;private final AlertStore.ReadAccess reads;
        DeliveryTypes(AlertStore store,AlertStore.ReadAccess reads){this.store=store;this.reads=reads;}
        Map<String,Object> unavailable(AlertIntent.Frozen frozen){
            return store.unavailableRule(frozen,reads).map(issue->map("reason",issue.reason().name(),"typeId",issue.typeId(),
                    "typeVersion",issue.typeVersion(),"availableVersion",issue.availableVersion(),"recordVersion",issue.recordVersion())).orElse(null);
        }
    }
    private static Map<String,Object> channelPolicies(AlertChannelCatalog catalog){var policies=new LinkedHashMap<String,Object>();for(var descriptor:catalog.descriptors())policies.put(descriptor.capability().typeId(),policy(descriptor.transportFamily()==AlertChannelCatalog.TransportFamily.EMAIL?ChannelRetryPolicy.emailDefaults():ChannelRetryPolicy.webhookDefaults()));return policies;}
    private static Map<String,Object> channelDescriptor(AlertChannelCatalog.Descriptor d){return map("id",d.capability().typeId(),"version",d.capability().version(),"label",d.label(),"transportFamily",d.transportFamily().name(),"transportVersion",d.transportVersion(),"fields",d.fields().stream().map(f->map("key",f.key(),"label",f.label(),"maximumBytes",f.maximumBytes(),"required",f.required(),"defaultValue",f.defaultValue())).toList());}
    private static Map<String,Object> policy(ChannelRetryPolicy p){return map("timeoutSeconds",p.attemptTimeout().toSeconds(),"attempts",p.attemptsPerCycle(),"maximumAgeSeconds",p.maximumAge().toSeconds(),"retryDelaysSeconds",p.retryDelays().stream().map(Duration::toSeconds).toList());}
    private static Map<String,Object> type(AlarmTypeCatalog.Type t,Set<String> providers,io.tapstate.adapters.alerting.AlarmTypeText descriptions){var d=descriptions.describe(t);
        return map("id",t.id(),"version",t.version(),"kind",t.kind().name(),"resourceKinds",t.resourceKinds().stream().sorted().toList(),"severity",t.defaultSeverity().name(),"enabledByDefault",t.enabledByDefault(),"condition",d.condition(),"recovery",d.recovery(),"guidance",d.guidance(),"inputAvailable",providers.contains(t.providerId()),"parameters",t.parameters().stream().map(p->map("name",p.name(),"kind",p.kind().name(),"unit",p.unit(),"minimum",p.minimum(),"maximum",p.maximum(),"defaultValue",p.defaultValue())).toList());
    }
    private String cursorToken(String view,Map<String,Object> payload){payload.put("view",view);payload.put("authorityId",scope.authorityId());payload.put("clusterId",scope.clusterId());return encode(payload);}
    private Map<String,Object> cursor(String token,String view){var m=decode(token);if(!view.equals(m.get("view"))||!scope.authorityId().equals(m.get("authorityId"))||!scope.clusterId().equals(m.get("clusterId")))throw invalid("cursor-context");return m;}
    private void requireOp(){if(deployment!=AlertChannelPolicy.Deployment.ON_PREM)throw stale("managed-external-operation");}
}
