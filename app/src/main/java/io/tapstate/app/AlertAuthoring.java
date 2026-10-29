package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import static io.tapstate.app.AlertHttpModel.*;

/** Request-only forms over the existing typed configuration contract. */
final class AlertAuthoring {
    private final AlertStore store;private final AlarmKey.Scope scope;private final AlertChannelPolicy.Deployment deployment;private final AlarmTypeCatalog catalog;private final Clock clock;
    AlertAuthoring(AlertStore store,AlarmKey.Scope scope,AlertChannelPolicy.Deployment deployment,AlarmTypeCatalog catalog,Clock clock){this.store=store;this.scope=scope;this.deployment=deployment;this.catalog=catalog;this.clock=clock;}
    AlertConfiguration.Definition save(Map<String,Object> input,AlertStore.Access access,boolean retire){
        fields(input,"kind","id","etag","form");var kind=value(AlertConfiguration.Kind.class,text(input,"kind"));external(kind);
        String id=text(input,"id");var before=store.definition(kind,id,access).orElse(null);long expected=number(input,"etag",0);
        if(expected!=(before==null?0:before.header().revision())||before!=null&&!before.header().active())throw stale("configuration-etag");
        var header=before==null?AlertConfiguration.Header.create(AlertConfiguration.Identity.create(kind,scope,id),deployment,clock.instant(),access.subject())
                :retire?before.header().retire(clock.instant(),access.subject()):before.header().next(clock.instant(),access.subject());
        if(retire&&before==null)throw stale("configuration-not-found");
        if(retire){
            if(before instanceof AlertReceiver receiver){var retired=receiver.retire(clock.instant(),access.subject());check(store.saveReceiver(receiver,retired,null,true,access));return retired;}
            AlertConfiguration.Definition retired;
            if(before instanceof AlertConfiguration.Rule rule)retired=new AlertConfiguration.Rule(header,rule.name(),rule.typeId(),rule.typeVersion(),rule.resources(),rule.parameters(),rule.severityOverride(),rule.enabled(),rule.metric());
            else if(before instanceof AlertConfiguration.Subscription route)retired=route.retire(clock.instant(),access.subject());
            else {var silence=(AlertConfiguration.Silence)before;retired=new AlertConfiguration.Silence(header,silence.reason(),silence.window());}
            check(store.saveDefinition(before,retired,access));return retired;
        }
        var form=object(input.get("form"));AlertConfiguration.Definition after;
        if(kind==AlertConfiguration.Kind.RECEIVER){
            var old=(AlertReceiver)before;var transport=transport(form,old);
            boolean retain=bool(form,"retainMaterial",false);if(retain&&(old==null||!old.transport().equals(transport)))throw invalid("retainMaterial");
            var credentials=needsPrivate(transport)?old!=null&&(retain||transport.equals(old.transport()))?old.credentials():AlertReceiver.CredentialReference.create(header.identity(),AlertReceiver.authenticationIdentity(transport)):null;
            var receiver=before==null?new AlertReceiver(header,text(form,"name"),bool(form,"enabled",true),1,transport,credentials)
                    :old.edit(text(form,"name"),bool(form,"enabled",true),transport,credentials,clock.instant(),access.subject());
            try(var material=material(form)){check(store.saveReceiver(old,receiver,material,retain,access));}return receiver;
        }
        if(kind==AlertConfiguration.Kind.RULE){
            var parameters=new LinkedHashMap<String,java.math.BigDecimal>();object(form.getOrDefault("parameters",Map.of())).forEach((k,v)->{try{if(v==null)throw invalid("parameters");parameters.put(k,new java.math.BigDecimal(v.toString()));}catch(NumberFormatException ex){throw invalid("parameters");}});
            after=new AlertConfiguration.Rule(header,text(form,"name"),text(form,"typeId"),integer(form,"typeVersion",1),resources(form),parameters,form.get("severity")==null||"DEFAULT".equals(form.get("severity"))?null:value(AlarmSeverity.class,text(form,"severity")),bool(form,"enabled",true),catalog.metric(text(form,"typeId"),parameters)).validated(catalog);
        }else if(kind==AlertConfiguration.Kind.SILENCE){
            var window=new AlertSilenceWindow(scope,bool(form,"allResources",true),selected(form),strings(form,"typeIds"),parseTime(form,"startsAt"),Duration.ofSeconds(number(form,"durationSeconds",3600)),form.get("periodSeconds")==null?null:Duration.ofSeconds(number(form,"periodSeconds",0)),form.get("endsAt")==null?null:parseTime(form,"endsAt"));
            after=new AlertConfiguration.Silence(header,text(form,"reason"),window);
        }else{
            var channels=new ArrayList<AlertConfiguration.RouteChannel>();for(var raw:list(form,"channels")){var c=object(raw);var refs=new ArrayList<AlertConfiguration.Identity>();for(var rid:strings(c,"receivers")){var receiver=store.definition(AlertConfiguration.Kind.RECEIVER,rid,access).filter(r->r.header().active()).orElseThrow(()->stale("receiver"));refs.add(receiver.header().identity());}
                channels.add(new AlertConfiguration.RouteChannel(text(c,"channelType"),refs,messages(object(c.getOrDefault("messages",Map.of())))));}
            var groups=new LinkedHashSet<AlertConfiguration.GroupBy>();for(var g:strings(form,"groupBy"))groups.add(value(AlertConfiguration.GroupBy.class,g));
            var grouping=new AlertGroupingClock.Policy(Duration.ofSeconds(number(form,"initialWaitSeconds",30)),Duration.ofSeconds(number(form,"changeIntervalSeconds",300)),Duration.ofSeconds(number(form,"repeatIntervalSeconds",14400)));
            var edges=new ArrayList<AlertInhibitionPolicy.Edge>();for(var e:list(form,"inhibition")){var edge=object(e);edges.add(new AlertInhibitionPolicy.Edge(text(edge,"rootType"),text(edge,"dependentType")));}
            var match=new AlertConfiguration.Match(resources(form),strings(form,"typeIds"),severities(form));
            after=before==null?new AlertConfiguration.Subscription(header,text(form,"name"),match,bool(form,"enabled",true),channels,groups,grouping,edges)
                    :((AlertConfiguration.Subscription)before).edit(text(form,"name"),match,bool(form,"enabled",true),channels,groups,grouping,edges,clock.instant(),access.subject());
        }
        check(store.saveDefinition(before,after,access));return after;
    }
    void external(AlertConfiguration.Kind kind){if(deployment!=AlertChannelPolicy.Deployment.ON_PREM&&(kind==AlertConfiguration.Kind.RECEIVER||kind==AlertConfiguration.Kind.SUBSCRIPTION))throw stale("managed-external-configuration");}
    static void check(AlertStore.Outcome outcome){if(outcome==AlertStore.Outcome.CONFLICT)throw stale("configuration-conflict");}
    static AlertConfiguration.Resources resources(Map<String,Object> m){return new AlertConfiguration.Resources(strings(m,"resourceKinds"),bool(m,"allResources",true),selected(m));}
    static Set<AlarmKey.Resource> selected(Map<String,Object> m){var r=new LinkedHashSet<AlarmKey.Resource>();for(var v:list(m,"resources"))r.add(resource(object(v)));return Set.copyOf(r);}
    static Set<AlarmSeverity> severities(Map<String,Object> m){var s=new LinkedHashSet<AlarmSeverity>();for(var v:strings(m,"severities"))s.add(value(AlarmSeverity.class,v));return Set.copyOf(s);}
    static Instant parseTime(Map<String,Object> m,String key){try{return Instant.parse(text(m,key));}catch(java.time.format.DateTimeParseException e){throw invalid(key);}}
    static boolean needsPrivate(AlertReceiver.Transport t){return t instanceof AlertReceiver.Webhook||t.authentication()!=AlertReceiver.Authentication.NONE||t.trust()==AlertReceiver.Trust.PRIVATE_CA;}
    static AlertReceiver.Transport transport(Map<String,Object> form,AlertReceiver old){
        var t=object(form.get("transport"));var auth=value(AlertReceiver.Authentication.class,text(t,"authentication"));var trust=value(AlertReceiver.Trust.class,text(t,"trust"));
        if(text(t,"channelType").equals("email"))return new AlertReceiver.Email(text(t,"host"),integer(t,"port",465),value(AlertReceiver.Encryption.class,text(t,"encryption")),auth,optional(t,"username"),text(t,"from"),new ArrayList<>(strings(t,"recipients")),trust);
        if(!text(t,"channelType").equals("webhook"))throw invalid("channelType");
        var endpoint=privateInput(form,"endpoint");if(endpoint==null&&old!=null&&old.transport() instanceof AlertReceiver.Webhook w){
            if(!w.authentication().equals(auth)||!Objects.equals(w.principal(),optional(t,"principal"))||!w.trust().equals(trust))throw invalid("endpoint-required-for-binding-change");return w;}
        try{return AlertReceiver.Webhook.fromEndpoint(URI.create(Objects.requireNonNull(endpoint)),auth,optional(t,"principal"),trust);}catch(IllegalArgumentException|NullPointerException ex){throw invalid("endpoint");}
    }
    static AlertStore.PrivateMaterial material(Map<String,Object> m){
        byte[] auth=null,ca=null;try{var secret=privateInput(m,"secret");if(secret!=null)auth=secret.getBytes(StandardCharsets.UTF_8);var pem=privateInput(m,"privateCa");if(pem!=null)ca=pem.getBytes(StandardCharsets.UTF_8);
            var endpoint=privateInput(m,"endpoint");return auth==null&&ca==null&&endpoint==null?null:new AlertStore.PrivateMaterial(auth,ca,endpoint==null?null:URI.create(endpoint));
        }catch(IllegalArgumentException ex){throw invalid("private-material");}finally{if(auth!=null)Arrays.fill(auth,(byte)0);if(ca!=null)Arrays.fill(ca,(byte)0);}
    }
    static Map<String,Object> project(AlertConfiguration.Definition d){
        var m=map("id",d.header().identity().id(),"etag",Long.toString(d.header().revision()),"kind",d.header().identity().kind().name(),"active",d.header().active());
        if(d instanceof AlertConfiguration.Rule r){m.putAll(map("name",r.name(),"typeId",r.typeId(),"typeVersion",r.typeVersion(),"parameters",r.parameters(),"severity",r.severityOverride()==null?"DEFAULT":r.severityOverride().name(),"enabled",r.enabled()));putResources(m,r.resources());}
        if(d instanceof AlertReceiver r){m.putAll(map("name",r.name(),"enabled",r.enabled(),"retainMaterial",r.credentials()!=null));var t=r.transport();var transport=map("channelType",t.channelType(),"authentication",t.authentication().name(),"trust",t.trust().name());
            if(t instanceof AlertReceiver.Email e)transport.putAll(map("host",e.host(),"port",e.port(),"encryption",e.encryption().name(),"username",e.username(),"from",e.from(),"recipients",e.recipients()));
            else transport.putAll(map("origin",((AlertReceiver.Webhook)t).origin().toString(),"principal",((AlertReceiver.Webhook)t).principal()));m.put("transport",transport);}
        if(d instanceof AlertConfiguration.Subscription r){m.putAll(map("name",r.name(),"enabled",r.enabled(),"typeIds",r.match().typeIds().stream().sorted().toList(),"severities",r.match().severities().stream().map(Enum::name).toList(),"groupBy",r.groupBy().stream().map(Enum::name).toList(),"initialWaitSeconds",r.grouping().initialWait().toSeconds(),"changeIntervalSeconds",r.grouping().changeInterval().toSeconds(),"repeatIntervalSeconds",r.grouping().repeatInterval().toSeconds(),"inhibition",r.inhibition().stream().map(e->map("rootType",e.rootType(),"dependentType",e.dependentType())).toList(),"channels",r.channels().stream().map(c->map("channelType",c.channelType(),"receivers",c.receivers().stream().map(AlertConfiguration.Identity::id).toList(),"messages",messages(c.messages()))).toList()));putResources(m,r.match().resources());}
        if(d instanceof AlertConfiguration.Silence s){var w=s.window();m.putAll(map("reason",s.reason(),"allResources",w.allResources(),"resources",w.resources().stream().map(AlertHttpModel::resource).toList(),"typeIds",w.typeIds().stream().sorted().toList(),"startsAt",instant(w.startsAt()),"durationSeconds",w.duration().toSeconds(),"periodSeconds",w.period()==null?null:w.period().toSeconds(),"endsAt",instant(w.endsAt())));}
        return m;
    }
    static void putResources(Map<String,Object> m,AlertConfiguration.Resources r){m.putAll(map("resourceKinds",r.kinds().stream().sorted().toList(),"allResources",r.all(),"resources",r.selected().stream().map(AlertHttpModel::resource).toList()));}
}
