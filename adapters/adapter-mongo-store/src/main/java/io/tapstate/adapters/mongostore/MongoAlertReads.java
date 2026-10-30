package io.tapstate.adapters.mongostore;

import com.mongodb.client.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertStore.*;
import java.time.Instant;
import java.util.*;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

/** Bounded keyset reads over declared indexes. Unknown formats remain visible as an isolation indication. */
final class MongoAlertReads {
    private final MongoDatabase db;private final AlarmKey.Scope scope;private final MongoAlertStateStore states;private final java.time.Clock clock;
    MongoAlertReads(MongoDatabase db,AlarmKey.Scope scope,MongoAlertStateStore states){this(db,scope,states,java.time.Clock.systemUTC());}
    MongoAlertReads(MongoDatabase db,AlarmKey.Scope scope,MongoAlertStateStore states,java.time.Clock clock){this.db=db;this.scope=scope;this.states=states;this.clock=clock;}
    private AlertStateRecord.Episode visibleEpisode(AlertStateRecord.Episode value){
        if(value.historyUntil()!=null&&!clock.instant().isBefore(value.historyUntil()))return null;
        if(value.diagnostics().status()==AlertStateRecord.DiagnosticStatus.CAPTURED&&value.diagnosticUntil()!=null&&!clock.instant().isBefore(value.diagnosticUntil()))
            return new AlertStateRecord.Episode(value.originalBinding(),value.value(),value.policy(),value.diagnostics().prune(value.diagnosticUntil()),value.summariesHandedOff());
        return value;
    }
    private AlertIntent.Record visibleDelivery(AlertIntent.Record value) {
        // Projection only: use the original control facts, never stop an eligible request in a read.
        if(value.state().retained().stoppedAt()!=null)return AlertMaintenancePolicy.prune(value,clock.instant());
        return value;
    }
    ActivePage active(ClientSession session,ActiveCursor after,int limit,ReadAccess access){
        return active(session,after,limit,access,null);
    }
    ActivePage active(ClientSession session,ActiveCursor after,int limit,ReadAccess access,AlarmFilter query){
        limit(limit);Document base=scopeFilter().append("formatVersion",new Document("$in",List.of(2,3,4))).append("status","FIRING");
        if(query!=null){
            if(query.severity()!=null)base=and(base,new Document("severityRank",rank(query.severity())));
            if(query.search()!=null&&!query.search().isBlank()){
                var pattern=java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(query.search()),java.util.regex.Pattern.CASE_INSENSITIVE);
                base=and(base,new Document("$or",List.of(new Document("_id.resourceId",pattern),new Document("_id.typeId",pattern))));
            }
            if(query.match()!=null){var m=query.match();var r=m.resources();base=and(base,new Document("_id.resourceKind",new Document("$in",r.kinds().stream().sorted().toList())));
                if(!r.all())base=and(base,new Document("$or",r.selected().stream().map(value->new Document("_id.resourceKind",value.kind()).append("_id.resourceId",value.id()).append("_id.resourceIncarnation",value.incarnation())).toList()));
                if(!m.typeIds().isEmpty())base=and(base,new Document("_id.typeId",new Document("$in",m.typeIds().stream().sorted().toList())));
                if(!m.severities().isEmpty())base=and(base,new Document("severityRank",new Document("$in",m.severities().stream().map(MongoAlertReads::rank).toList())));
            }
        }
        if(!access.allResources()){
            var resources=access.resources().stream().map(r->new Document("_id.resourceKind",r.kind()).append("_id.resourceId",r.id()).append("_id.resourceIncarnation",r.incarnation())).toList();
            base=and(base,resources.isEmpty()?new Document("_id",new Document("$exists",false)):new Document("$or",resources));
        }
        var heads=SystemCollections.ALERT_INSTANCES.on(db);long count=heads.countDocuments(session,base);
        Document filter=base;
        if(after!=null){if(!scope.equals(after.key().scope())||!access.permits(after.key()))throw unreadable("cursor-scope");
            filter=and(filter,lexAfter(List.of("severityRank","recentSecondOrder","recentNanoOrder","_id"),List.of(after.severityRank(),after.secondOrder(),after.nanoOrder(),MongoAlertStateCodec.key(after.key()))));}
        var raw=heads.find(session,filter).sort(new Document("severityRank",1).append("recentSecondOrder",1).append("recentNanoOrder",1).append("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();var values=new ArrayList<Stored<AlertStateRecord.Head>>();
        for(var row:raw){var key=MongoAlertStateCodec.readKey(row.get("_id",Document.class));var stored=states.loadEnvelope(session,key).orElseThrow(()->unreadable("missing-head"));values.add(new Stored<>(stored.revision(),stored.value()));}
        ActiveCursor next=null;if(more){var r=raw.getLast();next=new ActiveCursor(r.getInteger("severityRank"),r.getLong("recentSecondOrder"),r.getInteger("recentNanoOrder"),values.getLast().value().snapshot().rule().key());}
        access.requireActive().run();return new ActivePage(count,values,next,isolated(session,SystemCollections.ALERT_INSTANCES,Set.of(2,3,4),access));
    }
    HistoryPage history(ClientSession s,AlarmKey key,Long after,int limit){
        limit(limit);var alarm=MongoAlertStateCodec.key(key);var range=new Document(after==null?"$gte":"$gt",new Document("alarm",alarm).append("number",after==null?1L:after))
                .append("$lte",new Document("alarm",alarm).append("number",Long.MAX_VALUE));
        var raw=SystemCollections.ALERT_EPISODES.on(db).find(s,new Document("_id",range)).sort(new Document("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();boolean isolated=false;var values=new ArrayList<AlertStateRecord.Episode>();
        for(var row:raw){if(!Integer.valueOf(2).equals(row.get("formatVersion"))){isolated=true;continue;}var visible=visibleEpisode(MongoAlertStateEnvelope.readEpisode(key,row.get("_id",Document.class).getLong("number"),row));if(visible!=null)values.add(visible);}
        return new HistoryPage(values,more?raw.getLast().get("_id",Document.class).getLong("number"):null,isolated);
    }
    EndedPage ended(ClientSession s,EpisodeCursor after,int limit,ReadAccess access,AlertSnapshot.EndReason reason){
        limit(limit);Objects.requireNonNull(reason);if(after!=null&&(!scope.equals(after.key().scope())||!access.permits(after.key())))throw unreadable("history-cursor-scope");
        var filter=new Document("_id.alarm.authorityId",scope.authorityId()).append("_id.alarm.clusterId",scope.clusterId()).append("formatVersion",2).append("endReason",reason.name());
        if(!access.allResources()){var allowed=access.resources().stream().map(r->new Document("_id.alarm.resourceKind",r.kind()).append("_id.alarm.resourceId",r.id()).append("_id.alarm.resourceIncarnation",r.incarnation())).toList();filter=and(filter,allowed.isEmpty()?new Document("_id",new Document("$exists",false)):new Document("$or",allowed));}
        if(after!=null)filter=and(filter,lexAfter(List.of("endedSecondOrder","endedNanoOrder","_id"),List.of(after.secondOrder(),after.nanoOrder(),new Document("alarm",MongoAlertStateCodec.key(after.key())).append("number",after.number()))));
        var raw=SystemCollections.ALERT_EPISODES.on(db).find(s,filter).hintString("alert_query_ended").sort(new Document("endedSecondOrder",1).append("endedNanoOrder",1).append("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();var rows=new ArrayList<AlertStateRecord.Episode>();EpisodeCursor cursor=null;
        for(var doc:raw){var id=doc.get("_id",Document.class);var key=MongoAlertStateCodec.readKey(id.get("alarm",Document.class));cursor=new EpisodeCursor(key,id.getLong("number"),doc.getLong("endedSecondOrder"),doc.getInteger("endedNanoOrder"));var visible=visibleEpisode(MongoAlertStateEnvelope.readEpisode(key,cursor.number(),doc));if(visible!=null)rows.add(visible);}
        var unsupported=new Document("_id.alarm.authorityId",scope.authorityId()).append("_id.alarm.clusterId",scope.clusterId()).append("formatVersion",new Document("$ne",2));
        if(!access.allResources()){var allowed=access.resources().stream().map(r->new Document("_id.alarm.resourceKind",r.kind()).append("_id.alarm.resourceId",r.id()).append("_id.alarm.resourceIncarnation",r.incarnation())).toList();unsupported=and(unsupported,allowed.isEmpty()?new Document("_id",new Document("$exists",false)):new Document("$or",allowed));}
        boolean isolated=SystemCollections.ALERT_EPISODES.on(db).find(s,unsupported).projection(new Document("_id",1)).limit(1).first()!=null;
        return new EndedPage(rows,more?cursor:null,isolated);
    }
    DeliveryPage deliveries(ClientSession s,String after,int limit,String channel,DeliveryEvidence.Outcome outcome,ReadAccess access){
        limit(limit);var low=MongoAlertDeliveryCodec.scope(scope).append("purpose","business").append("deliveryId",after==null?new MinKey():after);
        var high=MongoAlertDeliveryCodec.scope(scope).append("purpose","business").append("deliveryId",new MaxKey());
        var raw=SystemCollections.ALERT_DELIVERY_INTENTS.on(db).find(s,new Document("_id",new Document(after==null?"$gte":"$gt",low).append("$lte",high)))
                .hintString("_id_").sort(new Document("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();boolean isolated=false;var rows=new ArrayList<Stored<AlertIntent.Record>>();
        for(var doc:raw){if(!MongoAlertEnvelopeSupport.intent(doc)){isolated=true;continue;}
            var value=MongoAlertDeliveryCodec.readAny(scope,doc);var r=visibleDelivery(value.value());if(!clock.instant().isBefore(r.state().retained().resultUntil()))continue;if(r.frozen().members().stream().anyMatch(m->!access.permits(m.evaluation().key())))continue;
            if((channel==null||channel.equals(r.frozen().channel().typeId()))&&(outcome==null||outcome==r.state().summary()))rows.add(new Stored<>(value.recordRevision(),r));}
        return new DeliveryPage(rows,more?raw.getLast().get("_id",Document.class).getString("deliveryId"):null,isolated);
    }
    Optional<Stored<AlertIntent.Record>> delivery(ClientSession s,String id,ReadAccess access){
        var raw=SystemCollections.ALERT_DELIVERY_INTENTS.on(db).find(s,new Document("_id",MongoAlertDeliveryCodec.scope(scope).append("purpose","business").append("deliveryId",id))).first();
        if(raw==null)return Optional.empty();var value=MongoAlertDeliveryCodec.readAny(scope,raw);if(!clock.instant().isBefore(value.value().state().retained().resultUntil()))return Optional.empty();if(value.value().frozen().members().stream().anyMatch(m->!access.permits(m.evaluation().key())))return Optional.empty();return Optional.of(new Stored<>(value.recordRevision(),visibleDelivery(value.value())));
    }
    TransitionPage transitions(ClientSession s,AlarmKey key,TransitionCursor after,int limit){
        limit(limit);var alarm=MongoAlertStateCodec.key(key);var low=new Document("alarm",alarm).append("recordRevision",after==null?1L:after.revision()).append("ordinal",after==null?0:after.ordinal());
        var high=new Document("alarm",alarm).append("recordRevision",Long.MAX_VALUE).append("ordinal",Integer.MAX_VALUE);
        var raw=SystemCollections.ALERT_TRANSITIONS.on(db).find(s,new Document("_id",new Document(after==null?"$gte":"$gt",low).append("$lte",high))).sort(new Document("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();boolean isolated=false;var values=new ArrayList<AlertStateRecord.Transition>();
        for(var row:raw){if(!Integer.valueOf(2).equals(row.get("formatVersion"))){isolated=true;continue;}var id=row.get("_id",Document.class);var value=MongoAlertStateEnvelope.readTransition(key,id.getLong("recordRevision"),id.getInteger("ordinal"),row);if(clock.instant().isBefore(value.cleanupAt()))values.add(value);}
        TransitionCursor next=null;if(more){var id=raw.getLast().get("_id",Document.class);next=new TransitionCursor(id.getLong("recordRevision"),id.getInteger("ordinal"));}
        return new TransitionPage(values,next,isolated);
    }
    DuePage due(ClientSession s,Instant through,DueCursor after,int limit){
        limit(limit);var filter=scopeFilter().append("formatVersion",new Document("$in",List.of(1,2,3))).append("nextCheckAt",new Document("$type","date").append("$lte",Date.from(through)));
        filter=and(filter,MongoAlertEnvelopeSupport.intentFilter());
        if(after!=null){var id=MongoAlertDeliveryCodec.scope(scope).append("purpose",after.purpose()).append("deliveryId",after.deliveryId());filter=and(filter,lexAfter(List.of("nextCheckAt","_id"),List.of(Date.from(after.candidateAt()),id)));}
        var raw=SystemCollections.ALERT_DELIVERY_INTENTS.on(db).find(s,filter).sort(new Document("nextCheckAt",1).append("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();var values=new ArrayList<Stored<AlertIntent.Record>>();
        for(var row:raw){try{var v=MongoAlertDeliveryCodec.readAny(scope,row);values.add(new Stored<>(v.recordRevision(),v.value()));}catch(IllegalArgumentException ex){throw unreadable("intent-format");}}
        DueCursor next=null;if(more){var r=raw.getLast();var id=r.get("_id",Document.class);next=new DueCursor(r.getDate("nextCheckAt").toInstant(),id.getString("purpose"),id.getString("deliveryId"));}
        return new DuePage(values,next,isolated(s,SystemCollections.ALERT_DELIVERY_INTENTS,Set.of(1,2,3),null));
    }
    SchedulePage schedules(ClientSession s,Instant through,ScheduleCursor after,int limit){
        limit(limit);var filter=scopeFilter().append("formatVersion",1).append("nextCheckAt",new Document("$type","date").append("$lte",Date.from(through)));
        if(after!=null){if(!scope.equals(after.key().scope()))throw unreadable("schedule-cursor-scope");filter=and(filter,lexAfter(List.of("nextCheckAt","_id"),List.of(Date.from(after.candidateAt()),MongoAlertScheduleCodec.key(after.key()))));}
        var raw=SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(db).find(s,filter).sort(new Document("nextCheckAt",1).append("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();var values=new ArrayList<Stored<AlertSchedule.Record>>();
        for(var row:raw){var v=MongoAlertScheduleCodec.read(scheduleKey(row.get("_id",Document.class)),row);values.add(new Stored<>(v.recordRevision(),v.value()));}
        ScheduleCursor next=more?new ScheduleCursor(raw.getLast().getDate("nextCheckAt").toInstant(),values.getLast().value().key()):null;
        return new SchedulePage(values,next,isolated(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,Set.of(1),null));
    }
    DefinitionPage definitions(ClientSession s,AlertConfiguration.Kind kind,String after,int limit){
        limit(limit);var low=MongoAlertDeliveryCodec.scope(scope).append("id",after==null?new MinKey():after);var high=MongoAlertDeliveryCodec.scope(scope).append("id",new MaxKey());
        var versions=kind==AlertConfiguration.Kind.SILENCE?List.of(1):List.of(1,2);
        int maximum=kind==AlertConfiguration.Kind.RECEIVER?MongoAlertConfigurationCodec.MAX_RECEIVER_BYTES:MongoAlertConfigurationCodec.MAX_BYTES;
        var known=new Document("$and",List.of(new Document("$in",List.of("$formatVersion",versions)),new Document("$lte",List.of(new Document("$bsonSize","$$ROOT"),maximum))));
        var projection=new Document("_id",1).append("formatVersion",1).append("kind",1)
                .append("record",new Document("$cond",Arrays.asList(known,"$$ROOT",null)));
        var raw=collection(kind).on(db).aggregate(s,List.of(new Document("$match",new Document("_id",new Document(after==null?"$gte":"$gt",low).append("$lte",high))),
                new Document("$sort",new Document("_id",1)),new Document("$limit",limit+1),new Document("$project",projection))).into(new ArrayList<>());
        boolean more=raw.size()>limit;if(more)raw.removeLast();var values=new ArrayList<AlertConfiguration.Definition>();var unavailable=new HashMap<String,RuleCapability>();
        for(var header:raw){
            if(!(header.get("_id") instanceof Document key)||!(key.get("id") instanceof String id))throw unreadable("definition-identity");
            var future=kind==AlertConfiguration.Kind.RULE?MongoAlertConfigurationCodec.futureRuleFormat(scope,id,header):Optional.<Integer>empty();
            if(future.isPresent()){unavailable.put(id,new RuleCapability(id,null,io.tapstate.core.alerting.AlarmTypeCatalog.Availability.UNSUPPORTED_RECORD_VERSION,null,null,future.orElseThrow()));continue;}
            if(!(header.get("record") instanceof Document row))throw unreadable("definition-format-or-budget");
            values.add(MongoAlertConfigurationCodec.read(kind,scope,id,row).definition());
        }
        return new DefinitionPage(values,more?raw.getLast().get("_id",Document.class).getString("id"):null,unavailable);
    }
    EvaluationPage evaluationStates(ClientSession s,AlarmKey after,int limit){
        limit(limit);if(after!=null&&!scope.equals(after.scope()))throw unreadable("evaluation-cursor-scope");
        var lower=after==null?MongoAlertDeliveryCodec.scope(scope).append("resourceKind",new MinKey()):MongoAlertStateCodec.key(after);
        var upper=MongoAlertDeliveryCodec.scope(scope).append("resourceKind",new MaxKey());
        var rows=SystemCollections.ALERT_INSTANCES.on(db).find(s,new Document("_id",new Document(after==null?"$gte":"$gt",lower).append("$lte",upper)))
                .hintString("_id_").sort(new Document("_id",1)).limit(limit+1).into(new ArrayList<>());
        boolean more=rows.size()>limit;if(more)rows.removeLast();var values=new ArrayList<Stored<AlertStateRecord.Head>>();
        for(var row:rows){var key=MongoAlertStateCodec.readKey(row.get("_id",Document.class));
            var value=states.loadEnvelope(s,key).orElseThrow(()->unreadable("evaluation-head"));values.add(new Stored<>(value.revision(),value.value()));}
        return new EvaluationPage(values,more?values.getLast().value().snapshot().rule().key():null);
    }
    List<AlertIntent.Record> testWindow(ClientSession s,Instant now){
        var filter=scopeFilter().append("_id.purpose","channel-test").append("quotaMinute",new Document("$gte",AlertTestQuota.minute(now.minusSeconds(300)).toString()));
        var raw=SystemCollections.ALERT_DELIVERY_INTENTS.on(db).find(s,filter).hintString("alert_query_test_window").sort(new Document("quotaMinute",1)).limit(61).into(new ArrayList<>());
        if(raw.size()>60)throw unreadable("test-window-budget-or-clock");var values=new ArrayList<AlertIntent.Record>();
        for(var row:raw){var v=MongoAlertDeliveryCodec.readAny(scope,row).value();if(v.frozen().createdAt().isAfter(now))throw unreadable("test-clock-moved-backwards");values.add(v);}return List.copyOf(values);
    }
    List<AlertIntent.Record> intentSnapshot(ClientSession s,int maximum){
        var lower=MongoAlertDeliveryCodec.scope(scope).append("purpose",new MinKey());var upper=MongoAlertDeliveryCodec.scope(scope).append("purpose",new MaxKey());
        var raw=SystemCollections.ALERT_DELIVERY_INTENTS.on(db).find(s,new Document("_id",new Document("$gte",lower).append("$lte",upper)))
                .sort(new Document("_id",1)).limit(maximum+1).into(new ArrayList<>());
        if(raw.size()>maximum)throw unreadable("target-proof-read-budget");var values=new ArrayList<AlertIntent.Record>();
        for(var row:raw){try{values.add(MongoAlertDeliveryCodec.readAny(scope,row).value());}catch(IllegalArgumentException invalid){throw unreadable("target-proof-isolated");}}return List.copyOf(values);
    }
    List<AlertStateRecord.Head> activeSnapshot(ClientSession s,int maximum){
        if(isolated(s,SystemCollections.ALERT_INSTANCES,Set.of(2,3,4),null))throw unreadable("inhibition-state-isolated");
        var raw=SystemCollections.ALERT_INSTANCES.on(db).find(s,scopeFilter().append("formatVersion",new Document("$in",List.of(2,3,4))).append("status","FIRING"))
                .hintString("alert_query_active").limit(maximum+1).into(new ArrayList<>());
        if(raw.size()>maximum)throw unreadable("inhibition-read-budget");var values=new ArrayList<AlertStateRecord.Head>();
        for(var row:raw)values.add(states.loadEnvelope(s,MongoAlertStateCodec.readKey(row.get("_id",Document.class))).orElseThrow(()->unreadable("inhibition-head")).value());return List.copyOf(values);
    }
    private boolean isolated(ClientSession s,SystemCollections c,Set<Integer> supported,ReadAccess access){
        var filter=c==SystemCollections.ALERT_DELIVERY_INTENTS?and(scopeFilter(),new Document("$nor",List.of(MongoAlertEnvelopeSupport.intentFilter()))):scopeFilter().append("formatVersion",new Document("$nin",supported.stream().sorted().toList()));
        if(access!=null&&!access.allResources()){
            var resources=access.resources().stream().map(r->new Document("_id.resourceKind",r.kind()).append("_id.resourceId",r.id()).append("_id.resourceIncarnation",r.incarnation())).toList();
            filter=and(filter,resources.isEmpty()?new Document("_id",new Document("$exists",false)):new Document("$or",resources));
        }
        return c.on(db).find(s,filter).projection(new Document("_id",1)).limit(1).first()!=null;
    }
    private Document scopeFilter(){return new Document("_id.authorityId",scope.authorityId()).append("_id.clusterId",scope.clusterId());}
    private static Document and(Document a,Document b){return new Document("$and",List.of(a,b));}
    private static Document lexAfter(List<String> names,List<?> values){var options=new ArrayList<Document>();for(int i=0;i<names.size();i++){var term=new Document();for(int j=0;j<i;j++)term.put(names.get(j),values.get(j));term.put(names.get(i),new Document("$gt",values.get(i)));options.add(term);}return new Document("$or",options);}
    private static void limit(int limit){if(limit<1||limit>100)throw new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","pageSize","reason","outside-1-100"),null);}
    private static int rank(AlarmSeverity value){return switch(value){case CRITICAL->0;case MAJOR->1;case MINOR->2;case WARNING->3;case INDETERMINATE->4;};}
    private static SystemCollections collection(AlertConfiguration.Kind kind){return switch(kind){case RULE->SystemCollections.ALERT_RULES;case SUBSCRIPTION->SystemCollections.ALERT_SUBSCRIPTIONS;case SILENCE->SystemCollections.ALERT_SILENCES;case RECEIVER->SystemCollections.ALERT_RECEIVERS;};}
    static AlertSchedule.Key scheduleKey(Document id){return new AlertSchedule.Key(MongoAlertDeliveryCodec.readScope(id),AlertSchedule.SubjectKind.valueOf(id.getString("subjectKind")),id.getString("subjectId"),id.getString("subjectIncarnation"),id.getString("routeId"),id.getString("routeIncarnation"),id.getString("channelType"),id.getString("receiverIncarnation"),id.getString("targetId"));}
    private static TapstateException unreadable(String field){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-query","field",field),null);}
}
