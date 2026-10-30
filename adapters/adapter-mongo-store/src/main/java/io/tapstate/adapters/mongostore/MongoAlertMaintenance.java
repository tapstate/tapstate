package io.tapstate.adapters.mongostore;

import com.mongodb.client.*;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.function.*;
import org.bson.Document;
import org.bson.conversions.Bson;

/** Indexed candidate discovery with process-local progress; mutations return to the original fragment owners. */
final class MongoAlertMaintenance implements AlertMaintenance {
    // Legal rows are at most 1 MiB. Leave room for authority, discovery and mutation
    // accounting in a fresh 4 MiB batch before fetching an opaque body.
    private static final long MAX_FETCH_BYTES=2L*1024*1024;
    static final List<SystemCollections> RESOURCES = List.of(SystemCollections.ALERT_DELIVERY_INTENTS,
            SystemCollections.ALERT_NOTIFICATION_SCHEDULES, SystemCollections.ALERT_EPISODES,
            SystemCollections.ALERT_TRANSITIONS, SystemCollections.ALERT_INSTANCES, SystemCollections.ALERT_RULES,
            SystemCollections.ALERT_RECEIVERS, SystemCollections.ALERT_SUBSCRIPTIONS, SystemCollections.ALERT_SILENCES,
            SystemCollections.ALERT_TARGET_LIMITS, SystemCollections.ALERT_TEST_QUOTAS, SystemCollections.ALERT_NODE_LIFECYCLES);
    interface Transactions { void verify(Duration remaining); <T> T run(boolean write, Duration remaining, Function<ClientSession,T> action); }
    interface Mutation { boolean apply(ClientSession session, SystemCollections collection, Document original, Document replacement); }
    private final Function<SystemCollections,MongoCollection<Document>> handles;
    private final AlarmKey.Scope scope;
    private final Clock clock;
    private final Transactions transactions;
    private final Mutation mutation;
    private final Consumer<Inventory> observed;
    private final Runnable capacityProtection;
    private final java.util.function.LongSupplier pendingAdmissions;
    private final Map<String,Long> writes=new TreeMap<>();
    private long ownerConflicts;
    synchronized void writeAttempt(String resource,int records){writes.merge(resource,(long)records,Long::sum);}
    synchronized void conflict(){ownerConflicts++;}
    @Override public Map<String,Long> diagnostics(){Map<String,Long> values;
        synchronized(this){values=new TreeMap<>(writes);values.put("ownerConflicts",ownerConflicts);}
        values.put("admissionReservedBytes",pendingAdmissions.getAsLong());return Map.copyOf(values);
    }
    @Override public void enableCapacityProtection(){capacityProtection.run();}
    private final BiPredicate<ClientSession,AlertNodeLifecycleStore.Row> nodeExpected;
    MongoAlertMaintenance(Function<SystemCollections,MongoCollection<Document>> handles, AlarmKey.Scope scope, Clock clock,
            Transactions transactions, Mutation mutation, Consumer<Inventory> observed, java.util.function.LongSupplier pendingAdmissions, Runnable capacityProtection, BiPredicate<ClientSession,AlertNodeLifecycleStore.Row> nodeExpected) {
        this.handles=handles;this.scope=scope;this.clock=clock;this.transactions=transactions;
        this.mutation=mutation;this.observed=observed;this.capacityProtection=capacityProtection;this.pendingAdmissions=pendingAdmissions;this.nodeExpected=nodeExpected;
    }
    // Only immutable, already-ended episode proofs are reusable. A different exact schedule discards them.
    // At most sixteen original schedules and two hundred SHA-256 identities per schedule are retained.
    private final Map<String,Map<String,Instant>> endedProofs=new LinkedHashMap<>() {
        @Override protected boolean removeEldestEntry(Map.Entry<String,Map<String,Instant>> entry){return size()>16;}
    };
    // Structural verification is reusable only until this original bounded round deadline.
    // Every transaction still revalidates the live host, feature version, key and CONFIG fence.
    private long schemaVerifiedUntil;
    private final Object[] pendingCandidate=new Object[12];
    private static final class Yield extends RuntimeException { }
    private static final int DISCOVERY_KEYS=8;
    private static final class Budget {
        final int rows; final long bytes, until; int used; long encoded;int proofSteps;boolean isolationChecked,isolated;
        Instant currentCandidate,oldestEligible,oldestProtected;
        void eligible(Instant at){if(at!=null&&(oldestEligible==null||at.isBefore(oldestEligible)))oldestEligible=at;}
        void protect(Instant at){if(at!=null&&(oldestProtected==null||at.isBefore(oldestProtected)))oldestProtected=at;}
        Budget(int rows,long bytes,Duration time) { this.rows=rows;this.bytes=bytes;this.until=System.nanoTime()+time.toNanos(); }
        void require(int operations,long size) { if (operations>rows-used || size>bytes-encoded || System.nanoTime()>=until) throw new Yield(); }
        void use(long size) { use(1,size); }
        void use(int operations,long size) { require(operations,size); used+=operations;encoded+=size; }
        Duration remaining(){long left=until-System.nanoTime();if(left<=0)throw new Yield();return Duration.ofNanos(left);}
        boolean available() { return used<rows && encoded<bytes && System.nanoTime()<until; }
    }
    @Override public Batch maintain(Cursor after,int maxRows,long maxBytes,Duration maxTime) {
        if(maxRows<1||maxRows>200||maxBytes<1||maxBytes>4L*1024*1024||maxTime.isNegative()||maxTime.isZero()||maxTime.compareTo(Duration.ofSeconds(10))>0)
            throw new IllegalArgumentException("outside maintenance batch budget");
        int index=after==null?0:after.collection();var collection=RESOURCES.get(index);var b=new Budget(maxRows,maxBytes,maxTime);
        Cursor cursor=after==null?new Cursor(index,null,null):after;int changed=0,conflicts=0;var skipped=new LinkedHashMap<String,Long>();Instant oldest=null;boolean exhausted=false;
        try { if(System.nanoTime()>=schemaVerifiedUntil){b.use(64,64*1024);transactions.verify(b.remaining());schemaVerifiedUntil=b.until;} }
        catch(Yield end){return new Batch(cursor,b.used,0,b.encoded,Map.of("budget",1L),null);}
        while(b.available()) {
            Cursor pageAfter=cursor;List<Document> candidates;
            try { b.use(18,32*1024);candidates=transactions.run(false,b.remaining(),s->next(s,collection,pageAfter,b)); }
            catch(Yield end){break;}
            if(candidates.isEmpty()){exhausted=true;break;}
            boolean yielded=false,retry=false;
            for(Document candidate:candidates){
            Cursor original=cursor;
            Instant candidateAt=candidate.get("cleanupAt") instanceof Date date?date.toInstant():null;
            b.currentCandidate=candidateAt;
            String encodedCursor=candidate.containsKey("cursor")?candidate.getString("cursor"):new Document("id",candidate.get("_id")).toJson();
            if(encodedCursor.length()>8192)throw new io.tapstate.core.common.TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-maintenance","field","candidate-cursor"),null);
            Cursor scanned=new Cursor(index,candidateAt,encodedCursor);
            int proofBefore=b.proofSteps;boolean resumingCandidate=Objects.equals(pendingCandidate[index],candidate.get("_id"));
            try {
                // Reserve before entering authority validation. Every attempted transaction is billed once,
                // including commit/abort allowance; maintenance does not run hidden callback retries.
                b.use(22,32*1024);
                String result=transactions.run(true,b.remaining(),s->{
                    Document row=read(s,collection,new Document("_id",candidate.get("_id")),b);
                    if(row==null)return "conflict";
                    return clean(s,collection,row,b);
                });
                if(result.equals("changed"))changed++;else {skipped.merge(result,1L,Long::sum);
                    if(result.equals("conflict"))b.eligible(b.currentCandidate);else b.protect(b.currentCandidate);}
                pendingCandidate[index]=null;cursor=scanned;
            }catch(Yield end){skipped.merge("budget",1L,Long::sum);
                // Budget exhaustion has not classified or committed this candidate. Retrying its
                // original key with a fresh batch must precede advancing to later candidates.
                b.eligible(b.currentCandidate);pendingCandidate[index]=candidate.get("_id");cursor=original;yielded=true;break;}
            catch(MongoReadBudget.MaintenanceConflict conflict){
                conflict();skipped.merge("conflict",1L,Long::sum);
                if(++conflicts>3)throw new io.tapstate.core.common.TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","maintenance-contention"),null);
                if(resumingCandidate||b.proofSteps>proofBefore)pendingCandidate[index]=candidate.get("_id");cursor=original;retry=true;break;
            }
            catch(IllegalArgumentException | ClassCastException | NullPointerException invalid) {
                // A malformed or unsupported record is retained, and its scanned key still advances.
                skipped.merge("unreadable",1L,Long::sum);b.protect(b.currentCandidate);pendingCandidate[index]=null;cursor=scanned;
            }catch(io.tapstate.core.common.TapstateException invalid){
                if(invalid.code()!=IoError.DOCUMENT_UNREADABLE)throw invalid;skipped.merge("unreadable",1L,Long::sum);b.protect(b.currentCandidate);pendingCandidate[index]=null;cursor=scanned;
            }
            }
            if(yielded)break;
            if(retry)continue;
            if(candidates.size()<DISCOVERY_KEYS){exhausted=true;break;}
        }
        if(b.isolated)skipped.merge("unreadable",1L,Long::sum);
        return new Batch(exhausted?null:cursor,b.used,changed,b.encoded,skipped,b.oldestEligible,b.oldestProtected);
    }
    private List<Document> next(ClientSession s,SystemCollections c,Cursor after,Budget b) {
        Document header=candidateHeader(c);
        if(!b.isolationChecked){b.use(4096);b.isolated=handles.apply(c).aggregate(s,List.of(new Document("$match",and(scope(c),new Document("$nor",List.of(header)))),
                new Document("$limit",1),new Document("$project",new Document("_id",0).append("present",new Document("$literal",true))))).first()!=null;b.isolationChecked=true;}
        Document filter=and(scope(c),header);Document sort;
        if(c==SystemCollections.ALERT_NODE_LIFECYCLES) {
            // The reviewed node namespace is capped at 512 rows and has no cleanup timestamp.
            filter.append("formatVersion",1).append("phase",new Document("$in",List.of("EXPECTED","RETIRING"))).append("nodeId",new Document("$type","string")).append("bootId",new Document("$type","string"));
            sort=new Document("phase",1).append("nodeId",1).append("bootId",1);
            if(after!=null&&after.id()!=null){var prior=Document.parse(after.id()).get("id");Document row=null;
                // Use a stable compound cursor even if the previously scanned row was deleted.
                var encoded=Document.parse(after.id());if(encoded.containsKey("phase"))filter=and(filter,lex(sort.keySet().stream().toList(),List.of(encoded.get("phase"),encoded.get("nodeId"),encoded.get("bootId"))));
                else if(row!=null)filter=and(filter,lex(sort.keySet().stream().toList(),List.of(row.get("phase"),row.get("nodeId"),row.get("bootId"))));}
        } else {
            sort=new Document("cleanupAt",1).append("_id",1);
            boolean definitions=MongoAlertConfigurationStore.COLLECTIONS.contains(c);
            filter.append("cleanupAt",definitions?null:new Document("$type","date").append("$lte",Date.from(clock.instant())));
            if(Set.of(SystemCollections.ALERT_RULES,SystemCollections.ALERT_RECEIVERS,SystemCollections.ALERT_SUBSCRIPTIONS).contains(c))
                filter.append("retiredAt",new Document("$ne",null));
            if(after!=null&&after.id()!=null){Object id=Document.parse(after.id()).get("id");
                filter=and(filter,definitions?new Document("_id",new Document("$gt",id)):
                        lex(List.of("cleanupAt","_id"),List.of(Date.from(after.candidateAt()),id)));}
        }
        // Fetch only eight bounded projected keys under one discovery authority check.
        // The header caps the BSON key at 16 KiB; 24 KiB also covers projected node fields.
        // One find obtains the bounded page. Reserve every projected byte before IO;
        // every processed or skipped key is additionally billed by its guarded row transaction.
        int pageSize=DISCOVERY_KEYS;b.use(1,pageSize*24L*1024);
        Document projection=new Document("_id",1);
        if(c==SystemCollections.ALERT_NODE_LIFECYCLES)projection.append("phase",1).append("nodeId",1).append("bootId",1);
        else projection.append("cleanupAt",1);
        var query=handles.apply(c).find(s,filter).hintString(c==SystemCollections.ALERT_NODE_LIFECYCLES?"alert_query_node_lifecycle":"alert_cleanup")
                .sort(sort).projection(projection).batchSize(pageSize).limit(pageSize);
        List<Document> results=query.into(new ArrayList<>());
        if(c==SystemCollections.ALERT_NODE_LIFECYCLES)for(Document result:results){
            // Copy the key, never an arbitrary original node object, into the process-local cursor.
            result.put("cursor",new Document("id",result.get("_id")).append("phase",result.get("phase")).append("nodeId",result.get("nodeId")).append("bootId",result.get("bootId")).toJson());
        }
        return results;
    }
    private static Document candidateHeader(SystemCollections collection) {
        var tests=new ArrayList<Object>();
        var versions=collection==SystemCollections.ALERT_INSTANCES?List.of(2,3,4):collection==SystemCollections.ALERT_DELIVERY_INTENTS?List.of(1,2,3):collection==SystemCollections.ALERT_RULES||collection==SystemCollections.ALERT_SUBSCRIPTIONS||collection==SystemCollections.ALERT_RECEIVERS?List.of(1,2):
                collection==SystemCollections.ALERT_EPISODES||collection==SystemCollections.ALERT_TRANSITIONS?List.of(2):List.of(1);
        tests.add(new Document("$lte",List.of(new Document("$bsonSize",new Document("key","$_id")),16*1024)));
        if(collection!=SystemCollections.ALERT_NODE_LIFECYCLES){
            var type=new Document("$type","$cleanupAt");
            // Protected live records legitimately have no cleanup timestamp. Eligibility
            // still requires a date below; arrays never enter either projected key shape.
            tests.add(new Document("$in",List.of(type,List.of("missing","null","date"))));
        }
        if(collection==SystemCollections.ALERT_NODE_LIFECYCLES){
            tests.add(textKey("$_id",64));tests.add(new Document("$cond",List.of(new Document("$eq",List.of(new Document("$type","$_id"),"string")),new Document("$regexMatch",new Document("input","$_id").append("regex","^[a-f0-9]{64}$")),false)));
            tests.add(textKey("$nodeId",256));tests.add(textKey("$bootId",256));tests.add(new Document("$in",List.of("$phase",List.of("EXPECTED","RETIRING"))));
        }else if(collection==SystemCollections.ALERT_EPISODES||collection==SystemCollections.ALERT_TRANSITIONS){
            var names=collection==SystemCollections.ALERT_EPISODES?List.of("alarm","number"):List.of("alarm","recordRevision","ordinal");tests.add(objectKeys("$_id",names));
            tests.add(objectKeys("$_id.alarm",List.of("authorityId","clusterId","resourceKind","resourceId","resourceIncarnation","typeId","qualifier")));
            for(String field:List.of("authorityId","clusterId","resourceKind","resourceId","resourceIncarnation","typeId","qualifier"))tests.add(textKey("$_id.alarm."+field,256));
            for(String field:names)if(!field.equals("alarm"))tests.add(new Document("$in",List.of(new Document("$type","$_id."+field),List.of("int","long"))));
        }else{
            List<String> fields=switch(collection){
                case ALERT_INSTANCES -> List.of("authorityId","clusterId","resourceKind","resourceId","resourceIncarnation","typeId","qualifier");
                case ALERT_NOTIFICATION_SCHEDULES -> List.of("authorityId","clusterId","subjectKind","subjectId","subjectIncarnation","routeId","routeIncarnation","channelType","receiverIncarnation","targetId");
                case ALERT_DELIVERY_INTENTS -> List.of("authorityId","clusterId","purpose","deliveryId");
                case ALERT_TARGET_LIMITS -> List.of("authorityId","clusterId","channelType","semanticTargetId","authenticationIdentity");
                case ALERT_TEST_QUOTAS -> List.of("authorityId","clusterId","minute");
                default -> List.of("authorityId","clusterId","id");
            };
            tests.add(objectKeys("$_id",fields));for(String field:fields)tests.add(textKey("$_id."+field,256));
        }
        var header=new Document("formatVersion",new Document("$in",versions)).append("$expr",new Document("$and",tests));
        if(collection==SystemCollections.ALERT_DELIVERY_INTENTS)return and(header,MongoAlertEnvelopeSupport.intentFilter());
        if(collection==SystemCollections.ALERT_RECEIVERS)return and(header,MongoAlertEnvelopeSupport.receiverFilter());
        return header;
    }
    private static Document objectKeys(String path,List<String> fields) {
        return new Document("$cond",List.of(new Document("$eq",List.of(new Document("$type",path),"object")),
                new Document("$setEquals",List.of(new Document("$map",new Document("input",new Document("$objectToArray",path)).append("as","item").append("in","$$item.k")),fields)),false));
    }
    private static Document textKey(String path,int maximum) {
        return new Document("$cond",List.of(new Document("$eq",List.of(new Document("$type",path),"string")),
                new Document("$and",List.of(new Document("$lte",List.of(new Document("$strLenCP",path),maximum)),
                        new Document("$not",List.of(new Document("$regexMatch",new Document("input",path).append("regex","[\\p{Cc}]")))))),false));
    }
    private String clean(ClientSession s,SystemCollections c,Document row,Budget b) {
        Instant now=clock.instant();
        if(c==SystemCollections.ALERT_DELIVERY_INTENTS) {
            var stored=MongoAlertDeliveryCodec.readAny(scope,row);var original=stored.value();
            var value=AlertMaintenancePolicy.prune(original,now);
            if(!value.equals(original))return replace(s,c,row,MongoAlertDeliveryCodec.write(value,Math.incrementExact(stored.recordRevision())),b);
            if(value.state().retained().stoppedAt()==null)return "unsettled";
            if(now.isBefore(value.state().retained().resultUntil()))return "retention";
            if(unknown(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,b))return "unknown-reference";
            if(value.frozen().identity() instanceof AlertIntent.Business business) {
                var key=business.key();
                var references=and(scope(SystemCollections.ALERT_NOTIFICATION_SCHEDULES),new Document("protectedIntents",MongoAlertDeliveryCodec.deliveryKey(key)));
                Document schedule=read(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,references,b);
                if(schedule!=null) {
                    var old=MongoAlertScheduleCodec.read(MongoAlertReads.scheduleKey(schedule.get("_id",Document.class)),schedule);
                    if(old.value().batch()!=null && old.value().batch().delivery().equals(key))return "original-batch";
                    var updated=handoff(old.value(),value);
                    return replace(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,schedule,MongoAlertScheduleCodec.write(updated,Math.incrementExact(old.recordRevision())),b);
                }
                // A frozen batch/disposition can still refer to an intent after its protected list changes.
                if(exists(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,new Document("$or",List.of(
                        new Document("batch.delivery",MongoAlertDeliveryCodec.deliveryKey(key)),new Document("disposition.original",MongoAlertDeliveryCodec.deliveryKey(key)),
                        new Document("disposition.successor",MongoAlertDeliveryCodec.deliveryKey(key)))),b))return "batch-reference";
            }
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_NOTIFICATION_SCHEDULES) {
            var stored=MongoAlertScheduleCodec.read(MongoAlertReads.scheduleKey(row.get("_id",Document.class)),row);var v=stored.value();
            if(v.retiredCleanupAt()==null||now.isBefore(v.retiredCleanupAt())||v.hasOpenWork()||v.owner().activeAt(now))return "schedule-work";
            String proofKey=AlertIntent.hash(row.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            var proofs=endedProofs.computeIfAbsent(proofKey,ignored->new HashMap<>());
            for(var key:v.protectedIntents()) {
                String original="intent:"+new AlertIntent.Business(key).deliveryId();var until=proofs.get(original);if(until!=null&&!now.isBefore(until))continue;
                Document intent=read(s,SystemCollections.ALERT_DELIVERY_INTENTS,new Document("_id",MongoAlertDeliveryCodec.key(new AlertIntent.Business(key))),b);
                if(intent==null)return "missing-reference";
                var value=MongoAlertDeliveryCodec.readAny(scope,intent).value();
                if(value.state().retained().stoppedAt()==null||now.isBefore(value.state().retained().resultUntil()))return "delivery-retention";
                if(proofs.size()<200&&!Objects.equals(proofs.put(original,value.state().retained().resultUntil()),value.state().retained().resultUntil()))b.proofSteps++;
            }
            // Closed permission/result windows and ended episode facts cannot legitimately reopen.
            for(var member:v.members())if(episodeReferenced(s,member.evaluation().key(),member.episode(),now,b,proofs))return "episode-retention";
            for(var summary:v.summaries())if(episodeReferenced(s,summary.alarm(),summary.episode(),now,b,proofs))return "summary-retention";
            if(unknown(s,SystemCollections.ALERT_DELIVERY_INTENTS,b))return "unknown-reference";
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_EPISODES) {
            var id=row.get("_id",Document.class);var key=MongoAlertStateCodec.readKey(id.get("alarm",Document.class));long number=id.getLong("number");
            var episode=MongoAlertStateEnvelope.readEpisode(key,number,row);
            if(episode.value().endedAt()==null)return "active";
            if(episode.diagnostics().status()==AlertStateRecord.DiagnosticStatus.CAPTURED&&episode.diagnosticUntil()!=null&&!now.isBefore(episode.diagnosticUntil())) {
                var pruned=new AlertStateRecord.Episode(episode.originalBinding(),episode.value(),episode.policy(),episode.diagnostics().prune(now),episode.summariesHandedOff());
                return replace(s,c,row,MongoAlertStateEnvelope.episode(pruned),b);
            }
            if(now.isBefore(episode.historyUntil()))return "retention";
            if(memberReferences(s,key,number,b))return "member-reference";
            Document head=read(s,SystemCollections.ALERT_INSTANCES,new Document("_id",MongoAlertStateCodec.key(key)),b);
            if(head!=null&&Objects.equals(head.get("episodeNumber"),number)) {
                var h=MongoAlertStateEnvelope.read(key,head,row);var a=h.value();var x=a.snapshot();
                if(x.status()==AlertSnapshot.Status.FIRING)return "active";
                var snapshot=new AlertSnapshot(x.rule(),x.enabled(),x.status(),x.quality(),x.lastSequence(),x.lastObservedAt(),x.pendingSince(),x.recoverySince(),x.episodeSequence(),null);
                var next=new AlertStateRecord.Head(snapshot,a.deployment(),a.owner(),a.retiredAt(),a.summariesHandedOff(),null,a.binding(),a.operationalEvidence());
                if(replace(s,SystemCollections.ALERT_INSTANCES,head,MongoAlertStateEnvelope.head(next,Math.incrementExact(h.revision())),b).equals("conflict"))return "conflict";
            }
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_TRANSITIONS) {
            var id=row.get("_id",Document.class);var key=MongoAlertStateCodec.readKey(id.get("alarm",Document.class));
            var value=MongoAlertStateEnvelope.readTransition(key,id.getLong("recordRevision"),id.getInteger("ordinal"),row);
            if(now.isBefore(value.cleanupAt()))return "retention";
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_INSTANCES) {
            var key=MongoAlertStateCodec.readKey(row.get("_id",Document.class));Document episode=null;
            if(row.get("episodeNumber")!=null)episode=read(s,SystemCollections.ALERT_EPISODES,new Document("_id",new Document("alarm",row.get("_id")).append("number",row.get("episodeNumber"))),b);
            var h=MongoAlertStateEnvelope.read(key,row,episode).value();
            if(h.cleanupAt()==null||now.isBefore(h.cleanupAt())||h.owner().activeAt(now)||h.snapshot().status()==AlertSnapshot.Status.FIRING)return "active";
            if(exists(s,SystemCollections.ALERT_EPISODES,new Document("_id.alarm",row.get("_id")),b)
                    ||exists(s,SystemCollections.ALERT_TRANSITIONS,new Document("_id.alarm",row.get("_id")),b)||memberReferences(s,key,null,b))return "history-reference";
            return remove(s,c,row,b);
        }
        if(MongoAlertConfigurationStore.COLLECTIONS.contains(c)) {
            var kind=c==SystemCollections.ALERT_RULES?AlertConfiguration.Kind.RULE:c==SystemCollections.ALERT_RECEIVERS?AlertConfiguration.Kind.RECEIVER:
                    c==SystemCollections.ALERT_SUBSCRIPTIONS?AlertConfiguration.Kind.SUBSCRIPTION:AlertConfiguration.Kind.SILENCE;
            var v=MongoAlertConfigurationCodec.read(kind,scope,row.get("_id",Document.class).getString("id"),row).definition();
            Instant retired=v.header().retiredAt();
            if(retired==null && v instanceof AlertConfiguration.Silence silence) {
                var window=silence.window();retired=window.endsAt();
                if(window.period()==null){Instant ended=window.startsAt().plus(window.duration());if(retired==null||ended.isBefore(retired))retired=ended;}
            }
            if(retired==null)return "current-definition";
            Instant cutoff=retired.plus(v.header().deployment()==AlertChannelPolicy.Deployment.ON_PREM?Duration.ofDays(7):Duration.ofDays(1));b.currentCandidate=cutoff;
            if(now.isBefore(cutoff))return "retention";
            var identity=MongoAlertConfigurationCodec.identity(v.header().identity());
            // Keep any retained binding, not only currently active bindings.
            if(kind==AlertConfiguration.Kind.RULE && (exists(s,SystemCollections.ALERT_INSTANCES,new Document("binding.ruleIdentity",identity),b)
                    ||exists(s,SystemCollections.ALERT_DELIVERY_INTENTS,new Document("frozen.members.ruleIdentity",identity),b)
                    ||exists(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,new Document("members.ruleIdentity",identity),b)))return "definition-reference";
            if(kind==AlertConfiguration.Kind.SUBSCRIPTION && (exists(s,SystemCollections.ALERT_DELIVERY_INTENTS,new Document("frozen.route.identity",identity),b)
                    ||exists(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,new Document("route.identity",identity),b)))return "definition-reference";
            if(kind==AlertConfiguration.Kind.RECEIVER && (exists(s,SystemCollections.ALERT_DELIVERY_INTENTS,new Document("$or",List.of(new Document("frozen.target.identity",identity),new Document("frozen.target.existingReceiver",identity))),b)
                    ||exists(s,SystemCollections.ALERT_SUBSCRIPTIONS,new Document("body.channels.receivers",identity),b)))return "definition-reference";
            for(var related:List.of(SystemCollections.ALERT_INSTANCES,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,SystemCollections.ALERT_DELIVERY_INTENTS))
                if(unknown(s,related,b))return "unknown-reference";
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_TEST_QUOTAS) {
            var minute=Instant.parse(row.get("_id",Document.class).getString("minute"));var value=MongoAlertNotificationCodec.readQuota(scope,minute,row).value();
            if(now.isBefore(value.cleanupAfter()))return "current-window";
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_TARGET_LIMITS) {
            var id=row.get("_id",Document.class);var d=new AlertTargetLimit.Domain(scope,id.getString("channelType"),id.getString("semanticTargetId"),id.getString("authenticationIdentity"));
            var value=MongoAlertNotificationCodec.readLimit(d,row).value();if(now.isBefore(value.notBefore().plusSeconds(3600)))return "not-before";
            if(unknown(s,SystemCollections.ALERT_DELIVERY_INTENTS,b))return "unknown-reference";
            if(targetReferenced(s,row,d,b))return "target-reference";
            return remove(s,c,row,b);
        }
        if(c==SystemCollections.ALERT_NODE_LIFECYCLES) {
            var value=MongoAlertNodeLifecycleStore.decode(row);b.use(16,32*1024);if(nodeExpected.test(s,value))return "expected-node";
            var r=value.registration().key().resource();
            if(resourceReferences(s,r,b))return "node-reference";
            b.use(16,32*1024);String removed=remove(s,c,row,b);if(nodeExpected.test(s,value))throw new IllegalStateException("node monitoring changed during cleanup");return removed;
        }
        return "unsupported";
    }
    private final Map<String,TargetScan> targetScans=new LinkedHashMap<>() {
        @Override protected boolean removeEldestEntry(Map.Entry<String,TargetScan> entry){return size()>16;}
    };
    private static final class TargetScan { Document after; boolean complete; }
    private boolean targetReferenced(ClientSession session,Document original,AlertTargetLimit.Domain domain,Budget budget) {
        // Every admission/permit for this exact semantic domain bumps the original limit CAS revision.
        // An unrelated destination does not invalidate these immutable negative target proofs.
        String key=AlertIntent.hash(original.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var scan=targetScans.computeIfAbsent(key,ignored->new TargetScan());
        if(scan.complete)return false;
        while(true) {
            budget.require(1,68*1024);
            var filter=scope(SystemCollections.ALERT_DELIVERY_INTENTS).append("channelType",domain.channelType());
            if(scan.after!=null)filter.append("_id",new Document("$gt",scan.after));
            var result=handles.apply(SystemCollections.ALERT_DELIVERY_INTENTS).aggregate(session,List.of(
                    new Document("$match",filter),new Document("$sort",new Document("_id",1)),new Document("$limit",1),
                    new Document("$project",new Document("_id",1).append("formatVersion",1).append("channelType",1).append("frozen.identity",1).append("frozen.target",1)),
                    new Document("$project",new Document("row",new Document("$cond",Arrays.asList(new Document("$lte",List.of(new Document("$bsonSize","$$ROOT"),64*1024)),"$$ROOT",null)))))).first();
            budget.use(4096L+(result==null?0:MongoAlertConfigurationCodec.size(result)));
            if(result==null){scan.complete=true;return false;}
            Document row=result.get("row",Document.class);if(row==null || !MongoAlertEnvelopeSupport.intent(row))return true;
            var frozen=row.get("frozen",Document.class);var identity=MongoAlertDeliveryCodec.readIdentity(frozen.get("identity",Document.class));
            var target=MongoAlertDeliveryCodec.readTarget(frozen.get("target",Document.class));
            if(!identity.scope().equals(scope))return true;
            var transport=target instanceof AlertIntent.ReceiverTarget r?r.transport():target instanceof AlertIntent.CandidateTarget c?c.transport():null;
            if(transport==null)return true;
            var actual=AlertTargetIdentity.domains(scope,transport).get(identity.targetId());if(actual==null)return true;
            if(actual.equals(domain))return true;
            scan.after=row.get("_id",Document.class);budget.proofSteps++;
        }
    }
    private boolean episodeReferenced(ClientSession s,AlarmKey key,long number,Instant now,Budget b,Map<String,Instant> proofs) {
        String identity=AlertIntent.hash(new Document("alarm",MongoAlertStateCodec.key(key)).append("number",number).toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var until=proofs.get(identity);if(until!=null&&!now.isBefore(until))return false;
        b.require(1,52*1024);
        var filter=and(scope(SystemCollections.ALERT_EPISODES),new Document("_id",new Document("alarm",MongoAlertStateCodec.key(key)).append("number",number)))
                .append("$expr",new Document("$lte",List.of(new Document("$bsonSize","$$ROOT"),48*1024)));
        Document ep=handles.apply(SystemCollections.ALERT_EPISODES).find(s,filter).batchSize(1).limit(1).first();
        b.use(4096L+(ep==null?0:MongoAlertConfigurationCodec.size(ep)));
        if(ep==null)throw new io.tapstate.core.common.TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-maintenance","field","missing-original-episode"),null);var value=MongoAlertStateEnvelope.readEpisode(key,number,ep);
        boolean protectedHistory=value.historyUntil()==null||now.isBefore(value.historyUntil());
        if(!protectedHistory && proofs.size()<200 && !Objects.equals(proofs.put(identity,value.historyUntil()),value.historyUntil()))b.proofSteps++;return protectedHistory;
    }
    private boolean memberReferences(ClientSession s,AlarmKey key,Long episode,Budget b) {
        var alarm=MongoAlertStateCodec.key(key);var condition=new Document("alarm",alarm);if(episode!=null)condition.append("episode",episode);
        if(unknown(s,SystemCollections.ALERT_DELIVERY_INTENTS,b)||unknown(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,b))return true;
        if(exists(s,SystemCollections.ALERT_DELIVERY_INTENTS,new Document("frozen.members",new Document("$elemMatch",condition)),b))return true;
        var summary=new Document("alarm",alarm);if(episode!=null)summary.append("episode",episode);
        return exists(s,SystemCollections.ALERT_NOTIFICATION_SCHEDULES,new Document("$or",List.of(new Document("members",new Document("$elemMatch",condition)),
                new Document("pending",new Document("$elemMatch",condition)),new Document("batch.members",new Document("$elemMatch",condition)),
                new Document("summaries",new Document("$elemMatch",summary)))),b);
    }
    private boolean resourceReferences(ClientSession s,AlarmKey.Resource resource,Budget b) {
        var r=new Document("resourceKind",resource.kind()).append("resourceId",resource.id()).append("resourceIncarnation",resource.incarnation());
        for(var c:List.of(SystemCollections.ALERT_INSTANCES,SystemCollections.ALERT_EPISODES,SystemCollections.ALERT_TRANSITIONS)) {
            if(unknown(s,c,b))return true;String path=c==SystemCollections.ALERT_INSTANCES?"_id.":"_id.alarm.";var f=new Document();r.forEach((k,v)->f.append(path+k,v));if(exists(s,c,f,b))return true;
        }
        for(var c:List.of(SystemCollections.ALERT_DELIVERY_INTENTS,SystemCollections.ALERT_NOTIFICATION_SCHEDULES)) {
            if(unknown(s,c,b))return true;String path=c==SystemCollections.ALERT_DELIVERY_INTENTS?"frozen.members.alarm.":"members.alarm.";
            var f=new Document();r.forEach((k,v)->f.append(path+k,v));if(exists(s,c,f,b))return true;
            if(c==SystemCollections.ALERT_NOTIFICATION_SCHEDULES){var summary=new Document();r.forEach((k,v)->summary.append("summaries.alarm."+k,v));if(exists(s,c,summary,b))return true;}
        }
        return false;
    }
    private boolean unknown(ClientSession s,SystemCollections c,Budget b) {
        if(c==SystemCollections.ALERT_DELIVERY_INTENTS)return exists(s,c,new Document("$nor",List.of(MongoAlertEnvelopeSupport.intentFilter())),b);
        if(c==SystemCollections.ALERT_RECEIVERS)return exists(s,c,new Document("$nor",List.of(MongoAlertEnvelopeSupport.receiverFilter())),b);
        var versions=c==SystemCollections.ALERT_INSTANCES?List.of(2,3,4):c==SystemCollections.ALERT_DELIVERY_INTENTS?List.of(1,2,3):c==SystemCollections.ALERT_RULES||c==SystemCollections.ALERT_SUBSCRIPTIONS||c==SystemCollections.ALERT_RECEIVERS?List.of(1,2):
                c==SystemCollections.ALERT_EPISODES||c==SystemCollections.ALERT_TRANSITIONS?List.of(2):List.of(1);
        return exists(s,c,new Document("formatVersion",new Document("$nin",versions)),b);
    }
    private boolean exists(ClientSession s,SystemCollections c,Document condition,Budget b) {
        b.use(4096);return handles.apply(c).aggregate(s,List.of(new Document("$match",and(scope(c),condition)),new Document("$limit",1),new Document("$project",new Document("_id",0).append("present",new Document("$literal",true))))).first()!=null;
    }
    private Document read(ClientSession s,SystemCollections c,Document filter,Budget b) {
        b.use(4096);
        // Inspect size on the server before fetching an untrusted or future-format body.
        var size=handles.apply(c).aggregate(s,List.of(new Document("$match",and(scope(c),filter)),new Document("$limit",1),new Document("$project",new Document("size",new Document("$bsonSize","$$ROOT"))))).first();
        if(size==null)return null;long bytes=((Number)size.get("size")).longValue();
        if(bytes>MAX_FETCH_BYTES)throw new IllegalArgumentException("oversized unknown maintenance record");b.use(bytes+4096+MongoAlertConfigurationCodec.size(filter));
        return handles.apply(c).find(s,and(scope(c),filter)).batchSize(1).limit(1).first();
    }
    static Document versionFilter(Document original) {
        if(!(original.get("recordRevision") instanceof Long))return original;
        var filter=new Document("_id",original.get("_id")).append("formatVersion",original.get("formatVersion")).append("recordRevision",original.get("recordRevision"));
        for(String field:List.of("incarnation","writeFence"))if(original.containsKey(field))filter.append(field,original.get(field));
        return filter;
    }
    private String replace(ClientSession s,SystemCollections c,Document before,Document after,Budget b) { b.use(8192L+MongoAlertConfigurationCodec.size(versionFilter(before))+MongoAlertConfigurationCodec.size(after));writeAttempt(c.collectionName(),1);boolean changed=mutation.apply(s,c,before,after);
        return changed?"changed":"conflict"; }
    private String remove(ClientSession s,SystemCollections c,Document row,Budget b) { b.use(8192L+MongoAlertConfigurationCodec.size(row));writeAttempt(c.collectionName(),1);return mutation.apply(s,c,row,null)?"changed":"conflict"; }
    private Document scope(SystemCollections c) {
        String prefix=c==SystemCollections.ALERT_NODE_LIFECYCLES?"":c==SystemCollections.ALERT_EPISODES||c==SystemCollections.ALERT_TRANSITIONS?"_id.alarm.":"_id.";
        return new Document(prefix+"authorityId",scope.authorityId()).append(prefix+"clusterId",scope.clusterId());
    }
    private static Document and(Document a,Document b) { return new Document("$and",List.of(a,b)); }
    private static Document lex(List<String> fields,List<?> values) {
        var choices=new ArrayList<Document>();for(int i=0;i<fields.size();i++){var term=new Document();for(int j=0;j<i;j++)term.append(fields.get(j),values.get(j));term.append(fields.get(i),new Document("$gt",values.get(i)));choices.add(term);}return new Document("$or",choices);
    }
    private static AlertSchedule.Record handoff(AlertSchedule.Record old,AlertIntent.Record intent) {
        var key=((AlertIntent.Business)intent.frozen().identity()).key();var summaries=new LinkedHashMap<FrozenGroupPolicy.Member,AlertSchedule.Previous>();
        old.summaries().forEach(v->summaries.put(new FrozenGroupPolicy.Member(v.alarm(),v.episode()),v));
        if(key.notification().kind()==DeliveryKey.Kind.FAULT||key.notification().kind()==DeliveryKey.Kind.REMINDER)for(var member:intent.frozen().members()) {
            var id=member.episodeMember();var prior=summaries.get(id);var outcome=prior==null?intent.state().summary():DeliveryEvidence.aggregate(List.of(prior.outcome(),intent.state().summary()));
            summaries.put(id,new AlertSchedule.Previous(id.key(),id.episode(),outcome,intent.frozen().content().renderingFailed()||prior!=null&&prior.renderingFailed()));
        }
        var refs=old.protectedIntents().stream().filter(v->!v.equals(key)).toList();
        return new AlertSchedule.Record(old.key(),old.route(),old.retention(),old.createdAt(),old.owner(),old.grouping(),old.clock(),old.batchSequence(),old.pending(),old.batch(),old.disposition(),List.copyOf(summaries.values()),refs,old.retiredAt(),old.summariesHandedOff(),old.members());
    }
    @Override public Inventory inventory() {
        transactions.verify(Duration.ofSeconds(5));
        var value=transactions.run(false,Duration.ofSeconds(5),s->{var resources=new ArrayList<Usage>();long pending=0,failed=0,exhausted=0;Instant oldestPending=null;
            for(var c:RESOURCES) {
                var size=new Document("$bsonSize","$$ROOT");
                var protectedCondition=c==SystemCollections.ALERT_DELIVERY_INTENTS?new Document("$eq",Arrays.asList("$state.retained.stoppedAt",null)):
                        new Document("$or",List.of(new Document("$eq",Arrays.asList(new Document("$ifNull",Arrays.asList("$cleanupAt",null)),null)),new Document("$gt",List.of("$cleanupAt",Date.from(clock.instant())))));
                var group=new Document("_id",null).append("rows",new Document("$sum",1)).append("bytes",new Document("$sum",size))
                        .append("protectedBytes",new Document("$sum",new Document("$cond",List.of(protectedCondition,size,0))))
                        .append("oldest",new Document("$min","$cleanupAt"));
                if(c==SystemCollections.ALERT_INSTANCES || c==SystemCollections.ALERT_NOTIFICATION_SCHEDULES) {
                    int maximum=c==SystemCollections.ALERT_INSTANCES?64*1024:AlertSchedule.MAX_BYTES;
                    group.append("reserved",new Document("$sum",new Document("$cond",List.of(new Document("$eq",Arrays.asList(new Document("$ifNull",Arrays.asList("$retiredAt",null)),null)),new Document("$max",List.of(c==SystemCollections.ALERT_NOTIFICATION_SCHEDULES?AlertIntent.MAX_BYTES:0,new Document("$subtract",List.of(maximum+(c==SystemCollections.ALERT_NOTIFICATION_SCHEDULES?AlertIntent.MAX_BYTES:0),size)))),0))));
                }
                if(c==SystemCollections.ALERT_DELIVERY_INTENTS) {
                    // Reserve the maximum original result allowance even before an attempt is granted.
                    var attempts=new Document("$multiply",List.of(new Document("$ifNull",List.of("$frozen.retry.attemptsPerCycle",30)),
                            new Document("$add",List.of(1,new Document("$ifNull",List.of("$frozen.retry.maximumManualCycles",0))))));
                    group.append("reserved",new Document("$sum",new Document("$cond",List.of(protectedCondition,new Document("$add",List.of(4096,new Document("$multiply",List.of(attempts,4096)))),0))));
                    var businessPending=new Document("$and",List.of(protectedCondition,new Document("$eq",List.of("$_id.purpose","business"))));
                    group.append("pending",new Document("$sum",new Document("$cond",List.of(businessPending,1,0))));
                    group.append("oldestPending",new Document("$min",new Document("$cond",List.of(businessPending,"$frozen.createdAt","$$REMOVE"))));
                    group.append("failed",new Document("$sum",new Document("$cond",List.of(new Document("$and",List.of(new Document("$eq",List.of("$_id.purpose","business")),new Document("$ne",Arrays.asList("$state.retained.stoppedAt",null)),new Document("$ne",List.of("$state.summary","ACCEPTED")))),1,0))));
                    group.append("exhausted",new Document("$sum",new Document("$cond",List.of(new Document("$and",List.of(new Document("$eq",List.of("$_id.purpose","business")),new Document("$eq",List.of("$state.stopReason","EXHAUSTED")))),1,0))));
                }
                var row=handles.apply(c).aggregate(s,List.of(new Document("$match",scope(c)),new Document("$group",group))).allowDiskUse(false).first();
                if(row==null)resources.add(new Usage(c.collectionName(),0,0,0,0,null));
                else {resources.add(new Usage(c.collectionName(),number(row,"rows"),number(row,"bytes"),number(row,"reserved"),number(row,"protectedBytes"),row.get("oldest") instanceof Date at?at.toInstant():null));
                    if(c==SystemCollections.ALERT_DELIVERY_INTENTS){pending=number(row,"pending");failed=number(row,"failed");exhausted=number(row,"exhausted");if(row.get("oldestPending") instanceof String at)oldestPending=Instant.parse(at);}}
            }
            return new Inventory(clock.instant(),resources,pending,oldestPending,failed,exhausted);
        });observed.accept(value);return value;
    }
    private static long number(Document row,String field) { return row.get(field) instanceof Number n?n.longValue():0; }
}
