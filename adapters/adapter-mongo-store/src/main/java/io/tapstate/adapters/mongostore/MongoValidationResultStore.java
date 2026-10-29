package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ValidationResultStore;
import io.tapstate.spi.store.ValidationReports;
import io.tapstate.core.validation.*;
import org.bson.Document;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;

/** Complete root replacement and closure share the original prepared CONFIG transaction domain. */
final class MongoValidationResultStore implements ValidationResultStore,ValidationReports {
    static final long MAX_REPORT_BYTES = 64L * 1024 * 1024;
    static final int MAX_ROOT_BYTES = 1024 * 1024;
    static final long MAX_STORED_ROOTS = 4096;
    private final MongoValidationAdmissionStore admission;
    private final MongoDatabase database;
    private final MongoCollection<Document> reports;
    private final MongoCollection<Document> differences;
    private final String clusterId;

    MongoValidationResultStore(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive) {
        this(client, database, clusterId, requireActive, null);
    }
    MongoValidationResultStore(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive,
            MongoValidationPreparations.Authority validation) {
        this.admission = new MongoValidationAdmissionStore(client, database, clusterId, requireActive, validation);
        this.database = Objects.requireNonNull(database); this.clusterId = Objects.requireNonNull(clusterId);
        reports = SystemCollections.VALIDATION_REPORTS.on(database); differences = SystemCollections.VALIDATION_DIFFERENCES.on(database);
    }

    @Override public Page<Summary> history(String pipeline,String task,ValidationMode mode,Paging page){
        Objects.requireNonNull(mode);Objects.requireNonNull(page);admission.requirePipeline(pipeline);
        ValidationError.fieldName(pipeline,"report.pipeline");ValidationError.fieldName(task,"report.task");
        return admission.transaction(session->{
            var filter=new Document("_id.clusterId",clusterId).append("_id.pipelineId",pipeline).append("_id.taskId",task).append("admission.mode",mode.name());
            var items=new ArrayList<Summary>();
            try(var cursor=reports.find(session,filter).sort(new Document("terminalMetadata.endedAt.seconds",-1).append("terminalMetadata.endedAt.nanos",-1).append("_id.runId",1))
                    .skip(page.offset()).limit(page.limit()).iterator()){
                while(cursor.hasNext()){
                    var value=snapshot(cursor.next());int mismatches=(int)value.counts().stream().filter(count->count.sourceCount()!=count.targetCount()).count();
                    items.add(new Summary(value.reference(),value.specification().method(),value.closed(),value.terminal(),value.terminalKnownAt(),
                            value.endedAt(),value.window(),value.rootCount(),value.retainedBytes(),value.counts().size(),mismatches));
                }
            }
            return new Page<>(items,reports.countDocuments(session,filter),page);
        });
    }
    @Override public java.util.Optional<Snapshot> find(Reference reference){
        Objects.requireNonNull(reference);admission.requirePipeline(reference.pipelineId());
        return admission.transaction(session->java.util.Optional.ofNullable(referenced(session,reference)).map(this::snapshot));
    }
    @Override public Page<RootSummary> roots(Reference reference,Paging page){
        Objects.requireNonNull(reference);Objects.requireNonNull(page);admission.requirePipeline(reference.pipelineId());
        return admission.transaction(this::requireDifferences,session->{
            var report=referenced(session,reference);if(report==null)return new Page<>(List.of(),0,page);
            var filter=new Document("_id.report",report.get("_id")).append("openVersion",reference.sequence());
            var specification=snapshot(report).specification();var items=new ArrayList<RootSummary>();
            try(var cursor=differences.find(session,filter).sort(new Document("_id.rootHash",1)).skip(page.offset()).limit(page.limit()).iterator()){
                while(cursor.hasNext()){
                    var row=cursor.next();var root=validateRoot(row,reference,specification);
                    var payload=row.get("payload",Document.class);var summary=new Document();
                    for(var field:List.of("method","counts","checks","targetPresence","samplesTruncated","confirmation"))summary.put(field,payload.get(field));
                    items.add(new RootSummary(rootHash(row),root,portable(summary)));
                }
            }
            return new Page<>(items,number(report,"rootCount"),page);
        });
    }
    @Override public java.util.Optional<RootDetail> root(Reference reference,String rootHash){
        Objects.requireNonNull(reference);admission.requirePipeline(reference.pipelineId());
        if(rootHash==null||!rootHash.matches("[a-f0-9]{64}"))throw invalid("report.rootHash");
        return admission.transaction(this::requireDifferences,session->{
            var report=referenced(session,reference);if(report==null)return java.util.Optional.empty();
            var id=new Document("report",report.get("_id")).append("rootHash",new Binary(java.util.HexFormat.of().parseHex(rootHash)));
            var row=differences.find(session,new Document("_id",id).append("openVersion",reference.sequence())).first();
            if(row==null)return java.util.Optional.empty();var root=validateRoot(row,reference,snapshot(report).specification());
            return java.util.Optional.of(new RootDetail(rootHash,number(row,"revision"),root,portable(row.get("payload",Document.class))));
        });
    }
    @Override public Mirror mirror(TerminalMirror command){
        Objects.requireNonNull(command);var known=command.known();var expected=known.run().report();requireCluster(expected);
        var next=new Document("formatVersion",1).append("state",known.state().name()).append("countingGeneration",known.run().countingGeneration())
                .append("knownAt",command.knownAt().toString());
        if(known.endedAt()!=null)next.put("endedAt",new Document("seconds",known.endedAt().getEpochSecond()).append("nanos",known.endedAt().getNano()));
        if(command.window()!=null)next.put("window",window(command.window()));
        if(command.failure()!=null)next.put("failure",failure(command.failure()));
        if(encoded(next).length>80*1024)throw MongoValidationObservationCodec.limit();
        return admission.transaction(session->{
            var report=report(session,expected);if(report==null)return Mirror.CLOSED_OR_EXPIRED;
            if(!known.run().admission().equals(MongoValidationAdmissionStore.decodeIdentity(report.get("admission",Document.class))))
                throw invalid("report.mirrorBirth");
            if(!Boolean.TRUE.equals(report.getBoolean("closed")))throw invalid("report.mirrorBeforeClosure");
            if(!MongoValidationTaskCodec.run(known.run().snapshot()).equals(report.get("specSnapshot")))throw invalid("report.mirrorSnapshot");
            var previous=terminal(report);
            if(previous!=null){
                if(!known.state().name().equals(previous.getString("state"))||number(previous,"countingGeneration")!=known.run().countingGeneration())
                    throw invalid("report.conflictingTerminal");
                next.put("knownAt",previous.get("knownAt"));
                if(!Objects.equals(previous.get("endedAt"),next.get("endedAt")))throw invalid("report.conflictingTerminalTime");
                mergeFacts(next,previous);
                if(previous.equals(next))return Mirror.DUPLICATE;
            }
            if(previous==null)mergeFacts(next,observation(report));
            if(encoded(next).length>80*1024)throw MongoValidationObservationCodec.limit();
            var replacement=new Document(report);replacement.remove("observationMetadata");replacement.put("terminalMetadata",next);requireReportCapacity(replacement);
            if(reports.updateOne(session,original(expected,report).append("closed",true),new Document("$set",new Document("terminalMetadata",next))
                    .append("$unset",new Document("observationMetadata",""))).getMatchedCount()!=1)
                throw unreadable("terminal-mirror-fence");
            return Mirror.APPLIED;
        });
    }
    @Override public Mirror observe(Observation command){
        Objects.requireNonNull(command);requireCluster(command.report());var next=new Document("formatVersion",1);
        if(command.window()!=null)next.put("window",window(command.window()));
        if(command.failure()!=null)next.put("failure",failure(command.failure()));
        return admission.transaction(session->{
            var report=report(session,command.report());
            if(report==null||Boolean.TRUE.equals(report.getBoolean("closed")))return Mirror.CLOSED_OR_EXPIRED;
            var start=MongoValidationAdmissionStore.decodeIdentity(report.get("admission",Document.class));
            if(!admission.bornInTransaction(session,start))return Mirror.CLOSED_OR_EXPIRED;
            var previous=observation(report);mergeFacts(next,previous);
            if(next.equals(previous))return Mirror.DUPLICATE;
            if(encoded(next).length>80*1024)throw MongoValidationObservationCodec.limit();
            var replacement=new Document(report);replacement.put("observationMetadata",next);requireReportCapacity(replacement);
            if(reports.updateOne(session,original(command.report(),report).append("closed",false),new Document("$set",new Document("observationMetadata",next))).getMatchedCount()!=1)
                throw unreadable("observation-metadata-fence");
            return Mirror.APPLIED;
        });
    }
    private static void mergeFacts(Document next,Document previous){
        if(previous==null)return;
                if(previous.containsKey("window")){
                    var old=previous.get("window",Document.class);var incoming=next.get("window",Document.class);
                    if(incoming==null)next.put("window",old);
                    else if(!Objects.equals(old.get("startedAt"),incoming.get("startedAt"))
                            ||old.get("finishedAt")!=null&&!Objects.equals(old.get("finishedAt"),incoming.get("finishedAt")))throw invalid("report.conflictingWindow");
                }
                if(previous.containsKey("failure")){
                    var old=previous.get("failure",Document.class);var incoming=next.get("failure",Document.class);
                    if(incoming!=null&&Boolean.TRUE.equals(incoming.getBoolean("fatal"))&&!Boolean.TRUE.equals(old.getBoolean("fatal")))next.put("earlierFailure",old);
                    else next.put("failure",old);
                }
                if(previous.containsKey("earlierFailure"))next.put("earlierFailure",previous.get("earlierFailure"));
    }
    private static Document observation(Document report){
        if(!report.containsKey("observationMetadata"))return null;
        if(!(report.get("observationMetadata") instanceof Document value)||!Integer.valueOf(1).equals(value.get("formatVersion"))
                ||!java.util.Set.of("formatVersion","window","failure","earlierFailure").containsAll(value.keySet())||encoded(value).length>80*1024)
            throw unreadable("observation-metadata");
        try{
            window(value);if(value.containsKey("failure"))failure(value.get("failure",Document.class));
            if(value.containsKey("earlierFailure"))failure(value.get("earlierFailure",Document.class));
        }catch(RuntimeException bad){throw unreadable("observation-metadata");}
        return value;
    }
    @Override public Cleanup retain(Retention command){
        Objects.requireNonNull(command);if(!clusterId.equals(command.task().clusterId()))throw invalid("report.clusterId");
        admission.requirePipeline(command.task().pipelineId());
        return admission.transaction(this::requireDifferences,session->{
            var current=admission.retentionTask(session,command.task());if(current==null)return new Cleanup(0,0,true);
            int keep=command.mode()==ValidationMode.FULL?current.settings().full().historyLimit():current.settings().incremental().historyLimit();
            if(keep!=command.keep())return new Cleanup(0,0,true);
            var filter=new Document("_id.clusterId",clusterId).append("_id.pipelineId",command.task().pipelineId()).append("_id.taskId",command.task().taskId())
                    .append("admission.mode",command.mode().name()).append("admission.taskGeneration",command.task().generation().toString());
            var rows=reports.find(session,filter).sort(new Document("openVersion",-1)).limit(1025)
                    .projection(new Document("_id",1).append("formatVersion",1).append("admission",1).append("openVersion",1).append("closed",1).append("terminalMetadata",1)).into(new ArrayList<>());
            if(rows.size()>1024)throw MongoValidationObservationCodec.limit();
            var pending=new java.util.HashMap<Reference,io.tapstate.spi.store.ValidationControlStore.Status>();
            for(var status:command.pending()){
                var key=reference(status.run().admission());var previous=pending.putIfAbsent(key,status);
                if(previous!=null&&!previous.equals(status))throw invalid("report.conflictingPendingTerminal");
            }
            for(var row:rows){var copy=pending.get(reference(start(row)));var terminal=terminal(row);
                if(copy!=null&&terminal!=null&&!Objects.equals(copy.endedAt(),terminalEnd(terminal)))throw invalid("report.conflictingPendingTime");}
            rows.sort((left,right)->{
                var leftEnd=end(left,pending);var rightEnd=end(right,pending);
                int order=java.util.Comparator.nullsLast(java.util.Comparator.<java.time.Instant>reverseOrder()).compare(leftEnd,rightEnd);
                return order!=0?order:left.get("_id",Document.class).getString("runId").compareTo(right.get("_id",Document.class).getString("runId"));
            });
            int retained=0,removed=0;boolean blocked=false;
            for(var row:rows){
                var terminal=terminal(row);var copy=pending.get(reference(start(row)));
                if(copy!=null&&(terminal!=null&&!copy.state().name().equals(terminal.getString("state"))
                        ||!copy.run().admission().equals(start(row))))throw invalid("report.conflictingPendingTerminal");
                if(!Boolean.TRUE.equals(row.getBoolean("closed"))||end(row,pending)==null){blocked=true;continue;}
                if(retained++<keep)continue;
                var exact=new Document("_id",row.get("_id")).append("openVersion",row.get("openVersion")).append("admission",row.get("admission"))
                        .append("closed",true);
                if(terminal!=null)exact.put("terminalMetadata",terminal);else exact.put("terminalMetadata",new Document("$exists",false));
                differences.deleteMany(session,new Document("_id.report",row.get("_id")).append("openVersion",row.get("openVersion")));
                if(reports.deleteOne(session,exact).getDeletedCount()!=1)throw unreadable("retention-fence");removed++;
            }
            return new Cleanup(removed,Math.min(retained,keep),blocked);
        });
    }

    @Override public Publication publish(RootWrite write) {
        Objects.requireNonNull(write); requireCluster(write.report()); admission.checkActive();
        if (admission.closing()) return Publication.CLOSED_OR_EXPIRED;
        var payload = MongoValidationObservationCodec.encode(write.check());
        var root = MongoValidationObservationCodec.root(write.check().observation().root());
        var id = new Document("report", MongoValidationAdmissionStore.reportId(write.report().run())).append("rootHash", new Binary(hash(encoded(root))));
        var next = new Document("_id", id).append("formatVersion", 1).append("openVersion", write.report().openVersion())
                .append("root", root).append("revision", write.check().observation().revision()).append("payload", payload).append("bytes", 0L);
        long bytes = encoded(next).length; if (bytes > MAX_ROOT_BYTES) throw MongoValidationObservationCodec.limit();
        next.put("bytes", bytes);
        return admission.transaction(this::requireDifferences, session -> {
            var report = report(session, write.report());
            if (report == null || report.getBoolean("closed")) return Publication.CLOSED_OR_EXPIRED;
            var start = MongoValidationAdmissionStore.decodeIdentity(report.get("admission", Document.class));
            if (!admission.bornInTransaction(session, start)) return Publication.CLOSED_OR_EXPIRED;
            var previous = differences.find(session, new Document("_id", id)).first(); long oldBytes = 0;
            if (previous != null) {
                if (!Integer.valueOf(1).equals(previous.get("formatVersion")) || !root.equals(previous.get("root"))
                        || !Long.valueOf(write.report().openVersion()).equals(previous.get("openVersion"))
                        || !(previous.get("revision") instanceof Long revision) || revision < 1
                        || !(previous.get("bytes") instanceof Long retained) || retained < 1 || retained > MAX_ROOT_BYTES
                        || !(previous.get("payload") instanceof Document) || encoded(previous).length != retained) throw unreadable("root-observation");
                oldBytes = retained;
                if (revision > write.check().observation().revision()) return Publication.STALE_OBSERVATION;
                if (revision == write.check().observation().revision()) {
                    if (!payload.equals(previous.get("payload"))) throw new TapstateException(ValidationError.INVALID_CONTRACT,
                            Map.of("field", "report.conflictingReplay"), null);
                    return Publication.DUPLICATE;
                }
            }
            long oldCount = number(report, "rootCount"), used = number(report, "resultBytes"), version = number(report, "publicationRevision");
            if (used < oldBytes || oldCount > MAX_STORED_ROOTS || used > MAX_REPORT_BYTES || previous != null && oldCount == 0)
                throw unreadable("report-accounting");
            long count = oldCount + (previous == null ? 1 : 0), total = used - oldBytes + bytes;
            // Keep the existing total bound, including fixed report metadata and its bounded final summary.
            var fixed=new Document(report);fixed.remove("observationMetadata");fixed.remove("terminalMetadata");
            if (count > MAX_STORED_ROOTS || total + encoded(fixed).length + 80L * 1024 > MAX_REPORT_BYTES || version == Long.MAX_VALUE)
                throw MongoValidationObservationCodec.limit();
            var filter = original(write.report(), report).append("closed", false).append("publicationRevision", version);
            if (reports.updateOne(session, filter, new Document("$set", new Document("rootCount", count).append("resultBytes", total)
                    .append("publicationRevision", version + 1))).getMatchedCount() != 1) throw unreadable("report-write-fence");
            if (previous == null) differences.insertOne(session, next);
            else if (differences.replaceOne(session, new Document("_id", id).append("revision", previous.get("revision")), next).getMatchedCount() != 1)
                throw unreadable("root-replacement");
            return Publication.APPLIED;
        });
    }

    @Override public Seal seal(Report expected) {
        Objects.requireNonNull(expected); requireCluster(expected);
        return admission.transaction(session -> {
            var report = report(session, expected); if (report == null) return Seal.CLOSED_OR_EXPIRED;
            boolean closed = report.getBoolean("closed");
            var start = MongoValidationAdmissionStore.decodeIdentity(report.get("admission", Document.class));
            // Close the exact current permission where retained; a superseded/deleted task never redirects this report.
            admission.closeInTransaction(session, start);
            if (reports.updateOne(session, original(expected, report), new Document("$set", new Document("closed", true))).getMatchedCount() != 1)
                throw unreadable("report-seal");
            return closed ? Seal.ALREADY_SEALED : Seal.SEALED;
        });
    }

    @Override public Publication publishCount(CountWrite write) {
        Objects.requireNonNull(write); requireCluster(write.report()); admission.checkActive();
        if (admission.closing()) return Publication.CLOSED_OR_EXPIRED;
        var check = write.check();
        var payload = new Document("formatVersion", 1).append("mappingId", check.mappingId()).append("outputId", check.outputId())
                .append("sourceCount", check.sourceCount()).append("targetCount", check.targetCount())
                .append("sourceRead", boundary(check.sourceRead())).append("targetRead", boundary(check.targetRead()));
        if (encoded(payload).length > 8 * 1024) throw MongoValidationObservationCodec.limit();
        return admission.transaction(session -> {
            var report = report(session, write.report());
            if (report == null || report.getBoolean("closed")) return Publication.CLOSED_OR_EXPIRED;
            var start = MongoValidationAdmissionStore.decodeIdentity(report.get("admission", Document.class));
            if (!admission.bornInTransaction(session, start)) return Publication.CLOSED_OR_EXPIRED;
            if (!MongoValidationTaskCodec.run(write.snapshot()).equals(report.get("specSnapshot")))
                throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "report.countSnapshot"), null);
            long previous = countSummaryBytes(report), used = number(report, "resultBytes"), version = number(report, "publicationRevision");
            if (used != previous || number(report, "rootCount") != 0) throw unreadable("count-summary-accounting");
            var summaries = new ArrayList<Document>();
            var seen = new java.util.HashSet<String>();
            boolean duplicate = false;
            if (report.containsKey("countSummaries")) {
                if (!(report.get("countSummaries") instanceof List<?> stored) || stored.size() > 1024) throw unreadable("count-summaries");
                for (var entry : stored) {
                    if (!(entry instanceof Document summary) || !Integer.valueOf(1).equals(summary.get("formatVersion"))
                            || !(summary.get("mappingId") instanceof String mapping) || encoded(summary).length > 8 * 1024)
                        throw unreadable("count-summary");
                    if (!seen.add(mapping) || write.snapshot().task().mappings().stream().noneMatch(declared ->
                            declared.mappingId().equals(mapping) && declared.outputId().equals(summary.get("outputId"))))
                        throw unreadable("count-summary-scope");
                    validateCount(summary, write.report());
                    if (mapping.equals(check.mappingId())) {
                        if (!payload.equals(summary)) throw new TapstateException(ValidationError.INVALID_CONTRACT,
                                Map.of("field", "report.conflictingCountReplay"), null);
                        duplicate = true;
                    }
                    summaries.add(summary);
                }
            }
            if (duplicate) return Publication.DUPLICATE;
            summaries.add(payload);
            if (summaries.size() > write.snapshot().task().mappings().size()) throw unreadable("count-summary-scope");
            long bytes = encoded(new Document("countSummaries", summaries)).length;
            if (previous > used || version == Long.MAX_VALUE) throw unreadable("count-summary-accounting");
            long total = used - previous + bytes;
            if (bytes > 512 * 1024 || total > MAX_REPORT_BYTES) throw MongoValidationObservationCodec.limit();
            var changes = new Document("countSummaries", summaries).append("countSummaryBytes", bytes)
                    .append("resultBytes", total).append("publicationRevision", version + 1);
            if (reports.updateOne(session, original(write.report(), report).append("closed", false).append("publicationRevision", version),
                    new Document("$set", changes)).getMatchedCount() != 1) throw unreadable("count-write-fence");
            return Publication.APPLIED;
        });
    }

    private static long countSummaryBytes(Document report) {
        if (!report.containsKey("countSummaries")) {
            if (report.containsKey("countSummaryBytes")) throw unreadable("count-summary-accounting");
            return 0;
        }
        long bytes = number(report, "countSummaryBytes");
        if (bytes > 512 * 1024 || bytes != encoded(new Document("countSummaries", report.get("countSummaries"))).length)
            throw unreadable("count-summary-accounting");
        return bytes;
    }
    private Document referenced(ClientSession session,Reference reference){
        var id=new Document("clusterId",clusterId).append("pipelineId",reference.pipelineId()).append("taskId",reference.taskId())
                .append("runId",reference.runId());
        var row=reports.find(session,new Document("_id",id).append("openVersion",reference.sequence())
                .append("admission.taskGeneration",reference.taskGeneration().toString()).append("admission.mode",reference.mode().name())).first();
        if(row!=null&&!reference(start(row)).equals(reference))throw unreadable("report-reference");return row;
    }
    private static io.tapstate.spi.store.ValidationReportAdmission.Start start(Document row){
        if(row==null||!Integer.valueOf(1).equals(row.get("formatVersion"))||!(row.get("admission") instanceof Document identity)
                ||!(row.get("closed") instanceof Boolean)||!(row.get("openVersion") instanceof Long sequence))throw unreadable("report");
        var start=MongoValidationAdmissionStore.decodeIdentity(identity);
        if(sequence!=start.sequence()||!MongoValidationAdmissionStore.reportId(start.run()).equals(row.get("_id")))throw unreadable("report-identity");
        return start;
    }
    private static Reference reference(io.tapstate.spi.store.ValidationReportAdmission.Start start){
        return new Reference(start.task().pipelineId(),start.task().taskId(),start.run().mode(),start.run().runId(),start.task().generation(),start.sequence());
    }
    private Snapshot snapshot(Document row){
        try{
            var start=start(row);var spec=row.get("specSnapshot",Document.class);if(spec==null)throw unreadable("report-specification");
            var specification=MongoValidationTaskCodec.run(spec);var terminal=terminal(row);var facts=terminal==null?observation(row):terminal;var counts=new ArrayList<ValidationCountCheck>();
            if(!specification.fingerprint().equals(start.run().specHash()))throw unreadable("report-specification");
            countSummaryBytes(row);var seen=new java.util.HashSet<String>();
            if(row.containsKey("countSummaries")){
                if(specification.method()!=ValidationMethod.COUNT||!(row.get("countSummaries") instanceof List<?> summaries)
                        ||summaries.size()>specification.task().mappings().size())throw unreadable("count-summaries");
                for(var entry:summaries){
                    if(!(entry instanceof Document summary))throw unreadable("count-summary");validateCount(summary,start.report());
                    String mapping=summary.getString("mappingId"),output=summary.getString("outputId");
                    if(!seen.add(mapping)||specification.task().mappings().stream().noneMatch(value->value.mappingId().equals(mapping)&&value.outputId().equals(output)))throw unreadable("count-summary-scope");
                    counts.add(new ValidationCountCheck(start.run(),mapping,output,number(summary,"sourceCount"),number(summary,"targetCount"),
                            readBoundary(summary.get("sourceRead",Document.class)),readBoundary(summary.get("targetRead",Document.class))));
                }
            }
            if(specification.method()==ValidationMethod.COUNT&&number(row,"rootCount")!=0)throw unreadable("count-roots");
            if(terminal!=null&&"COMPLETED".equals(terminal.get("state"))&&specification.method()==ValidationMethod.COUNT&&counts.size()!=specification.task().mappings().size())throw unreadable("incomplete-count-report");
            if(number(row,"rootCount")>MAX_STORED_ROOTS||number(row,"resultBytes")>MAX_REPORT_BYTES)throw unreadable("report-accounting");
            return new Snapshot(start,specification,row.getBoolean("closed"),terminal==null?null:ValidationRunState.valueOf(terminal.getString("state")),
                    terminal==null?null:java.time.Instant.parse(terminal.getString("knownAt")),terminalEnd(terminal),window(facts),
                    facts==null||!facts.containsKey("failure")?null:failure(facts.get("failure",Document.class)),number(row,"rootCount"),number(row,"resultBytes"),counts);
        }catch(RuntimeException bad){throw unreadable("report-snapshot");}
    }
    private static java.time.Instant end(Document row,Map<Reference,io.tapstate.spi.store.ValidationControlStore.Status> pending){
        var terminal=terminal(row);if(terminal!=null)return terminalEnd(terminal);
        var known=pending.get(reference(start(row)));return known==null?null:known.endedAt();
    }
    private static java.time.Instant terminalEnd(Document terminal){
        if(terminal==null||!terminal.containsKey("endedAt"))return null;
        if(!(terminal.get("endedAt") instanceof Document value)||!value.keySet().equals(java.util.Set.of("seconds","nanos"))
                ||!(value.get("seconds") instanceof Long seconds)||!(value.get("nanos") instanceof Integer nanos)||nanos<0||nanos>999999999)throw unreadable("report-terminal-time");
        return java.time.Instant.ofEpochSecond(seconds,nanos);
    }
    private static Document terminal(Document row){
        if(row==null||!row.containsKey("terminalMetadata"))return null;
        if(!(row.get("terminalMetadata") instanceof Document terminal)||!Integer.valueOf(1).equals(terminal.get("formatVersion"))
                ||!Boolean.TRUE.equals(row.getBoolean("closed")))throw unreadable("report-terminal");
        try{
            if(!ValidationRunState.valueOf(terminal.getString("state")).terminal()||number(terminal,"countingGeneration")<1)throw unreadable("report-terminal");
            java.time.Instant.parse(terminal.getString("knownAt"));terminalEnd(terminal);window(terminal);
            if(terminal.containsKey("failure"))failure(terminal.get("failure",Document.class));
            if(terminal.containsKey("earlierFailure"))failure(terminal.get("earlierFailure",Document.class));
        }catch(RuntimeException bad){throw unreadable("report-terminal");}
        return terminal;
    }
    private static Document window(ValidationReportMetadata.Window window){
        return new Document("startedAt",window.startedAt().toString()).append("finishedAt",window.finishedAt()==null?null:window.finishedAt().toString());
    }
    private static ValidationReportMetadata.Window window(Document terminal){
        if(terminal==null||!terminal.containsKey("window"))return null;
        var window=terminal.get("window",Document.class);if(window==null||!window.keySet().equals(java.util.Set.of("startedAt","finishedAt")))throw unreadable("report-window");
        return new ValidationReportMetadata.Window(java.time.Instant.parse(window.getString("startedAt")),
                window.get("finishedAt")==null?null:java.time.Instant.parse(window.getString("finishedAt")));
    }
    private static Document failure(ValidationReportMetadata.Failure failure){
        var params=new Document();new java.util.TreeMap<>(failure.params()).forEach(params::put);
        return new Document("code",failure.code()).append("params",params).append("exceptionType",failure.exceptionType()).append("message",failure.message())
                .append("technical",failure.technical()).append("fatal",failure.fatal());
    }
    private static ValidationReportMetadata.Failure failure(Document failure){
        if(failure==null||!failure.keySet().equals(java.util.Set.of("code","params","exceptionType","message","technical","fatal"))
                ||!(failure.get("params") instanceof Document params)||!(failure.get("fatal") instanceof Boolean fatal))throw unreadable("report-failure");
        var values=new java.util.TreeMap<String,String>();
        params.forEach((key,value)->{if(!(value instanceof String text))throw unreadable("report-failure-parameter");values.put(key,text);});
        return new ValidationReportMetadata.Failure(failure.getString("code"),values,failure.getString("exceptionType"),failure.getString("message"),failure.getString("technical"),fatal);
    }
    private static int integer(Document row,String field){
        if(!(row.get(field) instanceof Integer value)||value<0||value>1024)throw unreadable(field);return value;
    }
    private static String rootHash(Document row){
        var id=row.get("_id",Document.class);if(id==null||!(id.get("rootHash") instanceof Binary hash)||hash.getData().length!=32)throw unreadable("root-id");
        return java.util.HexFormat.of().formatHex(hash.getData());
    }
    private static void validateRootHeader(Document row,Reference reference){
        if(!Integer.valueOf(1).equals(row.get("formatVersion"))||number(row,"openVersion")!=reference.sequence()||number(row,"revision")<1
                ||!(row.get("root") instanceof Document root)||!(row.get("payload") instanceof Document payload))throw unreadable("root-observation");
        if(!rootHash(row).equals(java.util.HexFormat.of().formatHex(hash(encoded(root)))))throw unreadable("root-identity");
        if(!"LOCAL_OBSERVATION".equals(payload.getString("confirmation")))throw unreadable("root-observation-kind");
    }
    private static ValidationRoot validateRoot(Document row,Reference reference,ValidationRunSpec specification){
        try{
            validateRootHeader(row,reference);var bytes=encoded(row).length;
            if(number(row,"bytes")!=bytes||bytes>MAX_ROOT_BYTES)throw unreadable("root-observation-accounting");
            return MongoValidationStoredObservation.validate(row.get("root",Document.class),row.get("payload",Document.class),specification);
        }catch(RuntimeException bad){throw unreadable("root-observation");}
    }
    private static Map<String,Object> portable(Document document){
        if(document==null)throw unreadable("report-payload");var copied=new java.util.LinkedHashMap<String,Object>();
        document.forEach((key,value)->copied.put(key,portable(value,0)));return copied;
    }
    private static Object portable(Object value,int depth){
        if(depth>64)throw unreadable("report-payload-depth");
        if(value==null||value instanceof String||value instanceof Boolean||value instanceof Integer||value instanceof Long||value instanceof Double)return value;
        if(value instanceof Binary binary)return new ValidationValues.Binary(binary.getType()&255,binary.getData());
        if(value instanceof Document document){var copied=new java.util.LinkedHashMap<String,Object>();document.forEach((key,child)->copied.put(key,portable(child,depth+1)));return copied;}
        if(value instanceof List<?> list){var copied=new ArrayList<Object>();for(var child:list)copied.add(portable(child,depth+1));return copied;}
        throw unreadable("report-payload-type");
    }
    private static void requireReportCapacity(Document report){
        long retained=encoded(report).length+number(report,"resultBytes")-countSummaryBytes(report);
        if(retained>MAX_REPORT_BYTES)throw MongoValidationObservationCodec.limit();
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
    private static void validateCount(Document row, Report report) {
        try {
            if (!row.keySet().equals(java.util.Set.of("formatVersion", "mappingId", "outputId", "sourceCount", "targetCount", "sourceRead", "targetRead")))
                throw unreadable("count-summary");
            if(!Integer.valueOf(1).equals(row.get("formatVersion")))throw unreadable("count-summary");
            new io.tapstate.core.validation.ValidationCountCheck(report.run(), row.getString("mappingId"), row.getString("outputId"),
                    number(row, "sourceCount"), number(row, "targetCount"), readBoundary(row.get("sourceRead", Document.class)),
                    readBoundary(row.get("targetRead", Document.class)));
        } catch (RuntimeException invalid) { throw unreadable("count-summary"); }
    }
    private static io.tapstate.core.validation.ValidationCheckEvent.ReadBoundary readBoundary(Document row) {
        if (row == null || !row.keySet().equals(java.util.Set.of("id", "contract", "startedAt", "finishedAt"))) throw unreadable("count-read");
        return new io.tapstate.core.validation.ValidationCheckEvent.ReadBoundary(row.getString("id"), row.getString("contract"),
                java.time.Instant.parse(row.getString("startedAt")), java.time.Instant.parse(row.getString("finishedAt")));
    }

    private static Document boundary(io.tapstate.core.validation.ValidationCheckEvent.ReadBoundary read) {
        return new Document("id", read.readId()).append("contract", read.contractVersion())
                .append("startedAt", read.startedAt().toString()).append("finishedAt", read.finishedAt().toString());
    }

    private Document report(ClientSession session, Report expected) {
        admission.requirePipeline(expected.run().owner().pipelineId());
        var row = reports.find(session, new Document("_id", MongoValidationAdmissionStore.reportId(expected.run()))).first();
        if (row == null) return null;
        if (!Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("admission") instanceof Document identity)
                || !(row.get("openVersion") instanceof Long version) || !(row.get("closed") instanceof Boolean)) throw unreadable("report");
        var start = MongoValidationAdmissionStore.decodeIdentity(identity);
        if (!start.report().equals(expected) || version != expected.openVersion()) return null;
        number(row, "publicationRevision"); number(row, "rootCount"); number(row, "resultBytes"); return row;
    }
    private static Document original(Report report, Document row) {
        return new Document("_id", MongoValidationAdmissionStore.reportId(report.run())).append("openVersion", report.openVersion()).append("admission", row.get("admission"));
    }
    private void requireDifferences() {
        var row = database.listCollections().filter(new Document("name", SystemCollections.VALIDATION_DIFFERENCES.collectionName())).first();
        if (row == null || !"collection".equals(row.get("type"))) throw unreadable("validation-differences");
    }
    private void requireCluster(Report report) {
        if (!clusterId.equals(report.run().owner().clusterId())) throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "report.clusterId"), null);
    }
    private static long number(Document row, String field) {
        if (!(row.get(field) instanceof Long value) || value < 0) throw unreadable(field); return value;
    }
    private static byte[] encoded(Document value) {
        try (var buffer = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(buffer)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); return buffer.toByteArray();
        }
    }
    private static byte[] hash(byte[] value) {
        try { return MessageDigest.getInstance("SHA-256").digest(value); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("required SHA-256 implementation missing", impossible); }
    }
    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "validation", "field", field), null);
    }
}
