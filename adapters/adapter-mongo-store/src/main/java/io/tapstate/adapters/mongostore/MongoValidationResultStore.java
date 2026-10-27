package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ValidationResultStore;
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

/** Complete root replacement and closure share the original prepared CONFIG transaction domain. */
final class MongoValidationResultStore implements ValidationResultStore {
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
            if (count > MAX_STORED_ROOTS || total > MAX_REPORT_BYTES || version == Long.MAX_VALUE) throw MongoValidationObservationCodec.limit();
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
