package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;

final class MongoResumeMetrics {
    private static final String FIELD = "resumeMetrics";
    private static final String UNKNOWN = "unavailableRevision";
    private static final int LIMIT = 1024 * 1024;
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MongoResumeMetrics.class);
    private MongoResumeMetrics() {}

    static long unavailableRevision(Document row) {
        var raw = row.get(FIELD);
        if (raw == null) return 0;
        if (raw instanceof Document value) {
            if (!value.containsKey(UNKNOWN)) return 0;
            if (value.get(UNKNOWN) instanceof Long revision && revision > 0
                    && value.get("jobId") instanceof Long && value.size() == 2) return revision;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", row.getString("_id"), "field", FIELD), null);
    }

    static ResumeMetrics.Handoff read(Document state) {
        String id = state.getString("_id");
        try {
            var value = state.get(FIELD, Document.class);
            if (value == null) return ResumeMetrics.Handoff.EMPTY;
            var active = value.get("applied", Document.class);
            var applied = active == null ? null : new ResumeMetrics.Binding(
                    baseline(active.get("baseline", Document.class), id), active.getLong("revision"),
                    WorkloadClaimDocuments.readPipeline(active.get("fence", Document.class)));
            return new ResumeMetrics.Handoff(applied, baseline(value.get("candidate", Document.class), id));
        } catch (ClassCastException | NullPointerException | IllegalArgumentException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", FIELD), invalid);
        }
    }

    private static ResumeMetrics.Baseline baseline(Document d, String id) {
        if (d == null) return null;
        return new ResumeMetrics.Baseline(d.getLong("revision"), d.getLong("epoch"),
                WorkloadClaimDocuments.readPipeline(d.get("origin", Document.class)), d.getLong("jobId"),
                MongoObservationStore.readFacts(d, id), d.getBoolean("unavailable"));
    }

    private static Document encode(ResumeMetrics.Baseline b) {
        return new Document("revision", b.pauseRevision()).append("epoch", b.epoch())
                .append("origin", WorkloadClaimDocuments.stored(b.origin())).append("jobId", b.jobId())
                .append("facts", b.facts().stream().map(MongoObservationStore::toDocument).toList())
                .append("unavailable", b.unavailable());
    }

    static void transition(MongoCachePipelineAccess access, ClientSession session, Document current,
            Document values, ResumeMetrics.Transition command) {
        if (command == null && current.get(FIELD) == null) return;
        if (unavailableRevision(current) != 0) {
            // An identity too large even for a loss marker cannot authorize any metric continuation.
            // Carry only unavailability through this resume. Ordinary transitions discard the marker.
            var lost = current.get(FIELD, Document.class);
            boolean nextResume = command != null && command.revision() == unavailableRevision(current) + 1
                    && (command.action() == ResumeMetrics.Action.PAUSE || command.jobId() == lost.getLong("jobId"));
            values.append(FIELD, nextResume ? unavailable(command.revision(), command.jobId()) : null);
            return;
        }
        var handoff = read(current);
        long epoch = current.getLong("epoch");
        List<MetricFact> facts = List.of();
        if (command != null && command.action() == ResumeMetrics.Action.PAUSE && !command.raw().isEmpty()
                && "RUNNING".equals(current.getString("stateJson"))) {
            facts = MongoCachePipelineObservationStore.pauseFacts(access, session, epoch);
        }
        write(current, values, handoff.transition(command, access.originalClaim(), epoch, facts));
    }

    static Document advance(Document current, WorkloadClaimFence before, WorkloadClaimFence after, long revision) {
        var values = new Document();
        if (unavailableRevision(current) != 0) {
            if (revision != unavailableRevision(current)) values.append(FIELD, null);
        } else if (current.get(FIELD) != null) write(current, values, read(current).advance(before, after, revision));
        return values;
    }

    private static void write(Document current, Document values, ResumeMetrics.Handoff handoff) {
        if (handoff.equals(ResumeMetrics.Handoff.EMPTY)) {
            if (current.get(FIELD) != null) values.append(FIELD, null);
            return;
        }
        var encoded = new Document();
        if (handoff.applied() != null) {
            var applied = handoff.applied();
            encoded.append("applied", new Document("baseline", encode(applied.baseline()))
                    .append("revision", applied.revision()).append("fence", WorkloadClaimDocuments.stored(applied.fence())));
        }
        if (handoff.candidate() != null) encoded.append("candidate", encode(handoff.candidate()));
        if (oversize(encoded)) {
            LOG.warn("Resume metric handoff unavailable", new TapstateException(IoError.DOCUMENT_TOO_LARGE,
                    Map.of("id", current.getString("_id")), null));
            if (handoff.candidate() != null) markUnavailable(encoded.get("candidate", Document.class));
            if (oversize(encoded) && handoff.applied() != null)
                markUnavailable(encoded.get("applied", Document.class).get("baseline", Document.class));
            if (oversize(encoded)) {
                var candidate = handoff.candidate();
                encoded = candidate != null ? unavailable(candidate.pauseRevision(), candidate.jobId())
                        : unavailable(handoff.applied().revision(), handoff.applied().baseline().jobId());
            }
        }
        values.append(FIELD, encoded);
    }

    private static Document unavailable(long revision, long jobId) {
        return new Document(UNKNOWN, revision).append("jobId", jobId);
    }

    private static boolean oversize(Document value) {
        return new RawBsonDocument(new Document(FIELD, value), new DocumentCodec()).getByteBuffer().remaining() > LIMIT;
    }

    private static void markUnavailable(Document baseline) {
        baseline.put("unavailable", true);
        baseline.put("facts", List.of());
    }
}
