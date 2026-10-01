package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.util.Map;

/** Exact BSON shape of the one bounded internal marker beside a pipeline checkpoint. */
final class StopReservationDocument {
    static final String FIELD = "stopReservation";
    static final int MAX_BYTES = 4 * 1024;

    private StopReservationDocument() { }

    static Document write(StopReservation reservation) {
        Document marker = new Document("token", reservation.token())
                .append("sourceEpoch", reservation.sourceEpoch())
                .append("reservedEpoch", reservation.reservedEpoch())
                .append("desired", desired(reservation.originalDesired()))
                .append("subject", subject(reservation.subject()));
        if (size(marker) > MAX_BYTES) {
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE,
                    Map.of("id", reservation.pipelineId()), null);
        }
        return marker;
    }

    static StopReservation read(String pipelineId, long checkpointEpoch, Document marker) {
        try {
            if (size(marker) > MAX_BYTES) { throw unreadable(pipelineId, FIELD); }
            String token = string(marker, "token", pipelineId, FIELD);
            long sourceEpoch = number(marker, "sourceEpoch", pipelineId, FIELD);
            long reservedEpoch = number(marker, "reservedEpoch", pipelineId, FIELD);
            if (reservedEpoch != checkpointEpoch) { throw unreadable(pipelineId, FIELD + ".reservedEpoch"); }
            DesiredState desired = desired(document(marker, "desired", pipelineId, FIELD), pipelineId);
            StopReservation.Subject subject = subject(document(marker, "subject", pipelineId, FIELD), pipelineId);
            return new StopReservation(pipelineId, token, sourceEpoch, reservedEpoch, desired, subject);
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException corrupt) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", FIELD), corrupt);
        }
    }

    private static Document desired(DesiredState desired) {
        return new Document("pipelineId", desired.pipelineId())
                .append("targetState", desired.targetState().name())
                .append("revision", desired.revision())
                .append("purgeState", desired.purgeState())
                .append("assemblyRevision", desired.assemblyRevision())
                .append("reassemble", desired.reassemble())
                .append("rebuiltAtStateEpoch", desired.rebuiltAtStateEpoch());
    }

    private static DesiredState desired(Document stored, String pipelineId) {
        String id = string(stored, "pipelineId", pipelineId, FIELD + ".desired");
        String state = string(stored, "targetState", pipelineId, FIELD + ".desired");
        String revision = string(stored, "revision", pipelineId, FIELD + ".desired");
        boolean purge = bool(stored, "purgeState", pipelineId, FIELD + ".desired");
        boolean reassemble = bool(stored, "reassemble", pipelineId, FIELD + ".desired");
        if (!stored.containsKey("assemblyRevision") || !stored.containsKey("rebuiltAtStateEpoch")) {
            throw unreadable(pipelineId, FIELD + ".desired");
        }
        Object assembly = stored.get("assemblyRevision");
        Object stamp = stored.get("rebuiltAtStateEpoch");
        if (assembly != null && !(assembly instanceof String)
                || stamp != null && !(stamp instanceof Long || stamp instanceof Integer)) {
            throw unreadable(pipelineId, FIELD + ".desired");
        }
        try {
            return new DesiredState(id, PipelineState.valueOf(state), revision, purge,
                    (String) assembly, reassemble, stamp == null ? null : ((Number) stamp).longValue());
        } catch (IllegalArgumentException invalid) {
            throw unreadable(pipelineId, FIELD + ".desired.targetState");
        }
    }

    private static Document subject(StopReservation.Subject subject) {
        return switch (subject) {
            case StopReservation.NoJob absent -> new Document("kind", "NO_JOB")
                    .append("clusterId", absent.clusterId())
                    .append("knownAuthority", absent.knownAuthority() == null
                            ? null : authority(absent.knownAuthority()));
            case StopReservation.ExistingJob old -> new Document("kind", "EXISTING_JOB")
                    .append("pipelineIncarnationId", old.pipelineIncarnationId())
                    .append("executionGeneration", old.executionGeneration())
                    .append("oldJob", new Document("clusterId", old.oldJob().clusterId())
                            .append("jobId", old.oldJob().jobId())
                            .append("bootId", old.oldJob().bootId()))
                    .append("authority", authority(old.authority()));
        };
    }

    private static StopReservation.Subject subject(Document stored, String pipelineId) {
        return switch (string(stored, "kind", pipelineId, FIELD + ".subject")) {
            case "NO_JOB" -> {
                if (!stored.containsKey("knownAuthority")) { throw unreadable(pipelineId, FIELD + ".subject.knownAuthority"); }
                Object value = stored.get("knownAuthority");
                if (value != null && !(value instanceof Document)) {
                    throw unreadable(pipelineId, FIELD + ".subject.knownAuthority");
                }
                yield new StopReservation.NoJob(
                        string(stored, "clusterId", pipelineId, FIELD + ".subject"),
                        value == null ? null : authority((Document) value, pipelineId));
            }
            case "EXISTING_JOB" -> {
                String incarnation = string(stored, "pipelineIncarnationId", pipelineId, FIELD + ".subject");
                long generation = number(stored, "executionGeneration", pipelineId, FIELD + ".subject");
                Document job = document(stored, "oldJob", pipelineId, FIELD + ".subject");
                StopReservation.JobIdentity identity = new StopReservation.JobIdentity(
                        string(job, "clusterId", pipelineId, FIELD + ".subject.oldJob"),
                        number(job, "jobId", pipelineId, FIELD + ".subject.oldJob"),
                        string(job, "bootId", pipelineId, FIELD + ".subject.oldJob"));
                yield new StopReservation.ExistingJob(incarnation, generation, identity,
                        authority(document(stored, "authority", pipelineId, FIELD + ".subject"), pipelineId));
            }
            default -> throw unreadable(pipelineId, FIELD + ".subject.kind");
        };
    }

    private static Document authority(StopAuthority authority) {
        Document result = new Document("clusterId", authority.clusterId())
                .append("executionGeneration", authority.executionGeneration());
        WorkloadClaimFence claim = authority.claim();
        result.append("claim", claim == null ? null : new Document("resourceType", claim.key().type().name())
                .append("resourceId", claim.key().resourceId())
                .append("ownerNodeId", claim.owner().nodeId())
                .append("ownerBootId", claim.owner().bootId())
                .append("claimGeneration", claim.claimGeneration())
                .append("topologyRevision", claim.topologyRevision()));
        return result;
    }

    private static StopAuthority authority(Document stored, String pipelineId) {
        String clusterId = string(stored, "clusterId", pipelineId, FIELD + ".authority");
        long generation = number(stored, "executionGeneration", pipelineId, FIELD + ".authority");
        if (!stored.containsKey("claim")) { throw unreadable(pipelineId, FIELD + ".authority.claim"); }
        Object rawClaim = stored.get("claim");
        if (rawClaim == null) { return StopAuthority.standalone(clusterId, generation); }
        if (!(rawClaim instanceof Document claim)) { throw unreadable(pipelineId, FIELD + ".authority.claim"); }
        WorkloadClaimType type;
        try {
            type = WorkloadClaimType.valueOf(string(claim, "resourceType", pipelineId, FIELD + ".authority.claim"));
        } catch (IllegalArgumentException invalid) {
            throw unreadable(pipelineId, FIELD + ".authority.claim.resourceType");
        }
        return StopAuthority.claimed(new WorkloadClaimFence(
                new WorkloadClaimKey(clusterId, type,
                        string(claim, "resourceId", pipelineId, FIELD + ".authority.claim")),
                new WorkloadOwner(
                        string(claim, "ownerNodeId", pipelineId, FIELD + ".authority.claim"),
                        string(claim, "ownerBootId", pipelineId, FIELD + ".authority.claim")),
                number(claim, "claimGeneration", pipelineId, FIELD + ".authority.claim"), generation,
                number(claim, "topologyRevision", pipelineId, FIELD + ".authority.claim")));
    }

    private static Document document(Document source, String field, String id, String prefix) {
        if (!(source.get(field) instanceof Document value)) { throw unreadable(id, prefix + "." + field); }
        return value;
    }

    private static String string(Document source, String field, String id, String prefix) {
        if (!(source.get(field) instanceof String value) || value.isBlank()) {
            throw unreadable(id, prefix + "." + field);
        }
        return value;
    }

    private static long number(Document source, String field, String id, String prefix) {
        Object raw = source.get(field);
        if (!(raw instanceof Long || raw instanceof Integer)) { throw unreadable(id, prefix + "." + field); }
        return ((Number) raw).longValue();
    }

    private static boolean bool(Document source, String field, String id, String prefix) {
        if (!(source.get(field) instanceof Boolean value)) { throw unreadable(id, prefix + "." + field); }
        return value;
    }

    private static int size(Document marker) {
        try (BasicOutputBuffer output = new BasicOutputBuffer();
                BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, marker, EncoderContext.builder().build());
            return output.getSize();
        }
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }
}
