package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
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
        if (reservation.legacy()) { return writeLegacy(reservation); }
        StopReservation.Source source = reservation.source();
        Document origin = new Document("c", source.clusterId())
                .append("i", source.scope() == null ? null : source.scope().pipelineIncarnationId())
                .append("g", source.scope() == null ? null : source.scope().executionGeneration())
                .append("j", source.oldJob() == null ? null : new Document("i", source.oldJob().jobId())
                        .append("b", source.oldJob().bootId()));
        StopAuthority authority = reservation.writerAuthority();
        Document writer = authority == null ? null : new Document("g", authority.executionGeneration());
        if (writer != null && authority.claim() != null) {
            WorkloadClaimFence claim = authority.claim();
            writer.append("n", claim.owner().nodeId()).append("b", claim.owner().bootId())
                    .append("c", claim.claimGeneration()).append("r", claim.topologyRevision());
        }
        StopReservation.Successor next = reservation.successor();
        Document target = next == null ? null : new Document("i", next.scope().pipelineIncarnationId())
                .append("g", next.scope().executionGeneration()).append("b", next.submissionBootId())
                .append("j", next.job() == null ? null : next.job().jobId());
        DesiredState intent = reservation.originalDesired();
        Document wanted = new Document("t", intent.targetState().name()).append("r", intent.revision())
                .append("u", intent.purgeState()).append("a", intent.assemblyRevision())
                .append("b", intent.reassemble()).append("z", intent.rebuiltAtStateEpoch());
        Document marker = new Document("v", StopReservation.CURRENT_FORMAT).append("t", reservation.token())
                .append("s", reservation.sourceEpoch()).append("e", reservation.reservedEpoch())
                .append("p", reservation.phase().name()).append("c", reservation.counterPolicy().name())
                .append("d", wanted).append("o", origin).append("w", writer).append("x", target);
        requireSize(marker, reservation.pipelineId());
        return marker;
    }

    static StopReservation read(String pipelineId, long checkpointEpoch, Document marker) {
        if (!marker.containsKey("v")) { return readLegacy(pipelineId, checkpointEpoch, marker); }
        try {
            if (size(marker) > MAX_BYTES || number(marker, "v", pipelineId, FIELD) != StopReservation.CURRENT_FORMAT) {
                throw unreadable(pipelineId, FIELD);
            }
            long epoch = number(marker, "e", pipelineId, FIELD);
            if (epoch != checkpointEpoch) { throw unreadable(pipelineId, FIELD + ".e"); }
            Document origin = document(marker, "o", pipelineId, FIELD);
            String cluster = string(origin, "c", pipelineId, FIELD + ".o");
            requireFields(origin, pipelineId, "i", "g", "j");
            ObservationStore.Scope scope = null;
            if (origin.get("i") != null || origin.get("g") != null) {
                scope = new ObservationStore.Scope(string(origin, "i", pipelineId, FIELD + ".o"),
                        number(origin, "g", pipelineId, FIELD + ".o"));
            }
            Document old = optionalDocument(origin, "j", pipelineId);
            StopReservation.JobIdentity oldJob = old == null ? null : new StopReservation.JobIdentity(cluster,
                    number(old, "i", pipelineId, FIELD + ".o.j"), string(old, "b", pipelineId, FIELD + ".o.j"));
            Document writer = optionalDocument(marker, "w", pipelineId);
            StopAuthority current = null;
            if (writer != null) {
                long generation = number(writer, "g", pipelineId, FIELD + ".w");
                if (writer.containsKey("n")) {
                    current = StopAuthority.claimed(new WorkloadClaimFence(
                            new WorkloadClaimKey(cluster, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId),
                            new WorkloadOwner(string(writer, "n", pipelineId, FIELD + ".w"),
                                    string(writer, "b", pipelineId, FIELD + ".w")),
                            number(writer, "c", pipelineId, FIELD + ".w"), generation,
                            number(writer, "r", pipelineId, FIELD + ".w")));
                } else {
                    if (writer.containsKey("b") || writer.containsKey("c") || writer.containsKey("r")) {
                        throw unreadable(pipelineId, FIELD + ".w");
                    }
                    current = StopAuthority.standalone(cluster, generation);
                }
            }
            Document target = optionalDocument(marker, "x", pipelineId);
            StopReservation.Successor successor = null;
            if (target != null) {
                String boot = string(target, "b", pipelineId, FIELD + ".x");
                requireFields(target, pipelineId, "j");
                StopReservation.JobIdentity job = target.get("j") == null ? null : new StopReservation.JobIdentity(
                        cluster, number(target, "j", pipelineId, FIELD + ".x"), boot);
                successor = new StopReservation.Successor(new ObservationStore.Scope(
                        string(target, "i", pipelineId, FIELD + ".x"),
                        number(target, "g", pipelineId, FIELD + ".x")), boot, job);
            }
            Document wanted = document(marker, "d", pipelineId, FIELD);
            requireFields(wanted, pipelineId, "a", "z");
            Object assembly = wanted.get("a"), stamp = wanted.get("z");
            if (assembly != null && !(assembly instanceof String)
                    || stamp != null && !(stamp instanceof Long || stamp instanceof Integer)) {
                throw unreadable(pipelineId, FIELD + ".d");
            }
            DesiredState intent = new DesiredState(pipelineId,
                    PipelineState.valueOf(string(wanted, "t", pipelineId, FIELD + ".d")),
                    string(wanted, "r", pipelineId, FIELD + ".d"), bool(wanted, "u", pipelineId, FIELD + ".d"),
                    (String) assembly, bool(wanted, "b", pipelineId, FIELD + ".d"),
                    stamp == null ? null : ((Number) stamp).longValue());
            return new StopReservation(pipelineId, string(marker, "t", pipelineId, FIELD),
                    number(marker, "s", pipelineId, FIELD), epoch, intent,
                    new StopReservation.Source(cluster, scope, oldJob),
                    StopReservation.Phase.valueOf(string(marker, "p", pipelineId, FIELD)),
                    StopReservation.CounterPolicy.valueOf(string(marker, "c", pipelineId, FIELD)),
                    current, successor, StopReservation.CURRENT_FORMAT);
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException corrupt) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId, "field", FIELD), corrupt);
        }
    }

    private static Document optionalDocument(Document source, String field, String id) {
        requireFields(source, id, field);
        Object value = source.get(field);
        if (value != null && !(value instanceof Document)) { throw unreadable(id, FIELD + "." + field); }
        return (Document) value;
    }

    private static void requireFields(Document source, String id, String... fields) {
        for (String field : fields) { if (!source.containsKey(field)) { throw unreadable(id, FIELD + "." + field); } }
    }

    private static void requireSize(Document marker, String id) {
        if (size(marker) > MAX_BYTES) {
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), null);
        }
    }

    private static Document writeLegacy(StopReservation reservation) {
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

    private static StopReservation readLegacy(String pipelineId, long checkpointEpoch, Document marker) {
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
