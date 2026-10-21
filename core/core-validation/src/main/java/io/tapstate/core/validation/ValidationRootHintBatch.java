package io.tapstate.core.validation;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Owned key facts for an admitted direct one-to-one root mapping, never row images or source offsets.
 * The caller supplies actual old/new keys under a registered lossless key contract. Constructing this
 * value does not prove capture completeness, authenticate a sender, or admit a Nest child as a root.
 * External decoders must bound their own materialization before constructing this already-owned batch.
 */
public record ValidationRootHintBatch(ValidationRunIdentity run, Sender sender, long sequence,
                                      List<Change> changes) {
    public static final int MAX_CHANGES = 128;
    public static final long MAX_BYTES = 4L * 1024 * 1024;

    public enum ChangeKind { INSERT, UPDATE, DELETE }

    /** Binary subtype is part of the logical domain; other kinds use -1. */
    public record Domain(ValidationKey.Kind kind, int binarySubtype) {
        public Domain {
            Objects.requireNonNull(kind, "kind");
            if (kind == ValidationKey.Kind.BINARY ? binarySubtype < 0 || binarySubtype > 255
                    : binarySubtype != -1) throw ValidationError.invalid("hint.domain");
        }
        public Domain(ValidationKey.Kind kind) { this(kind, -1); }
        private boolean accepts(ValidationKey.Part part) {
            return part.kind() == kind && part.binarySubtype() == binarySubtype;
        }
    }

    /**
     * Fixed registration supplied by the control path after actual execution and mapping admission.
     * Identifiers are not credentials. A new capture tenure or routing/key contract needs a new run;
     * no field follows the latest Pipeline, source, member boot, or validation owner automatically.
     */
    public record Sender(String registrationId, String pipelineExecutionId, String sourceId, String tableId,
                         String nodeId, String bootId, long captureEpoch, long captureFence,
                         String routeRevision, String keyContractVersion, String mappingId, String outputId,
                         List<Domain> domains) {
        public Sender {
            registrationId = ValidationError.identifier(registrationId, "hint.registrationId");
            pipelineExecutionId = ValidationError.identifier(pipelineExecutionId, "hint.pipelineExecutionId");
            sourceId = ValidationError.identifier(sourceId, "hint.sourceId");
            tableId = ValidationError.identifier(tableId, "hint.tableId");
            nodeId = ValidationError.identifier(nodeId, "hint.nodeId");
            bootId = ValidationError.identifier(bootId, "hint.bootId");
            if (captureEpoch < 0 || captureFence < 0) throw ValidationError.invalid("hint.captureTenure");
            routeRevision = ValidationError.identifier(routeRevision, "hint.routeRevision");
            keyContractVersion = ValidationError.identifier(keyContractVersion, "hint.keyContractVersion");
            mappingId = ValidationError.identifier(mappingId, "hint.mappingId");
            outputId = ValidationError.identifier(outputId, "hint.outputId");
            if (domains == null || domains.isEmpty() || domains.size() > ValidationKey.MAX_COMPONENTS) throw ValidationError.invalid("hint.domains");
            for (Domain domain : domains) if (domain == null) throw ValidationError.invalid("hint.domains");
            domains = List.copyOf(domains);
        }
        public long retainedBytes() {
            return 512L + 64L * domains.size() + 2L * (registrationId.length() + pipelineExecutionId.length()
                    + sourceId.length() + tableId.length() + nodeId.length() + bootId.length()
                    + routeRevision.length() + keyContractVersion.length() + mappingId.length() + outputId.length());
        }
        private void check(ValidationKey key) {
            if (key == null) return;
            if (key.parts().size() != domains.size()) throw ValidationError.invalid("hint.keyDomain");
            for (int i = 0; i < domains.size(); i++) {
                if (!domains.get(i).accepts(key.parts().get(i))) throw ValidationError.invalid("hint.keyDomain");
            }
        }
    }

    /** UPDATE always carries both actual identities, even when they are equal. */
    public record Change(ChangeKind kind, ValidationKey before, ValidationKey after) {
        public Change {
            Objects.requireNonNull(kind, "kind");
            boolean valid = switch (kind) {
                case INSERT -> before == null && after != null;
                case UPDATE -> before != null && after != null;
                case DELETE -> before != null && after == null;
            };
            if (!valid) throw ValidationError.invalid("hint.changeKeys");
        }
        /** Conservative owned-key charge without exposing or copying binary payloads. */
        public long retainedBytes() {
            return 64L + (before == null ? 0 : before.retainedBytes()) + (after == null ? 0 : after.retainedBytes());
        }
    }

    public ValidationRootHintBatch {
        Objects.requireNonNull(run, "run"); Objects.requireNonNull(sender, "sender");
        if (run.mode() != ValidationMode.INCREMENTAL) throw ValidationError.invalid("hint.mode");
        ValidationError.positive(sequence, "hint.sequence");
        if (changes == null || changes.isEmpty() || changes.size() > MAX_CHANGES) {
            throw ValidationError.invalid("hint.changes");
        }
        long bytes = headerBytes(run, sender);
        for (Change change : changes) {
            if (change == null) throw ValidationError.invalid("hint.change");
            sender.check(change.before()); sender.check(change.after());
            bytes += change.retainedBytes();
            if (bytes > MAX_BYTES) throw ValidationError.invalid("hint.bytes");
        }
        changes = List.copyOf(changes);
    }

    public long retainedBytes() {
        long bytes = headerBytes(run, sender);
        for (Change change : changes) bytes += change.retainedBytes();
        return bytes;
    }

    /** Caller reserves this bounded batch plus digest/root-collection scratch separately from the queue. */
    public long materializationReservationBytes() {
        return retainedBytes() + 1024L * changes.size() + 4L * ValidationKey.MAX_COMPONENT_BYTES;
    }

    /** Only the admitted direct mapping may interpret these identities as complete roots. */
    public List<ValidationRoot> oneToOneRoots() {
        var keys = new LinkedHashSet<ValidationKey>();
        for (Change change : changes) {
            if (change.before() != null) keys.add(change.before());
            if (change.after() != null) keys.add(change.after());
        }
        var roots = new ArrayList<ValidationRoot>(keys.size());
        for (ValidationKey key : keys) roots.add(new ValidationRoot(sender.mappingId(), sender.outputId(), key, List.of()));
        return List.copyOf(roots);
    }

    /**
     * Bounded content identity for retries after run, sender and sequence have been checked separately.
     * It is not an authenticity signature or a source/target commit receipt. Typed length framing keeps
     * values distinct without string coercion or expanding a decimal exponent into a huge string.
     */
    public String contentFingerprint() {
        MessageDigest digest;
        try { digest = MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
        integer(digest, changes.size());
        for (Change change : changes) {
            integer(digest, change.kind().ordinal());
            key(digest, change.before()); key(digest, change.after());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void key(MessageDigest digest, ValidationKey key) {
        integer(digest, key == null ? -1 : key.parts().size());
        if (key == null) return;
        for (ValidationKey.Part part : key.parts()) {
            integer(digest, part.kind().ordinal()); integer(digest, part.binarySubtype());
            Object value = part.value();
            switch (part.kind()) {
                case EXACT_NUMBER -> {
                    BigDecimal number = (BigDecimal) value;
                    integer(digest, number.scale()); bytes(digest, number.unscaledValue().toByteArray());
                }
                case TEXT -> bytes(digest, ((String) value).getBytes(StandardCharsets.UTF_8));
                case BOOLEAN -> digest.update((byte) ((Boolean) value ? 1 : 0));
                case BINARY -> bytes(digest, (byte[]) value);
                case DATE -> number(digest, ((LocalDate) value).toEpochDay());
                case TIME -> number(digest, ((LocalTime) value).toNanoOfDay());
                case LOCAL_DATE_TIME -> {
                    LocalDateTime time = (LocalDateTime) value;
                    number(digest, time.toLocalDate().toEpochDay()); number(digest, time.toLocalTime().toNanoOfDay());
                }
                case INSTANT -> {
                    Instant time = (Instant) value;
                    number(digest, time.getEpochSecond()); integer(digest, time.getNano());
                }
            }
        }
    }
    private static void bytes(MessageDigest digest, byte[] bytes) { integer(digest, bytes.length); digest.update(bytes); }
    private static void integer(MessageDigest digest, int value) {
        for (int shift = 24; shift >= 0; shift -= 8) digest.update((byte) (value >>> shift));
    }
    private static void number(MessageDigest digest, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) digest.update((byte) (value >>> shift));
    }
    /** Conservative immutable header charge for bounded producers before they copy a change list. */
    public static long headerBytes(ValidationRunIdentity run, Sender sender) {
        ValidationOwner owner = run.owner();
        return 1024L + sender.retainedBytes() + 2L * (run.taskId().length() + run.runId().length()
                + run.specHash().length() + run.comparisonRulesVersion().length() + owner.clusterId().length()
                + owner.pipelineId().length() + owner.nodeId().length() + owner.bootId().length());
    }
}
