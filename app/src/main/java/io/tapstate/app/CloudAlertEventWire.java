package io.tapstate.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmSeverity;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.DeliveryKey;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel.Destination;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Encodes frozen, already authorized identities; no arbitrary message, secret or business row is accepted. */
final class CloudAlertEventWire {
    static final int VERSION = 1;
    static final int MAX_BYTES = 64 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper();
    private CloudAlertEventWire() { }

    static byte[] encode(URI origin, Destination destination, DeliveryKey key, AlarmKey alarm,
            Instant occurredAt, AlarmSeverity severity, String safeSubject, boolean recoverySummary) {
        Objects.requireNonNull(origin); Objects.requireNonNull(destination); Objects.requireNonNull(key);
        Objects.requireNonNull(alarm); Objects.requireNonNull(occurredAt); Objects.requireNonNull(severity);
        if (!destination.scope().equals(key.notification().scope()) || !alarm.scope().equals(destination.scope())
                || !destination.channelType().equals("cloud-sdk") || !key.channelType().equals("cloud-sdk")
                || !destination.receiverIncarnation().equals(key.receiverIncarnation()) || !destination.targetId().equals(key.targetId())
                || key.notification().kind() == DeliveryKey.Kind.EVENT
                || (recoverySummary && key.notification().kind() != DeliveryKey.Kind.RECOVERY)) { throw invalid("eventBinding"); }
        Destination.validateIdentity(safeSubject, "eventSubject");
        String kind = key.notification().kind().name().toLowerCase(Locale.ROOT);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("wireVersion", VERSION); payload.put("alarmTypeId", alarm.typeId());
        payload.put("resourceKind", alarm.resource().kind()); payload.put("resourceId", alarm.resource().id());
        payload.put("resourceIncarnation", alarm.resource().incarnation()); payload.put("qualifier", alarm.qualifier());
        payload.put("subjectId", key.notification().subjectId()); payload.put("subjectIncarnation", key.notification().subjectIncarnation());
        payload.put("notificationKind", kind); payload.put("round", key.notification().round());
        payload.put("originalSeverity", severity.name()); payload.put("recoverySummary", recoverySummary);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "cluster.alert." + alarm.typeId() + "." + kind);
        event.put("severity", cloudSeverity(key.notification().kind(), severity));
        event.put("subject", safeSubject); event.put("occurredAt", occurredAt.toString());
        event.put("dedupeKey", dedupe(origin, destination, key)); event.put("payload", payload);
        try {
            byte[] bytes = JSON.writeValueAsBytes(event);
            if (bytes.length > MAX_BYTES) { throw invalid("eventPayload"); }
            return bytes;
        } catch (com.fasterxml.jackson.core.JsonProcessingException bug) { throw new IllegalStateException("fixed event encoding failed", bug); }
    }
    static String cloudSeverity(DeliveryKey.Kind kind, AlarmSeverity severity) {
        if (kind == DeliveryKey.Kind.RECOVERY) { return "INFO"; }
        return severity == AlarmSeverity.CRITICAL || severity == AlarmSeverity.MAJOR ? "CRITICAL" : "WARNING";
    }
    static String dedupe(URI origin, Destination destination, DeliveryKey key) {
        var n = key.notification();
        List<String> fields = new ArrayList<>(List.of("cloud-alert-v1", origin.normalize().toASCIIString(),
                n.scope().authorityId(), n.scope().clusterId(), n.subjectId(), n.subjectIncarnation(), n.routeId(), n.routeIncarnation(),
                n.kind().name(), Long.toString(n.round()), key.channelType(), key.receiverIncarnation(), key.targetId(), destination.bindingId()));
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            for (String field : fields) { byte[] b = field.getBytes(StandardCharsets.UTF_8); hash.update(ByteBuffer.allocate(4).putInt(b.length).array()); hash.update(b); }
            return java.util.HexFormat.of().formatHex(hash.digest());
        } catch (NoSuchAlgorithmException bug) { throw new IllegalStateException("required SHA-256 unavailable", bug); }
    }
    static TapstateException invalid(String field) {
        return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", field, "reason", "invalid-cloud-event"), null);
    }
}
