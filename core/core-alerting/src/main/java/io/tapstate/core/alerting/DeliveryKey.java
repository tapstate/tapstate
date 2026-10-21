package io.tapstate.core.alerting;

import java.util.Objects;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** The complete target key; attempt ids and mutable presentation never participate. */
public record DeliveryKey(NotificationRound notification, String channelType,
        String receiverIncarnation, String targetId) {
    public enum Kind { FAULT, REMINDER, RECOVERY, EVENT }
    public record NotificationRound(AlarmKey.Scope scope, String subjectId, String subjectIncarnation,
            String routeId, String routeIncarnation, Kind kind, long round) {
        public static NotificationRound forEpisode(AlarmKey key, long episode, String routeId,
                String routeIncarnation, Kind kind, long round) {
            Objects.requireNonNull(key, "key");
            AlertValidation.require(episode > 0 && kind != Kind.EVENT, "episodeSubject", "invalid-episode-or-kind");
            String subject = "episode-" + fingerprint(List.of(key.scope().authorityId(), key.scope().clusterId(),
                    key.resource().kind(), key.resource().id(), key.resource().incarnation(), key.typeId(), key.qualifier()));
            return new NotificationRound(key.scope(), subject, Long.toString(episode), routeId, routeIncarnation, kind, round);
        }
        public static NotificationRound forGroup(AlarmKey.Scope scope, String groupId, String groupIncarnation,
                long batch, String routeId, String routeIncarnation, Kind kind, long round) {
            Objects.requireNonNull(scope, "scope");
            AlertValidation.text(groupId, "groupId", 256); AlertValidation.text(groupIncarnation, "groupIncarnation", 256);
            AlertValidation.require(batch > 0 && kind != Kind.EVENT, "groupBatch", "invalid-batch-or-kind");
            String subject = "group-" + fingerprint(List.of(scope.authorityId(), scope.clusterId(), groupId, groupIncarnation));
            return new NotificationRound(scope, subject, Long.toString(batch), routeId, routeIncarnation, kind, round);
        }
        public NotificationRound {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(kind, "kind");
            subjectId = AlertValidation.text(subjectId, "notificationSubject", 256);
            subjectIncarnation = AlertValidation.text(subjectIncarnation, "subjectIncarnation", 256);
            routeId = AlertValidation.text(routeId, "routeId", 256);
            routeIncarnation = AlertValidation.text(routeIncarnation, "routeIncarnation", 256);
            AlertValidation.require(round > 0, "notificationRound", "must-be-positive");
        }
    }
    public DeliveryKey {
        Objects.requireNonNull(notification, "notification");
        channelType = AlertValidation.text(channelType, "channelType", 64);
        receiverIncarnation = AlertValidation.text(receiverIncarnation, "receiverIncarnation", 256);
        targetId = AlertValidation.text(targetId, "targetId", 256);
    }
    private static String fingerprint(List<String> fields) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : fields) {
                byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array()); digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException ex) { throw new IllegalStateException("required identity digest unavailable", ex); }
    }
}
