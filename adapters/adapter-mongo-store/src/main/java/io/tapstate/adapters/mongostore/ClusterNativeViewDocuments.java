package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNativeMember;
import io.tapstate.spi.store.ClusterNativeView;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Strict bounded native-view storage; publisher metadata never changes the common cohort hash. */
final class ClusterNativeViewDocuments {
    private ClusterNativeViewDocuments() { }

    static String hash(ClusterExecutionProfile profile, long topologyRevision,
            Map<String, ClusterNativeMember> membersByNode) {
        StringBuilder value = new StringBuilder("native-view:1;");
        field(value, profile.clusterId());
        field(value, Long.toString(profile.generation()));
        field(value, profile.profile().hash());
        field(value, Long.toString(topologyRevision));
        field(value, Integer.toString(membersByNode.size()));
        new TreeMap<>(membersByNode).forEach((nodeId, member) -> {
            field(value, nodeId);
            field(value, member.identity().bootId());
            field(value, member.identity().memberUuid());
            field(value, member.memberAddress());
        });
        return CanonicalHash.ofText(value.toString());
    }

    static Document document(ClusterNativeView view) {
        if (!hash(view.profile(), view.topologyRevision(), view.membersByNode()).equals(view.cohortHash())) {
            throw new IllegalArgumentException("native view hash must describe its complete common scope");
        }
        List<Document> members = new TreeMap<>(view.membersByNode()).values().stream().map(member ->
                new Document("nodeId", member.identity().nodeId()).append("bootId", member.identity().bootId())
                        .append("memberUuid", member.identity().memberUuid()).append("memberAddress", member.memberAddress())).toList();
        return new Document("schemaVersion", 1).append("publisherClaim", WorkloadClaimDocuments.stored(view.publisherClaim()))
                .append("profile", ClusterRecoveryDocuments.profile(view.profile()))
                .append("topologyRevision", view.topologyRevision()).append("viewRevision", view.viewRevision())
                .append("cohortHash", view.cohortHash()).append("observedAt", Date.from(view.observedAt()))
                .append("validUntil", Date.from(view.validUntil())).append("members", members);
    }

    static ClusterNativeView read(Document registry) {
        try {
            Document view = required(registry, "nativeView", registry);
            if (number(view, "schemaVersion", registry) != 1) {
                throw unreadable(registry, "nativeView.schemaVersion", null);
            }
            Document publisher = required(view, "publisherClaim", registry);
            if (!WorkloadClaimType.NODE_SESSION.name().equals(string(publisher, "resourceType", registry))) {
                throw unreadable(registry, "nativeView.publisherClaim.resourceType", null);
            }
            WorkloadClaimFence fence = new WorkloadClaimFence(new WorkloadClaimKey(
                    string(publisher, "clusterId", registry), WorkloadClaimType.NODE_SESSION,
                    string(publisher, "resourceId", registry)), new WorkloadOwner(
                    string(publisher, "ownerNodeId", registry), string(publisher, "ownerBootId", registry)),
                    number(publisher, "claimGeneration", registry), number(publisher, "executionGeneration", registry),
                    number(publisher, "topologyRevision", registry), number(publisher, "profileGeneration", registry));
            Document rawProfile = required(view, "profile", registry);
            Document rawAttributes = required(rawProfile, "attributes", registry);
            Map<String, String> attributes = new TreeMap<>();
            rawAttributes.forEach((key, value) -> {
                if (!(value instanceof String text)) { throw unreadable(registry, "nativeView.profile.attributes", null); }
                attributes.put(key, text);
            });
            ExecutionProfile immutable = new ExecutionProfile(Math.toIntExact(number(rawProfile, "formatVersion", registry)), attributes);
            if (!immutable.hash().equals(string(rawProfile, "hash", registry))) { throw unreadable(registry, "nativeView.profile.hash", null); }
            ClusterExecutionProfile profile = new ClusterExecutionProfile(string(rawProfile, "clusterId", registry),
                    number(rawProfile, "generation", registry), immutable);
            Map<String, ClusterNativeMember> members = new LinkedHashMap<>();
            Object rawMembers = view.get("members");
            if (!(rawMembers instanceof List<?> rows)) {
                throw unreadable(registry, "nativeView.members", null);
            }
            for (Object raw : rows) {
                if (!(raw instanceof Document row)) {
                    throw unreadable(registry, "nativeView.members", null);
                }
                String nodeId = string(row, "nodeId", registry);
                ClusterNativeMember member = new ClusterNativeMember(new ClusterExecutionMember(nodeId,
                        string(row, "bootId", registry), string(row, "memberUuid", registry)),
                        string(row, "memberAddress", registry));
                if (members.putIfAbsent(nodeId, member) != null) {
                    throw unreadable(registry, "nativeView.members.duplicateNode", null);
                }
            }
            ClusterNativeView restored = new ClusterNativeView(fence, profile,
                    number(view, "topologyRevision", registry), number(view, "viewRevision", registry),
                    string(view, "cohortHash", registry), date(view, "observedAt", registry).toInstant(),
                    date(view, "validUntil", registry).toInstant(), members);
            if (!hash(profile, restored.topologyRevision(), members).equals(restored.cohortHash())) {
                throw unreadable(registry, "nativeView.cohortHash", null);
            }
            if (!fence.key().clusterId().equals(string(registry, "clusterId", registry))
                    || !fence.owner().nodeId().equals(string(registry, "nodeId", registry))
                    || !fence.owner().bootId().equals(string(registry, "bootId", registry))
                    || profile.generation() != number(registry, "profileGeneration", registry)
                    || !profile.profile().hash().equals(string(registry, "hash", registry))) {
                throw unreadable(registry, "nativeView.publisherScope", null);
            }
            return restored;
        } catch (RuntimeException invalid) {
            if (invalid instanceof TapstateException coded) { throw coded; }
            throw unreadable(registry, "nativeView", invalid);
        }
    }

    static boolean sameCohort(ClusterNativeView first, ClusterNativeView second) {
        return first.profile().equals(second.profile()) && first.topologyRevision() == second.topologyRevision()
                && first.cohortHash().equals(second.cohortHash()) && first.membersByNode().equals(second.membersByNode());
    }

    private static void field(StringBuilder out, String value) {
        out.append(value.getBytes(StandardCharsets.UTF_8).length).append(':').append(value);
    }

    private static Document required(Document value, String field, Document registry) {
        Object found = value.get(field);
        if (!(found instanceof Document document)) { throw unreadable(registry, "nativeView." + field, null); }
        return document;
    }

    private static String string(Document value, String field, Document registry) {
        Object found = value.get(field);
        if (!(found instanceof String text) || text.isBlank()) { throw unreadable(registry, "nativeView." + field, null); }
        return text;
    }

    private static long number(Document value, String field, Document registry) {
        Object found = value.get(field);
        if (!(found instanceof Integer) && !(found instanceof Long)) { throw unreadable(registry, "nativeView." + field, null); }
        return ((Number) found).longValue();
    }

    private static Date date(Document value, String field, Document registry) {
        Object found = value.get(field);
        if (!(found instanceof Date date)) { throw unreadable(registry, "nativeView." + field, null); }
        return date;
    }

    private static TapstateException unreadable(Document registry, String field, Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(registry.get("_id")), "field", field), cause);
    }
}
