package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterNativeViewDocumentsTest {
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("east", 1,
            new ExecutionProfile(1, Map.of("profile-protocol", "2", "haProfile", "PRODUCTION_HA", "bootstrapMinMembers", "3")));
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void commonHashIgnoresPublisherOrderAndHeartbeatButKeepsEveryMemberAndScopeField() {
        Map<String, ClusterNativeMember> first = new LinkedHashMap<>();
        first.put("a", member("a", "boot-a", "uuid-a", "a:5701"));
        first.put("b", member("b", "boot-b", "uuid-b", "b:5701"));
        Map<String, ClusterNativeMember> reversed = new LinkedHashMap<>();
        reversed.put("b", first.get("b")); reversed.put("a", first.get("a"));
        var one = view("a", first, 1, NOW);
        var another = view("b", reversed, 19, NOW.plusSeconds(2));
        assertThat(ClusterNativeViewDocuments.sameCohort(one, another)).isTrue();
        for (var changed : List.of(member("b", "next-boot", "uuid-b", "b:5701"),
                member("b", "boot-b", "next-uuid", "b:5701"), member("b", "boot-b", "uuid-b", "b:5702"))) {
            var changedMembers = new LinkedHashMap<>(first); changedMembers.put("b", changed);
            assertThat(ClusterNativeViewDocuments.hash(PROFILE, 4, changedMembers)).isNotEqualTo(one.cohortHash());
        }
        assertThat(ClusterNativeViewDocuments.hash(PROFILE, 5, first)).isNotEqualTo(one.cohortHash());
        assertThat(ClusterNativeViewDocuments.hash(new ClusterExecutionProfile("east", 2, PROFILE.profile()), 4, first))
                .isNotEqualTo(one.cohortHash());
        assertThat(ClusterNativeViewDocuments.hash(PROFILE, 4, Map.of("a", first.get("a")))).isNotEqualTo(one.cohortHash());
    }

    @Test
    void missingDuplicateAndFractionalMemberDataCannotBecomeAnEmptyOrPartialView() {
        var good = registry();
        var missing = copy(good);
        missing.get("nativeView", Document.class).getList("members", Document.class).getFirst().remove("memberAddress");
        var duplicateNode = copy(good);
        var rows = duplicateNode.get("nativeView", Document.class).getList("members", Document.class);
        rows.add(new Document(rows.getFirst()));
        var duplicateUuid = copy(good);
        duplicateUuid.get("nativeView", Document.class).getList("members", Document.class).get(1).put("memberUuid", "uuid-a");
        var fractional = copy(good);
        fractional.get("nativeView", Document.class).put("viewRevision", 1.5);
        var empty = copy(good);
        empty.get("nativeView", Document.class).put("members", List.of());
        var missingSchema = copy(good);
        missingSchema.get("nativeView", Document.class).remove("schemaVersion");
        for (var malformed : List.of(missing, duplicateNode, duplicateUuid, fractional, empty, missingSchema)) {
            assertThatThrownBy(() -> ClusterNativeViewDocuments.read(malformed)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> {
                        assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(failure.args()).containsKey("id").containsKey("field");
                    });
        }
    }

    @Test
    void duplicateAddressesCannotBeHiddenBehindACorrectCommonHash() {
        var malformed = registry();
        malformed.get("nativeView", Document.class).getList("members", Document.class).get(1).put("memberAddress", "a:5701");
        Map<String, ClusterNativeMember> duplicateAddresses = Map.of("a", member("a", "boot-a", "uuid-a", "a:5701"),
                "b", member("b", "boot-b", "uuid-b", "a:5701"));
        malformed.get("nativeView", Document.class).put("cohortHash", ClusterNativeViewDocuments.hash(PROFILE, 4, duplicateAddresses));
        assertThatThrownBy(() -> ClusterNativeViewDocuments.read(malformed)).isInstanceOfSatisfying(TapstateException.class,
                failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                    assertThat(failure.args()).containsKey("id").containsKey("field");
                });
    }

    @Test
    void aCallerHashOrForeignPublisherCannotQualifyStoredMemberFacts() {
        var forgedHash = registry();
        forgedHash.get("nativeView", Document.class).put("cohortHash", "0".repeat(64));
        var foreign = registry();
        foreign.put("bootId", "different-boot");
        for (var malformed : List.of(forgedHash, foreign, new Document("_id", "node-a").append("nativeView", null))) {
            assertThatThrownBy(() -> ClusterNativeViewDocuments.read(malformed)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    private static ClusterNativeMember member(String node, String boot, String uuid, String address) {
        return new ClusterNativeMember(new ClusterExecutionMember(node, boot, uuid), address);
    }

    private static ClusterNativeView view(String publisher, Map<String, ClusterNativeMember> members, long revision, Instant observed) {
        return new ClusterNativeView(new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.NODE_SESSION, publisher),
                new WorkloadOwner(publisher, "boot-" + publisher), 1, 0, 0, 1), PROFILE, 4, revision,
                ClusterNativeViewDocuments.hash(PROFILE, 4, members), observed, observed.plusSeconds(3), members);
    }

    private static Document registry() {
        Map<String, ClusterNativeMember> members = Map.of("a", member("a", "boot-a", "uuid-a", "a:5701"),
                "b", member("b", "boot-b", "uuid-b", "b:5701"));
        return new Document("_id", "node-a").append("clusterId", "east").append("nodeId", "a")
                .append("bootId", "boot-a").append("profileGeneration", 1L).append("hash", PROFILE.profile().hash())
                .append("nativeView", ClusterNativeViewDocuments.document(view("a", members, 1, NOW)));
    }

    private static Document copy(Document value) {
        Document result = new Document(value);
        Document nested = new Document(value.get("nativeView", Document.class));
        var members = nested.getList("members", Document.class);
        nested.put("members", members.stream().map(Document::new).collect(java.util.stream.Collectors.toCollection(ArrayList::new)));
        result.put("nativeView", nested);
        return result;
    }
}
