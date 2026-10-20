package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Fixed counters from a failed prerequisite, collected before releasing its owned schema barrier. */
final class CoordinatorBlockEvidence {
    private CoordinatorBlockEvidence() {}

    static void retain(MongoClient client, MongoDatabase store, String cluster, String pipeline, String key,
            Collection<ClusterMemberView> survivors, DerivedSchema original, Map<String, String> columns,
            long executionGeneration) {
        retain(client, store, cluster, pipeline, key, survivors, original, columns, executionGeneration, null);
    }

    record Arm(Long controllerGeneration, Long topologyRevision, Date retirementHorizon) { }

    static Arm arm(MongoDatabase store, String cluster, String pipeline) {
        try {
            Document claim = SystemCollections.WORKLOAD_CLAIMS.on(store).find(new Document("clusterId", cluster)
                    .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name()).append("resourceId", pipeline)).first();
            return claim == null ? null : new Arm(integer(claim.get("claimGeneration")), integer(claim.get("topologyRevision")),
                    claim.get("retiredAuthorizationUntil") instanceof Date date ? date : null);
        } catch (RuntimeException unavailable) { return null; }
    }

    static void retain(MongoClient client, MongoDatabase store, String cluster, String pipeline, String key,
            Collection<ClusterMemberView> survivors, DerivedSchema original, Map<String, String> columns,
            long executionGeneration, Arm arm) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("schema", 1);
        // These are sequential reads after the failed wait, not a snapshot of its last poll.
        evidence.put("afterFailedWait", true);
        try {
            List<String> tags = survivors.stream().map(member -> "tapstate/" + member.bootId()).toList();
            String application = "(?:^| )(?:" + tags.stream().map(java.util.regex.Pattern::quote)
                    .collect(java.util.stream.Collectors.joining("|")) + ")$";
            var operations = client.getDatabase("admin").aggregate(List.of(
                    new Document("$currentOp", new Document("allUsers", true).append("idleConnections", false)),
                    new Document("$match", new Document("active", true).append("appName", new Document("$regex", application)))))
                    .into(new ArrayList<>());
            String schemaNamespace = SystemCollections.DERIVED_SCHEMAS.on(store).getNamespace().getFullName();
            evidence.put("ownedOperationDomains", operationDomains(operations, store.getName(), schemaNamespace));
            evidence.put("operations", operationCounts(operations.stream().filter(operation -> schemaNamespace.equals(operation.get("ns")))
                    .toList(), tags, key, original, columns));
            Document queue = SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store).find(
                    new Document("clusterId", cluster).append("pipelineId", pipeline)).first();
            evidence.put("queue", queueFacts(queue));
            Document claim = SystemCollections.WORKLOAD_CLAIMS.on(store).find(new Document("clusterId", cluster)
                    .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name()).append("resourceId", pipeline)).first();
            evidence.put("claimPresent", claim != null);
            evidence.put("executionUnchanged", claim != null && claim.get("executionGeneration") instanceof Number value
                    && value.longValue() == executionGeneration);
            evidence.put("arm", armFacts(arm, claim));
            Date observedAt = client.getDatabase("admin").runCommand(new Document("hello", 1)).getDate("localTime");
            List<Document> claims = SystemCollections.WORKLOAD_CLAIMS.on(store)
                    .find(new Document("clusterId", cluster)).into(new ArrayList<>());
            List<Document> registry = SystemCollections.CLUSTER_NODE_REGISTRY.on(store)
                    .find(new Document("clusterId", cluster)).into(new ArrayList<>());
            Set<String> expectedNodes = survivors.stream().map(ClusterMemberView::nodeId)
                    .collect(java.util.stream.Collectors.toSet());
            Document recoveryClaim = claims.stream().filter(value -> WorkloadClaimType.CLUSTER_RECOVERY.name()
                    .equals(value.getString("resourceType")) && cluster.equals(value.getString("resourceId")))
                    .findFirst().orElse(null);
            Document committed = SystemCollections.CLUSTER_MEMBERSHIP.on(store).find(new Document("_id", cluster)).first();
            evidence.put("admission", admissionFacts(observedAt, queue, claim, recoveryClaim, claims, registry, committed, expectedNodes));
            evidence.put("captureComplete", true);
        } catch (RuntimeException unavailable) {
            evidence.put("captureComplete", false);
        }
        write("coordinator-precondition", evidence);
    }

    static Map<String, Boolean> armFacts(Arm arm, Document claim) {
        Map<String, Boolean> facts = new LinkedHashMap<>();
        facts.put("snapshotPresent", arm != null);
        facts.put("retirementComparable", arm != null && arm.retirementHorizon() != null && claim != null
                && claim.get("retiredAuthorizationUntil") instanceof Date);
        facts.put("retirementMoved", arm != null && arm.retirementHorizon() != null && claim != null
                && claim.get("retiredAuthorizationUntil") instanceof Date date && date.after(arm.retirementHorizon()));
        facts.put("controllerComparable", arm != null && arm.controllerGeneration() != null && claim != null
                && integer(claim.get("claimGeneration")) != null);
        facts.put("controllerChanged", arm != null && arm.controllerGeneration() != null && claim != null
                && integer(claim.get("claimGeneration")) != null && !arm.controllerGeneration().equals(integer(claim.get("claimGeneration"))));
        facts.put("topologyComparable", arm != null && arm.topologyRevision() != null && claim != null
                && integer(claim.get("topologyRevision")) != null);
        facts.put("topologyChanged", arm != null && arm.topologyRevision() != null && claim != null
                && integer(claim.get("topologyRevision")) != null && !arm.topologyRevision().equals(integer(claim.get("topologyRevision"))));
        return facts;
    }

    private static Long integer(Object value) {
        return value instanceof Integer || value instanceof Long ? ((Number) value).longValue() : null;
    }

    /** Coarse sequential observations, never an independent implementation of the admission guard. */
    static Map<String, Object> admissionFacts(Date now, Document queue, Document pipeline, Document recovery,
            List<Document> claims, List<Document> registry, Document committed, Set<String> expected) {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("serverClockPresent", now != null);
        facts.put("pipelineLeasePresent", pipeline != null && pipeline.get("leaseUntil") instanceof Date);
        facts.put("pipelineLeaseLive", after(pipeline, "leaseUntil", now));
        facts.put("pipelineControlAdvanced", pipeline != null && pipeline.get("claimGeneration") instanceof Number control
                && pipeline.get("executionClaimGeneration") instanceof Number execution && control.longValue() > execution.longValue());
        facts.put("pipelineControlAdvancedRepeatedly", pipeline != null && pipeline.get("claimGeneration") instanceof Number control
                && pipeline.get("executionClaimGeneration") instanceof Number execution && control.longValue() - execution.longValue() > 1);
        facts.put("failureVerdictPresent", pipeline != null && pipeline.get("failureClaimGeneration") instanceof Number failure
                && failure.longValue() > 0);
        facts.put("failureAfterMemberLoss", pipeline != null && Boolean.TRUE.equals(pipeline.get("failureAfterMemberLoss")));
        facts.put("pipelineOwnerExpected", pipeline != null && expected.contains(pipeline.getString("ownerNodeId")));
        Document currentPipeline = pipeline == null ? null : claims.stream().filter(value ->
                java.util.Objects.equals(value.get("resourceType"), pipeline.get("resourceType"))
                        && java.util.Objects.equals(value.get("resourceId"), pipeline.get("resourceId"))).findFirst().orElse(null);
        facts.put("pipelineAuthorityStableAcrossReads", pipeline != null && currentPipeline != null && sameAuthority(pipeline, currentPipeline));
        facts.put("retirementHorizonPresent", pipeline != null && pipeline.get("retiredAuthorizationUntil") instanceof Date);
        facts.put("retirementDue", due(pipeline, "retiredAuthorizationUntil", now));
        facts.put("queueEligibilityPresent", queue != null && queue.get("nextEligibleAt") instanceof Date);
        facts.put("queueEligible", queue != null && (queue.get("nextEligibleAt") == null || due(queue, "nextEligibleAt", now)));
        facts.put("recoveryClaimPresent", recovery != null);
        facts.put("recoveryLeaseLive", after(recovery, "leaseUntil", now));
        facts.put("recoveryOwnerExpected", recovery != null && expected.contains(recovery.getString("ownerNodeId")));
        int present = 0, fresh = 0, exact = 0, publishers = 0, scoped = 0;
        List<Document> views = new ArrayList<>();
        for (String node : expected) {
            Document row = registry.stream().filter(value -> node.equals(value.getString("nodeId"))).findFirst().orElse(null);
            if (row == null || !(row.get("nativeView") instanceof Document view)) continue;
            present++;
            views.add(view);
            if (after(view, "validUntil", now)) fresh++;
            if (committed != null && view.get("profile") instanceof Document profile
                    && sameNumber(view.get("topologyRevision"), committed.get("revision"))
                    && sameNumber(profile.get("generation"), committed.get("profileGeneration"))
                    && sameNumber(profile.get("generation"), row.get("profileGeneration"))
                    && profile.get("hash") != null && profile.get("hash").equals(row.get("hash"))) scoped++;
            if (view.get("members") instanceof List<?> members) {
                Set<String> ids = new java.util.HashSet<>();
                boolean valid = true;
                for (Object member : members) {
                    if (!(member instanceof Document item) || !(item.get("nodeId") instanceof String id) || !ids.add(id)) valid = false;
                }
                if (valid && members.size() == expected.size() && ids.equals(expected)) exact++;
            }
            Document current = claims.stream().filter(value -> WorkloadClaimType.NODE_SESSION.name().equals(value.getString("resourceType"))
                    && node.equals(value.getString("resourceId"))).findFirst().orElse(null);
            if (view.get("publisherClaim") instanceof Document publisher && current != null
                    && after(current, "leaseUntil", now) && samePublisher(publisher, current)) publishers++;
        }
        facts.put("expectedPublisherCount", expected.size());
        facts.put("nativeViewPresentCount", present);
        facts.put("nativeViewFreshCount", fresh);
        facts.put("nativeViewExactMemberSetCount", exact);
        facts.put("nativeViewScopeCurrentCount", scoped);
        facts.put("nativePublisherLiveCurrentCount", publishers);
        facts.put("nativeSamplesMatch", views.size() == expected.size() && !views.isEmpty()
                && views.stream().allMatch(value -> sameSample(views.getFirst(), value)));
        Document ownerRow = recovery == null ? null : registry.stream()
                .filter(value -> java.util.Objects.equals(recovery.get("ownerNodeId"), value.get("nodeId"))).findFirst().orElse(null);
        facts.put("recoveryOwnerViewPresent", ownerRow != null && ownerRow.get("nativeView") instanceof Document);
        facts.put("recoveryOwnerBootMatchesView", recovery != null && ownerRow != null
                && ownerRow.get("nativeView") instanceof Document view
                && view.get("publisherClaim") instanceof Document publisher
                && recovery.get("ownerBootId") != null && recovery.get("ownerBootId").equals(publisher.get("ownerBootId")));
        return facts;
    }

    static Map<String, Integer> operationDomains(List<Document> operations, String store, String schemaNamespace) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String key : List.of("active", "schemaNamespace", "otherOwnedStoreNamespace", "outsideOwnedStoreNamespace",
                "waitingForLock", "waitingForFlowControl", "command", "update", "query", "getmore", "otherOperation")) counts.put(key, 0);
        for (Document operation : operations) {
            increment(counts, "active");
            Object namespace = operation.get("ns");
            increment(counts, schemaNamespace.equals(namespace) ? "schemaNamespace"
                    : namespace instanceof String text && text.startsWith(store + ".")
                            ? "otherOwnedStoreNamespace" : "outsideOwnedStoreNamespace");
            if (Boolean.TRUE.equals(operation.get("waitingForLock"))) increment(counts, "waitingForLock");
            if (Boolean.TRUE.equals(operation.get("waitingForFlowControl"))) increment(counts, "waitingForFlowControl");
            String kind = operation.getString("op");
            increment(counts, kind != null && Set.of("command", "update", "query", "getmore").contains(kind) ? kind : "otherOperation");
        }
        return Map.copyOf(counts);
    }

    private static boolean after(Document document, String field, Date now) {
        return now != null && document != null && document.get(field) instanceof Date value && value.after(now);
    }

    private static boolean due(Document document, String field, Date now) {
        return now != null && document != null && document.get(field) instanceof Date value && !value.after(now);
    }

    private static boolean samePublisher(Document first, Document second) {
        return List.of("clusterId", "resourceType", "resourceId", "ownerNodeId", "ownerBootId", "claimGeneration",
                "executionGeneration", "topologyRevision", "profileGeneration").stream()
                .allMatch(field -> first.get(field) != null && (first.get(field) instanceof Number
                        ? sameNumber(first.get(field), second.get(field)) : first.get(field).equals(second.get(field))));
    }

    private static boolean sameAuthority(Document first, Document second) {
        return List.of("clusterId", "resourceType", "resourceId", "ownerNodeId", "ownerBootId", "claimGeneration",
                "executionGeneration", "profileGeneration").stream()
                .allMatch(field -> first.get(field) != null && (first.get(field) instanceof Number
                        ? sameNumber(first.get(field), second.get(field)) : first.get(field).equals(second.get(field))));
    }

    private static boolean sameNumber(Object first, Object second) {
        return (first instanceof Integer || first instanceof Long) && (second instanceof Integer || second instanceof Long)
                && ((Number) first).longValue() == ((Number) second).longValue();
    }

    private static boolean sameSample(Document first, Document second) {
        return List.of("schemaVersion", "profile", "topologyRevision", "cohortHash", "members").stream()
                .allMatch(field -> first.get(field) != null && first.get(field).equals(second.get(field)));
    }

    static Map<String, Object> takeoverFacts(Map<String, Boolean> observation) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("schema", 1);
        evidence.put("lastObservationOnly", true);
        evidence.put("observationPresent", observation.keySet().containsAll(List.of("newLiveOwner", "nativeCohortReady",
                "quorumReady", "itemPresent", "permitPresent", "originalReservationPresent")));
        for (String field : List.of("newLiveOwner", "nativeCohortReady", "quorumReady", "itemPresent",
                "permitPresent", "originalReservationPresent")) {
            evidence.put(field, Boolean.TRUE.equals(observation.get(field)));
        }
        return evidence;
    }

    static void retainTakeover(Map<String, Boolean> observation) {
        write("coordinator-takeover", takeoverFacts(observation));
    }

    private static void write(String category, Map<String, Object> evidence) {
        try {
            Path folder = Path.of(System.getProperty("tapstate.e2e.build-directory", "target"))
                    .resolve("failure-scenes").resolve(category);
            Files.createDirectories(folder);
            Files.writeString(folder.resolve(UUID.randomUUID() + ".json"),
                    new tools.jackson.databind.ObjectMapper().writeValueAsString(evidence));
        } catch (IOException | RuntimeException unavailable) {
            // Diagnostics must preserve the original prerequisite failure and cleanup path.
        }
    }

    static Map<String, Object> queueFacts(Document queue) {
        if (queue == null) return Map.of("present", false);
        String status = "UNRECOGNIZED";
        try { status = ClusterRecoveryStatus.valueOf(queue.getString("status")).name(); }
        catch (IllegalArgumentException | NullPointerException ignored) { }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("present", true);
        result.put("status", status);
        result.put("permitPresent", queue.get("permit") instanceof Document);
        result.put("successorPresent", queue.get("successor") instanceof Document);
        result.put("attemptZero", queue.get("attempt") instanceof Number value && value.longValue() == 0);
        return result;
    }

    static Map<String, Integer> operationCounts(List<Document> operations, List<String> tags, String key,
            DerivedSchema original, Map<String, String> columns) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String name : List.of("active", "tagged", "keyMatched", "twoVersions", "nextVersion", "sameStatement",
                "sameDeriver", "changedInput", "expectedColumns", "exactMutation")) counts.put(name, 0);
        for (Document operation : operations) {
            increment(counts, "active");
            String app = operation.getString("appName");
            if (app == null || tags.stream().noneMatch(tag -> app.equals(tag) || app.endsWith(" " + tag))) continue;
            increment(counts, "tagged");
            Document command = operation.get("command", Document.class);
            if (command == null) continue;
            List<Document> updates = new ArrayList<>();
            if (command.get("q") != null) updates.add(command);
            if (command.get("updates") instanceof List<?> batch)
                batch.stream().filter(Document.class::isInstance).map(Document.class::cast).forEach(updates::add);
            for (Document update : updates) {
                if (!new Document("_id", key).equals(update.get("q"))) continue;
                increment(counts, "keyMatched");
                Document mutation = update.get("u", Document.class);
                if (mutation == null || !(mutation.get("$set") instanceof Document fields)
                        || !(fields.get("versions") instanceof List<?> versions) || versions.size() != 2
                        || !(versions.getLast() instanceof Document version)) continue;
                increment(counts, "twoVersions");
                boolean next = version.get("version") instanceof Number value && value.longValue() == 1;
                boolean statement = original.statement().equals(version.get("statement"));
                boolean deriver = original.derivedBy().equals(version.get("derivedBy"));
                boolean changed = version.get("derivedFrom") instanceof String input && !original.derivedFrom().equals(input);
                Map<String, String> actual = new LinkedHashMap<>();
                boolean validColumns = version.get("columns") instanceof List<?>;
                if (version.get("columns") instanceof List<?> raw) for (Object field : raw) {
                    if (!(field instanceof Document column) || !(column.get("name") instanceof String name)
                            || !(column.get("type") instanceof String type) || actual.put(name, type) != null) {
                        validColumns = false;
                    }
                }
                boolean expected = validColumns && actual.equals(columns) && "STRING NULL".equals(actual.get("note"));
                if (next) increment(counts, "nextVersion");
                if (statement) increment(counts, "sameStatement");
                if (deriver) increment(counts, "sameDeriver");
                if (changed) increment(counts, "changedInput");
                if (expected) increment(counts, "expectedColumns");
                if (next && statement && deriver && changed && expected) increment(counts, "exactMutation");
            }
        }
        return Map.copyOf(counts);
    }

    private static void increment(Map<String, Integer> counts, String key) { counts.compute(key, (ignored, value) -> value + 1); }
}
