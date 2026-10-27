package io.tapstate.control.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared management-operation wire vocabulary. Credentials are request-only and never result fields. */
final class StorageOperationSchemas {
    private StorageOperationSchemas() {}
    static final Map<String,String> STEMS = Map.ofEntries(
            Map.entry("storage.status", "StorageStatus"), Map.entry("storage.test", "StorageTest"),
            Map.entry("storage.setup.commit", "StorageSetupCommit"), Map.entry("storage.setup.status", "StorageSetupStatus"),
            Map.entry("storage.recovery.plan", "StorageRecoveryPlan"), Map.entry("storage.recovery.confirm", "StorageRecoveryConfirm"),
            Map.entry("storage.recovery.status", "StorageRecoveryStatus"),
            Map.entry("configuration.export", "ConfigurationExport"),
            Map.entry("configuration.import.preflight", "ConfigurationImportPreflight"),
            Map.entry("configuration.import.commit", "ConfigurationImportCommit"), Map.entry("configuration.import.status", "ConfigurationImportStatus"));
    static Map<String,Object> definitions() {
        Map<String,Object> defs = new LinkedHashMap<>();
        var connection = object(List.of("kind", "uri", "database"), Map.of("kind", choice("MONGODB", "ATLAS"),
                "uri", text(), "database", text(), "caPem", text()));
        var candidate = object(List.of("cache", "master"), Map.of(
                "configuration", connection, "cache", connection, "master", connection));
        var stamps = object(List.of("CONFIG", "CACHE", "MASTER"), Map.of("CONFIG", text(), "CACHE", text(), "MASTER", text()));
        var transfer = Map.<String,Object>of("type", "string", "minLength", 1,
                "description", "Opaque versioned encrypted transfer package; plaintext definitions are not accepted.");
        var transferKey = Map.<String,Object>of("type", "string", "minLength", 43, "maxLength", 43, "pattern", "^[A-Za-z0-9_-]{43}$",
                "description", "Caller-supplied unpadded base64url encoding of an independent 32-byte key; never stored or returned.");
        defs.put("StorageStatusRequest", object(List.of(), Map.of()));
        defs.put("StorageTestRequest", object(List.of("operationId", "generation", "candidate", "layer"), Map.of(
                "operationId", text(), "generation", positive(), "candidate", candidate, "layer", choice("CONFIG", "CACHE", "MASTER"))));
        defs.put("StorageSetupCommitRequest", object(List.of("operationId", "generation", "candidate", "testStamps"), Map.of(
                "operationId", text(), "generation", positive(), "candidate", candidate, "testStamps", stamps)));
        defs.put("StorageSetupStatusRequest", operation());
        defs.put("StorageRecoveryPlanRequest", object(List.of("scope", "pipelineIds"), Map.of(
                "scope", choice("PIPELINE", "CLUSTER"), "pipelineIds", Map.of("type", "array", "items", text(), "maxItems", 64), "operationId", text(), "previousOperationId", text())));
        defs.put("StorageRecoveryConfirmRequest", object(List.of("operationId", "planHash", "controlRevision", "confirmed"), Map.of(
                "operationId", text(), "planHash", text(), "controlRevision", positive(),
                "confirmed", Map.of("type", "boolean", "const", true))));
        defs.put("StorageRecoveryStatusRequest", operation());
        defs.put("ConfigurationExportRequest", object(List.of("resourceIds", "usernames", "transferKey"), Map.of(
                "resourceIds", Map.of("type", "array", "items", text(), "maxItems", 64),
                "usernames", Map.of("type", "array", "items", text(), "maxItems", 64), "transferKey", transferKey)));
        defs.put("ConfigurationExportResult", object(List.of("transfer", "defaultSourceId"), Map.of(
                "transfer", transfer, "defaultSourceId", Map.of("anyOf", List.of(text(), Map.of("type", "null"))))));
        defs.put("ConfigurationImportPreflightRequest", object(List.of("transfer", "transferKey", "defaultSourceMapping"), Map.of(
                "transfer", transfer, "transferKey", transferKey, "defaultSourceMapping", stringMap())));
        defs.put("ConfigurationImportCommitRequest", object(List.of("importId", "transfer", "transferKey", "defaultSourceMapping"), Map.of(
                "importId", text(), "transfer", transfer, "transferKey", transferKey, "defaultSourceMapping", stringMap())));
        defs.put("ConfigurationImportStatusRequest", object(List.of("importId"), Map.of("importId", text())));
        var setupStatus = object(List.of("mode", "clusterId", "operationId", "generation", "setupPhase", "bindingVersion"), Map.of(
                "mode", choice("CLOUD", "OP"), "clusterId", text(), "operationId", text(), "generation", positive(),
                "setupPhase", choice("LOCAL_SETUP", "CONFIG_PREPARED", "STORAGE_BOUND", "COMPLETE"), "bindingVersion", Map.of("type", "integer", "minimum", 0)));
        defs.put("StorageSetupCommitResult", setupStatus); defs.put("StorageSetupStatusResult", setupStatus);
        defs.put("StorageTestResult", object(List.of("layer", "testStamp", "expiresAt", "checks"), Map.of(
                "layer", choice("CONFIG", "CACHE", "MASTER"), "testStamp", text(), "expiresAt", text(),
                "checks", Map.of("type", "array", "items", text()))));
        var health = object(List.of("CONFIG", "CACHE", "MASTER"), Map.of("CONFIG", choice("UNKNOWN", "AVAILABLE", "UNAVAILABLE", "AUTH_READY"),
                "CACHE", choice("UNKNOWN", "AVAILABLE", "UNAVAILABLE", "AUTH_READY"), "MASTER", choice("UNKNOWN", "AVAILABLE", "UNAVAILABLE", "AUTH_READY")));
        var snapshot = new LinkedHashMap<String,Object>();
        snapshot.put("mode", choice("CLOUD", "OP")); snapshot.put("clusterId", text()); snapshot.put("operationId", text());
        snapshot.put("setupGeneration", positive()); snapshot.put("bindingVersion", nonnegative()); snapshot.put("authGeneration", nonnegative()); snapshot.put("stateVersion", nonnegative());
        snapshot.put("setupPhase", choice("LOCAL_SETUP", "CONFIG_PREPARED", "STORAGE_BOUND", "COMPLETE"));
        snapshot.put("businessPhase", choice("NOT_CONFIGURED", "PREPARING", "RESTART_REQUIRED", "FIRST_ADMIN_REQUIRED", "AUTH_READY", "READY", "UNAVAILABLE", "AWAITING_REBUILD_CONFIRMATION", "RECOVERING", "CLEANUP_REQUIRED"));
        snapshot.put("health", health); snapshot.put("allowedActions", Map.of("type", "array", "items", text())); snapshot.put("restartRequired", Map.of("type", "boolean"));
        var nullableTime = Map.<String,Object>of("anyOf", List.of(Map.of("type", "string", "format", "date-time"), Map.of("type", "null")));
        snapshot.put("applicationDataVerification", object(List.of("status", "checkedAt", "validUntil"), Map.of(
                "status", choice("UNKNOWN", "CHECKING", "PASSED", "FAILED", "EXPIRED"), "checkedAt", nullableTime, "validUntil", nullableTime)));
        defs.put("StorageStatusResult", object(List.copyOf(snapshot.keySet()), snapshot));
        var recovery = new LinkedHashMap<String,Object>();
        recovery.put("operationId", text()); recovery.put("previousOperationId", Map.of("anyOf", List.of(text(), Map.of("type", "null"))));
        recovery.put("phase", choice("PLANNED", "CONFIRMED", "RUNNING", "COMPLETE")); recovery.put("scope", choice("PIPELINE", "CLUSTER"));
        recovery.put("pipelineIds", Map.of("type", "array", "items", text())); recovery.put("affectedPipelineIds", Map.of("type", "array", "items", text()));
        recovery.put("definitionHashes", stringMap()); recovery.put("dependencyHash", text()); recovery.put("configurationRevision", nonnegative());
        recovery.put("controlRevision", positive()); recovery.put("planHash", text()); recovery.put("clusterId", text()); recovery.put("bindingVersion", positive());
        recovery.put("mode", choice("CLOUD", "OP")); recovery.put("cacheIncarnation", text()); recovery.put("authGeneration", positive());
        recovery.put("preflightCurrent",Map.of("type","boolean"));
        recovery.put("targetPolicies",Map.of("type","array","items",object(List.of("node","source","policy"),Map.of("node",text(),"source",text(),"policy",choice("APPEND","CLEAR","FAIL")))));
        recovery.put("executionErrorCode",Map.of("anyOf",List.of(text(),Map.of("type","null"))));
        var recoveryResult = object(List.copyOf(recovery.keySet()), recovery);
        defs.put("StorageRecoveryPlanResult", recoveryResult); defs.put("StorageRecoveryConfirmResult", recoveryResult);
        defs.put("StorageRecoveryStatusResult", Map.of("anyOf", List.of(recoveryResult, object(List.of("operationId", "phase"),
                Map.of("operationId", text(), "phase", choice("UNKNOWN"))))));
        var nullableText = Map.<String,Object>of("anyOf", List.of(text(), Map.of("type", "null")));
        var scope = object(List.of("clusterId", "bindingVersion", "mode", "organizationId"), Map.of(
                "clusterId", text(), "bindingVersion", positive(), "mode", choice("CLOUD", "OP"), "organizationId", nullableText));
        var ticket = object(List.of("id", "scope", "inputHash", "mappingHash", "destinationRevision", "createdAt", "expiresAt"), Map.of(
                "id", text(), "scope", scope, "inputHash", text(), "mappingHash", text(), "destinationRevision", nonnegative(),
                "createdAt", text(), "expiresAt", text()));
        var receipt = object(List.of("id", "inputHash", "configurationRevision", "resourceHashes", "usernames", "layoutIds"), Map.of(
                "id", text(), "inputHash", text(), "configurationRevision", positive(), "resourceHashes", stringMap(),
                "usernames", Map.of("type", "array", "items", text()), "layoutIds", Map.of("type", "array", "items", text())));
        var nullableReceipt = Map.<String,Object>of("anyOf", List.of(receipt, Map.of("type", "null")));
        var importOperation = object(List.of("ticket", "phase", "attempts", "receipt"), Map.of(
                "ticket", ticket, "phase", choice("PENDING", "EXPIRED", "COMMITTING", "COMMITTED", "ABORTED"),
                "attempts", nonnegative(), "receipt", nullableReceipt));
        defs.put("ConfigurationImportPreflightResult", object(List.of("operation", "preparedTransfer"), Map.of(
                "operation", importOperation, "preparedTransfer", transfer)));
        defs.put("ConfigurationImportCommitResult", receipt);
        defs.put("ConfigurationImportStatusResult", object(List.of("importId", "phase", "ticket", "attempts", "receipt"), Map.of(
                "importId", text(), "phase", choice("PENDING", "EXPIRED", "COMMITTING", "COMMITTED", "ABORTED", "UNKNOWN"),
                "ticket", Map.of("anyOf", List.of(ticket, Map.of("type", "null"))),
                "attempts", nonnegative(), "receipt", nullableReceipt)));
        return defs;
    }
    private static Map<String,Object> operation() { return object(List.of("operationId"), Map.of("operationId", text())); }
    private static Map<String,Object> text() { return Map.of("type", "string", "minLength", 1); }
    private static Map<String,Object> nonnegative() { return Map.of("type", "integer", "minimum", 0); }
    private static Map<String,Object> positive() { return Map.of("type", "integer", "minimum", 1); }
    private static Map<String,Object> stringMap() { return Map.of("type", "object", "additionalProperties", text()); }
    private static Map<String,Object> choice(String... values) { return Map.of("type", "string", "enum", List.of(values)); }
    private static Map<String,Object> object(List<String> required, Map<String,Object> properties) {
        return Map.of("type", "object", "properties", properties, "required", required, "additionalProperties", false);
    }
}
