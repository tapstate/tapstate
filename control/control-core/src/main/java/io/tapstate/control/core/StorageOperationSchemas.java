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
            Map.entry("configuration.import.preflight", "ConfigurationImportPreflight"),
            Map.entry("configuration.import.commit", "ConfigurationImportCommit"), Map.entry("configuration.import.status", "ConfigurationImportStatus"));
    static Map<String,Object> definitions() {
        Map<String,Object> defs = new LinkedHashMap<>();
        var connection = object(List.of("kind", "uri", "database"), Map.of("kind", choice("MONGODB", "ATLAS"),
                "uri", text(), "database", text(), "caPem", text()));
        var candidate = object(List.of("configuration", "cache", "master"), Map.of(
                "configuration", connection, "cache", connection, "master", connection));
        var stamps = object(List.of("CONFIG", "CACHE", "MASTER"), Map.of("CONFIG", text(), "CACHE", text(), "MASTER", text()));
        var transfer = object(List.of("version", "nonce", "ciphertext"), Map.of(
                "version", Map.of("type", "integer", "const", 1), "nonce", text(), "ciphertext", text()));
        defs.put("StorageStatusRequest", object(List.of(), Map.of()));
        defs.put("StorageTestRequest", object(List.of("operationId", "generation", "candidate", "layer"), Map.of(
                "operationId", text(), "generation", positive(), "candidate", candidate, "layer", choice("CONFIG", "CACHE", "MASTER"))));
        defs.put("StorageSetupCommitRequest", object(List.of("operationId", "generation", "candidate", "testStamps"), Map.of(
                "operationId", text(), "generation", positive(), "candidate", candidate, "testStamps", stamps)));
        defs.put("StorageSetupStatusRequest", operation());
        defs.put("StorageRecoveryPlanRequest", object(List.of("scope", "pipelineIds"), Map.of(
                "scope", choice("PIPELINE", "CLUSTER"), "pipelineIds", Map.of("type", "array", "items", text(), "maxItems", 64))));
        defs.put("StorageRecoveryConfirmRequest", object(List.of("operationId", "planHash", "configurationRevision", "confirmed"), Map.of(
                "operationId", text(), "planHash", text(), "configurationRevision", Map.of("type", "integer", "minimum", 0),
                "confirmed", Map.of("type", "boolean", "const", true))));
        defs.put("StorageRecoveryStatusRequest", operation());
        defs.put("ConfigurationImportPreflightRequest", object(List.of("transfer", "transferKey", "defaultSourceMapping"), Map.of(
                "transfer", transfer, "transferKey", text(), "defaultSourceMapping", stringMap())));
        defs.put("ConfigurationImportCommitRequest", object(List.of("importId", "transfer", "transferKey", "defaultSourceMapping"), Map.of(
                "importId", text(), "transfer", transfer, "transferKey", text(), "defaultSourceMapping", stringMap())));
        defs.put("ConfigurationImportStatusRequest", object(List.of("importId"), Map.of("importId", text())));
        var setupStatus = object(List.of("mode", "clusterId", "operationId", "generation", "setupPhase", "bindingVersion"), Map.of(
                "mode", choice("CLOUD", "OP"), "clusterId", text(), "operationId", text(), "generation", positive(),
                "setupPhase", choice("CONFIG_PREPARED", "STORAGE_BOUND", "COMPLETE"), "bindingVersion", Map.of("type", "integer", "minimum", 0)));
        defs.put("StorageSetupCommitResult", setupStatus); defs.put("StorageSetupStatusResult", setupStatus);
        defs.put("StorageTestResult", object(List.of("layer", "testStamp", "expiresAt", "checks"), Map.of(
                "layer", choice("CONFIG", "CACHE", "MASTER"), "testStamp", text(), "expiresAt", text(),
                "checks", Map.of("type", "array", "items", text()))));
        return defs;
    }
    private static Map<String,Object> operation() { return object(List.of("operationId"), Map.of("operationId", text())); }
    private static Map<String,Object> text() { return Map.of("type", "string", "minLength", 1); }
    private static Map<String,Object> positive() { return Map.of("type", "integer", "minimum", 1); }
    private static Map<String,Object> stringMap() { return Map.of("type", "object", "additionalProperties", text()); }
    private static Map<String,Object> choice(String... values) { return Map.of("type", "string", "enum", List.of(values)); }
    private static Map<String,Object> object(List<String> required, Map<String,Object> properties) {
        return Map.of("type", "object", "properties", properties, "required", required, "additionalProperties", false);
    }
}
