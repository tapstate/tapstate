package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageRecoveryCommands;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** The shared storage-operation decoder, authorization gate and secret-free wire projection. */
public final class StorageControlDispatcher {
    public enum BusinessPhase { NOT_CONFIGURED, PREPARING, RESTART_REQUIRED, FIRST_ADMIN_REQUIRED, AUTH_READY,
        READY, UNAVAILABLE, AWAITING_REBUILD_CONFIRMATION, RECOVERING, CLEANUP_REQUIRED }
    public enum Health { UNKNOWN, AVAILABLE, UNAVAILABLE, AUTH_READY }
    /** Supplied by the selected server assembly; reads never initialize storage or start recovery. */
    public interface Snapshots { Snapshot load(StorageControlAccess caller, Runnable requireActive); }
    public record Snapshot(StorageBinding.Mode mode, String clusterId, String operationId, long setupGeneration,
            long bindingVersion, long authGeneration, long stateVersion, String setupPhase, BusinessPhase businessPhase,
            Map<StorageSetupService.Layer,Health> health, List<String> allowedActions, boolean restartRequired) {
        public Snapshot {
            Objects.requireNonNull(mode, "mode"); Objects.requireNonNull(businessPhase, "businessPhase");
            if (clusterId == null || clusterId.isBlank() || operationId == null || operationId.isBlank()
                    || setupGeneration < 1 || bindingVersion < 0 || authGeneration < 0 || stateVersion < 0
                    || !Set.of("LOCAL_SETUP", "CONFIG_PREPARED", "STORAGE_BOUND", "COMPLETE").contains(setupPhase))
                throw new IllegalArgumentException("invalid storage snapshot");
            health = Map.copyOf(health); allowedActions = List.copyOf(allowedActions);
            if (health.size() != StorageSetupService.Layer.values().length || allowedActions.stream().anyMatch(
                    action -> !StorageOperationSchemas.STEMS.containsKey(action) && !Set.of("login", "restart", "first-admin").contains(action)))
                throw new IllegalArgumentException("invalid storage snapshot actions or health");
        }
    }
    private final OperationRegistry operations;
    private final java.util.function.Supplier<StorageSetupService> setups;
    private final java.util.function.Supplier<StorageRecoveryService> recoveries;
    private final java.util.function.Supplier<ConfigurationImportService> importers;
    private final Snapshots snapshots;
    public StorageControlDispatcher(OperationRegistry operations, StorageSetupService setup, StorageRecoveryService recovery,
            ConfigurationImportService imports, Snapshots snapshots) {
        this(operations, constant(setup), constant(recovery), constant(imports), snapshots);
    }
    public static StorageControlDispatcher staged(OperationRegistry operations, java.util.function.Supplier<StorageSetupService> setups,
            java.util.function.Supplier<StorageRecoveryService> recoveries,
            java.util.function.Supplier<ConfigurationImportService> importers, Snapshots snapshots) {
        return new StorageControlDispatcher(operations, setups, recoveries, importers, snapshots);
    }
    private StorageControlDispatcher(OperationRegistry operations, java.util.function.Supplier<StorageSetupService> setups,
            java.util.function.Supplier<StorageRecoveryService> recoveries,
            java.util.function.Supplier<ConfigurationImportService> importers, Snapshots snapshots) {
        this.operations = Objects.requireNonNull(operations, "operations"); this.setups = Objects.requireNonNull(setups, "setups");
        this.recoveries = Objects.requireNonNull(recoveries, "recoveries"); this.importers = Objects.requireNonNull(importers, "importers");
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
    }
    private static <T> java.util.function.Supplier<T> constant(T value) { Objects.requireNonNull(value); return () -> value; }
    private static <T> T available(java.util.function.Supplier<T> provider) {
        T value = provider.get();
        if (value == null) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        return value;
    }
    public Map<String,Object> dispatch(String operationId, StorageControlAccess caller, Map<String,Object> body) {
        try { return execute(operationId, caller, body); }
        catch (TapstateException failure) { throw safe(failure); }
    }
    private Map<String,Object> execute(String operationId, StorageControlAccess caller, Map<String,Object> body) {
        if (!StorageOperationSchemas.STEMS.containsKey(operationId)) throw malformed();
        var operation = operations.resolve(operationId);
        Runnable active = caller.guard(operation); active.run();
        var input = new Input(body);
        Map<String,Object> result = switch (operationId) {
            case "storage.status" -> {
                input.only(); var snapshot = snapshots.load(caller, active); active.run();
                if (snapshot.mode() != caller.mode() || !snapshot.clusterId().equals(caller.setup().clusterId())
                        || snapshot.setupGeneration() != caller.setup().generation()) throw conflict();
                caller.cacheScope().ifPresent(scope -> {
                    if (snapshot.bindingVersion() != scope.bindingVersion() || snapshot.authGeneration() != scope.authGeneration()) throw conflict();
                });
                yield snapshot(snapshot);
            }
            case "storage.test" -> {
                input.only("operationId", "generation", "candidate", "layer"); setupScope(input, caller);
                var tested = available(setups).test(caller.setup(), active, candidate(input.child("candidate")), input.enumeration("layer", StorageSetupService.Layer.class));
                yield Map.of("layer", tested.layer().name(), "testStamp", tested.testStamp(), "expiresAt", tested.expiresAt().toString(), "checks", tested.checks());
            }
            case "storage.setup.commit" -> {
                input.only("operationId", "generation", "candidate", "testStamps"); setupScope(input, caller);
                var stamps = input.child("testStamps"); stamps.only("CONFIG", "CACHE", "MASTER");
                Map<StorageSetupService.Layer,String> proofs = new EnumMap<>(StorageSetupService.Layer.class);
                for (var layer : StorageSetupService.Layer.values()) proofs.put(layer, stamps.text(layer.name()));
                yield setup(available(setups).commit(caller.subject(), caller.setup(), active, candidate(input.child("candidate")), proofs));
            }
            case "storage.setup.status" -> {
                input.only("operationId"); originalSetup(input, caller); yield setup(available(setups).status(caller.setup(), active));
            }
            case "storage.recovery.plan" -> {
                input.only("scope", "pipelineIds", "operationId", "previousOperationId");
                var extent = input.enumeration("scope", StorageRecoveryCommands.Extent.class); var ids = input.ids("pipelineIds");
                if ((extent == StorageRecoveryCommands.Extent.PIPELINE && ids.isEmpty()) || (extent == StorageRecoveryCommands.Extent.CLUSTER && !ids.isEmpty())) throw malformed();
                yield recovery(available(recoveries).plan(caller.subject(), caller.cache(), active, new StorageRecoveryCommands.Selection(extent, ids),
                        input.optionalUuid("operationId"), input.optionalUuid("previousOperationId")));
            }
            case "storage.recovery.confirm" -> {
                input.only("operationId", "planHash", "controlRevision", "confirmed");
                if (!Boolean.TRUE.equals(input.values.get("confirmed"))) throw malformed();
                yield recovery(available(recoveries).confirm(caller.subject(), caller.cache(), active, input.uuid("operationId"), input.hash("planHash"), input.number("controlRevision", 1), true));
            }
            case "storage.recovery.status" -> {
                input.only("operationId"); String id = input.uuid("operationId");
                yield available(recoveries).status(caller.cache(), active, id).map(StorageControlDispatcher::recovery)
                        .orElseGet(() -> Map.of("operationId", id, "phase", "UNKNOWN"));
            }
            case "configuration.import.preflight" -> {
                input.only("transfer", "transferKey", "defaultSourceMapping");
                var resultView = available(importers).preflight(caller.subject(), caller.importScope(), active, submission(input));
                yield Map.of("operation", importOperation(resultView.operation()), "preparedTransfer", resultView.preparedTransfer());
            }
            case "configuration.import.commit" -> {
                input.only("importId", "transfer", "transferKey", "defaultSourceMapping");
                yield receipt(available(importers).commit(caller.subject(), caller.importScope(), active, input.uuid("importId"), submission(input)));
            }
            case "configuration.import.status" -> {
                input.only("importId"); var found = available(importers).status(caller.importScope(), active, input.uuid("importId"));
                var out = new LinkedHashMap<String,Object>(); out.put("importId", found.importId()); out.put("phase", found.phase());
                out.put("ticket", found.ticket() == null ? null : ticket(found.ticket())); out.put("attempts", found.attempts());
                out.put("receipt", found.receipt() == null ? null : receipt(found.receipt())); yield out;
            }
            default -> throw new IllegalStateException("registered storage operation has no dispatch");
        };
        boolean completedSetup = operationId.equals("storage.setup.commit") && "COMPLETE".equals(result.get("setupPhase"));
        boolean completedInstallationRecovery = caller.initialInstallationRecovery() && operationId.equals("storage.recovery.confirm") && "COMPLETE".equals(result.get("phase"));
        if (!completedSetup && !completedInstallationRecovery) active.run();
        return result;
    }
    private static void originalSetup(Input input, StorageControlAccess caller) {
        if (!input.text("operationId").equals(caller.setup().operationId())) throw conflict();
    }
    private static void setupScope(Input input, StorageControlAccess caller) {
        originalSetup(input, caller); if (input.number("generation", 1) != caller.setup().generation()) throw conflict();
    }
    private static StorageSetupService.Candidate candidate(Input input) {
        input.only("configuration", "cache", "master");
        return new StorageSetupService.Candidate(input.values.containsKey("configuration") ? connection(input.child("configuration")) : null,
                connection(input.child("cache")), connection(input.child("master")));
    }
    private static StorageBinding.Connection connection(Input input) {
        input.only("kind", "uri", "database", "caPem");
        String uri = input.text("uri", 64 * 1024), database = input.text("database", 64 * 1024);
        String ca = input.values.containsKey("caPem") ? input.text("caPem", 64 * 1024) : null;
        if (ca != null && ca.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 64 * 1024) throw malformed();
        return new StorageBinding.Connection(input.enumeration("kind", StorageBinding.Connection.Kind.class), uri, database, ca);
    }
    private static ConfigurationImportService.Submission submission(Input input) {
        var mapping = input.child("defaultSourceMapping"); if (mapping.values.size() > ConfigurationImportStore.MAX_OBJECTS) throw malformed();
        var values = new LinkedHashMap<String,String>(); mapping.values.keySet().forEach(key -> values.put(key, mapping.text(key)));
        return new ConfigurationImportService.Submission(input.text("transfer", 8 * 1024 * 1024), input.text("transferKey", 4096), values);
    }
    private static Map<String,Object> snapshot(Snapshot value) {
        var out = new LinkedHashMap<String,Object>(); out.put("mode", value.mode().name()); out.put("clusterId", value.clusterId());
        out.put("operationId", value.operationId()); out.put("setupGeneration", value.setupGeneration()); out.put("bindingVersion", value.bindingVersion());
        out.put("authGeneration", value.authGeneration()); out.put("stateVersion", value.stateVersion()); out.put("setupPhase", value.setupPhase());
        out.put("businessPhase", value.businessPhase().name()); var health = new LinkedHashMap<String,Object>();
        value.health().forEach((layer, state) -> health.put(layer.name(), state.name())); out.put("health", health);
        out.put("allowedActions", value.allowedActions()); out.put("restartRequired", value.restartRequired()); return out;
    }
    private static Map<String,Object> setup(StorageSetupService.Status value) {
        return Map.of("mode", value.mode().name(), "clusterId", value.clusterId(), "operationId", value.operationId(), "generation", value.generation(),
                "setupPhase", value.setupPhase().name(), "bindingVersion", value.bindingVersion());
    }
    private static Map<String,Object> recovery(StorageRecoveryCommands.Plan plan) {
        var out = new LinkedHashMap<String,Object>(); out.put("operationId", plan.operationId()); out.put("previousOperationId", plan.previousOperationId());
        out.put("phase", plan.phase().name()); out.put("scope", plan.manifest().selection().extent().name());
        out.put("pipelineIds", new ArrayList<>(plan.manifest().selection().pipelineIds())); out.put("affectedPipelineIds", new ArrayList<>(plan.manifest().affectedPipelineIds()));
        out.put("definitionHashes", plan.manifest().definitionHashes()); out.put("dependencyHash", plan.manifest().dependencyHash());
        out.put("configurationRevision", plan.manifest().configurationRevision()); out.put("controlRevision", plan.controlRevision()); out.put("planHash", plan.planHash());
        var scope = plan.originalScope(); out.put("clusterId", scope.clusterId()); out.put("bindingVersion", scope.bindingVersion());
        out.put("mode", scope.mode().name()); out.put("cacheIncarnation", scope.cacheIncarnation()); out.put("authGeneration", scope.authGeneration()); return out;
    }
    private static Map<String,Object> ticket(ConfigurationImportStore.Ticket ticket) {
        var scope = new LinkedHashMap<String,Object>(); scope.put("clusterId", ticket.scope().clusterId()); scope.put("bindingVersion", ticket.scope().bindingVersion());
        scope.put("mode", ticket.scope().mode().name()); scope.put("organizationId", ticket.scope().organizationId());
        return Map.of("id", ticket.id(), "scope", scope, "inputHash", ticket.inputHash(), "mappingHash", ticket.mappingHash(),
                "destinationRevision", ticket.destinationRevision(), "createdAt", ticket.createdAt().toString(), "expiresAt", ticket.expiresAt().toString());
    }
    private static Map<String,Object> receipt(ConfigurationImportStore.Receipt value) {
        return Map.of("id", value.id(), "inputHash", value.inputHash(), "configurationRevision", value.configurationRevision(), "resourceHashes", value.resourceHashes(),
                "usernames", new ArrayList<>(value.usernames()), "layoutIds", new ArrayList<>(value.layoutIds()));
    }
    private static Map<String,Object> importOperation(ConfigurationImportStore.Status value) {
        var out = new LinkedHashMap<String,Object>(); out.put("ticket", ticket(value.ticket())); out.put("phase", value.phase().name());
        out.put("attempts", value.attempts()); out.put("receipt", value.receipt() == null ? null : receipt(value.receipt())); return out;
    }
    private static final class Input {
        final Map<String,Object> values;
        Input(Map<String,Object> values) { if (values == null) throw malformed(); this.values = values; }
        void only(String... allowed) { if (!Set.of(allowed).containsAll(values.keySet())) throw malformed(); }
        String text(String key) { return text(key, 4096); }
        String text(String key, int maximum) {
            if (!(values.get(key) instanceof String value) || value.isBlank() || value.length() > maximum) throw malformed(); return value;
        }
        long number(String key, long minimum) {
            Object value = values.get(key);
            if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)) throw malformed();
            long result = ((Number) value).longValue(); if (result < minimum) throw malformed(); return result;
        }
        Input child(String key) {
            if (!(values.get(key) instanceof Map<?,?> value) || value.keySet().stream().anyMatch(field -> !(field instanceof String))) throw malformed();
            @SuppressWarnings("unchecked") Map<String,Object> typed = (Map<String,Object>) value; return new Input(typed);
        }
        <E extends Enum<E>> E enumeration(String key, Class<E> type) {
            String value = text(key); try { return Enum.valueOf(type, value); } catch (IllegalArgumentException invalid) { throw malformed(); }
        }
        Set<String> ids(String key) {
            if (!(values.get(key) instanceof List<?> list) || list.size() > 64) throw malformed(); var ids = new TreeSet<String>();
            for (Object value : list) { if (!(value instanceof String id) || id.isBlank() || id.length() > 4096 || !ids.add(id)) throw malformed(); } return ids;
        }
        String hash(String key) { String value = text(key); if (!value.matches("[0-9a-f]{64}")) throw malformed(); return value; }
        String uuid(String key) {
            String value = text(key); try { if (!java.util.UUID.fromString(value).toString().equals(value)) throw malformed(); }
            catch (IllegalArgumentException invalid) { throw malformed(); } return value;
        }
        String optionalUuid(String key) { return values.containsKey(key) ? uuid(key) : null; }
    }
    private static TapstateException safe(TapstateException failure) {
        if (failure.code() == ControlError.MALFORMED_REQUEST) return malformed();
        if (failure.code() == io.tapstate.spi.store.IoError.CONFIGURATION_IMPORT_REFUSED)
            return new TapstateException(failure.code(), Map.of("reason", "original-import-conditions-not-satisfied"), null);
        if (failure.code() == io.tapstate.spi.store.IoError.CONFIGURATION_IMPORT_LIMIT)
            return new TapstateException(failure.code(), Map.of("limit", "configured-import-budget"), null);
        String code = failure.code().code();
        if (code.startsWith("io.") || code.startsWith("store.") && !Set.of("store.storage-connection-unsupported", "store.invalid-uri", "store.tls-trust-invalid", "store.initialization-refused").contains(code))
            return new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        // Do not retain a nested connector or driver cause in any storage-facing diagnostic.
        return new TapstateException(failure.code(), failure.args(), null);
    }
    private static TapstateException malformed() { return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "storage operation input does not match its schema"), null); }
    private static TapstateException conflict() { return new TapstateException(ControlError.STORAGE_SCOPE_CONFLICT, Map.of(), null); }
}
