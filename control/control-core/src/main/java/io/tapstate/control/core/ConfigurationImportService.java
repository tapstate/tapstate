package io.tapstate.control.core;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.ConfigurationImportStore.Payload;
import io.tapstate.spi.store.ConfigurationImportStore.Scope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Import admission and original-result queries through the configuration transaction port. */
public final class ConfigurationImportService {
    /** The transfer implementation enforces version, authenticated decryption and the decoded size budget. */
    public interface TransferCodec {
        Payload open(String encryptedPackage, String transferKey, Map<String,String> confirmedMapping);
        String seal(Payload prepared, String transferKey);
    }
    public record Submission(String transfer, String transferKey, Map<String,String> defaultSourceMapping) {
        public Submission { defaultSourceMapping = Map.copyOf(defaultSourceMapping); }
        @Override public String toString() { return "ConfigurationImportSubmission[redacted]"; }
    }
    /** The normalized confirmation remains encrypted; neither Source credentials nor a transfer key is returned. */
    public record Preflight(ConfigurationImportStore.Status operation, String preparedTransfer) {
        @Override public String toString() { return "ConfigurationImportPreflight[redacted]"; }
    }
    public record Status(String importId, String phase, ConfigurationImportStore.Ticket ticket,
            int attempts, ConfigurationImportStore.Receipt receipt) {}

    private final ConfigurationImportStore imports;
    private final TransferCodec transfer;
    private final java.util.function.Supplier<ApplyService> validation;
    private final AuditGate audit;
    private final java.util.function.Consumer<java.util.Set<String>> published;
    private final java.util.function.Function<String, java.util.Optional<Resource>> destination;
    private final CanonicalWriter writer = new CanonicalWriter();
    private final DslParser parser = new DslParser();

    public ConfigurationImportService(ConfigurationImportStore imports, TransferCodec transfer,
            ApplyService validation, AuditGate audit) {
        this(imports, transfer, validation, audit, id -> java.util.Optional.empty());
    }
    public ConfigurationImportService(ConfigurationImportStore imports, TransferCodec transfer,
            ApplyService validation, AuditGate audit, java.util.function.Function<String, java.util.Optional<Resource>> destination) {
        this(imports, transfer, () -> validation, audit, destination);
    }
    public ConfigurationImportService(ConfigurationImportStore imports, TransferCodec transfer,
            java.util.function.Supplier<ApplyService> validation, AuditGate audit,
            java.util.function.Function<String, java.util.Optional<Resource>> destination) {
        this(imports, transfer, validation, audit, destination, ids -> {});
    }
    public ConfigurationImportService(ConfigurationImportStore imports, TransferCodec transfer,
            java.util.function.Supplier<ApplyService> validation, AuditGate audit,
            java.util.function.Function<String, java.util.Optional<Resource>> destination,
            java.util.function.Consumer<java.util.Set<String>> published) {
        this.published = Objects.requireNonNull(published, "published");
        this.destination = Objects.requireNonNull(destination, "destination");
        this.imports = Objects.requireNonNull(imports, "imports"); this.transfer = Objects.requireNonNull(transfer, "transfer");
        this.validation = Objects.requireNonNull(validation, "validation"); this.audit = Objects.requireNonNull(audit, "audit");
    }

    public Preflight preflight(String subject, Scope scope, Runnable active, Submission submission) {
        active.run();
        Payload decoded = open(scope, submission);
        ResourceAttributionPolicy attribution = scope.mode() == StorageBinding.Mode.CLOUD
                ? ResourceAttributionPolicy.managedCloud() : ResourceAttributionPolicy.onPrem();
        var attributed = decoded.resources().stream().map(resource -> attribution.attribute(subject, resource, null)).toList();
        var validationPlan = validate(attributed);
        var prepared = new Payload(validationPlan.artifacts().stream().map(PreparedArtifact::resource).toList(),
                decoded.users(), decoded.layouts(), references(validationPlan, decoded.defaultSourceMapping()), decoded.defaultSourceMapping());
        // Seal before admission so a codec failure never leaves a preflight the caller cannot confirm.
        String confirmation = transfer.seal(prepared, submission.transferKey());
        active.run();
        var result = audit.dispatch(ControlOperations.CONFIGURATION_IMPORT_PREFLIGHT,
                new AuditContext(subject, scope.clusterId()), () -> imports.guardedBy(active).preflight(scope, prepared));
        return new Preflight(result, confirmation);
    }

    public ConfigurationImportStore.Receipt commit(String subject, Scope scope, Runnable active,
            String originalId, Submission confirmation) {
        active.run();
        Payload prepared = open(scope, confirmation);
        var guarded = imports.guardedBy(active);
        var original = guarded.find(scope, originalId).orElseThrow(() -> refused("unknown-operation"));
        // A completed retry verifies the exact sealed input through the store, without loading mutable
        // resources or re-attributing their original creator to the person who queries the result.
        if (original.phase() == ConfigurationImportStore.Phase.COMMITTED)
            return guarded.commit(original.ticket(), prepared);
        if (original.phase() == ConfigurationImportStore.Phase.PENDING) {
            var current = validate(prepared.resources());
            if (!references(current, prepared.defaultSourceMapping()).equals(prepared.referenceHashes())) throw refused("destination-changed");
        }
        var receipt = audit.dispatch(ControlOperations.CONFIGURATION_IMPORT_COMMIT,
                new AuditContext(subject, originalId), () -> guarded.commit(original.ticket(), prepared));
        // This observes confirmed publication only. Original committed-result retries above stay CONFIG-only.
        published.accept(receipt.resourceHashes().keySet());
        return receipt;
    }

    /** An unknown or expired original id is a result view, never an invitation to register another import. */
    public Status status(Scope scope, Runnable active, String originalId) {
        active.run();
        var found = imports.guardedBy(active).find(scope, originalId);
        return found.map(value -> new Status(originalId, value.phase().name(), value.ticket(), value.attempts(), value.receipt()))
                .orElseGet(() -> new Status(originalId, "UNKNOWN", null, 0, null));
    }

    private Payload open(Scope scope, Submission input) {
        if (input == null || input.transfer() == null || input.transfer().isBlank() || input.transfer().length() > 8 * 1024 * 1024
                || input.transferKey() == null || input.transferKey().isBlank()) throw refused("invalid-package");
        Payload decoded = Objects.requireNonNull(transfer.open(input.transfer(), input.transferKey(), input.defaultSourceMapping()), "decoded transfer");
        if (!input.defaultSourceMapping().equals(decoded.defaultSourceMapping())) throw refused("mapping-mismatch");
        if (scope.mode() == StorageBinding.Mode.CLOUD && !decoded.users().isEmpty()) throw refused("cloud-users");
        int objects = decoded.resources().size() + decoded.users().size() + decoded.layouts().size();
        if (objects < 1 || objects > ConfigurationImportStore.MAX_OBJECTS) throw refused("object-budget");
        var resources = new ArrayList<Resource>();
        long bytes = 0;
        for (var resource : decoded.resources()) {
            String canonical = JsonWriter.write(writer.tree(resource));
            int length = canonical.getBytes(StandardCharsets.UTF_8).length;
            if (length > ConfigurationImportStore.MAX_OBJECT_BYTES) throw refused("object-budget");
            bytes += length;
            if (bytes > ConfigurationImportStore.MAX_PAYLOAD_BYTES) throw refused("payload-budget");
            @SuppressWarnings("unchecked") var tree = (Map<String,Object>) io.tapstate.core.common.JsonReader.parse(canonical);
            resources.add(parser.fromTree(tree));
        }
        return new Payload(resources, decoded.users(), decoded.layouts(), decoded.referenceHashes(), decoded.defaultSourceMapping());
    }

    private Map<String,String> references(ApplyPlan plan, Map<String,String> mapping) {
        var result = new java.util.TreeMap<>(plan.workspacePreconditions());
        for (String target : mapping.values()) {
            var resource = destination.apply(target).orElseThrow(() -> refused("default-target-unavailable"));
            if (!(resource instanceof io.tapstate.core.model.SourceResource)) throw refused("default-target-unavailable");
            String hash = io.tapstate.core.model.canonical.CanonicalHash.of(resource);
            String prior = result.putIfAbsent(target, hash);
            if (prior != null && !prior.equals(hash)) throw refused("destination-changed");
        }
        return result;
    }

    private ApplyPlan validate(List<Resource> resources) {
        if (resources.isEmpty()) return new ApplyPlan(List.of());
        try {
            return validation.get().planPreviewWorkspace(resources.stream()
                    .map(resource -> new ArtifactDraft(null, writer.write(resource))).toList()).plan();
        } catch (io.tapstate.core.dsl.DslException invalid) {
            // Validation may name a supplied config value. Neither decrypted values nor nested causes
            // are part of an import diagnostic; the ordinary authoring face can diagnose its own DSL.
            throw refused("invalid-definitions");
        }
    }
    private static TapstateException refused(String reason) {
        return new TapstateException(IoError.CONFIGURATION_IMPORT_REFUSED, Map.of("reason", reason), null);
    }
}
