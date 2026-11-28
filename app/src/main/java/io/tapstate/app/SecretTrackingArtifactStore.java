package io.tapstate.app;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.SecretConfigPaths;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StoredArtifactRecord;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Keeps the process-wide log redactor aligned with the Sources in the authoritative artifact store.
 * Mutation methods update the registry only after the delegated write succeeds.
 */
final class SecretTrackingArtifactStore implements ArtifactStore {

    private final ArtifactStore delegate;
    private final Supplier<TapstateCatalog> catalog;
    private final SecretRedactor redactor;
    private long trackingRevision;
    private int activeTrackingReads;
    // Only owners published during overlapping reads need an ordering receipt.
    private final Map<String, Long> recentRegistrations = new HashMap<>();

    SecretTrackingArtifactStore(
            ArtifactStore delegate, Supplier<TapstateCatalog> catalog, SecretRedactor redactor) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.redactor = Objects.requireNonNull(redactor, "redactor");
        // Startup secret tracking must not make the whole service fail because one stored body is
        // unreadable. The browse projection keeps the good rows available and leaves the bad row for
        // repair/diagnostics; its secrets cannot be inferred safely, so it is skipped here.
        DslParser parser = new DslParser();
        var startupCatalog = operationCatalog();
        delegate.listStored().stream()
                .filter(StoredArtifactRecord::readable)
                .filter(row -> row.canonicalForm() != null)
                .forEach(row -> {
                    try {
                        track(parser.parse(row.canonicalForm()), startupCatalog);
                    } catch (RuntimeException ignored) {
                        // The store already classified this row; tracking is best effort on startup.
                    }
                });
    }

    @Override
    public synchronized ArtifactMutation create(Resource artifact) {
        ArtifactMutation result = delegate.create(artifact);
        if (result == ArtifactMutation.CREATED) {
            trackMutation(List.of(artifact));
        }
        return result;
    }

    @Override
    public synchronized ArtifactMutation replace(
            String id, String expectedContentHash, Resource replacement) {
        ArtifactMutation result = delegate.replace(id, expectedContentHash, replacement);
        if (result == ArtifactMutation.REPLACED) {
            trackMutation(List.of(replacement));
        }
        return result;
    }

    @Override
    public synchronized ArtifactMutation delete(String id, String expectedContentHash) {
        ArtifactMutation result = delegate.delete(id, expectedContentHash);
        if (result == ArtifactMutation.DELETED) {
            redactor.remove(id);
            recordRegistration(id);
        }
        return result;
    }

    @Override
    public synchronized ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
        ArtifactBatchWrite result = delegate.writeAll(writes);
        if (result.appliedSuccessfully()) {
            trackMutation(writes.stream().map(ArtifactWrite::resource).toList());
        }
        return result;
    }

    @Override
    public synchronized void saveAll(List<Resource> artifacts) {
        delegate.saveAll(artifacts);
        trackMutation(artifacts);
    }

    @Override
    public synchronized Optional<String> saveAll(
            List<Resource> artifacts, Map<String, String> expectedContentHashes) {
        Optional<String> conflicted = delegate.saveAll(artifacts, expectedContentHashes);
        // A refused batch wrote nothing, so nothing it named is in the store to track — tracking it
        // would teach the redactor secrets that were never stored.
        if (conflicted.isEmpty()) {
            trackMutation(artifacts);
        }
        return conflicted;
    }

    @Override public Optional<Resource> get(String id) {
        return trackedRead(() -> delegate.get(id), Optional::stream);
    }

    @Override public Optional<io.tapstate.spi.store.SourceBindingSnapshot> sourceBinding(String id) {
        return trackedRead(() -> delegate.sourceBinding(id), value -> value.stream().map(binding -> binding.source()));
    }

    @Override public Optional<io.tapstate.spi.store.SourceBindingSnapshot> sourceBindingForSinkStartup(String id) {
        return trackedRead(() -> delegate.sourceBindingForSinkStartup(id), value -> value.stream().map(binding -> binding.source()));
    }

    @Override public Optional<String> managedDefaultSourceId() { return delegate.managedDefaultSourceId(); }

    /** Registers only sources that were committed by the native publication. */
    void refreshLoaded(java.util.Set<String> ids) {
        trackedRead(() -> ids.stream().flatMap(id -> delegate.get(id).stream()).toList(), List::stream);
    }

    @Override public List<Resource> list() { return trackedRead(delegate::list, List::stream); }

    @Override public List<StoredArtifactRecord> listStored() { return delegate.listStored(); }

    @Override public List<Resource> listReadable() { return readableInventory().resources(); }

    @Override public ReadableInventory readableInventory() {
        return trackedInventory(() -> Optional.of(delegate.readableInventory())).orElseThrow();
    }

    @Override public Optional<ReadableInventory> draftPublicationInventory(PipelineResource candidate) {
        return trackedInventory(() -> delegate.draftPublicationInventory(candidate));
    }

    @Override public Optional<ReadableInventory> runtimeStartInventory(String pipelineId) {
        return trackedInventory(() -> delegate.runtimeStartInventory(pipelineId));
    }

    @Override public Optional<ReadableInventory> startPlanningInventory(PipelineResource candidate) {
        return trackedInventory(() -> delegate.startPlanningInventory(candidate));
    }

    @Override public ReadableInventory readableInventory(String kind) {
        return trackedInventory(() -> Optional.of(delegate.readableInventory(kind))).orElseThrow();
    }

    @Override public ReadableInventory readableInventory(String kind, String id) {
        return trackedInventory(() -> Optional.of(delegate.readableInventory(kind, id))).orElseThrow();
    }

    private Optional<ReadableInventory> trackedInventory(Supplier<Optional<ReadableInventory>> read) {
        return tracked(() -> {
            var result = read.get();
            if (result.isEmpty()) return new Registration<>(result, Map.of());
            var inventory = result.orElseThrow();
            var selected = operationCatalog(() -> inventory.connectorCatalog().orElseGet(catalog));
            var values = secrets(inventory.resources().stream(), selected);
            if (inventory.resources().stream().anyMatch(SourceResource.class::isInstance)) {
                result = Optional.of(new ReadableInventory(inventory.resources(), Optional.of(selected.get()),
                        inventory.managedDefaultSourceId(), inventory.exactLookupIds()));
            }
            return new Registration<>(result, values);
        });
    }

    private record Registration<T>(T value, Map<String, Collection<String>> secrets) {}

    private <T> T trackedRead(Supplier<T> read, Function<T, java.util.stream.Stream<? extends Resource>> resources) {
        return tracked(() -> {
            T value = read.get();
            return new Registration<>(value, secrets(resources.apply(value), operationCatalog()));
        });
    }

    private <T> T tracked(Supplier<Registration<T>> read) {
        for (int attempt = 0; attempt < 2; attempt++) {
            final long before;
            synchronized (this) {
                activeTrackingReads++;
                before = trackingRevision;
            }
            try {
                var result = read.get();
                if (result.secrets().isEmpty()) return result.value();
                synchronized (this) {
                    // Resolve disagreement for the same owner with a fresh read outside the lock.
                    if (result.secrets().entrySet().stream().anyMatch(entry ->
                            recentRegistrations.getOrDefault(entry.getKey(), 0L) > before
                                    && !redactor.matches(entry.getKey(), entry.getValue()))) continue;
                    publish(result.secrets());
                    return result.value();
                }
            } finally {
                synchronized (this) {
                    if (--activeTrackingReads == 0) recentRegistrations.clear();
                }
            }
        }
        throw new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "Source credentials changed during the artifact read"), null);
    }

    private void trackMutation(Collection<Resource> resources) {
        publish(secrets(resources.stream(), operationCatalog()));
    }

    private void publish(Map<String, Collection<String>> values) {
        values.forEach((owner, secrets) -> {
            redactor.replace(owner, secrets);
            // Even identical values prove a newer observation of this owner.
            recordRegistration(owner);
        });
    }

    private void recordRegistration(String owner) {
        if (activeTrackingReads != 0) recentRegistrations.put(owner, ++trackingRevision);
    }

    private Map<String, Collection<String>> secrets(java.util.stream.Stream<? extends Resource> resources,
            Supplier<TapstateCatalog> selected) {
        var result = new LinkedHashMap<String, Collection<String>>();
        resources.forEach(resource -> result.put(resource.id(), resource instanceof SourceResource source
                ? secretValues(source, selected) : List.of()));
        return result;
    }

    @Override
    public List<StoredArtifactRecord> listStored(String kind) {
        // Delegated rather than left to the interface default: the default would filter here, over rows
        // the delegate had already read and returned, which is the read this decorator has no reason to
        // make more expensive than the store it wraps.
        return delegate.listStored(kind);
    }

    @Override public java.util.function.Function<SourceResource, Runnable> resolvedSourceTracking() {
        TapstateCatalog captured = catalog.get();
        return source -> {
            String probe = "start-probe:" + java.util.UUID.randomUUID();
            redactor.replace(probe, secretValues(source, () -> captured));
            return () -> redactor.remove(probe);
        };
    }

    /** One operation's successful catalog reading is shared; no facts survive into the next operation. */
    private Supplier<TapstateCatalog> operationCatalog() { return operationCatalog(catalog); }

    private static Supplier<TapstateCatalog> operationCatalog(Supplier<TapstateCatalog> catalog) {
        return new Supplier<>() {
            private TapstateCatalog captured;
            @Override public TapstateCatalog get() {
                if (captured == null) captured = catalog.get();
                return captured;
            }
        };
    }

    private void track(Resource resource, Supplier<TapstateCatalog> selectedCatalog) {
        redactor.replace(resource.id(), resource instanceof SourceResource source
                ? secretValues(source, selectedCatalog) : List.of());
    }

    private Collection<String> secretValues(SourceResource source, Supplier<TapstateCatalog> selectedCatalog) {
        List<String> secrets = new ArrayList<>();
        if (("mongodb".equals(source.connector()) || "mongodb-atlas".equals(source.connector()))
                && source.config().get(io.tapstate.control.core.StorageSetupService.CA_BUNDLE_FIELD) instanceof String ca) {
            secrets.add(ca);
            // Structured and multiline logs can escape or indent the original bundle differently.
            ca.lines().filter(line -> !line.isBlank()).forEach(secrets::add);
        }
        ConnectorCatalogEntry connector;
        try {
            connector = selectedCatalog.get().byId(source.connector());
        } catch (IllegalArgumentException unavailable) {
            secrets.addAll(namedSecretValues(source.config()));
            SecretConfigPaths.shared().forEach(path -> collectScalars(path.value(source.config()), secrets));
            return secrets;
        }

        SecretConfigPaths.forConnector(connector).stream()
                .map(path -> path.value(source.config()))
                .filter(Objects::nonNull)
                .forEach(value -> collectScalars(value, secrets));
        boolean managedDefaultTarget = ViewTargetResolver.STATE_STORE_SOURCE_ID.equals(source.id())
                || StorageBinding.defaultSourceId(StorageBinding.Mode.CLOUD).equals(source.id());
        boolean trackedMongoUri = "mongodb-atlas".equals(source.connector())
                || ("mongodb".equals(source.connector()) && managedDefaultTarget);
        if (trackedMongoUri && source.config().get("uri") instanceof String uri) {
            secrets.addAll(MongoUriUserInfo.secretValues(uri));
        }
        return secrets;
    }

    private static Collection<String> namedSecretValues(Object value) {
        List<String> values = new ArrayList<>();
        collectNamedSecretValues(value, null, values);
        return values;
    }

    private static void collectNamedSecretValues(
            Object value, String fieldName, Collection<String> target) {
        if (value instanceof Map<?, ?> map) {
            map.forEach((name, child) -> collectNamedSecretValues(child, String.valueOf(name), target));
        } else if (value instanceof Iterable<?> iterable) {
            iterable.forEach(child -> collectNamedSecretValues(child, fieldName, target));
        } else if (value != null && fieldName != null && isLikelySecretField(fieldName)) {
            target.add(String.valueOf(value));
        }
    }

    private static boolean isLikelySecretField(String fieldName) {
        String normalized = fieldName.toLowerCase(Locale.ROOT);
        return normalized.contains("password")
                || normalized.contains("secret")
                || normalized.contains("token")
                || normalized.contains("credential")
                || normalized.equals("key")
                || normalized.endsWith("key")
                || normalized.contains("uri")
                || normalized.contains("connectionstring");
    }

    private static void collectScalars(Object value, Collection<String> target) {
        if (value instanceof Map<?, ?> map) {
            map.values().forEach(child -> collectScalars(child, target));
        } else if (value instanceof Iterable<?> iterable) {
            iterable.forEach(child -> collectScalars(child, target));
        } else if (value != null) {
            target.add(String.valueOf(value));
        }
    }
}
