package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkConnectionTester;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StorageSetupService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTester;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.StorageBinding;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Tests the exact registered artifact; persistent Source trust never depends on a probe file. */
final class RegisteredMongoDefaultTarget implements StorageSetupService.DefaultTarget {
    static final String CA_BUNDLE = StorageSetupService.CA_BUNDLE_FIELD;
    private final ConnectorRegistry registry;
    private final ConnectorCatalogView catalog;
    private final ConnectorProvisioner provisioner;
    private final Function<ConnectorProvisioner,ConnectionTester> testers;
    RegisteredMongoDefaultTarget(ConnectorRegistry registry, ConnectorCatalogView catalog, ConnectorProvisioner provisioner, Clock clock) {
        this(registry, catalog, provisioner, pinned -> new PdkConnectionTester(pinned, clock));
    }
    RegisteredMongoDefaultTarget(ConnectorRegistry registry, ConnectorCatalogView catalog, ConnectorProvisioner provisioner,
            Function<ConnectorProvisioner,ConnectionTester> testers) {
        this.registry = Objects.requireNonNull(registry, "registry"); this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.provisioner = Objects.requireNonNull(provisioner, "provisioner"); this.testers = Objects.requireNonNull(testers, "testers");
    }
    @Override public StorageSetupService.ConnectorIdentity identity(StorageBinding.Connection master) {
        String id = id(master); var registrations = registry.findAll(id); var detail = catalog.detail(id);
        if (registrations.size() != 1 || !detail.runtimeAvailable() || !detail.sink() || !"registered".equals(detail.origin())
                || detail.spec().unavailable() != null || !CanonicalHash.ofText(detail.spec().text()).equals(detail.spec().contentHash())) throw unavailable();
        String artifact = registrations.getFirst().contentHash(), spec = detail.spec().contentHash();
        if (artifact == null || !artifact.matches("[0-9a-f]{64}") || spec == null || !spec.matches("[0-9a-f]{64}")) throw unavailable();
        return new StorageSetupService.ConnectorIdentity(artifact, spec);
    }
    @Override public SourceResource definition(StorageBinding.Connection master, String sourceId) {
        var settings = new LinkedHashMap<String,Object>(); settings.put("isUri", true); settings.put("uri", master.uri());
        if (master.caPem() != null) settings.put(CA_BUNDLE, master.caPem());
        return new SourceResource(sourceId, null, id(master), settings, null, null, null, null);
    }
    @Override public void test(StorageBinding.Connection master, SourceResource source, StorageSetupService.ConnectorIdentity original) {
        if (!identity(master).equals(original)) throw unavailable();
        var ref = provisioner.resolve(id(master), original.artifactHash());
        if (ref.spec() == null || !CanonicalHash.ofText(ref.spec()).equals(original.specHash())) throw unavailable();
        ConnectorProvisioner pinned = requested -> {
            if (!requested.equals(id(master))) throw unavailable(); return ref;
        };
        try (var material = io.tapstate.adapters.pdk.ConnectorTrustMaterial.open(source.connector(), source.config())) {
            var result = testers.apply(pinned).test(new ConnectionConfig(source.id(), source.connector(), material.settings()));
            if (result.outcome() != ConnectionTestResult.Outcome.PASSED || result.items().isEmpty())
                throw new TapstateException(ControlError.STORAGE_TEST_FAILED, Map.of("layer", "MASTER", "causeCode", io.tapstate.adapters.pdk.ConnectorError.TEST_FAILED.code()), null);
            if (!identity(master).equals(original)) throw unavailable();
        }
    }
    private static String id(StorageBinding.Connection input) { return input.kind() == StorageBinding.Connection.Kind.ATLAS ? "mongodb-atlas" : "mongodb"; }
    private static TapstateException unavailable() { return new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null); }
}
