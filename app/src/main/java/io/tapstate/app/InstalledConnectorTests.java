package io.tapstate.app;

import io.tapstate.adapters.pdk.*;
import io.tapstate.control.core.*;
import io.tapstate.core.catalog.*;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.*;

/** Request-independent, process-local registration of release artifacts for setup tests, without a CACHE client. */
final class InstalledConnectorTests {
    private final StorageSetupService.DefaultTarget target;

    InstalledConnectorTests(ConnectorPluginProperties properties, Clock clock) {
        var registrations = new LinkedHashMap<String,ConnectorRegistration>();
        var artifacts = new LinkedHashMap<String,byte[]>();
        ConnectorRegistry registry = new ConnectorRegistry() {
            @Override public RegistrationOutcome register(String id, String api, RegistrationSource source, byte[] bytes) {
                String hash = ContentHash.of(bytes);
                var existing = registrations.get(hash);
                if (existing != null) return new RegistrationOutcome(existing, false);
                var created = new ConnectorRegistration(id, hash, api, source);
                registrations.put(hash, created); artifacts.put(hash, bytes.clone());
                return new RegistrationOutcome(created, true);
            }
            @Override public List<ConnectorRegistration> list() { return List.copyOf(registrations.values()); }
            @Override public Optional<byte[]> artifact(String hash) { return Optional.ofNullable(artifacts.get(hash)).map(byte[]::clone); }
            @Override public boolean hasArtifact(String hash) { return artifacts.containsKey(hash); }
        };
        var rows = new LinkedHashMap<String,ConnectorCatalogEntry>();
        ConnectorCatalogStore catalog = new ConnectorCatalogStore() {
            @Override public void upsert(ConnectorCatalogEntry row) { rows.put(row.id(), row); }
            @Override public Optional<ConnectorCatalogEntry> get(String id) { return Optional.ofNullable(rows.get(id)); }
            @Override public List<ConnectorCatalogEntry> list() { return List.copyOf(rows.values()); }
        };
        var sources = new LinkedHashMap<String,byte[]>();
        ConnectorSpecStore specs = new ConnectorSpecStore() {
            @Override public void put(String hash, byte[] bytes) { sources.putIfAbsent(hash, bytes.clone()); }
            @Override public Optional<byte[]> get(String hash) { return Optional.ofNullable(sources.get(hash)).map(byte[]::clone); }
        };
        var introspector = new ConnectorIntrospector();
        var provisioner = new RegistryConnectorProvisioner(registry, introspector, properties.getPluginsDir());
        var registrar = new ConnectorArtifactRegistrar(registry, introspector, new PdkCapabilityDeriver(provisioner),
                catalog, specs, properties.getAlsoAcceptIds());
        new SeedConnectorSweep(registrar).sweep(properties.getSeedDir());
        target = new RegisteredMongoDefaultTarget(registry,
                new ConnectorCatalogView(TapstateCatalog.load(), catalog, specs, registry), provisioner, clock);
    }
    StorageSetupService.DefaultTarget target() { return target; }
}
