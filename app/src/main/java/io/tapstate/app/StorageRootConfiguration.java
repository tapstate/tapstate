package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.control.restapi.StorageControlCallers;
import io.tapstate.control.restapi.StorageHttpFace;
import io.tapstate.spi.store.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import java.time.Clock;
import java.util.Map;

/** One CONFIG/authentication root with an explicitly admitted, separately owned business runtime. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({MongoProperties.class, CloudProperties.class, MetricsHistoryProperties.class,
        ControlAuthProperties.class, ConnectorPluginProperties.class})
@Import({ClusterMembershipConfiguration.class, StorageHttpFace.class})
class StorageRootConfiguration {
    @Bean CloudRuntimeSettings cloudRuntimeSettings(CloudProperties properties, MongoProperties mongo, Environment environment) {
        var settings = CloudRuntimeSettings.resolve(properties, environment.getProperty("CLUSTER_ID"));
        if (settings.cloud() && !mongo.isEnabled())
            throw new io.tapstate.core.common.TapstateException(BootError.CLOUD_STORE_REQUIRED, Map.of(), null);
        return settings;
    }
    @Bean CloudSdkBridge cloudSdkBridge(CloudRuntimeSettings cloud) { return cloud.cloud() ? new CloudSdkBridge(cloud) : null; }
    @Bean Clock clock() { return Clock.systemUTC(); }
    @Bean StorageAvailability applicationAvailability(StorageRoot root) {
        return new StorageAvailability(root::businessAvailability);
    }
    @Bean TokenSecrets tokenSecrets() { return new RandomTokenSecrets(); }
    @Bean ControlPlaneConfiguration.SigningSecret signingSecret(ControlAuthProperties properties) {
        return new ControlPlaneConfiguration().signingSecret(properties);
    }
    @Bean(destroyMethod = "close") StorageRoot storageRoot(MongoProperties mongo, CloudRuntimeSettings cloud,
            ObjectProvider<CloudSdkBridge> bridge, BootId boot, MetricsHistoryProperties history,
            TokenSecrets secrets, Clock clock, ControlPlaneConfiguration.SigningSecret signing,
            ObjectProvider<InstallationFiles> installationFiles, Environment environment,
            java.util.Optional<InstallationFiles.RuntimeOwner> installationOwner) {
        installationOwner.ifPresent(InstallationFiles.RuntimeOwner::requireActive);
        var files = installationFiles.getIfAvailable();
        if (files != null) {
            if (cloud.cloud() || !mongo.isEnabled()) throw InstallationFiles.refused();
            var uri = files.configurationUri();
            String explicit = environment.getProperty("tapstate.store.mongo.uri");
            if (explicit != null && !explicit.isBlank() && !uri.filter(explicit::equals).isPresent()) throw InstallationFiles.refused();
            mongo.setUri(uri.orElse(null));
            mongo.setTlsCaFile(files.configurationCa().isPresent() ? files.caFile().toString() : null);
            var stage = files.stage();
            if (stage.handoff().ordinal() >= InstallationFiles.Handoff.URI_WRITTEN.ordinal() && uri.isEmpty()) throw InstallationFiles.refused();
            if (stage.configurationHash() != null && uri.isPresent()
                    && !stage.configurationHash().equals(OnPremInstallation.configurationHash(OnPremInstallation.connection(files)))) throw InstallationFiles.refused();
        }
        return new StorageRoot(mongo, cloud, bridge.getIfAvailable(), boot, history, secrets, clock, signing.bytes(),
                () -> installationOwner.ifPresent(InstallationFiles.RuntimeOwner::requireActive));
    }
    @Bean AuthenticationMode authenticationMode(CloudRuntimeSettings cloud) {
        return cloud.cloud() ? AuthenticationMode.CLOUD : AuthenticationMode.ON_PREM;
    }
    @Bean OperationRegistry operationRegistry() { return ControlOperations.registry(); }
    @Bean io.tapstate.control.restapi.SystemDataVersion systemDataVersion(StorageRoot root) {
        return () -> root.runtime().configuration().map(io.tapstate.adapters.mongostore.MongoConfigurationDomain::schemaVersion).orElse(null);
    }
    @Bean CredentialAuthenticator credentialAuthenticator(StorageRoot root) {
        return CredentialAuthenticator.scoped(presented -> root.authentication().credentials().authenticateContext(presented));
    }
    @Bean LocalAuthenticationContext.Provider localAuthenticationContexts(StorageRoot root) { return root::localAuthentication; }
    @Bean ManagedAuthenticationContext.Provider managedAuthenticationContexts(StorageRoot root) { return root::managedAuthentication; }
    @Bean ClusterIdentityService clusterIdentityService(StorageRoot root) {
        return new ClusterIdentityService(new ClusterIdentityStore() {
            @Override public java.util.Optional<ClusterIdentity> find() { return root.runtime().requireConfiguration().clusterIdentity().find(); }
            @Override public ClusterIdentity createIfAbsent(ClusterIdentity identity) { throw StorageRuntime.unavailable(); }
        });
    }
    @Bean StorageControlCaller storageControlCaller(StorageRoot root, ObjectProvider<OnPremInstallation> installations) {
        return StorageControlCallers.withInstallation(StorageControlCallers.ordinary((principal, original) -> StorageControlAccess.ordinary(root.mode(),
                () -> root.runtime().requireConfiguration().setupStates().load().scope(), principal, original)), installations::getIfAvailable);
    }
    @Bean StorageControlDispatcher.Snapshots storageSnapshots(StorageRoot root, ObjectProvider<OnPremInstallation> installations) {
        return (caller, active) -> {
            var installation = installations.getIfAvailable();
            return installation != null && (caller.cacheScope().isEmpty() || caller.initialInstallationRecovery()) ? installation.snapshot(caller, active) : root.snapshot(caller, active);
        };
    }
    @Bean io.tapstate.control.restapi.StorageEntryStatus storageEntryStatus(StorageRoot root, ObjectProvider<OnPremInstallation> installations) {
        return () -> {
            var entry = io.tapstate.control.restapi.StorageEntryStatus.Entry.UNAVAILABLE;
            try {
                var phase = root.phase();
                var installation = installations.getIfAvailable();
                if (phase == StorageRoot.Phase.CONFIGURATION_REQUIRED) {
                    if (installation != null && installation.files().stage().handoff() == InstallationFiles.Handoff.LOCAL)
                        entry = io.tapstate.control.restapi.StorageEntryStatus.Entry.SETUP_REQUIRED;
                } else if (phase != StorageRoot.Phase.CONFIGURATION_UNAVAILABLE && phase != StorageRoot.Phase.CACHE_UNAVAILABLE) {
                    var configuration = root.runtime().requireConfiguration();
                    var state = configuration.setupStates().load();
                    if (state.phase() != StorageSetupStateStore.Phase.COMPLETE) {
                        entry = io.tapstate.control.restapi.StorageEntryStatus.Entry.SETUP_REQUIRED;
                    } else if (installation != null && !installation.normalStartupAllowed()) {
                        entry = io.tapstate.control.restapi.StorageEntryStatus.Entry.RESTART_REQUIRED;
                    } else if (phase == StorageRoot.Phase.AUTH_READY || phase == StorageRoot.Phase.BUSINESS_READY) {
                        entry = io.tapstate.control.restapi.StorageEntryStatus.Entry.LOGIN;
                        if (installation != null && configuration.firstAdmin().load().orElseThrow(StorageRuntime::unavailable).phase() == FirstAdminStore.Phase.PENDING)
                            entry = phase == StorageRoot.Phase.BUSINESS_READY ? io.tapstate.control.restapi.StorageEntryStatus.Entry.FIRST_ADMIN_REQUIRED
                                    : io.tapstate.control.restapi.StorageEntryStatus.Entry.UNAVAILABLE;
                    }
                }
            } catch (io.tapstate.core.common.TapstateException unavailable) {
                entry = io.tapstate.control.restapi.StorageEntryStatus.Entry.UNAVAILABLE;
            }
            return new io.tapstate.control.restapi.StorageEntryStatus.Summary(root.mode().name(), entry);
        };
    }
    @Bean @org.springframework.context.annotation.Lazy
    InstalledConnectorTests installedConnectorTests(ConnectorPluginProperties properties, Clock clock) {
        return new InstalledConnectorTests(properties, clock);
    }
    @Bean @org.springframework.context.annotation.Lazy
    StorageSetupService.DefaultTarget storageDefaultTarget(ObjectProvider<InstalledConnectorTests> connectors) {
        return new StorageSetupService.DefaultTarget() {
            @Override public StorageSetupService.ConnectorIdentity identity(StorageBinding.Connection master) {
                return connectors.getObject().target().identity(master);
            }
            @Override public io.tapstate.core.model.SourceResource definition(StorageBinding.Connection master, String id) {
                return connectors.getObject().target().definition(master, id);
            }
            @Override public void test(StorageBinding.Connection master, io.tapstate.core.model.SourceResource source,
                    StorageSetupService.ConnectorIdentity original) { connectors.getObject().target().test(master, source, original); }
        };
    }
    @Bean @org.springframework.context.annotation.Lazy
    StorageRecoveryService storageRecoveryService(StorageRoot root, Clock clock, ObjectProvider<OnPremInstallation> installations) {
        var installation = installations.getIfAvailable();
        return installation == null || installation.normalStartupAllowed() ? root.recoveryService(clock) : installation.recoveryService();
    }
    @Bean @org.springframework.context.annotation.Lazy
    StorageSetupService storageSetupService(StorageRoot root, ObjectProvider<InstalledConnectorTests> connectors,
            MongoProperties mongo, Clock clock, ObjectProvider<StorageBusinessFactory> factories, ObjectProvider<OnPremInstallation> installations) {
        var installation = installations.getIfAvailable();
        if (installation != null) return installation.setups();
        var configuration = root.runtime().requireConfiguration();
        StorageSetupService.DefaultTarget target = new StorageSetupService.DefaultTarget() {
            @Override public StorageSetupService.ConnectorIdentity identity(StorageBinding.Connection master) {
                return connectors.getObject().target().identity(master);
            }
            @Override public io.tapstate.core.model.SourceResource definition(StorageBinding.Connection master, String id) {
                return connectors.getObject().target().definition(master, id);
            }
            @Override public void test(StorageBinding.Connection master, io.tapstate.core.model.SourceResource source,
                    StorageSetupService.ConnectorIdentity original) { connectors.getObject().target().test(master, source, original); }
        };
        return new StorageSetupService(root.mode(), configuration.bindings(), configuration.setupStates(),
                new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(mongo.getServerSelectionTimeout()),
                target, root::configurationEndpoint,
                new AuditGate(configuration.audit(), clock), clock, root::requireActive,
                committed -> {
                    if (root.mode() == StorageBinding.Mode.CLOUD)
                        root.continueCloudSetup(committed, target, original -> factories.getObject().create(original));
                }, root::coordinateSetup);
    }
    @Bean StorageBusinessFactory storageBusinessFactory(StorageRoot root, org.springframework.context.ConfigurableApplicationContext context) {
        return original -> new NativeBusinessContext(root, original, context);
    }
    @Bean io.tapstate.control.restapi.RuntimeHttpHandlers runtimeHttpHandlers(StorageRoot root) {
        return request -> {
            var business = root.runtime().businessForLiveness();
            var selected = business.isPresent() && business.orElseThrow() instanceof io.tapstate.control.restapi.RuntimeHttpHandlers handlers
                    ? handlers.handlerFor(request) : null;
            root.requireActive();
            return selected;
        };
    }
    @Bean StorageControlDispatcher storageControlDispatcher(OperationRegistry operations, ObjectProvider<StorageSetupService> setups,
            ObjectProvider<StorageRecoveryService> recoveries, ObjectProvider<ConfigurationImportService> imports,
            StorageControlDispatcher.Snapshots snapshots, StorageRoot root, Clock clock) {
        return StorageControlDispatcher.staged(operations, setups::getIfAvailable, recoveries::getIfAvailable, () -> {
            var custom = imports.getIfAvailable(); if (custom != null) return custom;
            var configuration = root.runtime().requireConfiguration();
            return new ConfigurationImportService(configuration.imports(), new EncryptedConfigurationTransfer(), () -> {
                var business = root.runtime().business().orElseThrow(StorageRuntime::unavailable);
                if (!(business instanceof NativeBusinessContext nativeBusiness)) throw StorageRuntime.unavailable();
                return nativeBusiness.importValidation();
            }, new AuditGate(configuration.audit(), clock), configuration.artifacts()::get, ids -> {
                var business = root.runtime().business();
                if (business.isPresent() && business.orElseThrow() instanceof NativeBusinessContext nativeBusiness)
                    nativeBusiness.refreshImportedSecrets(ids);
            });
        }, () -> {
            var configuration = root.runtime().requireConfiguration();
            return new ConfigurationExportService(configuration.exports(), new EncryptedConfigurationTransfer(), new AuditGate(configuration.audit(), clock));
        }, snapshots);
    }
    @Bean ApplicationRunner loadRetainedStorage(StorageRoot root, ObjectProvider<StorageBusinessFactory> factories,
            ObjectProvider<StorageSetupService.DefaultTarget> targets, ObjectProvider<OnPremInstallation> installations) {
        return arguments -> {
            var installation = installations.getIfAvailable();
            if (installation != null) {
                try { installation.resumePreparation(); }
                catch (io.tapstate.core.common.TapstateException unavailable) { return; }
                if (!installation.normalStartupAllowed()) return;
            }
            root.loadExisting(original -> {
            var factory = factories.getIfAvailable();
            if (factory == null) throw StorageRuntime.unavailable();
            return factory.create(original);
        }, targets::getObject);
            if (installation != null) {
                try { installation.afterNormalStartup(); }
                catch (io.tapstate.core.common.TapstateException unavailable) { /* Keep diagnostics available without reopening installation. */ }
            }
        };
    }
}
