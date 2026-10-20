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
    @Bean TokenSecrets tokenSecrets() { return new RandomTokenSecrets(); }
    @Bean ControlPlaneConfiguration.SigningSecret signingSecret(ControlAuthProperties properties) {
        return new ControlPlaneConfiguration().signingSecret(properties);
    }
    @Bean(destroyMethod = "close") StorageRoot storageRoot(MongoProperties mongo, CloudRuntimeSettings cloud,
            ObjectProvider<CloudSdkBridge> bridge, BootId boot, MetricsHistoryProperties history,
            TokenSecrets secrets, Clock clock, ControlPlaneConfiguration.SigningSecret signing) {
        return new StorageRoot(mongo, cloud, bridge.getIfAvailable(), boot, history, secrets, clock, signing.bytes());
    }
    @Bean AuthenticationMode authenticationMode(CloudRuntimeSettings cloud) {
        return cloud.cloud() ? AuthenticationMode.CLOUD : AuthenticationMode.ON_PREM;
    }
    @Bean OperationRegistry operationRegistry() { return ControlOperations.registry(); }
    @Bean CredentialAuthenticator credentialAuthenticator(StorageRoot root) { return root.authentication().credentials(); }
    @Bean LocalAuthenticationContext.Provider localAuthenticationContexts(StorageRoot root) { return root::localAuthentication; }
    @Bean ManagedAuthenticationContext.Provider managedAuthenticationContexts(StorageRoot root) { return root::managedAuthentication; }
    @Bean ClusterIdentityService clusterIdentityService(StorageRoot root) {
        return new ClusterIdentityService(new ClusterIdentityStore() {
            @Override public java.util.Optional<ClusterIdentity> find() { return root.runtime().requireConfiguration().clusterIdentity().find(); }
            @Override public ClusterIdentity createIfAbsent(ClusterIdentity identity) { throw StorageRuntime.unavailable(); }
        });
    }
    @Bean StorageControlCaller storageControlCaller(StorageRoot root) {
        return StorageControlCallers.ordinary((principal, original) -> StorageControlAccess.ordinary(root.mode(),
                () -> root.runtime().requireConfiguration().setupStates().load().scope(), principal, original));
    }
    @Bean StorageControlDispatcher.Snapshots storageSnapshots(StorageRoot root) { return root::snapshot; }
    @Bean @org.springframework.context.annotation.Lazy
    InstalledConnectorTests installedConnectorTests(ConnectorPluginProperties properties, Clock clock) {
        return new InstalledConnectorTests(properties, clock);
    }
    @Bean @org.springframework.context.annotation.Lazy
    StorageSetupService storageSetupService(StorageRoot root, ObjectProvider<InstalledConnectorTests> connectors,
            MongoProperties mongo, Clock clock) {
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
                new AuditGate(configuration.audit(), clock), clock, root::requireActive);
    }
    @Bean StorageBusinessFactory storageBusinessFactory(StorageRoot root, org.springframework.context.ConfigurableApplicationContext context) {
        return original -> new NativeBusinessContext(root, original, context);
    }
    @Bean io.tapstate.control.restapi.RuntimeHttpHandlers runtimeHttpHandlers(StorageRoot root) {
        return request -> {
            var business = root.runtime().business();
            return business.isPresent() && business.orElseThrow() instanceof io.tapstate.control.restapi.RuntimeHttpHandlers handlers
                    ? handlers.handlerFor(request) : null;
        };
    }
    @Bean StorageControlDispatcher storageControlDispatcher(OperationRegistry operations, ObjectProvider<StorageSetupService> setups,
            ObjectProvider<StorageRecoveryService> recoveries, ObjectProvider<ConfigurationImportService> imports,
            StorageControlDispatcher.Snapshots snapshots) {
        return StorageControlDispatcher.staged(operations, setups::getIfAvailable, recoveries::getIfAvailable, imports::getIfAvailable, snapshots);
    }
    @Bean ApplicationRunner loadRetainedStorage(StorageRoot root, ObjectProvider<StorageBusinessFactory> factories) {
        return arguments -> root.loadExisting(original -> {
            var factory = factories.getIfAvailable();
            if (factory == null) throw StorageRuntime.unavailable();
            return factory.create(original);
        });
    }
}
