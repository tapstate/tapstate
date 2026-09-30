package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudAuthenticationObserver;
import io.tapstate.control.core.CloudRuntimeStatusProvider;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.CloudStatusReporter;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.StorePort;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.Ordered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Locale;
import java.util.UUID;

/** Binds and validates the managed Cloud runtime's external startup contract. */
@Configuration
@EnableConfigurationProperties({CloudProperties.class, MongoProperties.class})
class CloudRuntimeConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(CloudRuntimeConfiguration.class);

    @Bean
    CloudRuntimeSettings cloudRuntimeSettings(
            CloudProperties properties, MongoProperties mongo, Environment environment) {
        CloudRuntimeSettings settings = CloudRuntimeSettings.resolve(properties, environment.getProperty("CLUSTER_ID"));
        if (settings.cloud() && !mongo.isEnabled()) {
            // The optional store switch serves on-prem substrate runs. A managed runtime must not
            // use it to bypass its mandatory metadata connection and persistence startup gates.
            throw new TapstateException(BootError.CLOUD_STORE_REQUIRED, Map.of(), null);
        }
        return settings;
    }

    @Bean
    CloudSdkBridge cloudSdkBridge(CloudRuntimeSettings settings) {
        return settings.cloud() ? new CloudSdkBridge(settings) : null;
    }

    @Bean
    FilterRegistrationBean<CloudHttpDiagnosticsFilter> cloudHttpDiagnostics(CloudRuntimeSettings settings) {
        FilterRegistrationBean<CloudHttpDiagnosticsFilter> registration =
                new FilterRegistrationBean<>(new CloudHttpDiagnosticsFilter());
        registration.setName("cloudHttpDiagnostics");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        registration.setEnabled(settings.cloud());
        return registration;
    }

    @Bean
    @ConditionalOnMissingBean(CloudAuthenticationService.class)
    CloudAuthenticationService cloudAuthenticationService(
            CloudRuntimeSettings settings, ObjectProvider<CloudSdkBridge> bridges,
            ObjectProvider<CloudSessionStore> stores, ObjectProvider<TokenSecrets> secrets,
            ObjectProvider<Clock> clocks) {
        if (!settings.cloud()) {
            return null;
        }
        CloudSdkBridge bridge = bridges.getIfAvailable();
        CloudSessionStore store = stores.getIfAvailable();
        TokenSecrets tokenSecrets = secrets.getIfAvailable();
        Clock clock = clocks.getIfAvailable();
        if (bridge == null || store == null || tokenSecrets == null || clock == null) {
            return null;
        }
        CloudSessionIdentity identity = new CloudSessionIdentity(
                settings.baseUrl().toString(), CloudSdkBridge.DEPLOYMENT_ORGANIZATION, settings.clusterId());
        CloudAuthenticationObserver observer = new CloudAuthenticationObserver() {
            @Override
            public void entering(Stage stage) {
                String label = stage.name().toLowerCase(Locale.ROOT).replace('_', '-');
                if (MDC.get(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC) != null) {
                    MDC.put(CloudHttpDiagnosticsFilter.STAGE_MDC, label);
                    MDC.remove(CloudHttpDiagnosticsFilter.REASON_MDC);
                }
                LOG.info("Cloud authentication step [request_id={}, stage={}]",
                        requestId(), label);
            }

            @Override
            public void sessionRejected(SessionRejection reason) {
                String label = reason.name().toLowerCase(Locale.ROOT).replace('_', '-');
                if (MDC.get(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC) != null) {
                    MDC.put(CloudHttpDiagnosticsFilter.REASON_MDC, label);
                }
                LOG.warn("Cloud authentication rejected [request_id={}, stage=session-create, reason={}]",
                        requestId(), label);
            }

            private String requestId() {
                String id = MDC.get(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC);
                return id == null ? "none" : id;
            }
        };
        return new CloudAuthenticationService(
                bridge, bridge, new CloudSessionService(store, identity, tokenSecrets, clock, observer), observer);
    }

    @Bean
    @ConditionalOnMissingBean(CloudRuntimeStatusProvider.class)
    CloudRuntimeStatusProvider cloudRuntimeStatusProvider(
            CloudRuntimeSettings settings, ObjectProvider<StorePort> stores, ObjectProvider<Clock> clocks) {
        if (!settings.cloud()) {
            return null;
        }
        StorePort store = stores.getIfAvailable();
        Clock clock = clocks.getIfAvailable();
        if (store == null || clock == null) {
            return null;
        }
        String version = Bootstrap.class.getPackage().getImplementationVersion();
        return new StoreBackedCloudRuntimeStatusProvider(
                store, version == null || version.isBlank() ? "development" : version, clock, Instant.now(clock));
    }

    @Bean
    @ConditionalOnMissingBean(CloudStatusReporter.class)
    CloudStatusReporter cloudStatusReporter(
            CloudRuntimeSettings settings, ObjectProvider<CloudSdkBridge> bridges,
            ObjectProvider<CloudRuntimeStatusProvider> providers) {
        if (!settings.cloud()) {
            return null;
        }
        CloudSdkBridge bridge = bridges.getIfAvailable();
        CloudRuntimeStatusProvider provider = providers.getIfAvailable();
        if (bridge == null || provider == null) {
            return null;
        }
        return new CloudStatusReporter(settings.clusterId(), provider, bridge, () -> UUID.randomUUID().toString());
    }

    @Bean(destroyMethod = "close")
    CloudStatusLifecycle cloudStatusLifecycle(
            CloudRuntimeSettings settings, ObjectProvider<CloudStatusReporter> reporters, Environment environment) {
        String configured = environment.getProperty(
                "SDK_STATUS_SENDER_ENABLED", settings.cloud() ? "true" : "false").trim();
        if (!"true".equalsIgnoreCase(configured) && !"false".equalsIgnoreCase(configured)) {
            throw new TapstateException(BootError.CLOUD_STATUS_CONFIG_INVALID, Map.of(), null);
        }
        if (!Boolean.parseBoolean(configured)) {
            LOG.info("Cloud status reporting is disabled");
            return new CloudStatusLifecycle(null);
        }
        if (!settings.cloud()) {
            throw new TapstateException(BootError.CLOUD_STATUS_MODE_REQUIRED, Map.of(), null);
        }
        CloudStatusReporter reporter = reporters.getIfAvailable();
        if (reporter == null) {
            throw new TapstateException(BootError.CLOUD_STATUS_SDK_REQUIRED, Map.of(), null);
        }
        LOG.info("Cloud status reporting is enabled");
        return new CloudStatusLifecycle(reporter);
    }

}
