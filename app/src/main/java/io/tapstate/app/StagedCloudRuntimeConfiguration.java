package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.control.restapi.RuntimeVersion;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.time.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;

/** Existing Cloud wire services observe the admitted owner without constructing a business runtime. */
@Configuration(proxyBeanMethods = false)
class StagedCloudRuntimeConfiguration {
    @Bean FilterRegistrationBean<CloudHttpDiagnosticsFilter> cloudHttpDiagnostics(CloudRuntimeSettings settings) {
        return new CloudRuntimeConfiguration().cloudHttpDiagnostics(settings);
    }
    @Bean CloudRuntimeStatusProvider cloudRuntimeStatusProvider(CloudRuntimeSettings settings, StorageRoot root, Clock clock) {
        if (!settings.cloud()) return null;
        Instant started = clock.instant();
        return () -> {
            var runtime = root.runtime();
            var original = runtime.business().orElse(null);
            if (original instanceof NativeBusinessContext business && business.available()) {
                var projection = new StoreBackedCloudRuntimeStatusProvider(business.statusStore(), RuntimeVersion.current(), clock, started).snapshot();
                if (!business.available() || runtime.business().orElse(null) != original) throw StorageRuntime.unavailable();
                return projection;
            }
            var configuration = runtime.requireConfiguration();
            if (root.phase() == StorageRoot.Phase.SETUP_REQUIRED
                    && configuration.setupStates().load().phase() == StorageSetupStateStore.Phase.CONFIG_PREPARED
                    && configuration.bindings().load().isEmpty() && configuration.bindings().cachePreparation().isEmpty()) {
                return new CloudRuntimeStatus(RuntimeVersion.current(), Math.max(0, Duration.between(started, clock.instant()).toMillis()), 0, null);
            }
            throw StorageRuntime.unavailable();
        };
    }
    @Bean CloudStatusReporter cloudStatusReporter(CloudRuntimeSettings settings, ObjectProvider<CloudSdkBridge> bridges,
            ObjectProvider<CloudRuntimeStatusProvider> providers) {
        return new CloudRuntimeConfiguration().cloudStatusReporter(settings, bridges, providers);
    }
    @Bean(destroyMethod = "close") CloudStatusLifecycle cloudStatusLifecycle(CloudRuntimeSettings settings,
            ObjectProvider<CloudStatusReporter> reporters, Environment environment) {
        return new CloudRuntimeConfiguration().cloudStatusLifecycle(settings, reporters, environment);
    }
}
