package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.CloudStatusReporter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Map;

/** Binds and validates the managed Cloud runtime's external startup contract. */
@Configuration
@EnableConfigurationProperties({CloudProperties.class, MongoProperties.class})
class CloudRuntimeConfiguration {

    @Bean
    CloudRuntimeSettings cloudRuntimeSettings(CloudProperties properties, MongoProperties mongo) {
        CloudRuntimeSettings settings = CloudRuntimeSettings.resolve(properties);
        if (settings.cloud() && !mongo.isEnabled()) {
            // The optional store switch serves on-prem substrate runs. A managed runtime must not
            // use it to bypass its mandatory metadata connection and persistence startup gates.
            throw new TapstateException(BootError.CLOUD_STORE_REQUIRED, Map.of(), null);
        }
        return settings;
    }

    @Bean(destroyMethod = "close")
    CloudStatusLifecycle cloudStatusLifecycle(
            CloudRuntimeSettings settings, ObjectProvider<CloudStatusReporter> reporters, Environment environment) {
        String configured = environment.getProperty("SDK_STATUS_SENDER_ENABLED", "false").trim();
        if (!"true".equalsIgnoreCase(configured) && !"false".equalsIgnoreCase(configured)) {
            throw new TapstateException(BootError.CLOUD_STATUS_CONFIG_INVALID, Map.of(), null);
        }
        if (!Boolean.parseBoolean(configured)) {
            return new CloudStatusLifecycle(null);
        }
        if (!settings.cloud()) {
            throw new TapstateException(BootError.CLOUD_STATUS_MODE_REQUIRED, Map.of(), null);
        }
        CloudStatusReporter reporter = reporters.getIfAvailable();
        if (reporter == null) {
            throw new TapstateException(BootError.CLOUD_STATUS_SDK_REQUIRED, Map.of(), null);
        }
        return new CloudStatusLifecycle(reporter);
    }

    // TODO Bind CloudCodeExchanger, CloudJwtValidator and CloudSessionCallbackVerifier to the published SDK
    // once the provider supplies stable user-id, online validation and authenticated invalidation contracts.
    // Construct CloudAuthenticationService with the validated deployment identity and local session store;
    // subsequent requests must use its 30-minute sliding local session without Cloud calls or JWT refresh.
    // The SDK adapter must also bind a CloudStatusReporter using its validated deployment identity
    // and the runtime status provider. Never substitute a no-op sender to make this opt-in pass.
}
