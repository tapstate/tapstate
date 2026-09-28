package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

    // TODO Bind CloudCodeExchanger, CloudTokenRefresher and CloudStatusSender to the published SDK
    // once the provider adds stable user-id and refresh contracts and publishes a consumable version.
}
