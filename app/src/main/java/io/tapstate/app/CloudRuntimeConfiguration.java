package io.tapstate.app;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Binds and validates the managed Cloud runtime's external startup contract. */
@Configuration
@EnableConfigurationProperties(CloudProperties.class)
class CloudRuntimeConfiguration {

    @Bean
    CloudRuntimeSettings cloudRuntimeSettings(CloudProperties properties) {
        return CloudRuntimeSettings.resolve(properties);
    }

    // TODO Bind CloudCodeExchanger, CloudTokenRefresher and CloudStatusSender to the published SDK
    // once the provider adds stable user-id and refresh contracts and publishes a consumable version.
}
