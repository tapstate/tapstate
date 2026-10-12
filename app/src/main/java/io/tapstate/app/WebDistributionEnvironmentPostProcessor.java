package io.tapstate.app;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.Objects;
import java.util.function.Supplier;

/** Rejects packaged Web/profile mismatches before an application context or network client exists. */
public final class WebDistributionEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private final Supplier<WebDistribution> distribution;

    public WebDistributionEnvironmentPostProcessor() {
        this(WebDistribution::bundled);
    }

    WebDistributionEnvironmentPostProcessor(Supplier<WebDistribution> distribution) {
        this.distribution = Objects.requireNonNull(distribution, "distribution");
    }

    @Override
    public int getOrder() {
        return ConfigDataEnvironmentPostProcessor.ORDER + 1;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        CloudProperties properties = Binder.get(environment)
                .bind("tapstate.cloud", Bindable.of(CloudProperties.class))
                .orElseGet(CloudProperties::new);
        CloudRuntimeSettings settings = CloudRuntimeSettings.resolve(properties, environment.getProperty("CLUSTER_ID"));
        distribution.get().validate(settings);
    }
}
