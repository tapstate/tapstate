package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/** Checks development-only Web metadata before any service or data-store bean is created. */
@Component
final class DevelopmentWebArtifactGuard implements BeanFactoryPostProcessor, EnvironmentAware, ResourceLoaderAware {
    private Environment environment;
    private ResourceLoader resources;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resources) {
        this.resources = resources;
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        var metadata = resources.getResource("classpath:META-INF/tapstate-web.properties");
        if (!metadata.exists()) return;
        Properties properties = new Properties();
        try (var input = metadata.getInputStream()) {
            properties.load(input);
        } catch (IOException | IllegalArgumentException invalid) {
            throw invalid("metadata cannot be read", invalid);
        }
        String purpose = properties.getProperty("purpose");
        // Existing release metadata predates the optional purpose field.
        if (purpose == null || "release".equals(purpose)) return;
        if (!"local-development".equals(purpose)) {
            throw invalid("unsupported artifact purpose", null);
        }
        if (!"op".equals(properties.getProperty("profile"))) {
            throw invalid("this runtime accepts only the OP development Web profile", null);
        }
        if (!"true".equals(environment.getProperty("tapstate.local-development.enabled"))) {
            throw new TapstateException(BootError.WEB_DEVELOPMENT_CONFIRMATION_REQUIRED, Map.of(), null);
        }
    }

    private static TapstateException invalid(String reason, Throwable cause) {
        return new TapstateException(BootError.WEB_DEVELOPMENT_ARTIFACT_INVALID, Map.of("reason", reason), cause);
    }
}
