package io.tapstate.app;

import org.springframework.core.env.StandardEnvironment;

/** Local fixtures own their settings and must not inherit a developer's Cloud deployment identity. */
final class CloudFixtureEnvironment {
    private CloudFixtureEnvironment() {}

    static StandardEnvironment isolated() {
        var environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        return environment;
    }
}
