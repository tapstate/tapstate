package io.tapstate.app;

import io.tapstate.control.core.StorageSetupService;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.server.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/** Reuses the ordinary service container; only the added connector is restricted to installation traffic. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = OnPremInstallation.DIRECTORY_PROPERTY)
class OnPremInstallationConfiguration {
    @Bean InstallationFiles installationFiles(Environment environment) {
        try { return new InstallationFiles(Path.of(environment.getRequiredProperty(OnPremInstallation.DIRECTORY_PROPERTY))); }
        catch (java.nio.file.InvalidPathException invalid) { throw InstallationFiles.refused(); }
    }
    @Bean(destroyMethod = "close") InstallationFiles.RuntimeOwner installationRuntimeOwner(InstallationFiles files) {
        return files.ownRuntime();
    }
    @Bean(destroyMethod = "close") OnPremInstallation onPremInstallation(StorageRoot root, InstallationFiles files,
            MongoProperties mongo, ObjectProvider<StorageSetupService.DefaultTarget> targets, Clock clock,
            org.springframework.context.ConfigurableApplicationContext context, Environment environment) {
        var installation = new OnPremInstallation(root, files, mongo, targets.getObject(), clock);
        installation.requireCredential(environment.getProperty("tapstate.installation.require-credential", Boolean.class, false));
        if ("true".equals(environment.getProperty("tapstate.installation.automatic-restart"))) {
            installation.onRestartRequired(new InstallationAutomaticRestart(context::close, Runtime.getRuntime()::halt));
            installation.onStartupCompleted(() -> {
                if (!installation.automaticAuthorization() && root.runtime().requireConfiguration().firstAdmin().load().orElseThrow(StorageRuntime::unavailable).phase()
                        == io.tapstate.spi.store.FirstAdminStore.Phase.PENDING) {
                    InstallationCommand.authorize(files, mongo, clock);
                    org.slf4j.LoggerFactory.getLogger(OnPremInstallationConfiguration.class)
                            .info("First-administrator authorization is ready in {}; continue at {}", files.directory().resolve("capability.json"), files.stage().managementUrl());
                }
            });
        }
        return installation;
    }
    @Bean WebServerFactoryCustomizer<TomcatServletWebServerFactory> installationConnector(OnPremInstallation installation, Environment environment) {
        return factory -> {
            var connector = new Connector(TomcatServletWebServerFactory.DEFAULT_PROTOCOL);
            int port = 0;
            var stage = installation.files().stage();
            if ("true".equals(environment.getProperty("tapstate.installation.automatic-restart"))
                    && stage.managementUrl() != null) {
                var address = java.net.URI.create(stage.managementUrl());
                if (!"http".equals(address.getScheme()) || !"127.0.0.1".equals(address.getHost()) || address.getPort() < 1)
                    throw InstallationFiles.refused();
                port = address.getPort();
            }
            connector.setPort(port); connector.setProperty("address", "127.0.0.1");
            connector.setProperty("maxPostSize", Integer.toString(9 * 1024 * 1024));
            installation.attach(connector);
            factory.addAdditionalConnectors(connector);
            factory.addEngineValves(new ValveBase(true) {
                @Override public void invoke(Request request, Response response) throws IOException, jakarta.servlet.ServletException {
                    installation.markRequest(request); getNext().invoke(request, response);
                }
            });
        };
    }
    @Bean ApplicationListener<WebServerInitializedEvent> installationAddress(OnPremInstallation installation) {
        return event -> installation.publishAddress();
    }
}
