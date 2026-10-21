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
    @Bean(destroyMethod = "close") OnPremInstallation onPremInstallation(StorageRoot root, InstallationFiles files,
            MongoProperties mongo, ObjectProvider<StorageSetupService.DefaultTarget> targets, Clock clock) {
        return new OnPremInstallation(root, files, mongo, targets.getObject(), clock);
    }
    @Bean WebServerFactoryCustomizer<TomcatServletWebServerFactory> installationConnector(OnPremInstallation installation) {
        return factory -> {
            var connector = new Connector(TomcatServletWebServerFactory.DEFAULT_PROTOCOL);
            connector.setPort(0); connector.setProperty("address", "127.0.0.1");
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
