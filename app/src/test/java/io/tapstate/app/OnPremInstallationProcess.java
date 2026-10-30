package io.tapstate.app;

import java.nio.file.Path;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;

/** An independent service process for the installation witness, using the actual registered release JARs. */
public final class OnPremInstallationProcess {
    private OnPremInstallationProcess() {}
    public static void main(String[] args) throws Exception {
        Path installation = Path.of(args[0]), plugins = Path.of(args[1]), release = Path.of(args[2]), setup = Path.of(args[3]);
        try (var context = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .initializers(application -> {
                    var properties = new ConnectorPluginProperties(); properties.setSeedDir(setup); properties.setPluginsDir(plugins.resolve("setup"));
                    var definition = new org.springframework.beans.factory.support.RootBeanDefinition(InstalledConnectorTests.class,
                            () -> new InstalledConnectorTests(properties, java.time.Clock.systemUTC()));
                    definition.setPrimary(true);
                    ((org.springframework.beans.factory.support.BeanDefinitionRegistry)application.getBeanFactory()).registerBeanDefinition("installationTestConnectors", definition);
                }).run("--tapstate.installation.require-credential=true", "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                        "--spring.config.location=optional:classpath:/application.properties",
                        "--tapstate.installation.directory=" + installation, "--tapstate.control.auth.jwt-secret=installation-process-test-signing-key",
                        "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                        "--tapstate.connectors.plugins-dir=" + plugins, "--tapstate.connectors.seed-dir=" + release)) {
            System.out.println("INSTALLATION_READY " + ((WebServerApplicationContext)context).getWebServer().getPort() + " " + ProcessHandle.current().pid());
            System.out.flush();
            while (System.in.read() != -1) { }
        }
    }
}
