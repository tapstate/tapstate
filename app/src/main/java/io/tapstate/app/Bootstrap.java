package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.pdk.PdkAdapter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.Severity;
import io.tapstate.messages.MessageCatalog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The service assembly root: the single entry point that brings the whole platform up in one JVM.
 * It resolves {@code --role} first and fails fast on an unsupported value — rendered as a coded
 * diagnostic — then starts the application context. At L1 the only supported role is {@code all};
 * conditional per-role wiring of the service rings lands later.
 *
 * <p>This is the only non-adapter module permitted to bind adapter bridges (rule R7).
 * The minimum root owns CONFIG and authentication. A separately owned business context is created
 * only after the retained CACHE identity, schema and completed setup are verified. Neither Spring
 * nor a status read creates an unmanaged Mongo client or starts a member.
 */
@SpringBootApplication
@org.springframework.context.annotation.ComponentScan(basePackageClasses = Bootstrap.class, excludeFilters = {
        @org.springframework.context.annotation.ComponentScan.Filter(type = org.springframework.context.annotation.FilterType.CUSTOM,
                classes = org.springframework.boot.context.TypeExcludeFilter.class),
        @org.springframework.context.annotation.ComponentScan.Filter(type = org.springframework.context.annotation.FilterType.CUSTOM,
                classes = org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter.class),
        @org.springframework.context.annotation.ComponentScan.Filter(type = org.springframework.context.annotation.FilterType.ASSIGNABLE_TYPE,
                classes = {StoreConfiguration.class, ControlPlaneConfiguration.class, HazelcastConfiguration.class,
                        DataPlaneActuationConfiguration.class, RuntimeConvergenceConfiguration.class, CloudRuntimeConfiguration.class})})
public class Bootstrap {

    /** Exit code when a coded diagnostic was reported (matches the CLI's convention). */
    private static final int EXIT_CODED_DIAGNOSTIC = 1;

    public static void main(String[] args) {
        ServerTimeZone timeZone = ServerTimeZone.initialize();
        Logger log = LoggerFactory.getLogger(Bootstrap.class);
        try {
            RoleArguments.parse(args);
        } catch (TapstateException e) {
            MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(e.code(), e.args());
            System.err.println(rendered.message());
            if (rendered.solution() != null) {
                System.err.println(rendered.solution());
            }
            System.exit(e.code().severity() == Severity.ERROR ? EXIT_CODED_DIAGNOSTIC : 0);
            return;
        }
        if (MigrateCommand.isRequested(args)) {
            // A read-only look at the system data, and the whole reason it is a mode of this binary:
            // the moment it matters most is the moment the rest of this refuses to start.
            System.exit(MigrateCommand.run(args, System.out, System.err));
            return;
        }
        if (InstallationCommand.isRequested(args)) {
            System.exit(InstallationCommand.run(args, System.out, System.err));
            return;
        }
        SpringApplication app = new SpringApplication(Bootstrap.class);
        // Logging is configured by this event, but context refresh (including connector creation)
        // has not begun. Record the choice once in both operational appenders, even if refresh fails.
        app.addListeners((ApplicationListener<ApplicationPreparedEvent>) event -> timeZone.log(log));
        // The server owns process termination. Spring's own shutdown hook would close the context on
        // SIGTERM but leave the JVM to exit with the signal's default disposition (128+15 = 143);
        // disabling it and installing our own hook lets the orderly stop finish and then force
        // exit 0, which is what an operator asking the process to stop expects.
        app.setRegisterShutdownHook(false);
        ConfigurableApplicationContext context = app.run(args);
        // run() returns only after all startup runners and the application-ready event complete.
        log.info("Tapstate application is ready");
        new GracefulShutdown(context::close, Runtime.getRuntime()::halt).install();
    }

    /** The explicitly owned storage domains and connector bridge used by the production assembly. */
    static List<Class<?>> adapterBridges() {
        return List.of(PdkAdapter.class, MongoConfigurationDomain.class, MongoCacheDomain.class);
    }
}
