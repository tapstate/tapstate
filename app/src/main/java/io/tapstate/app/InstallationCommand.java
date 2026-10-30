package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.SetupCapabilityService;
import io.tapstate.control.core.StorageSetupService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.*;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Arrays;
import java.util.Optional;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Local deployment-owner mode of the existing server binary; never reachable through HTTP. */
final class InstallationCommand {
    private InstallationCommand() {}
    static boolean isRequested(String[] args) { return Arrays.asList(args).contains("installation"); }
    static int run(String[] args, PrintStream out, PrintStream err) {
        var application = new SpringApplication(SettingsOnly.class);
        application.setWebApplicationType(WebApplicationType.NONE); application.setBannerMode(Banner.Mode.OFF);
        application.setRegisterShutdownHook(false);
        try (var context = application.run(args)) {
            var environment = context.getEnvironment();
            var cloud = CloudRuntimeSettings.resolve(context.getBean(CloudProperties.class), environment.getProperty("CLUSTER_ID"));
            if (cloud.cloud()) throw InstallationFiles.refused();
            boolean initialize = Arrays.asList(args).contains("--initialize");
            boolean authorize = Arrays.asList(args).contains("--authorize");
            String selected = environment.getProperty(OnPremInstallation.DIRECTORY_PROPERTY);
            if (initialize == authorize || selected == null || selected.isBlank()) throw InstallationFiles.refused();
            Path directory;
            try { directory = Path.of(selected); }
            catch (java.nio.file.InvalidPathException invalid) { throw InstallationFiles.refused(); }
            var files = initialize ? InstallationFiles.initialize(directory) : new InstallationFiles(directory);
            authorize(files, context.getBean(MongoProperties.class), Clock.systemUTC());
            out.println("Installation authorization saved to " + files.directory().resolve("capability.json"));
            out.println("Start or continue this installation with --" + OnPremInstallation.DIRECTORY_PROPERTY + "=" + files.directory());
            return 0;
        } catch (TapstateException failure) {
            var rendered = MessageCatalog.bundled().render(failure.code(), failure.args());
            err.println(rendered.message()); return 1;
        }
    }

    static SetupCapabilityService.Issued authorize(InstallationFiles files, MongoProperties mongo, Clock clock) {
            var stage = files.stage();
            MongoConfigurationDomain configuration = null;
            try {
                if (stage.handoff().ordinal() >= InstallationFiles.Handoff.CREATE_STARTED.ordinal()) {
                    var endpoint = OnPremInstallation.connection(files);
                    if (!OnPremInstallation.configurationHash(endpoint).equals(stage.configurationHash())) throw InstallationFiles.refused();
                    configuration = stage.handoff() == InstallationFiles.Handoff.CREATE_STARTED
                            ? MongoConfigurationBootstrap.resume(OnPremInstallation.settings(files, mongo), stage.scope().clusterId(), stage.scope().installationId(), stage.scope().operationId(), stage.scope().generation())
                            : MongoConfigurationDomain.openExisting(OnPremInstallation.settings(files, mongo));
                }
                var original = configuration;
                var states = new InstallationSetupStates(files, () -> Optional.ofNullable(original).map(MongoConfigurationDomain::setupStates));
                var status = states.load();
                var purpose = status.phase() == StorageSetupStateStore.Phase.COMPLETE ? StorageSetupStateStore.Purpose.FIRST_ADMIN : StorageSetupStateStore.Purpose.SETUP;
                var capabilities = new SetupCapabilityService(states, clock, files::requireOwner,
                        () -> { throw InstallationFiles.refused(); }, () -> requireFirstAdministratorReady(files, original, mongo));
                var issued = capabilities.issue(status.scope(), purpose);
                files.locked(() -> {
                    if (!files.stage().scope().equals(issued.scope())) throw InstallationFiles.refused();
                    files.saveCapability(issued); return null;
                });
                capabilities.close(); return issued;
            } finally { if (configuration != null) configuration.close(); }
    }

    private static void requireFirstAdministratorReady(InstallationFiles files, MongoConfigurationDomain configuration, MongoProperties mongo) {
        if (configuration == null || files.stage().handoff() != InstallationFiles.Handoff.RESTARTED) throw InstallationFiles.refused();
        var status = configuration.firstAdmin().load().orElseThrow(InstallationFiles::refused);
        if (status.phase() != FirstAdminStore.Phase.PENDING) throw InstallationFiles.refused();
        var binding = configuration.bindings().load().orElseThrow(StorageRuntime::unavailable);
        var preparation = configuration.bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable);
        try (var cache = MongoCacheDomain.connect(binding.cache(), mongo.getServerSelectionTimeout(), "installation-authorization")) {
            cache.loadAuthScope(preparation.authScope()); cache.loadBusinessScope(preparation.businessScope());
        }
        var source = configuration.artifacts().get(binding.defaultSourceId()).filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast).orElseThrow(StorageRuntime::unavailable);
        if (!(source.config().get("uri") instanceof String uri)) throw StorageRuntime.unavailable();
        var master = new StorageBinding.Connection(source.connector().equals("mongodb-atlas") ? StorageBinding.Connection.Kind.ATLAS : StorageBinding.Connection.Kind.MONGODB,
                uri, MongoDatabaseNames.fromUri(uri), source.config().get(StorageSetupService.CA_BUNDLE_FIELD) instanceof String ca ? ca : null);
        new MongoStorageConnectionProbe(mongo.getServerSelectionTimeout()).test(master);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({MongoProperties.class, CloudProperties.class})
    static class SettingsOnly { }
}
