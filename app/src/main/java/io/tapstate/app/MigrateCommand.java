package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * The read-only look at the system data, run as a mode of the server binary rather than as a command
 * of the CLI.
 *
 * <p>Two reasons, and the second is the one that decided it. The CLI is compiled ahead of time and does
 * not carry a store driver, so it could not open the store to answer. And the moment these questions
 * matter most is the moment the server will not start — it refused because the store is at a version
 * this build does not know, or because another member has been holding the migration lock for twenty
 * minutes. A tool that only runs when the server runs is not there when it is needed.
 *
 * <p>It writes nothing, ever: it opens the store the way a start does, stops before the migration, and
 * reads. That is why the connection exposes reaching the store separately from bringing it forward.
 */
final class MigrateCommand {

    /** The bare argument that selects this mode. */
    static final String VERB = "migrate";

    private static final String STATUS = "--status";
    private static final String DRY_RUN = "--dry-run";
    private static final String LIST = "--list";

    /** Exit code when a coded diagnostic was reported, matching the rest of the binary. */
    private static final int EXIT_CODED_DIAGNOSTIC = 1;

    private MigrateCommand() {
    }

    /** Whether these arguments ask for the inspection rather than for a server. */
    static boolean isRequested(String[] args) {
        return Arrays.asList(args).contains(VERB);
    }

    /**
     * Runs the inspection and returns the process exit code. The store's own settings are resolved the
     * way the server resolves them -- the same property sources, the same defaults -- rather than being
     * read again here, so an operator cannot end up inspecting a different store from the one that
     * refused to start.
     */
    static int run(String[] args, PrintStream out, PrintStream err) {
        SpringApplication application = new SpringApplication(SettingsOnly.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        try (ConfigurableApplicationContext context = application.run(args)) {
            MongoProperties properties = context.getBean(MongoProperties.class);
            report(args, properties, out);
            return 0;
        } catch (TapstateException e) {
            MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(e.code(), e.args());
            err.println(rendered.message());
            if (rendered.solution() != null) {
                err.println(rendered.solution());
            }
            return e.code().severity() == Severity.ERROR ? EXIT_CODED_DIAGNOSTIC : 0;
        }
    }

    private static void report(String[] args, MongoProperties properties, PrintStream out) {
        List<String> given = Arrays.asList(args);
        if (given.contains(LIST)) {
            // Answerable without a store at all, and deliberately so: at release time this is what one
            // build carries, asked of a build rather than of an installation.
            printChangeSets(out, "carries", MigrationRunner.changeSets().stream()
                    .map(changeSet -> changeSet.version() + " " + changeSet.changeSetName()).toList());
            return;
        }
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                properties.getUri(), properties.getTlsCaFile(), properties.getServerSelectionTimeout()))) {
            connection.verifyConnectivity();
            MigrationRunner.Status status = connection.systemDataStatus();
            out.println("installed: " + status.installed());
            out.println("supported: " + status.supported());
            if (given.contains(DRY_RUN)) {
                printChangeSets(out, "would run", connection.systemDataDryRun());
            } else {
                // --status, and the default when neither is named: what has not run yet.
                printChangeSets(out, "pending", status.pending());
            }
        }
    }

    /**
     * One labelled block. The label is passed rather than fixed because the three modes are answering
     * three different questions, and calling all of them "pending" would report a changeset that has
     * already run as one that has not -- which is the opposite of the answer, and the release
     * comparison is one of the readers.
     */
    private static void printChangeSets(PrintStream out, String label, List<String> lines) {
        String heading = String.format("%-11s", label + ":");
        String indent = " ".repeat(heading.length());
        if (lines.isEmpty()) {
            out.println(heading + "none");
            return;
        }
        out.println(heading + lines.get(0));
        for (String line : lines.subList(1, lines.size())) {
            out.println(indent + line);
        }
    }

    /**
     * Enough of an application context to resolve the store settings and nothing else: no web server,
     * no cluster, no store bean. The point of this mode is to work on an installation the full context
     * refuses to come up on, so it must not bring that context up to find out.
     */
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(MongoProperties.class)
    static class SettingsOnly {
    }
}
