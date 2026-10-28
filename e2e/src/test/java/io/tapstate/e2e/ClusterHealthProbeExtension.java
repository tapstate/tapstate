package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** An explicit invocation allowlist enables diagnostics without changing a test outcome. */
public final class ClusterHealthProbeExtension implements BeforeEachCallback, AfterEachCallback {
    private static final ExtensionContext.Namespace STORE = ExtensionContext.Namespace.create(ClusterHealthProbeExtension.class);
    private record Capture(ClusterHealthProbe.Scope scope, Path directory) { }
    private final ThreadLocal<Capture> owned = new ThreadLocal<>();
    @Override public void beforeEach(ExtensionContext context) {
        ClusterHealthProbe.Scope scope = null;
        try {
            String directory = System.getProperty("tapstate.test.health-probe-directory");
            if (directory == null || directory.isBlank()) return;
            String declared = System.getProperty("tapstate.test.health-probe-invocations", "");
            String method = context.getRequiredTestClass().getName() + "#" + context.getRequiredTestMethod().getName();
            if (Arrays.stream(declared.split(",")).map(String::trim).noneMatch(method::equals)) return;
            scope = ClusterHealthProbe.install(ClusterHealthProbe.hash(context.getUniqueId()),
                    System.getenv().getOrDefault("GITHUB_RUN_ID", "local"), System.getenv().getOrDefault("GITHUB_RUN_ATTEMPT", "1"));
            Capture capture = new Capture(scope, Path.of(directory));
            owned.set(capture);
            context.getStore(STORE).put("capture", capture);
        } catch (RuntimeException | Error failure) {
            owned.remove();
            if (scope != null) scope.close();
            System.err.println("Cluster health capture scope is incomplete");
        }
    }
    @Override public void afterEach(ExtensionContext context) {
        // Cleanup ownership must remain available when the JUnit store cannot be read.
        Capture capture = owned.get();
        owned.remove();
        try {
            Capture stored = context.getStore(STORE).remove("capture", Capture.class);
            if (capture == null) capture = stored;
            else if (stored != capture) capture.scope().incomplete();
        } catch (RuntimeException | Error failure) {
            if (capture != null) capture.scope().incomplete();
            System.err.println("Cluster health capture metadata is incomplete");
        }
        if (capture == null) return;
        try {
            capture.scope().close();
            capture.scope().write(capture.directory(), context.getExecutionException().isPresent());
        } catch (Exception | Error failure) {
            if (ClusterHealthProbe.activeScope() == capture.scope()) capture.scope().close();
            System.err.println("Cluster health capture output is incomplete");
        }
    }
}
