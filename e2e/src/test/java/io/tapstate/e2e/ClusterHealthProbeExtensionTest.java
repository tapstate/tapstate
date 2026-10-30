package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Callback failures cannot carry one invocation's health buffer into the next. */
class ClusterHealthProbeExtensionTest {
    @TempDir Path scratch;
    @BeforeEach void canonicalDirectory() throws Exception { scratch = scratch.toRealPath(); }

    @ParameterizedTest @ValueSource(strings = {"lookup", "remove"})
    void failedStoreTeardownStillRestoresThePreviousInvocation(String boundary) throws Exception {
        var before = ClusterHealthProbe.activeScope();
        var extension = new ClusterHealthProbeExtension();
        var fail = new AtomicBoolean();
        var context = context(fail, boundary, Optional.empty());
        try (var properties = properties(scratch, true)) {
            extension.beforeEach(context);
            var installed = ClusterHealthProbe.activeScope();
            assertThat(installed).isNotSameAs(before);
            try {
                fail.set(true);
                assertThatCode(() -> extension.afterEach(context)).doesNotThrowAnyException();
                assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
                System.setProperty("tapstate.test.health-probe-invocations", "unselected#method");
                extension.beforeEach(context);
                assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
            } finally {
                if (ClusterHealthProbe.activeScope() == installed) installed.close();
            }
        }
    }

    @Test void preservesTheOriginalFailureAndWritesOnlyItsHashedInvocation() throws Exception {
        var extension = new ClusterHealthProbeExtension();
        var original = new AssertionError("original action failure");
        var context = context(new AtomicBoolean(), "none", Optional.of(original));
        var before = ClusterHealthProbe.activeScope();
        try (var properties = properties(scratch, true)) {
            extension.beforeEach(context);
            extension.afterEach(context);
        }
        assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
        try (var files = Files.list(scratch)) {
            Path file = files.findFirst().orElseThrow();
            Map<?, ?> record = (Map<?, ?>) JsonReader.parse(Files.readString(file));
            assertThat(record.get("test_outcome")).isEqualTo("failed");
            assertThat(record.get("case_sha256")).isEqualTo(ClusterHealthProbe.hash("private-invocation-sentinel"));
            assertThat(Files.readString(file)).doesNotContain("private-invocation-sentinel", original.getMessage());
        }
        assertThat(context.getExecutionException()).containsSame(original);
    }

    @Test void outputFailureAndDisabledCaptureLeaveTheOwningScopeRestored() throws Exception {
        var before = ClusterHealthProbe.activeScope();
        var extension = new ClusterHealthProbeExtension();
        var original = new AssertionError("original failure");
        var context = context(new AtomicBoolean(), "none", Optional.of(original));
        Path ordinary = scratch.resolve("ordinary"); Files.writeString(ordinary, "retained");
        try (var properties = properties(ordinary, true)) {
            extension.beforeEach(context);
            assertThatCode(() -> extension.afterEach(context)).doesNotThrowAnyException();
        }
        assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
        assertThat(Files.readString(ordinary)).isEqualTo("retained");
        try (var properties = properties(scratch, false)) {
            extension.beforeEach(context); extension.afterEach(context);
            assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
        }
    }

    @Test void aStoreFailureDuringConfigurationCannotRetainAnInstalledScope() throws Exception {
        var before = ClusterHealthProbe.activeScope();
        var extension = new ClusterHealthProbeExtension();
        var context = context(new AtomicBoolean(true), "put", Optional.empty());
        try (var properties = properties(scratch, true)) {
            assertThatCode(() -> extension.beforeEach(context)).doesNotThrowAnyException();
            assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
            assertThatCode(() -> extension.afterEach(context)).doesNotThrowAnyException();
        }
        assertThat(ClusterHealthProbe.activeScope()).isSameAs(before);
    }

    private void observedInvocation() { }
    private static ExtensionContext context(AtomicBoolean fail, String boundary, Optional<Throwable> outcome) {
        Map<Object, Object> values = new HashMap<>();
        ExtensionContext.Store store = (ExtensionContext.Store) Proxy.newProxyInstance(
                ExtensionContext.Store.class.getClassLoader(), new Class<?>[] {ExtensionContext.Store.class},
                (proxy, method, args) -> {
                    if (fail.get() && method.getName().equals(boundary)) throw new IllegalStateException("store unavailable");
                    return switch (method.getName()) {
                        case "put" -> { values.put(args[0], args[1]); yield null; }
                        case "remove" -> values.remove(args[0]);
                        case "get" -> values.get(args[0]);
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        return (ExtensionContext) Proxy.newProxyInstance(ExtensionContext.class.getClassLoader(),
                new Class<?>[] {ExtensionContext.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getRequiredTestClass" -> ClusterHealthProbeExtensionTest.class;
                    case "getRequiredTestMethod" -> ClusterHealthProbeExtensionTest.class.getDeclaredMethod("observedInvocation");
                    case "getUniqueId" -> "private-invocation-sentinel";
                    case "getExecutionException" -> outcome;
                    case "getStore" -> {
                        if (fail.get() && boundary.equals("lookup")) throw new IllegalStateException("store lookup failed");
                        yield store;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
    private static AutoCloseable properties(Path directory, boolean selected) {
        String directoryKey = "tapstate.test.health-probe-directory", selectedKey = "tapstate.test.health-probe-invocations";
        String oldDirectory = System.getProperty(directoryKey), oldSelected = System.getProperty(selectedKey);
        if (selected) System.setProperty(directoryKey, directory.toString()); else System.clearProperty(directoryKey);
        System.setProperty(selectedKey, ClusterHealthProbeExtensionTest.class.getName() + "#observedInvocation");
        return () -> { restore(directoryKey, oldDirectory); restore(selectedKey, oldSelected); };
    }
    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key); else System.setProperty(key, value);
    }
}
