package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

class ClusterFixtureTimingBoundaryTest {
    @TempDir Path directory;

    @Test void theBoundedBufferReportsOverflowWithoutReplacingTheAction() {
        try (var scope = ClusterFixtureTiming.install()) {
            for (int i = 0; i < 10001; i++) ClusterFixtureTiming.beginPreparation();
            assertThat(scope.events()).hasSize(10000);
            assertThat(scope.valid()).isFalse();
            assertThat(17 + 25).isEqualTo(42);
        }
    }

    @Test void aRepeatedCloseCannotRestoreOrModifyAnUnrelatedScope() {
        var before = ClusterFixtureTiming.activeScope();
        var closed = ClusterFixtureTiming.install();
        closed.close();
        try (var other = ClusterFixtureTiming.install()) {
            closed.close();
            assertThat(closed.valid()).isFalse();
            assertThat(ClusterFixtureTiming.activeScope()).isSameAs(other);
        }
        assertThat(ClusterFixtureTiming.activeScope()).isSameAs(before);
    }

    @Test void aWriterFailureCannotReplaceTheOriginalException() throws Exception {
        var original = new AssertionError("original fixture action");
        Path file = Files.writeString(directory.resolve("ordinary-file"), "owned");
        var scope = ClusterFixtureTiming.install();
        try {
            assertThatThrownBy(() -> { throw original; }).isSameAs(original);
        } finally {
            scope.close();
        }
        assertThatThrownBy(() -> scope.write(file)).isInstanceOf(IOException.class);
        assertThat(Files.readString(file)).isEqualTo("owned");
    }

    @Test void linkedOutputDirectoriesAreRefused() throws Exception {
        Path link = directory.resolve("link");
        Files.createSymbolicLink(link, directory);
        var scope = ClusterFixtureTiming.install();
        scope.close();
        assertThatThrownBy(() -> scope.write(link)).isInstanceOf(IOException.class);
        try (var paths = Files.list(directory)) { assertThat(paths.count()).isEqualTo(1); }
    }

    @Test void serializedEventsAndFlushReceiptsContainHashesOnly() throws Exception {
        String raw = "mongodb://fixture-owned-secret@host/database";
        String cluster = "fixture-private-cluster";
        var scope = ClusterFixtureTiming.install();
        long ticket = ClusterFixtureTiming.beginPreparation();
        ClusterFixtureTiming.bind(ticket, 12345);
        ClusterFixtureTiming.prepared(12345, raw, "operator-private", "127.0.0.1",
                List.of("-Dprivate-option=private-value"), List.of("--tapstate.cluster.id=" + cluster));
        scope.close();
        Path trace = scope.write(directory);
        String text = Files.readString(trace);
        assertThat(text).doesNotContain(raw, cluster, "private-value", "operator-private");
        var prepared = scope.events().get(1);
        assertThat(prepared.parameters()).matches("[0-9a-f]{64}");
        assertThat(prepared.cluster()).isEqualTo(hash(cluster));
        String metadata = Files.readString(Path.of(trace + ".flush.json"));
        assertThat(metadata).contains(hash(text)).contains("\"metadata_write_unmeasured\":true");
    }

    @Test void missingOutputCannotLeakTheInvocationScope() throws Exception {
        try (var properties = properties(directory.resolve("absent"), true)) {
            var before = ClusterFixtureTiming.activeScope();
            var extension = new ClusterFixtureTimingExtension();
            var context = context(Optional.empty());
            extension.beforeEach(context);
            assertThat(ClusterFixtureTiming.activeScope()).isNotSameAs(before);
            extension.afterEach(context);
            assertThat(ClusterFixtureTiming.activeScope()).isSameAs(before);
            assertThat(directory.resolve("absent")).doesNotExist();
        }
    }

    @Test void outsideTheDeclaredSetIsSuspendedAndReceipted() throws Exception {
        try (var properties = properties(directory, false)) {
            var before = ClusterFixtureTiming.activeScope();
            var extension = new ClusterFixtureTimingExtension();
            var context = context(Optional.empty());
            extension.beforeEach(context);
            assertThat(ClusterFixtureTiming.beginPreparation()).isZero();
            extension.afterEach(context);
            assertThat(ClusterFixtureTiming.activeScope()).isSameAs(before);
            String text = onlyReceipt();
            assertThat(text).contains("\"timing_applicable\":false", "\"capture_status\":\"not_applicable\"");
            assertThat(text).doesNotContain(getClass().getName(), "boundaryInvocation");
        }
    }

    @Test void theOriginalFailureRemainsAuthoritativeWhenOutputAlsoFails() throws Exception {
        var original = new AssertionError("original test failure with private content");
        try (var properties = properties(directory.resolve("absent"), true)) {
            var before = ClusterFixtureTiming.activeScope();
            var extension = new ClusterFixtureTimingExtension();
            var context = context(Optional.of(original));
            extension.beforeEach(context);
            assertThatThrownBy(() -> { throw original; }).isSameAs(original);
            extension.afterEach(context);
            assertThat(context.getExecutionException()).containsSame(original);
            assertThat(ClusterFixtureTiming.activeScope()).isSameAs(before);
        }
    }

    @Test void aFailedInvocationIsReceiptedAsFailed() throws Exception {
        var original = new AssertionError("private diagnostic");
        try (var properties = properties(directory, true)) {
            var extension = new ClusterFixtureTimingExtension();
            var context = context(Optional.of(original));
            extension.beforeEach(context);
            ClusterFixtureTiming.beginPreparation();
            extension.afterEach(context);
            assertThat(onlyReceipt()).contains("\"test_outcome\":\"failed\"")
                    .doesNotContain("private diagnostic");
        }
    }

    private String onlyReceipt() throws IOException {
        try (var paths = Files.list(directory)) {
            var receipts = paths.filter(p -> p.getFileName().toString().startsWith("fixture-invocation-")).toList();
            assertThat(receipts).hasSize(1);
            return Files.readString(receipts.getFirst());
        }
    }

    private AutoCloseable properties(Path output, boolean applicable) {
        String outputKey = "tapstate.test.fixture-timing-directory";
        String setKey = "tapstate.test.fixture-timing-invocations";
        String oldOutput = System.getProperty(outputKey);
        String oldSet = System.getProperty(setKey);
        System.setProperty(outputKey, output.toString());
        System.setProperty(setKey, applicable ? getClass().getName() + "#boundaryInvocation" : "");
        return () -> { restore(outputKey, oldOutput); restore(setKey, oldSet); };
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key); else System.setProperty(key, value);
    }

    private void boundaryInvocation() { }

    // Only the extension's observation context is synthetic; Jupiter runs every control normally.
    private ExtensionContext context(Optional<Throwable> failure) throws Exception {
        Map<Object, Object> values = new HashMap<>();
        var store = (ExtensionContext.Store) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ExtensionContext.Store.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "put" -> { values.put(args[0], args[1]); yield null; }
                    case "remove" -> values.remove(args[0]);
                    case "get" -> values.get(args[0]);
                    default -> throw new AssertionError("unexpected store operation");
                });
        var method = getClass().getDeclaredMethod("boundaryInvocation");
        return (ExtensionContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ExtensionContext.class}, (proxy, operation, args) -> switch (operation.getName()) {
                    case "getRequiredTestClass" -> getClass();
                    case "getRequiredTestMethod" -> method;
                    case "getUniqueId" -> "[engine:junit-jupiter]/[class:io.tapstate.e2e.ClusterFixtureTimingBoundaryTest]/[method:boundaryInvocation()]";
                    case "getStore" -> store;
                    case "getExecutionException" -> failure;
                    default -> throw new AssertionError("unexpected context operation");
                });
    }

    private static String hash(String text) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    }
}
