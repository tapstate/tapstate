package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** An opt-in invocation scope; observation never replaces a test action or its failure. */
public final class ClusterFixtureTimingExtension implements BeforeEachCallback, AfterEachCallback {
    private static final ExtensionContext.Namespace STORE = ExtensionContext.Namespace.create(ClusterFixtureTimingExtension.class);
    private record Capture(ClusterFixtureTiming.Scope scope, Path directory, String identity, boolean applicable) { }

    @Override public void beforeEach(ExtensionContext context) {
        try { configure(context); }
        catch (RuntimeException | Error failure) { System.err.println("Fixture timing scope is incomplete"); }
    }
    private void configure(ExtensionContext context) {
        String directory = System.getProperty("tapstate.test.fixture-timing-directory");
        if (directory == null || directory.isBlank()) return;
        String declared = System.getProperty("tapstate.test.fixture-timing-invocations", "");
        String method = context.getRequiredTestClass().getName()+"#"+context.getRequiredTestMethod().getName();
        boolean applicable = Arrays.stream(declared.split(",")).map(String::trim).anyMatch(method::equals);
        // All other controls and proof paths still run, with their complete cost retained.
        var scope = applicable ? ClusterFixtureTiming.install() : ClusterFixtureTiming.suspend();
        try {
            String identity = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(context.getUniqueId().getBytes(StandardCharsets.UTF_8)));
            context.getStore(STORE).put("capture", new Capture(scope, Path.of(directory), identity, applicable));
        } catch (Exception | Error failure) {
            scope.close();
            System.err.println("Fixture timing invocation metadata is incomplete");
        }
    }
    @Override public void afterEach(ExtensionContext context) {
        Capture capture = context.getStore(STORE).remove("capture", Capture.class);
        if (capture == null) return;
        String status = capture.applicable() ? "unknown" : "not_applicable";
        String trace = "-";
        capture.scope().close();
        try {
            if (capture.applicable()) {
                Path file = capture.scope().write(capture.directory());
                trace = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(Files.readAllBytes(file)));
                status = capture.scope().valid() ? "captured" : "unknown";
            }
        } catch (Exception | Error failure) {
            System.err.println("Fixture timing invocation output is incomplete");
        }
        try {
            String outcome = context.getExecutionException().isPresent() ? "failed" : "returned";
            String receipt = "{\"schema\":1,\"case_sha256\":\""+capture.identity()
                    +"\",\"timing_applicable\":"+capture.applicable()+",\"applicability\":\""
                    +(capture.applicable() ? "declared_original" : "outside_declared_timing_set")+"\",\"test_outcome\":\""+outcome
                    +"\",\"capture_status\":\""+status+"\",\"trace_sha256\":\""+trace+"\"}";
            if (!Files.isDirectory(capture.directory(), java.nio.file.LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(capture.directory())) throw new java.io.IOException("owned timing directory unavailable");
            Files.writeString(capture.directory().resolve("fixture-invocation-"+UUID.randomUUID()+".json"),
                    receipt, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (Exception | Error failure) {
            System.err.println("Fixture timing invocation receipt is incomplete");
        }
    }
}
