package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PdkRuntimeDiagnosticTest {
    private static final String SPEC = """
            {"properties":{"id":"fixture","name":"fixture"},"configOptions":{"connection":{"properties":{
            "credential":{"type":"string","x-component":"Password"}}}}}
            """;

    @Test void actualPluginInitializationRetainsItsCatalogAndSqlCauseWithoutSecrets(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        String channel = "tapstate.test.refused-init." + java.util.UUID.randomUUID();
        var original = new io.tapdata.exception.TapCodeException("043", "init-secret denied");
        original.initCause(new java.sql.SQLException("init-secret database refusal", "28000"));
        System.getProperties().put(channel, original);
        try {
            var jar = Synthetic.refusedInitSource(dir, channel);
            var catalog = dir.resolve("catalog.jar");
            catalogJar(catalog, "Original plugin catalog");
            try (var source = new java.util.jar.JarFile(catalog.toFile());
                    var archive = java.nio.file.FileSystems.newFileSystem(jar)) {
                var entry = archive.getPath("/" + PdkDiagnosticCatalog.RESOURCE);
                java.nio.file.Files.createDirectories(entry.getParent());
                try (var input = source.getInputStream(source.getJarEntry(PdkDiagnosticCatalog.RESOURCE))) {
                    java.nio.file.Files.write(entry, input.readAllBytes());
                }
            }
            var hash = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(java.nio.file.Files.readAllBytes(jar)));
            var ref = new ConnectorRef(List.of(jar), "synthetic.RefusedInitSource", "2.0.8", null, SPEC, hash, false);
            var config = new io.tapstate.spi.capture.CaptureConfig("fixture", Map.of("credential", "init-secret"), List.of("t1"));
            for (boolean streaming : List.of(false, true)) {
                var facts = new java.util.ArrayList<io.tapstate.core.common.FailureFact>();
                try (var port = new PdkCapturePort(id -> ref).withOpeningFailure(facts::add)
                        .withStreamPreparation(tables -> {})) {
                    var thrown = org.assertj.core.api.Assertions.catchThrowable(() -> {
                        if (streaming) port.cdc(config, io.tapstate.spi.capture.CaptureStart.present(), (events, position) -> {});
                        else port.snapshot(config);
                    });
                    assertThat(thrown).isInstanceOf(TapstateException.class).hasCause(original);
                    assertThat(facts).singleElement().satisfies(fact -> {
                        assertThat(fact.cause()).isSameAs(thrown);
                        assertThat(fact.diagnostic().toString()).contains("EXT043", "28000", "Original plugin catalog")
                                .doesNotContain("init-secret");
                        assertThat(fact.publicFailure().params().toString()).doesNotContain("init-secret");
                    });
                }
            }
        } finally { System.getProperties().remove(channel); }
    }

    @Test void aLaterReaderFailureCannotEraseTheAlreadyObservedDecodeFailure(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        var jar = Synthetic.countingSink(dir);
        var connector = PdkConnector.open("fixture", new ConnectorRef(List.of(jar), "synthetic.CountingSink", "2.0.8", null, SPEC),
                Map.of("credential", "snapshot-secret"));
        var decode = new TapstateException(ConnectorError.CAPTURE_FAILED, Map.of("connector", "fixture", "detail", "snapshot-secret decode"), null);
        var reader = new TapstateException(ConnectorError.CAPTURE_FAILED, Map.of("connector", "fixture", "detail", "snapshot-secret reader"), null);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var batch = PdkCaptureBatch.start(connector, reading -> {
            reading.seamSampled(java.util.Optional.empty());
            reading.rowsRead(() -> { throw decode; });
            if (!release.await(3, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("reader fixture deadline");
            throw reader;
        }, "snapshot-pair-test")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(batch::hasNext).isSameAs(decode);
            release.countDown();
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(3).toNanos();
            while (batch.isReading() && System.nanoTime() < deadline) Thread.sleep(5);
            assertThat(batch.isReading()).isFalse();
            assertThat(batch.failureFact(decode).publicFailure()).isNotNull();
            assertThat(batch.failureFact(decode).publicFailure().params().toString()).doesNotContain("snapshot-secret");
            assertThat(batch.failureFact(reader).publicFailure()).isNotNull();
        } finally { release.countDown(); }
    }

    @Test void aSnapshotWorkerKeepsTheFailureOfItsOwnConnector(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        var jar = Synthetic.countingSink(dir);
        var ref = new ConnectorRef(List.of(jar), "synthetic.CountingSink", "2.0.8", null, SPEC);
        var connector = PdkConnector.open("fixture", ref, Map.of("credential", "snapshot-secret"));
        var failure = new TapstateException(ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "fixture", "detail", "snapshot-secret"), null);
        try (var batch = PdkCaptureBatch.start(connector, reading -> {
            reading.seamSampled(java.util.Optional.empty());
            throw failure;
        }, "snapshot-owner-test")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(batch::hasNext).isSameAs(failure);
            var fact = batch.failureFact(failure);
            assertThat(fact.cause()).isSameAs(failure);
            assertThat(fact.publicFailure()).isNotNull();
            assertThat(fact.publicFailure().params().toString()).doesNotContain("snapshot-secret");
            assertThat(batch.failureFact(new IllegalStateException("other owner")).diagnostic()).isEmpty();
        }
    }

    @Test void missingLogicalNodeDoesNotDisableSecretProtection() {
        var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null, SPEC),
                Map.of("credential", "node-less-secret"), null);
        var failure = new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "fixture", "detail", "node-less-secret"), null);
        assertThat(context.failureFact(failure).publicFailure().params().toString()).doesNotContain("node-less-secret");
        assertThat(context.capture(failure)).doesNotContainKey("references");
    }

    @Test void capturesTheOriginalCauseWithFrozenSecretsAndLogicalOwnership() {
        var settings = new java.util.HashMap<String, Object>(Map.of("credential", "original-secret"));
        var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null, SPEC),
                settings, new PipelineNode("pipeline-a", "sink-a"));
        settings.put("credential", "later-secret");
        var driver = new java.sql.SQLException("refused original-secret mongodb://account:password@localhost/db", "28000");
        var failure = new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "fixture", "detail", "original-secret"), driver);
        var projection = context.capture(failure);
        assertThat(failure.getCause()).isSameAs(driver);
        assertThat(projection).containsEntry("code", "TS0060027").containsEntry("codeSource", "tapstate-standard");
        assertThat(projection.toString()).doesNotContain("original-secret", "account:password");
        assertThat(projection.get("references")).isEqualTo(List.of(Map.of("type", "pipeline-node", "id", "sink-a", "pipelineId", "pipeline-a")));
        assertThat(((Map<?, ?>) projection.get("cause")).get("codeSource")).isEqualTo("jdbc-sqlstate");
    }

    @Test void longSecretsAreNotLeakedAsUnmatchableTruncatedPrefixes() {
        String secret = "credential-prefix-" + "x".repeat(5000);
        var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null, SPEC),
                Map.of("credential", secret), new PipelineNode("p", "s"));
        var failure = new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "fixture", "detail", secret), null);
        assertThat(context.capture(failure).toString()).doesNotContain("credential-prefix-");
    }

    @Test void identicalRawCodesKeepTheCatalogOfTheirAdmittedArtifact(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var first = directory.resolve("first.jar");
        var second = directory.resolve("second.jar");
        catalogJar(first, "first owner");
        catalogJar(second, "second owner");
        var a = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(first), "fixture", null, null, SPEC,
                "a".repeat(64), false), Map.of(), new PipelineNode("p", "a"));
        var b = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(second), "fixture", null, null, SPEC,
                "b".repeat(64), false), Map.of(), new PipelineNode("p", "b"));
        catalogJar(first, "replacement after admission");
        var sameFailure = new io.tapdata.exception.TapCodeException("043", "driver refusal");
        assertThat(a.capture(sameFailure)).containsEntry("message", "first owner");
        assertThat(b.capture(sameFailure)).containsEntry("message", "second owner");
        assertThat(a.capture(sameFailure)).containsEntry("codeSource", "connector");
    }

    @Test void primaryParametersAreRedactedBeforeAnyTruncation() {
        for (String secret : List.of("short-secret", "credential-prefix-" + "x".repeat(5000))) {
            var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null, SPEC),
                    Map.of("credential", secret), new PipelineNode("p", "s"));
            var coded = new TapstateException(ConnectorError.WRITE_FAILED,
                    Map.of("connector", "fixture", "detail", secret), null);
            var wrapper = new IllegalStateException("wrapper", coded);
            var fact = context.failureFact(wrapper);
            assertThat(fact.cause()).isSameAs(wrapper);
            assertThat(fact.publicFailure().code()).isEqualTo(coded.code().code());
            assertThat(fact.publicFailure().params()).containsKey("detail");
            assertThat(fact.publicFailure().params().toString()).doesNotContain(secret, "credential-prefix-");
        }
    }

    @Test void unavailableContextCarriesExplicitSafePrimaryValues() {
        var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null),
                Map.of(), new PipelineNode("p", "s"));
        var coded = new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "fixture", "detail", "secret"), null);
        assertThat(context.failureFact(coded).publicFailure().params())
                .containsEntry("detail", "[diagnostic unavailable]");
        assertThat(context.failureFact(new IllegalStateException("secret")).publicFailure()).isNotNull();
    }

    private static void catalogJar(java.nio.file.Path path, String message) throws Exception {
        try (var jar = new java.util.jar.JarOutputStream(java.nio.file.Files.newOutputStream(path))) {
            jar.putNextEntry(new java.util.jar.JarEntry(PdkDiagnosticCatalog.RESOURCE));
            jar.write(("{\"formatVersion\":1,\"connectorId\":\"fixture\",\"definitions\":["
                    + "{\"rawCode\":\"043\",\"code\":\"EXT043\",\"en\":{\"message\":\"" + message + "\"}}]}")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }

    @Test void aCustomCodeCannotClaimProductAuthorityByCopyingItsSpelling() {
        var custom = new io.tapstate.core.common.TapstateErrorCode() {
            public String code() { return ConnectorError.WRITE_FAILED.code(); }
            public io.tapstate.core.common.Severity severity() { return io.tapstate.core.common.Severity.ERROR; }
            public java.util.Set<String> placeholders() { return java.util.Set.of("connector", "detail"); }
        };
        var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null, SPEC),
                Map.of(), new PipelineNode("p", "s"));
        var diagnostic = context.capture(new TapstateException(custom, Map.of("connector", "fixture", "detail", "refused"), null));
        assertThat(diagnostic).containsEntry("code", ConnectorError.WRITE_FAILED.code())
                .containsEntry("definitionStatus", "unverified").doesNotContainKey("codeSource");
    }

    @Test void unavailableSecretContextNeverPublishesRawFailureText() {
        var context = PdkRuntimeDiagnostic.bind("fixture", new ConnectorRef(List.of(), "fixture", null, null),
                Map.of("credential", "secret"), new PipelineNode("p", "s"));
        assertThat(context.capture(new IllegalStateException("secret"))).isEqualTo(Map.of("truncated", true));
    }
}
