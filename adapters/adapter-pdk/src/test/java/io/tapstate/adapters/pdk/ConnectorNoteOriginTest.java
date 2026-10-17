package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorNoteOriginTest {
    @Test
    void completeSettingsAreStableAcrossMapOrderingAndDetachedFromMutation() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("tls", true);
        nested.put("ca", List.of("ca-one", "ca-two"));
        Map<String, Object> supplied = new LinkedHashMap<>();
        supplied.put("uri", "mongodb://user:secret-sentinel@host/db");
        supplied.put("trust", nested);
        var retained = ConnectorNoteOrigin.capture("mongo", "a".repeat(64), supplied);
        var reordered = ConnectorNoteOrigin.capture("mongo", "a".repeat(64), Map.of("trust", Map.of("ca", List.of("ca-one", "ca-two"), "tls", true),
                "uri", "mongodb://user:secret-sentinel@host/db"));
        assertThat(retained).isEqualTo(reordered);
        supplied.put("uri", "database-y");
        nested.put("ca", List.of("different-ca"));
        assertThat(ConnectorNoteOrigin.settings(retained)).containsEntry("uri", "mongodb://user:secret-sentinel@host/db");
        assertThat(ConnectorNoteOrigin.settings(retained).get("trust")).isEqualTo(Map.of("ca", List.of("ca-one", "ca-two"), "tls", true));
        assertThat(retained.toString()).doesNotContain("secret-sentinel");
    }

    @Test
    void anotherConnectionOrArtifactRefusesBeforeThePluginCanLoad(@TempDir Path dir) {
        var retained = ConnectorNoteOrigin.capture("mongo", "a".repeat(64), Map.of("uri", "database-x"));
        var scope = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.state.p1.src", RequiredNoteOwner.Kind.PIPELINE, "p1", 1), 1);
        RequiredNoteStore store = (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "guardedBy" -> proxy;
                    case "origin" -> retained;
                    default -> throw new AssertionError("unexpected note operation: " + method.getName());
                });
        var scopes = ConnectorNoteScopes.of(store, ignored -> scope, () -> {});
        for (String hash : List.of("a".repeat(64), "b".repeat(64))) {
            var reference = new ConnectorRef(List.of(dir.resolve("absent.jar")), "absent.Plugin", "2.0.8", null, null, hash, false);
            assertThatThrownBy(() -> PdkConnector.open("mongo", reference, Map.of("uri", "database-y"), new PipelineNode("p1", "src"), null, null, scopes))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.NOTE_ORIGIN_MISMATCH);
                        assertThat(error.getMessage()).doesNotContain("database-x", "database-y");
                        assertThat(error.getCause()).isNull();
                    });
        }
    }

    @Test
    void aMissingOriginDoesNotPrepareOneOrFallBackToCurrentSettings(@TempDir Path dir) {
        var scope = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.state.p1.src", RequiredNoteOwner.Kind.PIPELINE, "p1", 1), 1);
        RequiredNoteStore store = (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("guardedBy")) return proxy;
                    if (method.getName().equals("origin")) throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
                    throw new AssertionError("unexpected note operation: " + method.getName());
                });
        var scopes = ConnectorNoteScopes.of(store, ignored -> scope, () -> {});
        var reference = new ConnectorRef(List.of(dir.resolve("absent.jar")), "absent.Plugin", "2.0.8", null, null, "a".repeat(64), false);
        assertThatThrownBy(() -> PdkConnector.open("mongo", reference, Map.of(), new PipelineNode("p1", "src"), null, null, scopes))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));
    }

    @Test
    void reopeningSettingsPreservesTheirRuntimeNumberTypesAndNullValues() {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("integer", 7);
        original.put("long", 8L);
        original.put("double", 1.5d);
        original.put("float", 1.25f);
        original.put("short", (short) 3);
        original.put("byte", (byte) 2);
        original.put("big", new java.math.BigInteger("18446744073709551615"));
        original.put("decimal", new java.math.BigDecimal("1.00"));
        original.put("optional", null);
        original.put("list", java.util.Arrays.asList(null, 7));
        var origin = ConnectorNoteOrigin.capture("mongo", "a".repeat(64), original);
        var reopened = ConnectorNoteOrigin.settings(origin);
        assertThat(reopened).isEqualTo(original);
        assertThat(ConnectorNoteOrigin.capture("mongo", "a".repeat(64), reopened)).isEqualTo(origin);
    }

    @Test
    void decimalScaleAndPrecisionSurviveTheRetainedSettingsRoundTrip() {
        var settings = Map.<String, Object>of("scale", new java.math.BigDecimal("1.00"),
                "precise", new java.math.BigDecimal("1.234567890123456789"), "scientific", 1.0E20);
        var retained = ConnectorNoteOrigin.capture("mongo", "a".repeat(64), settings);
        var reopened = ConnectorNoteOrigin.settings(retained);
        assertThat(reopened.get("scale")).isEqualTo(new java.math.BigDecimal("1.00"));
        assertThat(reopened.get("precise")).isEqualTo(new java.math.BigDecimal("1.234567890123456789"));
        assertThat(ConnectorNoteOrigin.capture("mongo", "a".repeat(64), reopened)).isEqualTo(retained);
    }

    @Test
    void pluginConstructionCannotChangeTheSettingsThatWereMatchedToRetainedNotes(@TempDir Path dir) {
        String key = "tapstate.test.origin-mutation." + dir.getFileName();
        Map<String, Object> nested = new LinkedHashMap<>(Map.of("uri", "database-x"));
        Map<String, Object> caller = new LinkedHashMap<>(Map.of("connection", nested));
        var retained = ConnectorNoteOrigin.capture("mongo", "a".repeat(64), caller);
        var primary = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.state.p1.src", RequiredNoteOwner.Kind.PIPELINE, "p1", 1), 1);
        var global = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.global-state", RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0), 1);
        RequiredNoteStore store = (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "guardedBy" -> proxy;
                    case "origin" -> retained;
                    default -> throw new AssertionError("unexpected note operation: " + method.getName());
                });
        var scopes = ConnectorNoteScopes.of(store, namespace -> namespace.equals("pdk.global-state") ? global : primary, () -> {});
        System.getProperties().put(key, (Runnable) () -> nested.put("uri", "database-y"));
        try {
            var ref = new ConnectorRef(List.of(Synthetic.originMutatingConnector(dir, key)), "synthetic.OriginMutating", "2.0.8", null, null, "a".repeat(64), false);
            try (var connector = PdkConnector.open("mongo", ref, caller, new PipelineNode("p1", "src"), null, null, scopes)) {
                assertThat(nested.get("uri")).isEqualTo("database-y");
                assertThat(connector.context().getConnectionConfig().get("connection")).isEqualTo(Map.of("uri", "database-x"));
            }
        } finally {
            System.getProperties().remove(key);
        }
    }

    @Test
    void malformedSettingsDiagnosticsDoNotRetainTheSecretInput() {
        var origin = new io.tapstate.spi.store.RequiredNoteOrigin("mongo", "a".repeat(64),
                "not-json-secret-sentinel".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThatThrownBy(() -> ConnectorNoteOrigin.settings(origin)).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getMessage()).doesNotContain("secret-sentinel");
            assertThat(error.getCause()).isNull();
        });
    }
}
